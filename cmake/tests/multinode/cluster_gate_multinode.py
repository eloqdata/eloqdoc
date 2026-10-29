#!/usr/bin/env python3
"""The cluster gate on a cluster of three node groups run as separate processes
(docs/cdc-change-stream.md §6.2 and §10 item 11; plan D3, D6, D9). cmake/tests/cluster_gate.py
covers what one node can show; this covers what needs several node groups, a replicated log group
and failures of some nodes while the others keep running.

Every node exports its metrics. A node killed with kill -9 and restarted recovers its node group
from the log.

  (a) everywhere: a pause from one node refuses every kind of DDL through every node and leaves
      DML alone; cluster_gate_paused reads 1 on every node; a second pause from another node finds
      the gate held; a resume from a third node opens the gate everywhere, and the gauge reads 0
      on every node;
  (b) restarts: a pause survives the kill of the log group's leader (a log group leader change),
      the kill of each other node in turn, and a clean restart of the whole cluster, and every
      node that restarted into the pause reads 1; each node group's replay installs the latest
      gate only (a log group may ship a replay list twice in one term, each with one gate item);
      a resume from a node other than the pauser's survives a whole cluster restart too;
  (c) log groups: with a log group of each node, a pause and a resume issued from different nodes
      both log to log group 0, so a whole cluster restart replays only the latest gate on every
      node group;
  (d) pause killed after its log: the coordinator dies after the gate's log entry, before its
      install, while the other node groups run; once it restarts every node group is paused and
      holds no lock of the gate tx (a resume succeeds and admits DDL everywhere);
  (e) resume killed after its log: likewise, the gate is open everywhere;
  (f) a pause, then a resume, killed after their log while a second node group fails over in the
      same window;
  (g) a pause whose coordinator dies in its own install (cluster_config_PostWriteAll_local), and
      one whose participant dies in its install (cluster_config_PostWriteAll_remote): the pause
      retries the install until the participant is back, and returns;
  (h) a range split under the gate: a pause, then a load past the range size, which a checkpoint
      splits while paused; every document reads, and the gate still refuses DDL;
  (i) a recovered DDL is not gated: a create held after its prepare log; a pause from another
      node waits for it; the create's node is killed and recovers the create from its prepare log,
      which finishes, and only then does the pause return, its timestamp above the create's;
  (j) an index build in flight finishes under a pause: a build held once its build is done,
      before its commit-stage write locks (alter_table_index_hold_before_commit_lock); a pause
      waits for it; released, the index commits, ungated, and the pause returns above it;
  (k) the same, but the build's node is killed while it is held: the build recovered from its
      last prepare data log commits under the waiting pause, which returns above it;
  (l) the gauge after a step-down: with node groups of three raft members (node i the candidate
      of node group i, the other two its voters), a pause, then nodes 1 and 2 killed: node 0
      loses its node group's majority and stops leading it while its process lives, and its
      cluster_gate_paused reads 0 (CcNode's leader stop resets it). The cluster is not restarted:
      voters are not kept from leading, and with local data store shards a voter that leads opens
      a stale copy of the shard.

Usage: cluster_gate_multinode.py --server <eloqdoc built with WITH_FAULT_INJECT> --root <dir>
       [--lib-dir <dir>] [--scenario a] ... [--keep] [--port-offset N] [--first-cpu N]
       [--cpus-per-node N]
"""

import argparse
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))
from cluster import Cluster, read_cloud_settings  # noqa: E402
from cluster_gate import (CONFLICTING_OPERATION_IN_PROGRESS, check_open,  # noqa: E402
                          check_paused, gate, gauge, refused, setup)
from ddl_split_recovery import (load, wait_documents, wait_split_done,  # noqa: E402
                                COLL as SPLIT_COLL, DB as SPLIT_DB)
from pymongo.errors import AutoReconnect, OperationFailure, PyMongoError  # noqa: E402

GATE_TIMEOUT_S = 300
REPLAY_INSTALLED = "Cluster gate replay: installed"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Via:
    """A cluster node as cluster_gate.py's helpers take one: with a client attribute."""

    def __init__(self, node):
        self.node = node

    @property
    def client(self):
        return self.node.client()


def pause(cluster, index):
    return gate(Via(cluster.nodes[index]), "pause")


def resume(cluster, index):
    return gate(Via(cluster.nodes[index]), "resume")


def check_paused_everywhere(cluster, label):
    """Every kind of DDL is refused through every node; DML passes."""
    for node in cluster.nodes:
        check_paused(Via(node), f"{label}_n{node.index}")


def check_open_everywhere(cluster, label):
    """A create, a drop and a view's create and drop pass through every node."""
    for node in cluster.nodes:
        check_open(Via(node), f"{label}_n{node.index}")


def gauges(cluster):
    """cluster_gate_paused on each node, for the node group it leads."""
    return [gauge(node.metrics_port, "cluster_gate_paused", ng_id=node.index)
            for node in cluster.nodes]


def wait_gauges(cluster, value, timeout_s=60):
    """Waits until cluster_gate_paused reads value on every node."""
    deadline = time.monotonic() + timeout_s
    seen = None
    while time.monotonic() < deadline:
        try:
            seen = gauges(cluster)
            if seen == [value] * len(cluster.nodes):
                return
        except (OSError, AssertionError):
            pass  # a node still starting
        time.sleep(0.5)
    raise AssertionError(f"cluster_gate_paused never read {value} everywhere: {seen}")


def replayed_gates(node):
    """The gate items the node's latest process replayed, as "paused" or "open"."""
    return [line.split(REPLAY_INSTALLED, 1)[1].split()[0]
            for line in node.current_log_lines(REPLAY_INSTALLED)]


def check_replayed(cluster, value):
    """Every node group's replay installed the latest gate once and nothing else: its node asked
    the log groups for the replay at its leader start, and each shipped the node group's replay
    list once. The node may have asked again while a log group had no leader (a send that finds
    none is repeated until one answers); a repeat that a leader accepts after another shipped would
    show here as a second gate."""
    for node in cluster.nodes:
        gates = replayed_gates(node)
        assert gates == [value], (node.index, gates)
        asked = node.current_log_lines(f"Requesting log replay for ng: {node.index} at term:")
        on_start = [line for line in asked if "on its leader start" in line]
        assert len(on_start) == 1, (node.index, asked)


def restart_cluster(cluster):
    cluster.restart_all()


def kill_restart(cluster, index):
    cluster.kill(index)
    cluster.restart(index)


def armed(node, fault):
    return node.client().admin.command({"eloqFaultInject": fault, "query": True})["armed"]


def arm(node, fault, params=""):
    node.client().admin.command({"eloqFaultInject": fault, "params": params})


class Background:
    """A call run in a thread: its result or error once done."""

    def __init__(self, fn):
        self.result = None
        self.error = None
        self.done = threading.Event()
        self.thread = threading.Thread(target=self._run, args=(fn,), daemon=True)
        self.thread.start()

    def _run(self, fn):
        try:
            self.result = fn()
        except Exception as exc:  # reported by the caller
            self.error = exc
        finally:
            self.done.set()

    def wait(self, timeout_s):
        assert self.done.wait(timeout_s), "the call never returned"
        if self.error is not None:
            raise self.error
        return self.result


def crash_in_gate(cluster, coordinator, op, fault, also_kill=None):
    """Arms a fault point that kills the coordinator, runs the gate op through it, and once the
    coordinator has died kills also_kill too (a second node group failing over in the window);
    then restarts them."""
    node = cluster.nodes[coordinator]
    arm(node, fault, "action=PANIC")
    try:
        gate(Via(node), op)
        raise AssertionError(f"{fault} did not kill node {coordinator}: the {op} returned")
    except AutoReconnect:
        pass
    node.process.wait(timeout=60)
    log(f"node {coordinator} died at {fault} in its {op}")
    if also_kill is not None:
        cluster.kill(also_kill)
        log(f"node {also_kill} killed in the same window")
    restarted = [coordinator] + ([also_kill] if also_kill is not None else [])
    for index in restarted:
        cluster.nodes[index].launch()
    for index in restarted:
        cluster.nodes[index].wait_serving()
        cluster.nodes[index].wait_recovered()
    cluster.wait_catalog_reads()


def scenario_everywhere(cluster):
    """(a)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    wait_gauges(cluster, 0)
    pause_ts = pause(cluster, 0)
    wait_gauges(cluster, 1)
    check_paused_everywhere(cluster, "a")
    try:
        pause(cluster, 1)
        raise AssertionError("a second pause from another node succeeded")
    except OperationFailure as exc:
        assert exc.code == CONFLICTING_OPERATION_IN_PROGRESS, exc
    resume_ts = resume(cluster, 2)
    assert resume_ts > pause_ts, (resume_ts, pause_ts)
    wait_gauges(cluster, 0)
    check_open_everywhere(cluster, "a")
    print("PASS (a) a pause from one node refused DDL through every node, the gauge read 1 on "
          "every node, and a resume from another opened the gate everywhere", flush=True)


def scenario_restarts(cluster):
    """(b)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    pause(cluster, 1)
    wait_gauges(cluster, 1)
    leader = cluster.log_leader()
    assert leader is not None, "no node leads the log group"
    log(f"node {leader} leads the log group; killing it")
    cluster.kill(leader)
    # The others enforce the pause meanwhile: the gate check reads their own node group's copy.
    for node in cluster.nodes:
        if node.index != leader:
            refused(lambda: node.client().cluster_gate.create_collection(f"refused_b_{node.index}"),
                    f"a create through node {node.index} while node {leader} is down")
    cluster.restart(leader)
    log(f"node {cluster.log_leader()} leads the log group after node {leader}'s restart")
    check_paused_everywhere(cluster, "b_leader")
    wait_gauges(cluster, 1)
    for index in range(len(cluster.nodes)):
        if index != leader:
            log(f"killing node {index}")
            kill_restart(cluster, index)
            check_paused(Via(cluster.nodes[index]), f"b_n{index}_restarted")
            wait_gauges(cluster, 1)
    log("restarting the whole cluster")
    restart_cluster(cluster)
    check_replayed(cluster, "paused")
    check_paused_everywhere(cluster, "b_cluster")
    wait_gauges(cluster, 1)
    resume(cluster, 2)
    wait_gauges(cluster, 0)
    check_open_everywhere(cluster, "b")
    restart_cluster(cluster)
    check_replayed(cluster, "open")
    wait_gauges(cluster, 0)
    check_open_everywhere(cluster, "b_cluster")
    print("PASS (b) a pause survived a log group leader change, each node's restart and a whole "
          "cluster restart; each node group replayed the latest gate only", flush=True)


def scenario_log_groups(cluster):
    """(c) Needs a cluster with a log group of each node."""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    pause(cluster, 2)
    resume(cluster, 1)
    pause(cluster, 0)
    log("paused from node 2, resumed from node 1, paused from node 0; restarting the cluster")
    restart_cluster(cluster)
    check_replayed(cluster, "paused")
    check_paused_everywhere(cluster, "c")
    resume(cluster, 2)
    restart_cluster(cluster)
    check_replayed(cluster, "open")
    check_open_everywhere(cluster, "c")
    print("PASS (c) gate ops from three nodes logged to one log group: each node group replayed "
          "the latest gate only", flush=True)


def scenario_pause_after_log(cluster):
    """(d)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    crash_in_gate(cluster, 1, "pause", "cluster_gate_after_log")
    check_paused_everywhere(cluster, "d")
    wait_gauges(cluster, 1)
    resume(cluster, 0)
    wait_gauges(cluster, 0)
    check_open_everywhere(cluster, "d")
    print("PASS (d) a pause whose coordinator died after its log was in force on every node "
          "group, and no node group kept its lock", flush=True)


def scenario_resume_after_log(cluster):
    """(e)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    pause(cluster, 0)
    crash_in_gate(cluster, 2, "resume", "cluster_gate_after_log")
    wait_gauges(cluster, 0)
    check_open_everywhere(cluster, "e")
    pause(cluster, 1)
    check_paused_everywhere(cluster, "e")
    resume(cluster, 1)
    print("PASS (e) a resume whose coordinator died after its log left the gate open on every "
          "node group, and no node group kept its lock", flush=True)


def scenario_second_failover(cluster):
    """(f)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    crash_in_gate(cluster, 1, "pause", "cluster_gate_after_log", also_kill=2)
    check_paused_everywhere(cluster, "f_pause")
    wait_gauges(cluster, 1)
    crash_in_gate(cluster, 0, "resume", "cluster_gate_after_log", also_kill=1)
    wait_gauges(cluster, 0)
    check_open_everywhere(cluster, "f")
    pause(cluster, 2)
    check_paused_everywhere(cluster, "f_again")
    resume(cluster, 2)
    print("PASS (f) a pause and a resume killed after their log while a second node group failed "
          "over were in force everywhere", flush=True)


def scenario_install_crashes(cluster):
    """(g)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    crash_in_gate(cluster, 2, "pause", "cluster_config_PostWriteAll_local")
    check_paused_everywhere(cluster, "g_local")
    wait_gauges(cluster, 1)
    resume(cluster, 0)
    wait_gauges(cluster, 0)
    # A participant dies in its install; the coordinator retries the install until it is back.
    participant = cluster.nodes[2]
    arm(participant, "cluster_config_PostWriteAll_remote", "action=PANIC")
    pausing = Background(lambda: pause(cluster, 0))
    participant.process.wait(timeout=60)
    log("node 2 died in the pause's install; restarting it")
    assert not pausing.done.is_set(), f"the pause returned before node 2 installed it: " \
                                      f"{pausing.error}"
    participant.launch()
    participant.wait_serving()
    participant.wait_recovered()
    cluster.wait_catalog_reads()
    pausing.wait(GATE_TIMEOUT_S)
    check_paused_everywhere(cluster, "g_remote")
    wait_gauges(cluster, 1)
    resume(cluster, 1)
    check_open_everywhere(cluster, "g")
    print("PASS (g) a pause whose coordinator, then whose participant, died in its install was in "
          "force everywhere", flush=True)


def scenario_split_under_gate(cluster):
    """(h)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    cluster.client(0)[SPLIT_DB].create_collection(SPLIT_COLL)
    pause(cluster, 0)
    docs = load(cluster)
    node, line = wait_split_done(cluster)
    log(f"split done on node {node} while paused: {line[-120:]}")
    wait_documents(cluster, docs)
    check_paused_everywhere(cluster, "h")
    wait_gauges(cluster, 1)
    resume(cluster, 0)
    check_open_everywhere(cluster, "h")
    print(f"PASS (h) a range split ran under the gate; all {docs} documents read through every "
          "node", flush=True)


def scenario_recovered_ddl(cluster):
    """(i)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    coordinator = cluster.nodes[1]
    arm(coordinator, "upsert_table_hold_before_commit_lock")
    creating = Background(lambda: coordinator.client().cluster_gate.create_collection("recovered"))
    deadline = time.monotonic() + 60
    while not armed(coordinator, "upsert_table_held_before_commit_lock"):
        assert time.monotonic() < deadline, "the create never reached its hold"
        time.sleep(0.5)
    log("the create from node 1 is held after its prepare log; pausing from node 0")
    pausing = Background(lambda: pause(cluster, 0))
    assert not pausing.done.wait(10), f"the pause did not wait for the create: {pausing.error}"
    log("the pause waits for the create; killing node 1")
    cluster.kill(1)
    cluster.restart(1)
    pause_ts = pausing.wait(GATE_TIMEOUT_S)
    try:
        creating.wait(10)
    except PyMongoError:
        pass  # the create's client lost its connection with node 1
    for node in cluster.nodes:
        names = node.client().cluster_gate.list_collection_names()
        assert "recovered" in names, (node.index, names)
    version = cluster.client(0).admin.command(
        {"eloqCatalogVersion": "cluster_gate.recovered"})["schemaTs"]
    assert 0 < version < pause_ts, (version, pause_ts)
    check_paused_everywhere(cluster, "i")
    resume(cluster, 2)
    check_open_everywhere(cluster, "i")
    print("PASS (i) a create recovered from its prepare log finished under a waiting pause, which "
          "returned after it", flush=True)


INDEX_HOLD = "alter_table_index_hold_before_commit_lock"
INDEX_HELD = "alter_table_index_held_before_commit_lock"


def hold_index_build(cluster, coordinator):
    """Starts an index build through the coordinator, holds it once its build is done (after its
    last prepare data log, before its commit-stage write locks), then starts a pause from node 0
    and checks that it waits; returns the build and the pause, both running."""
    db = cluster.client(0).cluster_gate
    db.built.insert_many([{"_id": i, "z": i % 97} for i in range(5000)])
    node = cluster.nodes[coordinator]
    arm(node, INDEX_HOLD)
    building = Background(lambda: node.client().cluster_gate.built.create_index("z", name="z_1"))
    deadline = time.monotonic() + 120
    while not armed(node, INDEX_HELD):
        assert time.monotonic() < deadline, "the index build never reached its hold"
        assert not building.done.is_set(), f"the index build ended: {building.error}"
        time.sleep(0.5)
    log(f"the index build from node {coordinator} is held after its build; pausing from node 0")
    pausing = Background(lambda: pause(cluster, 0))
    assert not pausing.done.wait(10), f"the pause did not wait for the build: {pausing.error}"
    return building, pausing


def check_index_before_pause(cluster, pause_ts):
    """The index is on the collection through every node, and was committed below the pause."""
    for node in cluster.nodes:
        names = [spec["name"] for spec in node.client().cluster_gate.built.list_indexes()]
        assert "z_1" in names, (node.index, names)
    version = cluster.client(0).admin.command(
        {"eloqCatalogVersion": "cluster_gate.built"})["schemaTs"]
    assert 0 < version < pause_ts, (version, pause_ts)


def scenario_index_build(cluster):
    """(j)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    building, pausing = hold_index_build(cluster, 1)
    log("the pause waits for the build; releasing the build")
    arm(cluster.nodes[1], INDEX_HOLD, "remove")
    building.wait(GATE_TIMEOUT_S)
    pause_ts = pausing.wait(GATE_TIMEOUT_S)
    check_index_before_pause(cluster, pause_ts)
    check_paused_everywhere(cluster, "j")
    resume(cluster, 2)
    check_open_everywhere(cluster, "j")
    print("PASS (j) an index build whose build finished after a pause was requested committed "
          "ungated, and the pause returned after it", flush=True)


def scenario_recovered_index_build(cluster):
    """(k)"""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    building, pausing = hold_index_build(cluster, 1)
    log("the pause waits for the build; killing node 1")
    cluster.kill(1)
    cluster.restart(1)
    pause_ts = pausing.wait(GATE_TIMEOUT_S)
    try:
        building.wait(10)
    except PyMongoError:
        pass  # the build's client lost its connection with node 1
    check_index_before_pause(cluster, pause_ts)
    check_paused_everywhere(cluster, "k")
    resume(cluster, 2)
    check_open_everywhere(cluster, "k")
    print("PASS (k) an index build recovered from its last prepare data log committed under a "
          "waiting pause, which returned after it", flush=True)


def scenario_gauge_step_down(cluster):
    """(l) Needs node groups with voters."""
    cluster.start()
    setup(Via(cluster.nodes[0]))
    pause(cluster, 0)
    wait_gauges(cluster, 1)
    first = cluster.nodes[0]
    log("paused; killing nodes 1 and 2, the voters of node 0's node group")
    cluster.kill(1)
    cluster.kill(2)
    deadline = time.monotonic() + 60
    while not first.current_log_lines("steps down as the leader of ng#0"):
        assert time.monotonic() < deadline, "node 0 never stopped leading its node group"
        time.sleep(0.5)
    assert first.process.poll() is None, "node 0 died"
    deadline = time.monotonic() + 30
    while gauge(first.metrics_port, "cluster_gate_paused", ng_id=0) != 0:
        assert time.monotonic() < deadline, "cluster_gate_paused stayed 1 after the step-down"
        time.sleep(0.5)
    log("node 0 stopped leading node group 0 and reads cluster_gate_paused 0")
    # Not restarted: a voter is not kept from leading a node group whose candidate is down, and
    # here each node's data store shard is local, so a voter that took node group 0 or 1 would
    # open its own stale copy of that shard (seen: EloqStore crashed on the first read).
    cluster.kill(0)
    print("PASS (l) a node that stopped leading its node group, its process alive, read "
          "cluster_gate_paused 0", flush=True)


SCENARIOS = (("a", scenario_everywhere, {}),
             ("b", scenario_restarts, {}),
             ("c", scenario_log_groups, {"log_replicas": 1}),
             ("d", scenario_pause_after_log, {}),
             ("e", scenario_resume_after_log, {}),
             ("f", scenario_second_failover, {}),
             ("g", scenario_install_crashes, {}),
             ("h", scenario_split_under_gate, {}),
             ("i", scenario_recovered_ddl, {}),
             ("j", scenario_index_build, {}),
             ("k", scenario_recovered_index_build, {}),
             ("l", scenario_gauge_step_down, {"ng_replicas": 3}))


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--lib-dir")
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--port-offset", type=int, default=0)
    parser.add_argument("--first-cpu", type=int, default=0)
    parser.add_argument("--cpus-per-node", type=int, default=8)
    parser.add_argument("--cloud", help="EloqStore cloud mode: a file of endpoint, bucket, "
                                        "access_key and secret_key lines")
    args = parser.parse_args()
    cloud = read_cloud_settings(args.cloud) if args.cloud else None
    cpus = [f"{args.first_cpu + args.cpus_per_node * i}-"
            f"{args.first_cpu + args.cpus_per_node * (i + 1) - 1}" for i in range(3)]
    failed = []
    for label, scenario, options in SCENARIOS:
        if args.scenario and label not in args.scenario:
            continue
        log(f"scenario ({label})")
        root = Path(args.root) / label
        try:
            with Cluster(args.server, root, lib_dir=args.lib_dir, cpus=cpus, keep=args.keep,
                         port_offset=args.port_offset, cloud=cloud, metrics=True, **options) as cluster:
                scenario(cluster)
        except Exception as exc:  # report and go on with the next scenario
            failed.append(label)
            print(f"FAIL ({label}) {type(exc).__name__}: {exc}", flush=True)
    if failed:
        print(f"FAILED: {' '.join(failed)}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
