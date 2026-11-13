#!/usr/bin/env python3
"""Concluded checkpoint cycles on live nodes (data substrate docs/07-durability-and-recovery.md,
"Each round is also one checkpoint cycle"): the per-node count that ages requests parked for
memory, read through each node's checkpoint_cycles_concluded_total metric. Rounds run every
CKPT_INTERVAL seconds here, and the checkpointer_skip_ckpt fault point holds new rounds off while a
scenario waits for the count to settle, so that the count moves only for the round under test.

  (d) deduplicated rounds: the data sync task of fresh dirty data is held at its start (the
      ckpt_hold_data_sync_task fault point) on the node whose group owns the range. The rounds
      that start meanwhile find the range's task pending and fold into it, and must not conclude
      before it lands: that node's count stays flat over several rounds while the other nodes'
      counts advance. Released, with new rounds held off, the count rises by exactly one for the
      held round plus one for every round folded into it, and advances again once rounds resume.
      The held data reads back afterwards.
  (a) abandoned part (cloud mode only, node group voters): node 1 is killed and a voter leads node
      group 1. The voter's round over node group 1 is held after its part joined the cycle (the
      ckpt_hold_round_after_join fault point) while node 1 returns and takes its group back.
      Released, the round finds the group's term changed, abandons its part, and the cycle
      concludes: the voter's count rises by exactly one with new rounds held off, and advances
      again afterwards.

Usage: ckpt_cycles.py --server <eloqdoc> --root <dir> [--cloud <file>] [--scenario d|a ...]
       [--lib-dir <dir>] [--keep] [--port-offset N] [--first-cpu N] [--cpus-per-node N]
"""

import argparse
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))
from cluster import Cluster, read_cloud_settings  # noqa: E402
from cluster_gate import scrape, wait_for  # noqa: E402
from ddl_split_recovery import arm, disarm  # noqa: E402
from voter_takeover import leads  # noqa: E402

DB = "ckpt_cycles"
COLL = "held"
CKPT_INTERVAL_S = 2
METRIC = "checkpoint_cycles_concluded_total"
HOLD_TASK = "ckpt_hold_data_sync_task"
HOLD_ROUND = "ckpt_hold_round_after_join"
SKIP_ROUNDS = "checkpointer_skip_ckpt"
DOWN = 1
TAKEOVER_TIMEOUT_S = 120


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def cycles(node):
    """The node's concluded checkpoint cycles, as its metrics endpoint reports them."""
    values = scrape(node.metrics_port, METRIC)
    assert len(values) <= 1, values
    return int(values[0]) if values else 0


def rounds(node):
    """How many timer-triggered checkpoint rounds the node's latest process has started."""
    return len(node.current_log_lines("Checkpoint triggered: reason=timer"))


def held(node, fault):
    """Whether the fault point has begun holding its thread (its log line)."""
    return bool(node.current_log_lines(f"{fault}: holding"))


def rounds_since_hold(node, fault):
    """How many timer-triggered rounds the node's latest process started after the fault point
    began holding (the log is written in order, one line at a time)."""
    try:
        with (node.root / "eloqdb.log.INFO").open("r", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return 0
    after = False
    count = 0
    for line in lines:
        if f"{fault}: holding" in line:
            after = True
        elif after and "Checkpoint triggered: reason=timer" in line:
            count += 1
    return count


def wait_cycles(node, predicate, timeout_s, what):
    wait_for(lambda: predicate(cycles(node)), timeout_s, f"node {node.index}'s count to {what}")


def check_steady(node, value, intervals, what):
    """Asserts that the node's count reads value for the given number of checkpoint intervals."""
    deadline = time.monotonic() + intervals * CKPT_INTERVAL_S
    while time.monotonic() < deadline:
        seen = cycles(node)
        assert seen == value, f"node {node.index}'s count moved to {seen} from {value} {what}"
        time.sleep(0.5)


FIRST_TX_RACE = b"Failed to initialize the transaction"
FIRST_TX_RACE_EXIT = 100


def server_log_size(node):
    """The size of the node's server.log, which every launch appends to."""
    try:
        return (node.root / "server.log").stat().st_size
    except OSError:
        return 0


def lost_first_tx_race(node, log_offset):
    """Whether the node's latest process, whose output starts at log_offset in server.log, exited
    at start-up because its first transaction ran before it followed its own node group, led by
    another node: mongod logs "Failed to initialize the transaction" and exits with status 100. A
    pre-existing start-up race of a node whose group a voter leads (the engine waits for the
    cluster to be ready, not for its own group), unrelated to the cycle count."""
    if node.process.returncode != FIRST_TX_RACE_EXIT:
        return False
    with (node.root / "server.log").open("rb") as f:
        f.seek(log_offset)
        return FIRST_TX_RACE in f.read()


def disarm_quietly(node, fault):
    try:
        disarm(node, fault)
    except Exception as exc:  # the node may be gone after a failed scenario
        log(f"could not disarm {fault} on node {node.index}: {exc}")


def scenario_dedup(cluster):
    cluster.start()
    coll = cluster.client(0)[DB][COLL]
    # Two rounds concluded on every node with the cluster idle: nothing of the start-up is still
    # flushing anywhere.
    for node in cluster.nodes:
        base = cycles(node)
        wait_cycles(node, lambda c, b=base: c >= b + 2, 60, "conclude two idle rounds")

    # The collection's range belongs to the node group that owns its bucket, so its data sync task
    # runs on that node, whichever it is: arm every node and follow the one that holds a task.
    for node in cluster.nodes:
        arm(node, HOLD_TASK)
    holder = None
    try:
        coll.insert_many([{"_id": f"doc-{i}", "v": i} for i in range(200)])
        wait_for(lambda: any(held(node, HOLD_TASK) for node in cluster.nodes), 60,
                 f"{HOLD_TASK} to hold a node's task")
        holder = next(node for node in cluster.nodes if held(node, HOLD_TASK))
        others = [node for node in cluster.nodes if node is not holder]
        for node in others:
            disarm(node, HOLD_TASK)
        flat = cycles(holder)
        others_at_hold = [cycles(node) for node in others]
        log(f"node {holder.index}'s task is held with its count at {flat}; "
            f"waiting for rounds to pile up")

        deadline = time.monotonic() + 60
        while rounds_since_hold(holder, HOLD_TASK) < 4:
            assert time.monotonic() < deadline, \
                f"node {holder.index} started fewer than four rounds in 60 s"
            seen = cycles(holder)
            assert seen == flat, f"node {holder.index}'s count moved to {seen} during the hold"
            time.sleep(0.5)
        for node, before in zip(others, others_at_hold):
            wait_cycles(node, lambda c, b=before: c > b, 30,
                        f"advance during node {holder.index}'s hold")

        # No new rounds from here on; a round logged just before is still being enqueued, so
        # give it a moment before counting. Every round started during the hold found the held
        # task still waiting for its timestamp and was folded into it.
        arm(holder, SKIP_ROUNDS)
        time.sleep(1)
        piled = rounds_since_hold(holder, HOLD_TASK)
        assert cycles(holder) == flat, f"node {holder.index}'s count moved during the hold"
        log(f"{piled} rounds started on node {holder.index} during the hold and were folded "
            f"into its held task; its count stayed {flat}; the other nodes' counts advanced")

        expected = flat + 1 + piled
        disarm(holder, HOLD_TASK)
        wait_cycles(holder, lambda c: c == expected, 60,
                    f"rise to {expected} (the held round and the {piled} folded into it)")
        check_steady(holder, expected, 3, "after the release, with new rounds held off")
        log(f"released: node {holder.index}'s count rose from {flat} to {expected}: the held "
            f"round and the {piled} rounds folded into it concluded when the flush landed")

        disarm(holder, SKIP_ROUNDS)
        wait_cycles(holder, lambda c: c > expected, 60, "advance again once rounds resume")
    finally:
        for node in cluster.nodes:
            disarm_quietly(node, HOLD_TASK)
            disarm_quietly(node, SKIP_ROUNDS)

    assert len(list(coll.find({}, {"_id": 1}))) == 200, "the held data did not read back"
    print(f"PASS (d) {piled} rounds deduplicated into node {holder.index}'s held flush concluded "
          f"no cycle until the flush landed, then one each, with the round that started it",
          flush=True)


def scenario_abandon(cluster):
    cluster.start()
    for node in cluster.nodes:
        node.client()[DB][COLL].insert_many([{"_id": f"n{node.index}-{i}"} for i in range(20)])

    log(f"killing node {DOWN}; waiting for a voter to lead node group {DOWN}")
    cluster.kill(DOWN)
    deadline = time.monotonic() + TAKEOVER_TIMEOUT_S
    voter = None
    while voter is None:
        assert time.monotonic() < deadline, f"no voter led node group {DOWN}"
        for node in cluster.nodes:
            if node.index != DOWN and node.process.poll() is None and leads(node, DOWN):
                voter = node
        time.sleep(0.5)
    cluster.wait_catalog_reads()
    log(f"node {voter.index} leads node group {DOWN}")

    arm(voter, HOLD_ROUND, f"ng_id={DOWN}")
    try:
        wait_for(lambda: held(voter, HOLD_ROUND), 60,
                 f"{HOLD_ROUND} to hold node {voter.index}'s round over node group {DOWN}")
        flat = cycles(voter)
        arm(voter, SKIP_ROUNDS)
        log(f"node {voter.index}'s round is held with its count at {flat}; "
            f"restarting node {DOWN}")

        returning = cluster.nodes[DOWN]
        log_offset = server_log_size(returning)
        relaunches = 3
        cluster.restart(DOWN, wait=False)
        deadline = time.monotonic() + TAKEOVER_TIMEOUT_S
        while not (leads(returning, DOWN) and not leads(voter, DOWN)):
            if returning.process.poll() is not None:
                assert relaunches > 0 and lost_first_tx_race(returning, log_offset), \
                    f"node {DOWN} exited (status {returning.process.returncode})"
                relaunches -= 1
                log(f"node {DOWN} exited at start-up: its first transaction ran while node "
                    f"{voter.index} still led node group {DOWN} (a known start-up race); "
                    f"relaunching it")
                log_offset = server_log_size(returning)
                returning.launch()
                continue
            assert voter.process.poll() is None, f"node {voter.index} exited"
            assert time.monotonic() < deadline, \
                f"node group {DOWN} did not come back to node {DOWN}"
            time.sleep(0.5)
        returning.wait_serving()
        returning.wait_recovered()
        assert cycles(voter) == flat, f"node {voter.index}'s count moved during the hold"
        log(f"node group {DOWN} is back on node {DOWN}; releasing node {voter.index}'s round")

        disarm(voter, HOLD_ROUND)
        wait_cycles(voter, lambda c: c == flat + 1, 60, "rise by one after the release")
        check_steady(voter, flat + 1, 3, "after the release, with new rounds held off")
        log(f"released: node {voter.index}'s count rose by one, to {flat + 1}")

        disarm(voter, SKIP_ROUNDS)
        wait_cycles(voter, lambda c: c > flat + 1, 60, "advance again once rounds resume")
    finally:
        disarm_quietly(voter, HOLD_ROUND)
        disarm_quietly(voter, SKIP_ROUNDS)

    cluster.wait_catalog_reads()
    for node in cluster.nodes:
        assert len(list(node.client()[DB][COLL].find({}, {"_id": 1}))) == 60, node.index
    print(f"PASS (a) node {voter.index}'s round over node group {DOWN}, held across the group's "
          f"return to node {DOWN}, abandoned its part and its cycle concluded", flush=True)


# label, scenario, needs cloud mode
SCENARIOS = (("d", scenario_dedup, False),
             ("a", scenario_abandon, True))


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
                                        "access_key and secret_key lines; scenario (a) needs it")
    args = parser.parse_args()
    cloud = read_cloud_settings(args.cloud) if args.cloud else None
    cpus = [f"{args.first_cpu + args.cpus_per_node * i}-"
            f"{args.first_cpu + args.cpus_per_node * (i + 1) - 1}" for i in range(3)]
    failed = []
    for label, scenario, needs_cloud in SCENARIOS:
        if args.scenario and label not in args.scenario:
            continue
        if needs_cloud and cloud is None:
            log(f"scenario ({label}) skipped: it needs --cloud")
            continue
        log(f"scenario ({label})")
        try:
            with Cluster(args.server, Path(args.root) / label, lib_dir=args.lib_dir, cpus=cpus,
                         checkpointer_interval=CKPT_INTERVAL_S, keep=args.keep,
                         port_offset=args.port_offset, cloud=cloud, metrics=True,
                         ng_replicas=3 if needs_cloud else 1) as cluster:
                scenario(cluster)
        except Exception as exc:  # report and go on with the next scenario
            failed.append(label)
            print(f"FAIL ({label}) {type(exc).__name__}: {exc}", flush=True)
    if failed:
        print(f"FAILED: {' '.join(failed)}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
