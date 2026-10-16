#!/usr/bin/env python3
"""Change streams across a log group leader's step-down, on a cluster of three node groups run as
separate processes, one log group replicated on all three (docs/cdc-change-stream.md §5.8 and §10
item 8; plan A5).

A change stream's getMore on an idle collection parks its CdcScan request on the log group's
leader (CdcLongPoll). A leader that stops leading completes every parked request with NotLeader
(LogInstance::on_leader_stop), and a reader whose connection breaks cancels its parked requests
(brpc NotifyOnCancel): whichever comes first answers, once. The streams must carry on against
the new leader with every event delivered exactly once, and nothing may stay parked.

Each reader drives its stream with raw aggregate and getMore commands, so an error the server
returns is seen rather than resumed over by the driver.

  (t) transfer: streams on every node, parked on the leader; the leadership is transferred to
      another node (the LOG_TRANSFER fault action, a braft transfer), three rounds, with a writer
      inserting throughout. Node 0, the group's preferred leader (its first member), may ask for
      the lead back 5 s after it starts following, a second step-down (seen after the first
      transfer, not after every one). Each node that stepped down drops its parked count to 0,
      every stream reads every insert exactly once, and the streams park on whichever node leads;
  (m) replica metrics: every replica of the log group, leader and followers, exports
      cdc_arrival_complete_ts for it and keeps it advancing, a follower restarted included (each
      replica publishes its meter once metrics exist, which the log server's start precedes);
  (q) quorum loss: streams on node 0, made the leader; the other two nodes are killed, so node 0
      steps down for want of a majority while its process lives; its parked count drops to 0;
      once the two restart and a leader is elected, the streams read the inserts made before the
      kill and after the restart, exactly once;
  (k) leader killed: the leader's process dies; the streams on the other nodes park on the new
      leader while it is down, and read every insert exactly once once it is back (its node group
      is down with it, so nothing is written meanwhile);
  (c) cancel against stop: many streams on a node that is not the leader park there; that node
      is killed (breaking its connections, so the leader's NotifyOnCancel fires for each parked
      request) at the same moment as the leadership is transferred (on_leader_stop completes
      them all); repeated; the old leader survives, nothing stays parked on any node, and new
      streams read new inserts.

Usage: cdc_step_down.py --server <eloqdoc built with WITH_FAULT_INJECT> --root <dir>
       [--lib-dir <dir>] [--scenario t] ... [--keep] [--port-offset N] [--first-cpu N]
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
from cluster_gate import scrape  # noqa: E402
from pymongo.errors import PyMongoError  # noqa: E402

DB = "cdc_step_down"
COLL = "events"
GETMORE_WAIT_MS = 5000
DELIVERY_TIMEOUT_S = 120


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Reader:
    """A change stream on the collection through one node, read by a thread with raw getMores;
    keeps the _id of every insert event, in order, and the first error."""

    def __init__(self, node):
        self.node_index = node.index
        self.client = node.client()
        reply = self.client[DB].command({"aggregate": COLL, "pipeline": [{"$changeStream": {}}],
                                         "cursor": {}})
        self.cursor_id = reply["cursor"]["id"]
        self.ids = [event["documentKey"]["_id"] for event in reply["cursor"]["firstBatch"]
                    if event["operationType"] == "insert"]
        self.error = None
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stop.is_set():
            try:
                reply = self.client[DB].command({"getMore": self.cursor_id, "collection": COLL,
                                                 "maxTimeMS": GETMORE_WAIT_MS})
            except PyMongoError as exc:
                if not self.stop.is_set():
                    self.error = exc
                return
            self.ids += [event["documentKey"]["_id"] for event in reply["cursor"]["nextBatch"]
                         if event["operationType"] == "insert"]

    def close(self):
        self.stop.set()
        self.thread.join(timeout=GETMORE_WAIT_MS / 1000 + 30)
        try:
            self.client[DB].command({"killCursors": COLL, "cursors": [self.cursor_id]})
        except PyMongoError:
            pass


def wait_delivered(readers, count, first=0, timeout_s=DELIVERY_TIMEOUT_S):
    """Waits until every reader has read the inserts first..count-1, exactly once each."""
    deadline = time.monotonic() + timeout_s
    while True:
        for reader in readers:
            assert reader.error is None, f"node {reader.node_index}'s stream failed: " \
                                         f"{reader.error}"
            assert len(reader.ids) == len(set(reader.ids)), \
                f"node {reader.node_index}'s stream read an event twice: {duplicates(reader.ids)}"
        if all(len(reader.ids) >= count - first for reader in readers):
            break
        if time.monotonic() > deadline:
            raise AssertionError(f"streams read {[len(r.ids) for r in readers]} of "
                                 f"{count - first} inserts")
        time.sleep(0.5)
    for reader in readers:
        assert sorted(reader.ids) == list(range(first, count)), \
            (reader.node_index, len(reader.ids), missing(reader.ids, first, count))


def duplicates(ids):
    seen, dups = set(), []
    for i in ids:
        if i in seen:
            dups.append(i)
        seen.add(i)
    return dups[:10]


def missing(ids, first, count):
    return sorted(set(range(first, count)) - set(ids))[:10]


class Writer:
    """Inserts _id first, first + 1, ... through a node, one per transaction, until stopped."""

    def __init__(self, cluster, index, first):
        self.coll = cluster.client(index)[DB][COLL]
        self.next = first
        self.error = None
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stop.is_set():
            try:
                self.coll.insert_one({"_id": self.next})
            except PyMongoError as exc:
                self.error = exc
                return
            self.next += 1
            time.sleep(0.01)

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=60)
        assert self.error is None, self.error
        return self.next


def insert(cluster, index, first, count):
    cluster.client(index)[DB][COLL].insert_many([{"_id": i} for i in range(first, first + count)])
    return first + count


def parked(node):
    """cdc_scan_parked_requests of log group 0 on a node: what it holds parked as the leader."""
    values = scrape(node.metrics_port, "cdc_scan_parked_requests", lg_id=0)
    return values[0] if values else 0.0


def wait_parked(node, predicate, what, timeout_s=60):
    deadline = time.monotonic() + timeout_s
    seen = None
    while time.monotonic() < deadline:
        try:
            seen = parked(node)
            if predicate(seen):
                return seen
        except OSError:
            pass
        time.sleep(0.2)
    raise AssertionError(f"node {node.index}'s parked requests never {what} (last {seen})")


def transfer_leader(cluster, to):
    """Transfers log group 0's leadership to node to (its index among the group's members) and
    waits until it leads."""
    before = cluster.log_leader()
    via = next(node for node in cluster.nodes if node.process.poll() is None)
    via.client().admin.command({"eloqFaultInject": "at_once",
                                "params": f"action=LOG_TRANSFER#0-{to}"})
    deadline = time.monotonic() + 60
    while cluster.log_leader() != to:
        assert time.monotonic() < deadline, f"node {to} never took the lead from node {before}"
        time.sleep(0.2)


def setup(cluster):
    cluster.start()
    cluster.client(0)[DB].create_collection(COLL)


TAKE_BACK_S = 8  # node 0, the preferred leader, may ask for the lead back 5 s after following


def settle(cluster):
    """Waits out a possible take-back of the lead by node 0; returns the node that leads then."""
    time.sleep(TAKE_BACK_S)
    return cluster.nodes[cluster.log_leader()]


def scenario_transfer(cluster):
    """(t)"""
    setup(cluster)
    readers = [Reader(node) for node in cluster.nodes]
    count = insert(cluster, 0, 0, 100)
    wait_delivered(readers, count)
    for round_ in range(3):
        leader = cluster.nodes[cluster.log_leader()]
        wait_parked(leader, lambda n: n >= len(readers), f"reached {len(readers)}")
        target = cluster.nodes[(leader.index + 1 + round_ % 2) % len(cluster.nodes)]
        writer = Writer(cluster, target.index, count)
        time.sleep(1)
        transfer_leader(cluster, target.index)
        wait_parked(leader, lambda n: n == 0, "dropped to 0 once it handed the lead over")
        final = settle(cluster)
        log(f"round {round_}: node {leader.index} handed the lead to node {target.index}, its "
            f"parked requests answered; node {final.index} leads now")
        for node in cluster.nodes:
            if node is not final:
                wait_parked(node, lambda n: n == 0, "dropped to 0 as a follower")
        count = writer.finish()
        wait_delivered(readers, count)
        wait_parked(final, lambda n: n >= 1, "reached 1")
        log(f"round {round_}: every stream read all {count} inserts and parks on node "
            f"{final.index}")
    for reader in readers:
        reader.close()
    print(f"PASS (t) streams on every node read all {count} inserts exactly once across three "
          f"leadership transfers", flush=True)


def scenario_quorum_loss(cluster):
    """(q)"""
    setup(cluster)
    if cluster.log_leader() != 0:
        transfer_leader(cluster, 0)
    readers = [Reader(cluster.nodes[0]) for _ in range(3)]
    count = insert(cluster, 0, 0, 100)
    wait_delivered(readers, count)
    wait_parked(cluster.nodes[0], lambda n: n >= len(readers), f"reached {len(readers)}")
    log("streams parked on node 0, the leader; killing nodes 1 and 2")
    cluster.kill(1)
    cluster.kill(2)
    started = time.monotonic()
    wait_parked(cluster.nodes[0], lambda n: n == 0, "dropped to 0 after the step-down")
    log(f"node 0 answered its parked requests {time.monotonic() - started:.1f} s after losing "
        f"its majority")
    time.sleep(5)  # the streams back off against a group without a leader
    for index in (1, 2):
        cluster.nodes[index].launch()
    for index in (1, 2):
        cluster.nodes[index].wait_serving()
        cluster.nodes[index].wait_recovered()
    cluster.wait_catalog_reads()
    log(f"nodes 1 and 2 restarted; node {cluster.log_leader()} leads the log group")
    count = insert(cluster, 0, count, 100)
    wait_delivered(readers, count)
    for reader in readers:
        reader.close()
    print(f"PASS (q) streams on a leader that lost its majority read all {count} inserts exactly "
          f"once after a new leader was elected", flush=True)


def scenario_leader_killed(cluster):
    """(k)"""
    setup(cluster)
    leader = cluster.log_leader()
    others = [node for node in cluster.nodes if node.index != leader]
    readers = [Reader(node) for node in others]
    count = insert(cluster, others[0].index, 0, 100)
    wait_delivered(readers, count)
    wait_parked(cluster.nodes[leader], lambda n: n >= len(readers), f"reached {len(readers)}")
    # The kill takes the node's node group down too, so nothing is written until it is back.
    log(f"killing node {leader}, the leader")
    cluster.kill(leader)
    deadline = time.monotonic() + 60
    while cluster.log_leader() in (None, leader):
        assert time.monotonic() < deadline, "no node took the lead"
        time.sleep(0.2)
    new_leader = cluster.nodes[cluster.log_leader()]
    wait_parked(new_leader, lambda n: n >= len(readers), f"reached {len(readers)}")
    log(f"the streams parked on node {new_leader.index}, the new leader")
    for reader in readers:
        assert reader.error is None, reader.error
    cluster.restart(leader)
    count = insert(cluster, others[0].index, count, 100)
    wait_delivered(readers, count)
    for reader in readers:
        reader.close()
    print(f"PASS (k) streams on the surviving nodes read all {count} inserts exactly once after "
          f"the leader died", flush=True)


def scenario_cancel_against_stop(cluster, rounds=5, streams=30):
    """(c)"""
    setup(cluster)
    count = 0
    for round_ in range(rounds):
        leader = cluster.log_leader()
        reader_node = cluster.nodes[(leader + 1 + round_ % 2) % 3]
        target = 3 - leader - reader_node.index
        readers = [Reader(reader_node) for _ in range(streams)]
        first = count
        count = insert(cluster, target, count, 10)
        wait_delivered(readers, count, first)
        wait_parked(cluster.nodes[leader], lambda n: n >= streams, f"reached {streams}")
        # The kill breaks the reader node's connections (NotifyOnCancel on every parked request)
        # while the transfer stops the leader (on_leader_stop completes them all).
        killer = threading.Thread(target=cluster.kill, args=(reader_node.index,))
        killer.start()
        transfer_leader(cluster, target)
        killer.join()
        for reader in readers:
            reader.stop.set()
        for node in cluster.nodes:
            if node.process.poll() is None:
                wait_parked(node, lambda n: n == 0, "dropped to 0", timeout_s=90)
        assert cluster.nodes[leader].process.poll() is None, f"node {leader} died"
        cluster.restart(reader_node.index)
        settle(cluster)
        log(f"round {round_}: {streams} parked requests from node {reader_node.index} were "
            f"cancelled while node {leader} handed the lead to node {target}; nothing stayed "
            f"parked")
    readers = [Reader(node) for node in cluster.nodes]
    first = count
    count = insert(cluster, 0, count, 50)
    wait_delivered(readers, count, first)
    for reader in readers:
        reader.close()
    print(f"PASS (c) {rounds} rounds of {streams} parked requests cancelled while their leader "
          f"stepped down: each answered once, nothing stayed parked, new streams read new inserts",
          flush=True)


def arrival_ts(node):
    """cdc_arrival_complete_ts of log group 0 on a node, in microseconds; None if not exported."""
    values = scrape(node.metrics_port, "cdc_arrival_complete_ts", lg_id=0)
    return values[0] if values else None


def wait_arrival_advancing(node, timeout_s=90):
    """Waits until the node exports cdc_arrival_complete_ts for log group 0 and it advances."""
    deadline = time.monotonic() + timeout_s
    first = None
    while time.monotonic() < deadline:
        try:
            value = arrival_ts(node)
        except OSError:
            value = None
        if value:
            if first is None:
                first = value
            elif value > first:
                return value
        time.sleep(0.5)
    raise AssertionError(f"node {node.index}'s cdc_arrival_complete_ts never advanced "
                         f"(first {first})")


def scenario_replica_metrics(cluster):
    """(m)"""
    setup(cluster)
    for node in cluster.nodes:
        value = wait_arrival_advancing(node)
        lag_s = time.time() - value / 1e6
        assert 0 <= lag_s < 120, (node.index, lag_s)
    leader = cluster.log_leader()
    follower = (leader + 1) % 3
    log(f"every replica's cdc_arrival_complete_ts advances; restarting node {follower}, a "
        f"follower")
    cluster.kill(follower)
    cluster.restart(follower)
    for node in cluster.nodes:
        wait_arrival_advancing(node)
    print("PASS (m) every replica of the log group exported an advancing "
          "cdc_arrival_complete_ts, a follower restarted included", flush=True)


SCENARIOS = (("t", scenario_transfer),
             ("m", scenario_replica_metrics),
             ("q", scenario_quorum_loss),
             ("k", scenario_leader_killed),
             ("c", scenario_cancel_against_stop))


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
    for label, scenario in SCENARIOS:
        if args.scenario and label not in args.scenario:
            continue
        log(f"scenario ({label})")
        try:
            with Cluster(args.server, Path(args.root) / label, lib_dir=args.lib_dir, cpus=cpus,
                         keep=args.keep, port_offset=args.port_offset, cloud=cloud, metrics=True) as cluster:
                scenario(cluster)
        except Exception as exc:  # report and go on with the next scenario
            failed.append(label)
            print(f"FAIL ({label}) {type(exc).__name__}: {exc}", flush=True)
    if failed:
        print(f"FAILED: {' '.join(failed)}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
