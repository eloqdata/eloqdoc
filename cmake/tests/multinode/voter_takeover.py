#!/usr/bin/env python3
"""One EloqStore hosting two data shards, on a cluster of three node groups with voters, run as
separate processes (EloqStore docs/architecture/02-runtime-and-lifecycle.md, "Data store shards").

With node_group_replica_num 3, every node votes in every node group, and a voter leads a node group
while its own node is down. The voter's data store service then attaches that node group's data
shard to its one EloqStore, beside its own shard, at the group's new term. The test kills node 1,
waits for a voter to lead node group 1, writes and reads through every node meanwhile, restarts
node 1, waits for node group 1 to come back to it (the voter detaches the shard; its store runs
on), and checks that the voter still serves and that every document, those written during the
takeover included, reads through every node. Use cloud mode: the shard's data is shared, so the
voter serves node group 1's current data (in local mode it would open its own stale copy).

Usage: voter_takeover.py --server <eloqdoc> --root <dir> --cloud <file> [--lib-dir <dir>] [--keep]
       [--port-offset N] [--first-cpu N] [--cpus-per-node N]
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cluster import Cluster, read_cloud_settings  # noqa: E402

DB = "voter_takeover"
COLLECTIONS = ("a", "b", "c", "d")
DOWN = 1
TAKEOVER_TIMEOUT_S = 120


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def leads(node, ng):
    """Whether the node's latest process leads node group ng (its last leader start for ng came
    after its last step-down from it)."""
    started = node.current_log_lines(f"becomes the leader of ng#{ng}.")
    stopped = node.current_log_lines(f"steps down as the leader of ng#{ng}.")
    return bool(started) and (not stopped or started[-1] > stopped[-1])


def insert_round(cluster, label, count=50):
    """Inserts count documents into each collection through each running node."""
    for node in cluster.nodes:
        if node.process is None or node.process.poll() is not None:
            continue
        db = node.client()[DB]
        for name in COLLECTIONS:
            db[name].insert_many([{"_id": f"{label}-{node.index}-{i}"} for i in range(count)])


def check_counts(cluster, expected, timeout_s=30):
    """Reads every collection through each running node until each reads every document. A read,
    not count_documents: EloqDoc answers an unfiltered count from collection statistics that reach
    the other nodes a few seconds later (seen: 0 on one node right after 100 inserts that find
    returned). Waits: with voters, a read through one node misses another node's writes for a few
    seconds (seen: node 2's writes read through node 0 only 3 s later)."""
    for node in cluster.nodes:
        if node.process is None or node.process.poll() is not None:
            continue
        deadline = time.monotonic() + timeout_s
        while True:
            counts = [len(list(node.client()[DB][name].find({}, {"_id": 1})))
                      for name in COLLECTIONS]
            if counts == [expected] * len(COLLECTIONS):
                break
            assert time.monotonic() < deadline, (node.index, counts, expected)
            time.sleep(1)


def scenario_takeover(cluster):
    cluster.start()
    insert_round(cluster, "before")
    expected = 50 * len(cluster.nodes)
    check_counts(cluster, expected)

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
    opened = voter.current_log_lines(f"OpenDataStore success for DSS shard {DOWN}")
    assert opened, f"node {voter.index} leads node group {DOWN} without opening its shard"
    log(f"node {voter.index} leads node group {DOWN} with its data shard attached beside its own")
    cluster.wait_catalog_reads()
    insert_round(cluster, "during")
    expected += 50 * (len(cluster.nodes) - 1)
    check_counts(cluster, expected)

    log(f"restarting node {DOWN}; waiting for node group {DOWN} to come back to it")
    cluster.restart(DOWN, wait=False)
    returning = cluster.nodes[DOWN]
    deadline = time.monotonic() + TAKEOVER_TIMEOUT_S
    while not (leads(returning, DOWN) and not leads(voter, DOWN)):
        assert returning.process.poll() is None, f"node {DOWN} exited"
        assert voter.process.poll() is None, f"node {voter.index} exited"
        assert time.monotonic() < deadline, \
            f"node group {DOWN} did not come back to node {DOWN}"
        time.sleep(0.5)
    returning.wait_serving()
    returning.wait_recovered()
    # The voter's store keeps running for its own shard: it detaches node group 1's.
    detached = voter.current_log_lines(f"Detached data store shard {DOWN}")
    assert detached, f"node {voter.index} never detached node group {DOWN}'s data shard"
    stopped = voter.current_log_lines("EloqStore is stopped.")
    assert not stopped, f"node {voter.index} stopped its EloqStore instead of detaching"
    log(f"node group {DOWN} is back on node {DOWN}; node {voter.index} detached its data shard")
    cluster.wait_catalog_reads()
    insert_round(cluster, "after")
    expected += 50 * len(cluster.nodes)
    check_counts(cluster, expected)
    assert voter.process.poll() is None, f"node {voter.index} exited"
    print(f"PASS node {voter.index} hosted node group {DOWN}'s data shard beside its own while "
          f"node {DOWN} was down, detached it when the group came back, and kept serving; all "
          f"{expected} documents per collection read through every node", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--cloud", required=True,
                        help="EloqStore cloud mode: a file of endpoint, bucket, access_key and "
                             "secret_key lines")
    parser.add_argument("--lib-dir")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--port-offset", type=int, default=0)
    parser.add_argument("--first-cpu", type=int, default=0)
    parser.add_argument("--cpus-per-node", type=int, default=8)
    args = parser.parse_args()
    cpus = [f"{args.first_cpu + args.cpus_per_node * i}-"
            f"{args.first_cpu + args.cpus_per_node * (i + 1) - 1}" for i in range(3)]
    with Cluster(args.server, args.root, lib_dir=args.lib_dir, cpus=cpus, keep=args.keep,
                 port_offset=args.port_offset, ng_replicas=3,
                 cloud=read_cloud_settings(args.cloud)) as cluster:
        scenario_takeover(cluster)


if __name__ == "__main__":
    main()
