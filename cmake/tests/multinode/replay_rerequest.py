#!/usr/bin/env python3
"""A node group's log replay asked for once, and asked again only when a log group goes silent, on a
cluster of three node groups run as separate processes (data substrate
docs/06-distribution-and-clustering.md, Sharder::RequestLogReplay and RecoveryService's watchdog).

A node asks the log groups to replay its node group's log once, at the end of its leader start. The
test restarts node 1 with the replay_drop_stream_once fault armed from the process's start
(ELOQ_TX_FAULT_INJECT): node 1 drops the first replay stream a log group opens to it, as a broken
connection would, so that log group goes silent. It checks that node 1 asked once at its leader
start and asked the silent log group again, that the log group replaced the node group's idle
shipping agent with a new one (the old one stopping at once), that node 1 then finished its replay,
and that every document reads through every node.

Usage: replay_rerequest.py --server <eloqdoc built with WITH_FAULT_INJECT> --root <dir>
       [--lib-dir <dir>] [--keep] [--port-offset N] [--first-cpu N] [--cpus-per-node N]
       [--cloud <file>]
"""

import argparse
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cluster import Cluster, read_cloud_settings  # noqa: E402

DB = "replay_rerequest"
FAULT = "replay_drop_stream_once"
RESTARTED = 1
# The old shipping agent is idle (its stream broke), so it stops at once when it is replaced.
STOP_BOUND_MS = 2000


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def scenario_silent_log_group(cluster):
    cluster.start()
    db = cluster.client(0)[DB]
    for name in ("a", "b", "c"):
        db[name].insert_many([{"_id": i} for i in range(100)])
    node = cluster.nodes[RESTARTED]
    log(f"killing node {RESTARTED}; restarting it with its first replay stream dropped")
    cluster.kill(RESTARTED)
    node.launch(extra_env={"ELOQ_TX_FAULT_INJECT": FAULT})
    node.wait_serving()
    node.wait_recovered()
    cluster.wait_catalog_reads()

    dropped = node.current_log_lines(f"FaultInject {FAULT}: dropping replay stream")
    assert len(dropped) == 1, dropped
    asked = node.current_log_lines(f"Requesting log replay for ng: {RESTARTED} at term:")
    on_start = [line for line in asked if "on its leader start" in line]
    again = [line for line in asked if " again: " in line]
    assert len(on_start) == 1, asked
    assert again and len(on_start) + len(again) == len(asked), asked
    log(f"node {RESTARTED} asked once at its leader start and {len(again)} time(s) again")

    replaced = [line for other in cluster.nodes for line in other.current_log_lines(
        f"Replaced the log shipping agent of cc node group {RESTARTED} ")]
    assert replaced, "no log group replaced the silent shipping agent"
    stopped_ms = [int(re.search(r"stopped in (\d+) ms", line).group(1)) for line in replaced]
    assert max(stopped_ms) <= STOP_BOUND_MS, replaced

    for each in cluster.nodes:
        counts = [each.client()[DB][name].count_documents({}) for name in ("a", "b", "c")]
        assert counts == [100, 100, 100], (each.index, counts)
    print(f"PASS node {RESTARTED} asked for its replay once at its leader start and again only for "
          f"the log group whose stream it dropped ({len(again)} time(s)); the log group replaced "
          f"the idle shipping agent (stopped in {max(stopped_ms)} ms) and node {RESTARTED} "
          f"recovered every document", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--lib-dir")
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--port-offset", type=int, default=0)
    parser.add_argument("--first-cpu", type=int, default=0)
    parser.add_argument("--cpus-per-node", type=int, default=8)
    parser.add_argument("--cloud", help="EloqStore cloud mode: a file of endpoint, bucket, "
                                        "access_key and secret_key lines")
    args = parser.parse_args()
    cpus = [f"{args.first_cpu + args.cpus_per_node * i}-"
            f"{args.first_cpu + args.cpus_per_node * (i + 1) - 1}" for i in range(3)]
    with Cluster(args.server, args.root, lib_dir=args.lib_dir, cpus=cpus, keep=args.keep,
                 port_offset=args.port_offset,
                 cloud=read_cloud_settings(args.cloud) if args.cloud else None) as cluster:
        scenario_silent_log_group(cluster)


if __name__ == "__main__":
    main()
