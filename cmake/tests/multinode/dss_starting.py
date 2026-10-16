#!/usr/bin/env python3
"""Requests to a data store shard while it opens, on a cluster of three node groups run as
separate processes (data substrate docs/09-store-handler.md, DataStoreService::WaitWhileStarting).

A data store shard is Starting while its node opens it, as the node becomes its node group's
leader after a restart or a failover. A request that meets it then waits until the shard is open
and is served, instead of failing. The test holds node 1's shard in Starting with the
dss_hold_starting fault point, armed from the process's start (ELOQ_DSS_FAULT_INJECT), lists the
collections through node 0, which reads the catalog from the data store, checks that the listing
waits rather than fail, releases the shard through the data store service's FaultInjectForTest
RPC, and checks that the listing then returns every collection. In this three-node layout the
catalog's partition belongs to node 1's shard (seen: a listing through node 0 scans node 1's data
store service); a listing that never waits means the layout moved it.

Usage: dss_starting.py --server <eloqdoc built with WITH_FAULT_INJECT> --root <dir> [--lib-dir
       <dir>] [--keep] [--port-offset N] [--first-cpu N] [--cpus-per-node N] [--cloud <file>]
"""

import argparse
import json
import sys
import threading
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cluster import Cluster, read_cloud_settings  # noqa: E402

DB = "dss_starting"
HOLD = "dss_hold_starting"
HELD = 1  # the node whose shard holds the catalog's partition
DSS_PORT_OFFSET = 7
# Long enough for the other nodes' brpc channels, held down since the node's kill, to reconnect
# to its data store service through their health check, so the listing reaches the shard.
RECONNECT_S = 8


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def dss_fault(node, name, params):
    """Arms or removes (params "remove") a fault in the node's data store service, through its
    FaultInjectForTest RPC, called as JSON over HTTP."""
    body = json.dumps({"fault_name": name, "fault_paras": params}).encode()
    request = urllib.request.Request(
        f"http://127.0.0.1:{node.tx_port + DSS_PORT_OFFSET}"
        f"/EloqDS.remote.DataStoreRpcService/FaultInjectForTest",
        data=body, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=10) as reply:
        return json.loads(reply.read() or b"{}")


class Background:
    """A call run in a thread: its result or error once done."""

    def __init__(self, fn):
        self.result = None
        self.error = None
        self.done = threading.Event()
        threading.Thread(target=self._run, args=(fn,), daemon=True).start()

    def _run(self, fn):
        try:
            self.result = fn()
        except Exception as exc:  # reported by the caller
            self.error = exc
        finally:
            self.done.set()


def scenario_listing_waits(cluster):
    cluster.start()
    db = cluster.client(0)[DB]
    for name in ("a", "b", "c"):
        db.create_collection(name)
        db[name].insert_many([{"_id": i} for i in range(100)])
    held = cluster.nodes[HELD]
    log(f"killing node {HELD}; restarting it with its data store shard held in Starting")
    cluster.kill(HELD)
    held.launch(extra_env={"ELOQ_DSS_FAULT_INJECT": HOLD})
    deadline = time.monotonic() + 180
    while not held.current_log_lines(f"FaultInject {HOLD}: holding DSS shard {HELD}"):
        assert held.process.poll() is None, \
            f"node {HELD} exited (status {held.process.returncode})"
        assert time.monotonic() < deadline, f"node {HELD} never reached the hold"
        time.sleep(0.5)
    time.sleep(RECONNECT_S)
    log(f"node {HELD}'s shard is Starting; listing the collections through node 0")
    listing = Background(lambda: cluster.client(0)[DB].list_collection_names())
    assert not listing.done.wait(5), \
        f"the listing did not wait for the shard: {listing.error or listing.result}"
    log(f"the listing waits; releasing node {HELD}'s shard")
    dss_fault(held, HOLD, "remove")
    assert listing.done.wait(60), "the listing never returned after the release"
    assert listing.error is None, listing.error
    assert sorted(listing.result) == ["a", "b", "c"], listing.result
    held.wait_serving()
    held.wait_recovered()
    cluster.wait_catalog_reads()
    for node in cluster.nodes:
        counts = [node.client()[DB][name].count_documents({}) for name in ("a", "b", "c")]
        assert counts == [100, 100, 100], (node.index, counts)
    ran = held.current_log_lines(f"DSS shard {HELD} left Starting; running again")
    assert ran, f"node {HELD} ran no waiting request again"
    print(f"PASS the listing waited for node {HELD}'s shard to open and then returned every "
          f"collection ({ran[-1].split('running again ', 1)[1].strip()})", flush=True)


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
        scenario_listing_waits(cluster)


if __name__ == "__main__":
    main()
