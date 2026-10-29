#!/usr/bin/env python3
"""Smoke test of the multi-node harness: three nodes come up, every node serves reads and writes of
one collection, and a node killed and restarted recovers what it held.

Usage: smoke.py --server <eloqdoc> --root <dir> [--lib-dir <dir>] [--nodes N] [--keep]
       [--port-offset N] [--first-cpu N] [--cpus-per-node N] [--cloud <settings file>]
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cluster import Cluster, read_cloud_settings  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--lib-dir")
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--port-offset", type=int, default=0)
    parser.add_argument("--first-cpu", type=int, default=0)
    parser.add_argument("--cpus-per-node", type=int, default=8)
    parser.add_argument("--cloud", help="EloqStore cloud mode: a file of endpoint, bucket, "
                                        "access_key and secret_key lines")
    args = parser.parse_args()
    cpus = [f"{args.first_cpu + args.cpus_per_node * i}-"
            f"{args.first_cpu + args.cpus_per_node * (i + 1) - 1}" for i in range(args.nodes)]
    with Cluster(args.server, args.root, nodes=args.nodes, lib_dir=args.lib_dir, cpus=cpus,
                 keep=args.keep, port_offset=args.port_offset,
                 cloud=read_cloud_settings(args.cloud) if args.cloud else None) as cluster:
        started = time.monotonic()
        cluster.start()
        print(f"up: {args.nodes} nodes in {time.monotonic() - started:.1f} s", flush=True)
        db0 = cluster.client(0).smoke
        db0.create_collection("c")
        for i in range(args.nodes):
            cluster.client(i).smoke.c.insert_many(
                [{"_id": f"n{i}-{k}", "node": i, "k": k} for k in range(1000)])
        want = [1000 * args.nodes] * args.nodes
        counts = [cluster.client(i).smoke.c.count_documents({}) for i in range(args.nodes)]
        first, waited = counts, 0
        while counts != want and waited < 60:
            time.sleep(1)
            waited += 1
            counts = [cluster.client(i).smoke.c.count_documents({}) for i in range(args.nodes)]
        assert counts == want, (first, counts)
        print(f"every node reads {counts[0]} documents (first counts {first}, settled after "
              f"{waited} s)", flush=True)
        cluster.kill(1)
        started = time.monotonic()
        cluster.restart(1)
        print(f"node 1 restarted in {time.monotonic() - started:.1f} s", flush=True)
        counts = [cluster.client(i).smoke.c.count_documents({}) for i in range(args.nodes)]
        assert counts == [1000 * args.nodes] * args.nodes, counts
        print(f"after node 1's restart every node reads {counts[0]} documents", flush=True)
    print("PASS smoke", flush=True)


if __name__ == "__main__":
    main()
