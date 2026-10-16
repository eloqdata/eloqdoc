#!/usr/bin/env python3
"""A schema op and a range split of the same table recovered together, on a cluster of three node
groups (data substrate docs/range-split-schema-op-recovery.md).

Each scenario starts a fresh cluster, loads a collection past the range size so that a checkpoint
splits its range, and holds the split in flight with a fault point
(split_flush_range_install_dirty_continue: after the split's prepare log, holding the table's
catalog read lock on the node group that runs it, X). An ALTER (collMod) from another node, Y,
then waits for X's write lock behind the split. Z is the third node group. A node killed with
kill -9 and restarted recovers its node group from the log.

  (i)   X, a participant with its own split, recovers: replay finishes (the split takes its catalog
        read lock again), the coordinator's write lock request, lost with X's old process, fails
        its liveness check and is sent again, and the ALTER commits once the split finishes;
  (ii)  Z, a participant with no split, recovers while the coordinator holds its write lock and
        waits on X: Z restores the write lock, so a catalog read through Z waits for the ALTER and
        then reads its version (before the fix Z restored an intent and the read went through);
  (iii) Y, the coordinator, recovers while its ALTER waits on X: the recovered ALTER takes its write
        locks again and commits once the split finishes;
  (iv)  X coordinates the ALTER itself and recovers with its own split in flight: replay finishes,
        and the recovered ALTER commits after the recovered split.

An ADD INDEX (createIndex) writes a prepare data log after each batch of its build, and after the
last one (last key +inf) takes its commit-stage write locks:
  (v)   no split: an index build from Y held after its last prepare data log
        (alter_table_index_hold_before_commit_lock); Z, a participant, recovers and restores the
        write lock, so it serves only once the released index commits (before the fix Z restored
        an intent and served at once);
  (vi)  an index build from Y held after its last prepare data log (it then holds only intents,
        which a split's catalog read lock admits; before its prepare log it takes the write lock,
        which a split excludes), then a load past the range size and X's split held; X, a
        participant with its own split in flight, recovers and restores an intent, so replay
        finishes (a write lock would stall it), and the released index commits.

Controls, without a schema op, for the split itself. Writes that commit after the split installs
its dirty range are double written to the new range's owner, whose part of each transaction's log
record is all its recovery has of them until it checkpoints them:
  (c0)  a split, nothing else; (c1) a split held and released;
  (c2)  a split held while the new range's owner-to-be restarts;
  (c3)  a split, then the new range's owner restarts at once; (c4) the same with a clean restart
        of the whole cluster; (c5) the same after 60 s of checkpoints;
  (c6)  1000 documents of the new range written during a held split, 250 per transaction, then the
        new range's owner restarts at once: all survive; (c7) the same, one per transaction;
        (c8) the same, 10 per transaction.
Every document must survive each of them.

Usage: ddl_split_recovery.py --server <eloqdoc built with WITH_FAULT_INJECT and a data store
service, EloqStore or RocksDB> --root <dir> [--lib-dir <dir>] [--scenario i] ...
"""

import argparse
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cluster import Cluster, read_cloud_settings  # noqa: E402
from pymongo.errors import OperationFailure, PyMongoError  # noqa: E402

DB = "split_ddl"
COLL = "orders"
NS = f"{DB}.{COLL}"
SPLIT_HOLD = "split_flush_range_install_dirty_continue"
LOAD_MB = 320          # past the 256 MB range size, so the next checkpoint splits the range
PRE_SPLIT_MB = 160     # below it: no split yet
DOC_BYTES = 4096
SPLIT_TIMEOUT_S = 300
DDL_TIMEOUT_S = 300
VALIDATOR = {"k": {"$exists": True}}
INDEX_NAME = "k_1"
# Holds an add index once its build is done: after its last prepare data log (last key +inf),
# before its commit-stage write locks; the coordinator sets INDEX_HELD while it waits (at most
# 60 s, UpsertTableIndexOp::HoldBeforeCommitLock).
INDEX_HOLD = "alter_table_index_hold_before_commit_lock"
INDEX_HELD = "alter_table_index_held_before_commit_lock"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def arm(node, fault, params=""):
    node.client().admin.command({"eloqFaultInject": fault, "params": params})


def disarm(node, fault):
    node.client().admin.command({"eloqFaultInject": fault, "params": "remove"})


def armed(node, fault):
    return node.client().admin.command({"eloqFaultInject": fault, "query": True})["armed"]


def catalog_version(node):
    return node.client().admin.command({"eloqCatalogVersion": NS})["schemaTs"]


def load(cluster, mb=LOAD_MB, start=0):
    """Fills the collection through node 0 up to mb MB (past the range size by default), going on
    from document start; returns the number of documents then in it."""
    coll = cluster.client(0)[DB][COLL]
    pad = "x" * (DOC_BYTES - 64)
    batch, doc_id = 500, start
    started = time.monotonic()
    while doc_id * DOC_BYTES < mb * 1024 * 1024:
        coll.insert_many([{"_id": doc_id + i, "k": (doc_id + i) % 7, "pad": pad}
                          for i in range(batch)], ordered=False)
        doc_id += batch
        if doc_id % 20000 == 0:
            log(f"loaded {doc_id} documents ({doc_id * DOC_BYTES >> 20} MB)")
    log(f"loaded {doc_id} documents in {time.monotonic() - started:.0f} s")
    return doc_id


def wait_split_held(cluster):
    """Waits until a node logs the start of a split of the collection's range ("Splitting table
    <name> range ..."); returns its index. The fault point then holds the split after its prepare
    log."""
    deadline = time.monotonic() + SPLIT_TIMEOUT_S
    while time.monotonic() < deadline:
        for node in cluster.nodes:
            if any(DB in line for line in node.log_lines("Splitting table")):
                time.sleep(5)  # the prepare log and the dirty range install come right after
                return node.index
        time.sleep(2)
    raise AssertionError("no split of the collection's range started")


def alter(client):
    client[DB].command({"collMod": COLL, "validator": VALIDATOR})


def add_index(client):
    client[DB][COLL].create_index("k", name=INDEX_NAME)


class Ddl:
    """A DDL run from a node in a thread, the ALTER by default: it returns once the DDL commits."""

    def __init__(self, node, run=alter):
        self.error = None
        self.done = threading.Event()
        self.thread = threading.Thread(target=self._run, args=(node.client(), run), daemon=True)
        self.thread.start()

    def _run(self, client, run):
        try:
            run(client)
        except PyMongoError as exc:
            self.error = exc
        finally:
            self.done.set()


def wait_index(cluster, index, timeout_s=DDL_TIMEOUT_S):
    """Waits until node index lists the added index, a read of the committed catalog."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            names = [spec["name"] for spec in cluster.client(index)[DB][COLL].list_indexes()]
            if INDEX_NAME in names:
                return
        except PyMongoError:
            pass
        time.sleep(1)
    raise AssertionError(f"node {index} never listed index {INDEX_NAME}")


def wait_validator(cluster, index, timeout_s=DDL_TIMEOUT_S):
    """Waits until node index reads the ALTER's validator, a read of the committed catalog."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            infos = cluster.client(index)[DB].list_collections(filter={"name": COLL})
            if any(info.get("options", {}).get("validator") == VALIDATOR for info in infos):
                return
        except PyMongoError:
            pass
        time.sleep(1)
    raise AssertionError(f"node {index} never read the ALTER's validator")


def setup(cluster):
    """A started cluster, the collection loaded and its range's split held; returns X."""
    cluster.start()
    cluster.client(0)[DB].create_collection(COLL)
    for node in cluster.nodes:
        arm(node, SPLIT_HOLD)
    docs = load(cluster)
    x = wait_split_held(cluster)
    log(f"the split runs on node {x}, held after its prepare log")
    return x, docs


def check_documents(cluster, docs):
    for node in cluster.nodes:
        count = node.client()[DB][COLL].count_documents({})
        assert count == docs, (node.index, count, docs)


def diagnose_missing(cluster, docs, samples=6):
    """For documents a scan does not return, whether a point read by _id through each node finds
    them: found means the store holds them and the scan's slice bookkeeping misses them; not found
    means they are gone."""
    present = set(cluster.client(0)[DB][COLL].distinct("_id"))
    missing = sorted(set(range(docs)) - present)
    if not missing:
        return "nothing missing"
    picks = missing[:samples // 2] + missing[len(missing) // 2:len(missing) // 2 + samples // 2]
    found = {i: [cluster.client(n)[DB][COLL].find_one({"_id": i}, {"_id": 1}) is not None
                 for n in range(len(cluster.nodes))] for i in picks}
    return (f"{len(missing)} missing, _id {missing[0]}..{missing[-1]}; point reads through each "
            f"node: {found}")


def wait_documents(cluster, docs, timeout_s=120):
    """Waits until every node counts docs documents; reports the counts, and what point reads find
    of the missing ones, if they never match."""
    deadline = time.monotonic() + timeout_s
    while True:
        counts = [node.client()[DB][COLL].count_documents({}) for node in cluster.nodes]
        if counts == [docs] * len(counts):
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"counts {counts}, loaded {docs}; "
                                 f"{diagnose_missing(cluster, docs)}")
        time.sleep(5)


def wait_split_done(cluster, timeout_s=SPLIT_TIMEOUT_S):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for node in cluster.nodes:
            done = [line for line in node.log_lines("Split range on table") if DB in line]
            if done:
                return node.index, done[0]
        time.sleep(2)
    raise AssertionError("the split never finished")


def control_split_only(cluster):
    """(c0) Load and split, nothing else: every document survives the split."""
    cluster.start()
    cluster.client(0)[DB].create_collection(COLL)
    docs = load(cluster)
    node, line = wait_split_done(cluster)
    log(f"split done on node {node}: {line[-120:]}")
    wait_documents(cluster, docs)
    print("PASS (c0) every document survived the split", flush=True)


def control_split_held(cluster):
    """(c1) Load, hold the split, release it: every document survives."""
    x, docs = setup(cluster)
    time.sleep(15)
    disarm(cluster.nodes[x], SPLIT_HOLD)
    wait_split_done(cluster)
    wait_documents(cluster, docs)
    print("PASS (c1) every document survived a split held and released", flush=True)


def control_split_held_node_restart(cluster):
    """(c2) Load, hold the split on X, kill and restart Z (neither X nor Y), release: every
    document survives. No schema op runs."""
    x, docs = setup(cluster)
    z = (x + 2) % 3
    log(f"killing node {z} while node {x}'s split is held")
    cluster.kill(z)
    cluster.restart(z)
    log(f"node {z} restarted; releasing node {x}'s split")
    disarm(cluster.nodes[x], SPLIT_HOLD)
    wait_split_done(cluster)
    wait_documents(cluster, docs)
    print("PASS (c2) every document survived a node restart during a held split", flush=True)


def new_range_owner(cluster):
    """The node group the split's new range went to, from the old owner's log ("Begin upload new
    range slices and records to future owner, ..., new_range_owner:<n>")."""
    for node in cluster.nodes:
        for line in node.log_lines("new_range_owner:"):
            return int(line.rsplit("new_range_owner:", 1)[1].split()[0].strip(","))
    raise AssertionError("no log names the new range's owner")


def control_split_then_owner_restart(cluster):
    """(c3) Load and split, nothing held; once the split has finished and every document reads,
    kill and restart the new range's owner: every document survives."""
    cluster.start()
    cluster.client(0)[DB].create_collection(COLL)
    docs = load(cluster)
    wait_split_done(cluster)
    wait_documents(cluster, docs)
    owner = new_range_owner(cluster)
    log(f"every document reads after the split; killing node {owner}, the new range's owner")
    cluster.kill(owner)
    cluster.restart(owner)
    wait_documents(cluster, docs)
    print("PASS (c3) every document survived a restart of the new range's owner after the split",
          flush=True)


def control_split_then_cluster_restart(cluster, settle_s=0, label="c4"):
    """(c4) Load and split, nothing held; once the split has finished and every document reads,
    stop every node cleanly and start the cluster again: every document survives."""
    cluster.start()
    cluster.client(0)[DB].create_collection(COLL)
    docs = load(cluster)
    wait_split_done(cluster)
    wait_documents(cluster, docs)
    if settle_s:
        log(f"every document reads after the split; waiting {settle_s} s of checkpoints")
        time.sleep(settle_s)
    log("every document reads after the split; restarting the whole cluster")
    cluster.restart_all()
    wait_documents(cluster, docs)
    print(f"PASS ({label}) every document survived a clean restart of the cluster after the "
          f"split", flush=True)


def control_dml_during_split(cluster, extra=1000, per_tx=250, label="c6"):
    """(c6) Load, hold the split after its prepare log, insert documents whose keys fall in the
    new range, release; once the split has finished, kill and restart the new range's owner at
    once: the documents written during the split survive (they reach the new owner's part of the
    log). Counts only those documents, apart from the old data of (c3)."""
    x, docs = setup(cluster)
    writer = cluster.client((x + 1) % 3)[DB][COLL]
    pad = "y" * (DOC_BYTES - 64)
    first = 100000
    for start in range(first, first + extra, per_tx):
        batch = [{"_id": i, "k": i % 7, "pad": pad} for i in range(start, start + per_tx)]
        if per_tx == 1:
            writer.insert_one(batch[0])
        else:
            writer.insert_many(batch)
    log(f"inserted {extra} documents in the new range, {per_tx} per transaction, while the split "
        f"was held; releasing")
    disarm(cluster.nodes[x], SPLIT_HOLD)
    wait_split_done(cluster)
    owner = new_range_owner(cluster)
    log(f"the split finished; killing node {owner}, the new range's owner")
    cluster.kill(owner)
    cluster.restart(owner)
    deadline = time.monotonic() + 60
    while True:
        counts = [node.client()[DB][COLL].count_documents({"_id": {"$gte": first}})
                  for node in cluster.nodes]
        if counts == [extra] * 3 or time.monotonic() > deadline:
            break
        time.sleep(2)
    total = [node.client()[DB][COLL].count_documents({}) for node in cluster.nodes]
    assert counts == [extra] * 3, (f"documents written during the split: {counts} of {extra}; "
                                   f"all documents: {total} of {docs + extra}")
    print(f"PASS ({label}) the {extra} documents written during the split survived the owner's "
          f"restart (all documents: {total} of {docs + extra})", flush=True)


def scenario_participant_with_split(cluster):
    """(i)"""
    x, docs = setup(cluster)
    y = (x + 1) % 3
    before = catalog_version(cluster.nodes[y])
    ddl = Ddl(cluster.nodes[y])
    assert not ddl.done.wait(10), f"the ALTER did not wait on the split: {ddl.error}"
    log(f"the ALTER from node {y} waits on node {x}'s split; killing node {x}")
    cluster.kill(x)
    started = time.monotonic()
    cluster.restart(x)
    log(f"node {x} recovered in {time.monotonic() - started:.1f} s")
    assert ddl.done.wait(DDL_TIMEOUT_S), "the ALTER never committed"
    assert ddl.error is None, ddl.error
    log(f"the ALTER committed {time.monotonic() - started:.1f} s after node {x}'s restart")
    retried = cluster.nodes[y].log_lines("failed to acquire write lock")
    assert retried, "the coordinator never retried its write locks"
    for node in cluster.nodes:
        assert catalog_version(node) > before
        wait_validator(cluster, node.index)
    check_documents(cluster, docs)
    print("PASS (i) a participant recovered with its own split in flight; the coordinator sent "
          "its lost write lock request again and the ALTER committed", flush=True)


def scenario_participant_without_split(cluster):
    """(ii)"""
    x, docs = setup(cluster)
    y, z = (x + 1) % 3, (x + 2) % 3
    before = catalog_version(cluster.nodes[z])
    ddl = Ddl(cluster.nodes[y])
    assert not ddl.done.wait(10), f"the ALTER did not wait on the split: {ddl.error}"
    log(f"the ALTER from node {y} holds node {z}'s write lock and waits on node {x}; "
        f"killing node {z}")
    cluster.kill(z)
    cluster.restart(z, wait=False)
    # Z restores the ALTER's write lock, so every read of the collection's catalog through it
    # waits for the ALTER, the read of every catalog its start-up makes (KVCatalog::init)
    # included: Z serves nothing before the ALTER commits. With only an intent restored it would
    # serve at once, at the old version.
    try:
        cluster.nodes[z].wait_serving(timeout_s=30)
        served = True
    except AssertionError:
        served = False
    assert not served, (f"node {z} served while the ALTER held its write lock, at catalog "
                        f"version {catalog_version(cluster.nodes[z])} (before {before})")
    log(f"node {z} waits for the ALTER to start serving; releasing node {x}'s split")
    disarm(cluster.nodes[x], SPLIT_HOLD)
    assert ddl.done.wait(DDL_TIMEOUT_S), "the ALTER never committed"
    assert ddl.error is None, ddl.error
    cluster.nodes[z].wait_serving()
    cluster.nodes[z].wait_recovered()
    after = catalog_version(cluster.nodes[z])
    assert after > before, (before, after)
    wait_validator(cluster, z)
    # Z may be the owner-to-be of the split's new range: its restart during the split loses none
    # of the documents written meanwhile (c2).
    check_documents(cluster, docs)
    print("PASS (ii) a participant without a split recovered with the coordinator's write lock; "
          "it served only once the ALTER committed", flush=True)


def scenario_coordinator(cluster):
    """(iii)"""
    x, docs = setup(cluster)
    y = (x + 1) % 3
    ddl = Ddl(cluster.nodes[y])
    assert not ddl.done.wait(10), f"the ALTER did not wait on the split: {ddl.error}"
    log(f"the ALTER from node {y} waits on node {x}'s split; killing node {y}, its coordinator")
    cluster.kill(y)
    cluster.restart(y, wait=False)
    # Y's recovered ALTER takes its write locks again, Y's own included, and waits on X's split
    # as before. Y's start-up reads the collection's catalog: if the recovered ALTER holds Y's
    # write lock by then, Y serves only once the ALTER commits, as reads through Y would wait
    # without the crash; otherwise it serves at once.
    time.sleep(15)
    log(f"releasing node {x}'s split")
    disarm(cluster.nodes[x], SPLIT_HOLD)
    started = time.monotonic()
    cluster.nodes[y].wait_serving()
    log(f"node {y} served {time.monotonic() - started:.1f} s after the split's release")
    cluster.nodes[y].wait_recovered()
    for node in cluster.nodes:
        wait_validator(cluster, node.index)
    check_documents(cluster, docs)
    print("PASS (iii) the coordinator recovered while its ALTER waited on a split; the recovered "
          "ALTER committed after it", flush=True)


def scenario_coordinator_with_split(cluster):
    """(iv)"""
    x, docs = setup(cluster)
    ddl = Ddl(cluster.nodes[x])
    assert not ddl.done.wait(10), f"the ALTER did not wait on the split: {ddl.error}"
    log(f"the ALTER from node {x} waits on its own split; killing node {x}")
    cluster.kill(x)
    started = time.monotonic()
    cluster.restart(x)
    log(f"node {x} recovered in {time.monotonic() - started:.1f} s")
    for node in cluster.nodes:
        wait_validator(cluster, node.index)
    check_documents(cluster, docs)
    print("PASS (iv) the coordinator recovered with its own split in flight; the recovered split "
          "and ALTER both finished", flush=True)


def scenario_add_index_participant(cluster):
    """(v)"""
    cluster.start()
    cluster.client(0)[DB][COLL].insert_many([{"_id": i, "k": i % 7} for i in range(5000)])
    y, z = 1, 2
    before = catalog_version(cluster.nodes[z])
    arm(cluster.nodes[y], INDEX_HOLD)
    build = Ddl(cluster.nodes[y], add_index)
    deadline = time.monotonic() + DDL_TIMEOUT_S
    while not armed(cluster.nodes[y], INDEX_HELD):
        assert time.monotonic() < deadline, "the index build never reached its hold"
        assert not build.done.is_set(), f"the index build ended: {build.error}"
        time.sleep(0.5)
    log(f"the index build from node {y} is held after its last prepare data log; "
        f"killing node {z}")
    cluster.kill(z)
    cluster.restart(z, wait=False)
    # Z replays the build's last prepare data log as a participant and restores the write lock,
    # since the coordinator may already hold it: every read of the collection's catalog through
    # Z waits for the index, its start-up's (KVCatalog::init) included, so Z serves nothing
    # before the index commits. With only an intent restored it would serve at once. The check
    # ends well within the hold's 60 s.
    try:
        cluster.nodes[z].wait_serving(timeout_s=30)
        served = True
    except AssertionError:
        served = False
    assert not served, (f"node {z} served while the index build held its write lock, at "
                        f"catalog version {catalog_version(cluster.nodes[z])} (before {before})")
    log(f"node {z} waits for the index to start serving; releasing the build")
    disarm(cluster.nodes[y], INDEX_HOLD)
    assert build.done.wait(DDL_TIMEOUT_S), "the index build never committed"
    assert build.error is None, build.error
    cluster.nodes[z].wait_serving()
    cluster.nodes[z].wait_recovered()
    after = catalog_version(cluster.nodes[z])
    assert after > before, (before, after)
    for node in cluster.nodes:
        wait_index(cluster, node.index)
    print("PASS (v) a participant recovered after the add index's last prepare data log with the "
          "write lock; it served only once the index committed", flush=True)


def scenario_add_index_participant_with_split(cluster):
    """(vi)"""
    # An add index takes the write lock across its prepare log, which a split's catalog read lock
    # excludes, so the build goes first: on a collection below the range size, held after its
    # last prepare data log, where it holds only intents. Then a load past the range size starts
    # a split, held after its prepare log. The collection's range is on node 0's node group in
    # this layout, so the build runs from node 1, a different node group.
    cluster.start()
    cluster.client(0)[DB].create_collection(COLL)
    docs = load(cluster, PRE_SPLIT_MB)
    y = 1
    arm(cluster.nodes[y], INDEX_HOLD, f"hold_s={DDL_TIMEOUT_S}")
    build = Ddl(cluster.nodes[y], add_index)
    deadline = time.monotonic() + DDL_TIMEOUT_S
    while not armed(cluster.nodes[y], INDEX_HELD):
        assert time.monotonic() < deadline, "the index build never reached its hold"
        assert not build.done.is_set(), f"the index build ended: {build.error}"
        time.sleep(0.5)
    log(f"the index build from node {y} is held after its last prepare data log; loading past "
        f"the range size")
    for node in cluster.nodes:
        arm(node, SPLIT_HOLD)
    docs = load(cluster, LOAD_MB, docs)
    x = wait_split_held(cluster)
    assert x != y, (f"the split runs on node {y}, the index build's coordinator: the "
                    f"collection's range moved, so the build must run from another node")
    log(f"the split runs on node {x}, held after its prepare log; killing node {x}")
    cluster.kill(x)
    started = time.monotonic()
    # X, a participant with its own split in flight, restores an intent, not the write lock:
    # replay finishes only once X's recovered split holds its catalog read lock again, which a
    # write lock would hold up until the index commits, and the build, still held, does not.
    cluster.restart(x)
    log(f"node {x} recovered in {time.monotonic() - started:.1f} s; releasing the build")
    disarm(cluster.nodes[y], INDEX_HOLD)
    assert build.done.wait(DDL_TIMEOUT_S), "the index build never committed"
    assert build.error is None, build.error
    log(f"the index committed {time.monotonic() - started:.1f} s after node {x}'s restart")
    for node in cluster.nodes:
        wait_index(cluster, node.index)
    check_documents(cluster, docs)
    print("PASS (vi) a participant recovered after the add index's last prepare data log with its "
          "own split in flight restored an intent; replay finished and the index committed",
          flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--lib-dir")
    parser.add_argument("--scenario", action="append",
                        help="Run only this scenario (c0-c8, i-vi); may be repeated")
    parser.add_argument("--keep", action="store_true", help="Keep every cluster's directory")
    parser.add_argument("--port-offset", type=int, default=0,
                        help="Added to every port, to run clusters side by side")
    parser.add_argument("--first-cpu", type=int, default=0,
                        help="The first of the CPUs the nodes are pinned to")
    parser.add_argument("--cpus-per-node", type=int, default=8)
    parser.add_argument("--cloud", help="EloqStore cloud mode: a file of endpoint, bucket, "
                                        "access_key and secret_key lines")
    args = parser.parse_args()
    cloud = read_cloud_settings(args.cloud) if args.cloud else None
    scenarios = (("c0", control_split_only),
                 ("c1", control_split_held),
                 ("c2", control_split_held_node_restart),
                 ("c3", control_split_then_owner_restart),
                 ("c4", control_split_then_cluster_restart),
                 ("c5", lambda cluster: control_split_then_cluster_restart(cluster, 60, "c5")),
                 ("c6", control_dml_during_split),
                 ("c7", lambda cluster: control_dml_during_split(cluster, 200, 1, "c7")),
                 ("c8", lambda cluster: control_dml_during_split(cluster, 1000, 10, "c8")),
                 ("i", scenario_participant_with_split),
                 ("ii", scenario_participant_without_split),
                 ("iii", scenario_coordinator),
                 ("iv", scenario_coordinator_with_split),
                 ("v", scenario_add_index_participant),
                 ("vi", scenario_add_index_participant_with_split))
    width = args.cpus_per_node
    cpus = [f"{args.first_cpu + width * i}-{args.first_cpu + width * i + width - 1}"
            for i in range(3)]
    failed = []
    for label, scenario in scenarios:
        if args.scenario and label not in args.scenario:
            continue
        log(f"scenario ({label})")
        try:
            with Cluster(args.server, args.root / label, nodes=3, lib_dir=args.lib_dir,
                         cpus=cpus, keep=args.keep, port_offset=args.port_offset, cloud=cloud) as cluster:
                scenario(cluster)
        except AssertionError as exc:
            print(f"FAIL ({label}) {exc}", flush=True)
            failed.append(label)
    if failed:
        print(f"FAILED: {', '.join(failed)}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
