#!/usr/bin/env python3
"""CREATE and DROP TABLE recovered after a crash (docs/cdc-change-stream.md §4.4(b); plan B3-B5).

Each scenario starts a fresh server, arms a data substrate fault point that kills the node in the
middle of a DDL, restarts it on the same data, and checks that recovery finished the DDL the way
its log decided:

  (i)   create, killed after its prepare log: recovery creates the kv tables, takes the lock,
        re-derives the commit-stage timestamp and commits;
  (ii)  create, killed after its commit log and before its catalog row: recovery writes the row at
        the timestamp the log state recorded and commits;
  (iii) drop, killed after its commit log: recovery deletes the kv table, and the name is free;
  (iv)  an add index whose build meets a key past the size limit rolls back; the table survives a
        restart at the version the rollback committed;
  (v)   the same rollback, killed after its commit log: recovery drops the index;
  (vi)  a create whose kv tables keep failing gives up, killed after the commit log (timestamp 0)
        that fails it: recovery rejects the dirty schema and releases the name;
  (vii) an ALTER (collMod), killed after its commit log: recovery finishes it under the write lock
        replay restores on the coordinator, and the new metadata survives a restart.

After recovery the collection is used, then the node restarts once more and it is used again: the
second restart loads the catalog from the store, so memory and store must agree on the table's
version, or the node rejects the writes. Needs a server built with the fault points
(WITH_FAULT_INJECT); skipped otherwise.

The server backend comes from server_smoke_config (the CI backends), or from --substrate-config
for any other local build. Requires pymongo.
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, OperationFailure, PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from server_smoke_config import substrate_config  # noqa: E402

COMMAND_NOT_SUPPORTED = 115
KEY_TOO_LONG = 17280
RECOVERY_TIMEOUT_S = 180


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Node:
    """One server on its own data directory, which survives restarts until close()."""

    def __init__(self, args, label):
        self.args = args
        self.root = Path(tempfile.mkdtemp(prefix=f"eloqdoc-ddl-recovery-{label}-"))
        self.port = args.port or free_port()
        self.process = None
        self.client = None
        (self.root / "db").mkdir()
        config = self.root / "data_substrate.cnf"
        if args.substrate_config:
            template = args.substrate_config.read_text(encoding="utf-8")
            config.write_text(template.replace("{root}", str(self.root)), encoding="utf-8")
        else:
            config.write_text(substrate_config(self.root, free_port(), free_port(),
                                               args.data_store, args.log_state, os.environ),
                              encoding="utf-8")
        (self.root / "eloqdoc.yaml").write_text(json.dumps({
            "storage": {"dbPath": str(self.root / "db")},
            "net": {"port": self.port, "bindIp": "127.0.0.1",
                    "serviceExecutor": "adaptive", "adaptiveThreadNum": 1},
        }), encoding="utf-8")

    def start(self):
        log = (self.root / "server.log").open("ab")
        self.process = subprocess.Popen(
            [str(self.args.server.resolve()),
             "--data_substrate_config=" + str(self.root / "data_substrate.cnf"),
             "--config=" + str(self.root / "eloqdoc.yaml"), "--log_dir=" + str(self.root),
             "--setParameter=enableTestCommands=1",
             "--setParameter=diagnosticDataCollectionEnabled=false"],
            cwd=self.root, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.client = MongoClient("127.0.0.1", self.port, directConnection=True,
                                  serverSelectionTimeoutMS=1000)
        deadline = time.monotonic() + RECOVERY_TIMEOUT_S
        while time.monotonic() < deadline:
            assert self.process.poll() is None, "server exited during start-up"
            try:
                if self.client.admin.command("ping")["ok"] == 1:
                    return
            except PyMongoError:
                time.sleep(0.2)
        raise AssertionError("server did not come up")

    def stop(self):
        """A clean shutdown, as an operator's restart does."""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()

    def wait_for_crash(self):
        """Waits for the node the armed fault point killed."""
        try:
            self.process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            raise AssertionError("the fault point did not kill the node")

    def close(self, keep_logs):
        self.stop()
        if keep_logs and self.args.diagnostics_dir:
            target = self.args.diagnostics_dir / self.root.name
            target.mkdir(parents=True, exist_ok=True)
            # server.log, the data substrate's logs (--log_dir) and the configs.
            for path in self.root.iterdir():
                if path.is_file():
                    shutil.copy(path, target / path.name)
        shutil.rmtree(self.root, ignore_errors=True)


def fault_points_compiled_in(node):
    try:
        node.client.admin.command({"eloqFaultInject": "upsert_table_crash_after_commit_log",
                                   "query": True})
        return True
    except OperationFailure as exc:
        if exc.code == COMMAND_NOT_SUPPORTED:
            return False
        raise


def crash_during(node, fault, ddl):
    """Arms a fault point that kills the node, runs the DDL that reaches it, and restarts."""
    node.client.admin.command({"eloqFaultInject": fault, "params": "action=PANIC"})
    try:
        ddl()
        raise AssertionError(f"{fault} did not kill the node: the DDL returned")
    except AutoReconnect:
        pass
    node.wait_for_crash()
    node.start()


def wait_until(predicate, what):
    deadline = time.monotonic() + RECOVERY_TIMEOUT_S
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"recovery never {what}")


def check_usable(coll, label, docs=20):
    """The collection takes writes and an index, and reads back what it holds through it."""
    start = coll.count_documents({})
    coll.insert_many([{"_id": f"{label}-{i}", "k": i % 4} for i in range(docs)])
    coll.create_index("k")
    assert coll.count_documents({}) == start + docs
    by_index = len(list(coll.find({"k": 2}).hint("k_1")))
    assert by_index == coll.count_documents({"k": 2}) > 0, by_index


def use_across_restart(node, coll_name, preloaded=0):
    """Uses the collection, restarts the node, which loads it from the store, and uses it again."""
    check_usable(node.client.ddl_recovery[coll_name], "before-restart")
    node.stop()
    node.start()
    coll = node.client.ddl_recovery[coll_name]
    assert coll.count_documents({}) == preloaded + 20, coll.count_documents({})
    check_usable(coll, "after-restart")


def scenario_create_after_prepare(node):
    """(i) A create killed after its prepare log is rolled forward from it."""
    db = node.client.ddl_recovery
    db.create_collection("anchor")  # the database exists before the fault is armed
    crash_during(node, "upsert_table_crash_after_prepare_log",
                 lambda: db.create_collection("after_prepare"))
    wait_until(lambda: "after_prepare" in node.client.ddl_recovery.list_collection_names(),
               "created the collection killed after its prepare log")
    use_across_restart(node, "after_prepare")
    print("PASS (i) a create killed after its prepare log was rolled forward", flush=True)


def scenario_create_after_commit(node):
    """(ii) A create killed after its commit log is rolled forward from it."""
    db = node.client.ddl_recovery
    db.create_collection("anchor")
    crash_during(node, "upsert_table_crash_after_commit_log",
                 lambda: db.create_collection("after_commit"))
    wait_until(lambda: "after_commit" in node.client.ddl_recovery.list_collection_names(),
               "created the collection killed after its commit log")
    use_across_restart(node, "after_commit")
    print("PASS (ii) a create killed after its commit log was rolled forward", flush=True)


def scenario_drop_after_commit(node):
    """(iii) A drop killed after its commit log is rolled forward from it."""
    db = node.client.ddl_recovery
    db.dropped.insert_many([{"_id": i} for i in range(100)])
    crash_during(node, "upsert_table_crash_after_commit_log", lambda: db.dropped.drop())
    wait_until(lambda: "dropped" not in node.client.ddl_recovery.list_collection_names(),
               "dropped the collection killed after its commit log")
    # The name is free, and a collection created under it starts empty.
    node.client.ddl_recovery.create_collection("dropped")
    assert node.client.ddl_recovery.dropped.count_documents({}) == 0
    use_across_restart(node, "dropped")
    print("PASS (iii) a drop killed after its commit log was rolled forward", flush=True)


def load_with_long_key(coll):
    """Documents whose "big" key includes one past the 1024-byte index key limit."""
    coll.insert_many([{"_id": i, "big": f"b{i}"} for i in range(50)])
    coll.insert_one({"_id": "long", "big": "x" * 1100})


def failed_index_build(coll):
    try:
        coll.create_index("big")
    except OperationFailure as exc:
        assert exc.code == KEY_TOO_LONG, exc.details
        return
    raise AssertionError("an index over a key past the limit was built")


def check_rolled_back(node, coll_name):
    """The index is absent and the collection keeps working across a restart."""
    coll = node.client.ddl_recovery[coll_name]
    assert "big_1" not in coll.index_information(), coll.index_information()
    use_across_restart(node, coll_name, preloaded=51)


def scenario_add_index_rolled_back(node):
    """(iv) A rolled-back add index leaves the table at a version that survives a restart."""
    coll = node.client.ddl_recovery.rolled_back
    load_with_long_key(coll)
    failed_index_build(coll)
    check_rolled_back(node, "rolled_back")
    print("PASS (iv) a rolled-back add index left a table that survives a restart", flush=True)


def scenario_add_index_rollback_after_commit(node):
    """(v) A rolled-back add index killed after its commit log is rolled forward from it."""
    coll = node.client.ddl_recovery.rollback_killed
    load_with_long_key(coll)
    crash_during(node, "alter_table_index_crash_after_commit_log",
                 lambda: failed_index_build(coll))
    wait_until(lambda: "big_1" not in
               node.client.ddl_recovery.rollback_killed.index_information(),
               "finished the rolled-back add index killed after its commit log")
    check_rolled_back(node, "rollback_killed")
    print("PASS (v) a rolled-back add index killed after its commit log was rolled forward",
          flush=True)


def create_when_free(db, name):
    """Creates the collection once recovery releases the name; True when it exists."""
    try:
        db.create_collection(name)
    except OperationFailure:
        pass
    return name in db.list_collection_names()


def scenario_abandoned_create_after_commit(node):
    """(vi) A create that gave up, killed after its failing commit log, stays failed."""
    db = node.client.ddl_recovery
    db.create_collection("anchor")
    # The kv tables fail once, before the commit log: the create gives up.
    node.client.admin.command({"eloqFaultInject": "upsert_table_create_kv_table_fail"})
    crash_during(node, "upsert_table_crash_after_commit_log",
                 lambda: db.create_collection("abandoned"))
    wait_until(lambda: create_when_free(node.client.ddl_recovery, "abandoned"),
               "released the name of the create that gave up")
    use_across_restart(node, "abandoned")
    print("PASS (vi) a create that gave up, killed after its commit log, stayed failed",
          flush=True)


def validator_of(db, name):
    """The validator in a collection's options, or None."""
    for info in db.list_collections(filter={"name": name}):
        return info.get("options", {}).get("validator")
    return None


def scenario_alter_after_commit(node):
    """(vii) An ALTER killed after its commit log is rolled forward from it."""
    db = node.client.ddl_recovery
    db.altered.insert_many([{"_id": i, "k": i % 4} for i in range(10)])
    validator = {"k": {"$exists": True}}
    crash_during(node, "upsert_table_crash_after_commit_log",
                 lambda: db.command({"collMod": "altered", "validator": validator}))
    wait_until(lambda: validator_of(node.client.ddl_recovery, "altered") == validator,
               "applied the ALTER killed after its commit log")
    use_across_restart(node, "altered", preloaded=10)
    assert validator_of(node.client.ddl_recovery, "altered") == validator
    print("PASS (vii) an ALTER killed after its commit log was rolled forward", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--data-store", default="ELOQDSS_ROCKSDB")
    parser.add_argument("--log-state", default="ROCKSDB")
    parser.add_argument("--substrate-config", type=Path,
                        help="A data substrate config to use instead of generating one; "
                             "'{root}' in it becomes each server's own directory")
    parser.add_argument("--port", type=int, help="Server port; default a free one")
    parser.add_argument("--diagnostics-dir", type=Path,
                        help="Copy a failed scenario's server log here")
    args = parser.parse_args()
    # Keep the server and its helpers within the user's CPU limit.
    if hasattr(os, "sched_getaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])

    for label, scenario in (("create-prepare", scenario_create_after_prepare),
                            ("create-commit", scenario_create_after_commit),
                            ("drop-commit", scenario_drop_after_commit),
                            ("index-rollback", scenario_add_index_rolled_back),
                            ("index-rollback-commit", scenario_add_index_rollback_after_commit),
                            ("create-abandoned-commit", scenario_abandoned_create_after_commit),
                            ("alter-commit", scenario_alter_after_commit)):
        node = Node(args, label)
        ok = False
        try:
            node.start()
            if not fault_points_compiled_in(node):
                print("SKIP the server was built without WITH_FAULT_INJECT", flush=True)
                ok = True
                return
            scenario(node)
            ok = True
        finally:
            node.close(keep_logs=not ok)


if __name__ == "__main__":
    main()
