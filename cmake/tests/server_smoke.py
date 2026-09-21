#!/usr/bin/env python3
"""Exercise the real CMake server over the wire (requires pymongo>=4.6,<4.11).

Uses a private Data Substrate fixture with two worker cores. Supports local RocksDB
(WAL disabled) and EloqStore/S3 with RocksDB/S3 logging (WAL enabled). The S3 fixture
requires S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY and a running disposable S3
service. Logs and local data are retained in the printed temporary directory.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import socket
import signal
import subprocess
import tempfile
import threading
import time

from pymongo import MongoClient
from bson.int64 import Int64
from pymongo.errors import AutoReconnect, OperationFailure, PyMongoError
from pymongo.read_concern import ReadConcern

from server_smoke_config import substrate_config


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually(predicate, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise AssertionError("condition did not become true within %ss" % timeout)


def command_fails(database, command, *, code=None, message):
    try:
        database.command(command)
    except OperationFailure as exc:
        if code is not None:
            assert exc.code == code, exc
        assert message in str(exc), exc
    else:
        raise AssertionError("command unexpectedly succeeded: " + repr(command))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--data-store", default="ELOQDSS_ROCKSDB")
    parser.add_argument("--log-state", default="ROCKSDB")
    args = parser.parse_args()
    server = args.server.resolve()
    # Keep every fixture child, including auxiliary services, within the user's CPU limit.
    if hasattr(os, "sched_getaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])
    root = Path(tempfile.mkdtemp(prefix="eloqdoc-server-smoke-"))
    print("Fixture:", root, flush=True)
    (root / "db").mkdir()
    port, tx_port = free_port(), free_port()
    config = root / "data_substrate.cnf"
    try:
        config_text = substrate_config(root, tx_port, free_port(), args.data_store,
                                       args.log_state, os.environ)
    except ValueError as exc:
        parser.error(str(exc))
    config.write_text(config_text, encoding="utf-8")
    server_config = root / "eloqdoc.yaml"
    # JSON is valid YAML and avoids adding another test-client dependency.
    server_config.write_text(json.dumps({
        "storage": {"dbPath": str(root / "db")},
        "net": {"port": port, "bindIp": "127.0.0.1",
                "serviceExecutor": "adaptive", "adaptiveThreadNum": 1},
    }), encoding="utf-8")
    common = [str(server), "--data_substrate_config=" + str(config),
              "--config=" + str(server_config), "--log_dir=" + str(root)]
    for option in ("--replSet=test", "--shardsvr", "--configsvr", "--storageEngine=wiredTiger"):
        result = subprocess.run(common + [option], cwd=root, capture_output=True, timeout=30)
        (root / (option.split("=")[0][2:] + ".log")).write_bytes(result.stdout + result.stderr)
        assert result.returncode > 0, option + " was accepted or crashed instead of being rejected"
        expected = (b"unknown storage engine: wiredTiger" if "storageEngine" in option
                    else b"supports standalone mode only")
        assert expected in result.stdout + result.stderr, "unexpected failure for " + option
    print("PASS unsupported topology/storage options rejected", flush=True)

    with (root / "server.log").open("wb") as log:
        process = subprocess.Popen(common + ["--auth", "--setParameter=ttlMonitorSleepSecs=1",
                                   # Exercise the two test-only maintenance-mode commands too.
                                   "--setParameter=enableTestCommands=1",
                                   "--setParameter=diagnosticDataCollectionEnabled=false"],
                                   cwd=root, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        anonymous = MongoClient("127.0.0.1", port, directConnection=True,
                                serverSelectionTimeoutMS=1000, socketTimeoutMS=15000)
        authenticated = None
        try:
            last_connection_error = []
            def ready():
                assert process.poll() is None, "server exited; see " + str(root / "server.log")
                try:
                    return anonymous.admin.command("ping")["ok"] == 1
                except PyMongoError as exc:
                    last_connection_error[:] = [str(exc)]
                    return False
            try:
                eventually(ready, 90)
            except AssertionError as exc:
                raise AssertionError(f"{exc}; last client error: {last_connection_error}") from exc
            hello = anonymous.admin.command("ismaster")
            assert hello["ismaster"] and "setName" not in hello
            anonymous.admin.command("createUser", "smoke_root", pwd="temporary-smoke-password",
                                    roles=["root"])
            authenticated = MongoClient("127.0.0.1", port, directConnection=True,
                                        username="smoke_root", password="temporary-smoke-password",
                                        authSource="admin", serverSelectionTimeoutMS=5000,
                                        socketTimeoutMS=15000)
            db = authenticated.cmake_smoke
            try:
                anonymous.cmake_smoke.records.insert_one({"unauthorized": True})
                raise AssertionError("unauthenticated write was permitted with --auth")
            except OperationFailure as exc:
                assert exc.code == 13, exc
            print("PASS authentication and authorization", flush=True)
            commands = authenticated.admin.command("listCommands")["commands"]
            for command in ("isMaster", "find", "insert", "aggregate", "mapReduce", "createUser",
                            "createIndexes", "shutdown", "fsync", "compact", "touch",
                            "repairDatabase", "invalidateViewCatalog", "restartCatalog"):
                assert command in commands, "missing standalone command: " + command
            for command in ("replSetInitiate", "replSetReconfig", "_configsvrAddShard",
                            "mapreduce.shardedfinish", "shardConnPoolStats"):
                assert command not in commands, "distributed command retained: " + command
            print("PASS standalone command registration", flush=True)
            db.records.insert_many([{"k": i, "v": i % 2} for i in range(10)])
            db.records.create_index("k", unique=True, background=True)
            assert db.records.find_one({"k": 3})["v"] == 1
            db.records.update_one({"k": 3}, {"$set": {"v": 7}})
            assert db.records.find_one({"k": 3})["v"] == 7
            rows = list(db.records.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}]))
            assert rows[0]["n"] == 10
            db.records.delete_one({"k": 9})
            assert db.records.count_documents({}) == 9
            print("PASS CRUD, background index, local aggregation", flush=True)
            # Collation metadata must remain compatible with the historical SCons build.
            collation = {"locale": "en_US", "strength": 2, "version": "57.1"}
            db.collated.create_index("s", collation=collation)
            db.collated.insert_one({"s": "Apple"})
            assert db.collated.find_one({"s": "APPLE"}, collation=collation)["s"] == "Apple"
            index = next(item for item in db.collated.list_indexes() if item["name"] == "s_1")
            assert index["collation"]["version"] == "57.1", index
            print("PASS SCons-compatible ICU 57.1 collation metadata and lookup", flush=True)
            # These must reach their handlers, which report the existing engine restrictions,
            # instead of throwing NoReplicationEnabled from MaintenanceModeSetter.
            command_fails(db, {"compact": "records"}, code=115, message="cannot compact")
            command_fails(db, {"touch": "records", "data": True}, code=115,
                          message="does not support touch")
            command_fails(db, {"repairDatabase": 1},
                          message="Eloq storage engine does not require manual database repair")
            db.command("create", "records_view", viewOn="records", pipeline=[])
            db.command("invalidateViewCatalog", 1)
            assert db.records_view.count_documents({}) == 9
            print("PASS maintenance-mode handlers and view catalog reload", flush=True)
            # getMore with a term requires internal authorization even on a standalone server.
            authenticated.admin.command("createUser", "smoke_internal",
                                        pwd="temporary-internal-password", roles=["__system"])
            with MongoClient("127.0.0.1", port, directConnection=True,
                             username="smoke_internal", password="temporary-internal-password",
                             authSource="admin", serverSelectionTimeoutMS=5000,
                             socketTimeoutMS=15000) as internal:
                cursor = internal.cmake_smoke.command("find", "records", batchSize=1)["cursor"]
                assert cursor["id"] != 0
                try:
                    command_fails(internal.cmake_smoke,
                                  {"getMore": cursor["id"], "collection": "records", "term": Int64(1)},
                                  code=2, message="cannot supply 'term' without active replication")
                finally:
                    internal.cmake_smoke.command("killCursors", "records", cursors=[cursor["id"]])
            authenticated.admin.command("dropUser", "smoke_internal")
            assert authenticated.admin.command("getParameter", 1, replIndexPrefetch=1)[
                "replIndexPrefetch"] == "uninitialized"
            command_fails(authenticated.admin, {"setParameter": 1, "replIndexPrefetch": "none"},
                          code=2, message="replication is not enabled")
            print("PASS getMore term and replication parameter status codes", flush=True)
            assert len(list(db.records.find({"$where": "function() { return this.k === 2; }"}))) == 1
            print("PASS server-side JavaScript", flush=True)
            # The existing Eloq command explicitly disallows map-reduce, independently of JS.
            try:
                db.command("mapReduce", "records",
                           map="function() { emit(this.v, 1); }",
                           reduce="function(key, values) { return Array.sum(values); }",
                           out={"inline": 1})
                raise AssertionError("existing Eloq map-reduce restriction changed")
            except OperationFailure as exc:
                assert "Eloq storage engine does not support mapreduce" in str(exc), exc
            print("PASS existing Eloq map-reduce restriction preserved", flush=True)
            db.expiring.create_index("expires", expireAfterSeconds=0)
            db.expiring.insert_one({"expires": datetime.datetime.now(datetime.timezone.utc)
                                   - datetime.timedelta(minutes=5)})
            eventually(lambda: db.expiring.count_documents({}) == 0, 30)
            print("PASS TTL deletion by background worker", flush=True)
            with authenticated.start_session() as session:
                assert db.command("ping", session=session)["ok"] == 1
                with session.start_transaction(read_concern=ReadConcern("local")):
                    db.records.update_one({"k": 0}, {"$set": {"v": 20}}, session=session)
                    db.records.update_one({"k": 1}, {"$set": {"v": 21}}, session=session)
                assert db.records.find_one({"k": 0})["v"] == 20
                assert db.records.find_one({"k": 1})["v"] == 21
                session.start_transaction(read_concern=ReadConcern("local"))
                db.records.update_one({"k": 0}, {"$set": {"v": -1}}, session=session)
                session.abort_transaction()
                assert db.records.find_one({"k": 0})["v"] == 20
            # Check the normal background refresh, without invoking refreshLogicalSessionCacheNow.
            eventually(lambda: "lsidTTLIndex" in
                       authenticated.config.system.sessions.index_information(), 30)
            print("PASS logical sessions, transaction commit/abort, and maintenance", flush=True)
            authenticated.admin.command("fsync")
            authenticated.admin.command({"fsync": 1, "async": True})
            # Exercise the lock thread's flush and backup path as well. Eloq's KV engine does
            # not implement backup mode, so the normal storage-engine error must survive.
            command_fails(authenticated.admin, {"fsync": 1, "lock": True}, code=115,
                          message="doesn't support backup mode")
            status = authenticated.admin.command("serverStatus")
            assert status["storageEngine"]["name"] == "eloq"
            assert "setName" not in status.get("repl", {})
            print("PASS fsync and server diagnostics", flush=True)
            for _ in range(2):
                # Eloq's no-op global locker cannot protect a live catalog reload across workers.
                # Reject safely before invalidating any catalog, then verify the server still works.
                command_fails(authenticated.admin, {"restartCatalog": 1}, code=115,
                              message="does not support online catalog restart")
                assert db.records.count_documents({}) == 9
                assert db.records_view.count_documents({}) == 9
                assert db.records.find_one({"k": 0})["v"] == 20
            print("PASS unsafe online catalog restart rejected without affecting data", flush=True)
            # The network integration suites can leave a long-running command behind. Shutdown
            # must interrupt it, not wait for its normal completion or require SIGTERM.
            sleep_finished = threading.Event()
            sleep_errors = []
            def sleeping_command():
                try:
                    authenticated.admin.command({"sleep": 1, "secs": 600, "lock": "none",
                                                  "comment": "shutdown-smoke"})
                    sleep_errors.append("sleep completed before shutdown")
                except AutoReconnect:
                    pass
                except OperationFailure as exc:
                    if exc.code not in (91, 11600):  # ShutdownInProgress, InterruptedAtShutdown
                        sleep_errors.append(str(exc))
                except Exception as exc:
                    sleep_errors.append(str(exc))
                finally:
                    sleep_finished.set()
            sleeper = threading.Thread(target=sleeping_command, daemon=True)
            sleeper.start()
            def sleep_is_active():
                assert not sleep_finished.is_set(), sleep_errors
                return any(op.get("command", {}).get("comment") == "shutdown-smoke"
                           for op in authenticated.admin.command("currentOp")["inprog"])
            eventually(sleep_is_active, 10)
            try:
                authenticated.admin.command("shutdown", force=True)
            except AutoReconnect:
                pass
            assert process.wait(timeout=30) == 0
            assert sleep_finished.wait(5), "sleep command did not finish during shutdown"
            assert not sleep_errors, sleep_errors
            print("PASS clean shutdown with an active command", flush=True)
        finally:
            anonymous.close()
            if authenticated:
                authenticated.close()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            # The fixture owns this new session. Also stop auxiliary children after a crash.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass


if __name__ == "__main__":
    main()
