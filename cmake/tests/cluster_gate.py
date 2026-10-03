#!/usr/bin/env python3
"""The cluster gate on a single node (docs/cdc-change-stream.md §6.2; plan D3, D4, D6, D8, D9).

The gate keeps new create, drop, index and collMod operations, and view changes (writes to
system.views), out while change data capture bootstrap copies the catalog. Each scenario starts a fresh server and drives the gate through the
eloqClusterGate admin command:

  (i)    pause and resume: a pause refuses every kind of DDL at once with ClusterGatePaused,
         changing nothing, and leaves DML alone; a second pause fails as held, a resume admits
         DDL again, and resuming an open gate is harmless; the pause's timestamp is above the
         catalog version of a DDL committed before it;
  (ii)   a pause waits for a DDL already admitted, which finishes ungated, and its timestamp is
         above that DDL's catalog version;
  (iii)  a pause survives a clean restart and a kill;
  (iv)   a pause killed after its log entry, before its install, is in force after the restart,
         and a resume can still take the cluster config lock;
  (v)    the same for a resume: the gate is open after the restart;
  (vi)   a pause killed in its install, at the cluster config map's post write, likewise;
  (vii)  after a pause and a resume, a create killed after its commit log is rolled forward:
         the gate's slot replays beside an unfinished schema op without blocking it;
  (viii) two pauses at once: one pauses, the other finds the gate held, and neither waits
         forever on the other (they serialize on the first one's write intent, or one backs off);
  (ix)   a pause whose write intents meet a conflict (forced by a fault point, since one node
         cannot produce one) releases everything, backs off and succeeds on its next attempt;
  (x)    with authorization on, clusterManager may pause and resume, and a user who may read,
         write and administer every database may not.

Needs a server built with the fault points (WITH_FAULT_INJECT); skipped otherwise. Requires
pymongo.
"""

import argparse
import json
import os
import sys
import threading
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import AutoReconnect, OperationFailure

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ddl_recovery import (Node, crash_during, fault_points_compiled_in,  # noqa: E402
                          wait_until)

UNAUTHORIZED = 13
CONFLICTING_OPERATION_IN_PROGRESS = 117
CLUSTER_GATE_PAUSED = 5001
# A refusal comes back at once; a DDL retried as a write conflict would take tens of seconds.
REFUSAL_DEADLINE_S = 5
# The view setup() defines, as system.views holds it.
ANCHOR_VIEW = {"_id": "cluster_gate.anchor_view", "viewOn": "anchor", "pipeline": []}


def gate(node, op):
    """Runs eloqClusterGate and returns the timestamp it replies with."""
    reply = node.client.admin.command({"eloqClusterGate": {"op": op, "mode": "ddl"}})
    return reply["pauseTs" if op == "pause" else "resumeTs"]


def catalog_version(node, ns):
    return node.client.admin.command({"eloqCatalogVersion": ns})["schemaTs"]


def refused(ddl, what):
    """Checks that the DDL fails at once with ClusterGatePaused."""
    start = time.monotonic()
    try:
        ddl()
    except OperationFailure as exc:
        assert exc.code == CLUSTER_GATE_PAUSED, (what, exc)
        took = time.monotonic() - start
        assert took < REFUSAL_DEADLINE_S, f"{what} took {took:.1f} s to be refused"
        return
    raise AssertionError(f"{what} passed the gate")


def check_paused(node, label):
    db = node.client.cluster_gate
    refused(lambda: db.create_collection(f"refused_{label}"), "a create")
    refused(lambda: db.command({"drop": "anchor"}), "a drop")
    refused(lambda: db.anchor.create_index("x"), "an index build")
    refused(lambda: db.anchor.drop_index("anchor_y"), "an index drop")
    refused(lambda: db.command({"collMod": "anchor", "validator": {"x": {"$exists": True}}}),
            "a collMod")
    refused(lambda: db.command({"create": f"refused_view_{label}", "viewOn": "anchor",
                                "pipeline": []}), "a view create")
    refused(lambda: db.command({"collMod": "anchor_view", "viewOn": "anchor",
                                "pipeline": [{"$match": {"x": 1}}]}), "a view collMod")
    refused(lambda: db.command({"drop": "anchor_view"}), "a view drop")
    refused(lambda: db.system.views.insert_one({"_id": f"cluster_gate.direct_{label}",
                                                "viewOn": "anchor", "pipeline": []}),
            "a direct write to system.views")
    # The refusals left the catalog as it was.
    assert "anchor" in db.list_collection_names()
    assert f"refused_{label}" not in db.list_collection_names()
    assert sorted(spec["name"] for spec in db.anchor.list_indexes()) == ["_id_", "anchor_y"]
    assert "validator" not in db.command({"listCollections": 1, "filter": {"name": "anchor"}})[
        "cursor"]["firstBatch"][0]["options"]
    assert list(db.system.views.find()) == [ANCHOR_VIEW], list(db.system.views.find())
    # DML is never gated.
    db.anchor.insert_one({"_id": f"while-paused-{label}"})
    assert db.anchor.find_one({"_id": f"while-paused-{label}"}) is not None


def check_open(node, label):
    db = node.client.cluster_gate
    db.create_collection(f"open_{label}")
    db[f"open_{label}"].drop()
    db.command({"create": f"open_view_{label}", "viewOn": "anchor", "pipeline": []})
    db.command({"drop": f"open_view_{label}"})


def setup(node):
    db = node.client.cluster_gate
    db.create_collection("anchor")
    db.anchor.create_index("y", name="anchor_y")
    db.command({"create": "anchor_view", "viewOn": "anchor", "pipeline": []})
    db.anchor.insert_many([{"_id": i} for i in range(10)])


def scenario_pause_resume(node):
    """(i) Pause and resume, the held gate, and the pause's bound."""
    setup(node)
    db = node.client.cluster_gate
    db.create_collection("before")
    before = catalog_version(node, "cluster_gate.before")
    pause_ts = gate(node, "pause")
    assert pause_ts > before, (pause_ts, before)
    check_paused(node, "i")
    try:
        gate(node, "pause")
        raise AssertionError("a second pause succeeded")
    except OperationFailure as exc:
        assert exc.code == CONFLICTING_OPERATION_IN_PROGRESS, exc
    resume_ts = gate(node, "resume")
    assert resume_ts > pause_ts, (resume_ts, pause_ts)
    check_open(node, "i")
    db.before.drop()
    assert gate(node, "resume") > resume_ts
    check_open(node, "i-again")
    print("PASS (i) pause and resume", flush=True)


def scenario_pause_waits(node):
    """(ii) A pause waits for an admitted DDL, which is not gated."""
    setup(node)
    db = node.client.cluster_gate
    admin = node.client.admin
    admin.command({"eloqFaultInject": "upsert_table_hold_before_commit_lock"})
    created = {}

    def create():
        try:
            db.create_collection("admitted")
            created["ok"] = True
        except Exception as exc:  # reported below
            created["error"] = exc

    def pause():
        try:
            created["pause_ts"] = gate(node, "pause")
        except Exception as exc:
            created["pause_error"] = exc

    ddl = threading.Thread(target=create)
    pauser = threading.Thread(target=pause)
    try:
        ddl.start()
        wait_until(lambda: admin.command({"eloqFaultInject": "upsert_table_held_before_commit_lock",
                                          "query": True})["armed"],
                   "held the create")
        pauser.start()
        pauser.join(timeout=5)
        assert pauser.is_alive(), f"the pause returned while a DDL was in flight: {created}"
    finally:
        admin.command({"eloqFaultInject": "upsert_table_hold_before_commit_lock",
                       "params": "remove"})
    ddl.join(timeout=120)
    pauser.join(timeout=120)
    assert created.get("ok"), created
    assert "pause_ts" in created, created
    version = catalog_version(node, "cluster_gate.admitted")
    assert created["pause_ts"] > version, (created["pause_ts"], version)
    check_paused(node, "ii")
    gate(node, "resume")
    check_open(node, "ii")
    print("PASS (ii) a pause waited for the DDL already admitted", flush=True)


def scenario_restart(node):
    """(iii) A pause survives a clean restart and a kill."""
    setup(node)
    gate(node, "pause")
    node.stop()
    node.start()
    check_paused(node, "iii-clean")
    node.process.kill()
    node.process.wait()
    node.start()
    check_paused(node, "iii-kill")
    gate(node, "resume")
    check_open(node, "iii")
    print("PASS (iii) a pause survived a clean restart and a kill", flush=True)


def scenario_pause_after_log(node):
    """(iv) A pause killed after its log entry is in force after the restart."""
    setup(node)
    crash_during(node, "cluster_gate_after_log", lambda: gate(node, "pause"))
    check_paused(node, "iv")
    gate(node, "resume")
    check_open(node, "iv")
    print("PASS (iv) a pause killed after its log was in force after the restart", flush=True)


def scenario_resume_after_log(node):
    """(v) A resume killed after its log entry leaves the gate open after the restart."""
    setup(node)
    gate(node, "pause")
    crash_during(node, "cluster_gate_after_log", lambda: gate(node, "resume"))
    check_open(node, "v")
    gate(node, "pause")
    check_paused(node, "v")
    gate(node, "resume")
    print("PASS (v) a resume killed after its log was in force after the restart", flush=True)


def scenario_pause_in_install(node):
    """(vi) A pause killed in its install is in force after the restart."""
    setup(node)
    crash_during(node, "cluster_config_PostWriteAll_local", lambda: gate(node, "pause"))
    check_paused(node, "vi")
    gate(node, "resume")
    check_open(node, "vi")
    print("PASS (vi) a pause killed in its install was in force after the restart", flush=True)


def scenario_ddl_recovered_after_gate(node):
    """(vii) A create killed after its commit log recovers beside the gate's slot."""
    setup(node)
    gate(node, "pause")
    gate(node, "resume")
    db = node.client.cluster_gate
    crash_during(node, "upsert_table_crash_after_commit_log",
                 lambda: db.create_collection("recovered"))
    wait_until(lambda: "recovered" in node.client.cluster_gate.list_collection_names(),
               "created the collection killed after its commit log")
    check_open(node, "vii")
    gate(node, "pause")
    check_paused(node, "vii")
    gate(node, "resume")
    print("PASS (vii) a create recovered beside the gate's slot", flush=True)


def scenario_concurrent_pauses(node):
    """(viii) Two pauses at once on one node: one pauses, the other finds the gate held."""
    setup(node)
    for round_ in range(5):
        results = []

        def pause():
            try:
                results.append(("ok", gate(node, "pause")))
            except OperationFailure as exc:
                results.append(("error", exc.code))

        threads = [threading.Thread(target=pause) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        assert not any(t.is_alive() for t in threads), \
            f"round {round_}: concurrent pauses never finished: {results}"
        assert sorted(r[0] for r in results) == ["error", "ok"], results
        assert ("error", CONFLICTING_OPERATION_IN_PROGRESS) in results, results
        check_paused(node, f"viii-{round_}")
        gate(node, "resume")
    check_open(node, "viii")
    print("PASS (viii) concurrent pauses serialized: one paused, one found the gate held",
          flush=True)


def scenario_intent_conflict_retried(node):
    """(ix) A pause whose write intents meet a conflict releases, backs off and retries."""
    setup(node)
    admin = node.client.admin
    admin.command({"eloqFaultInject": "cluster_gate_intent_conflict_once"})
    start = time.monotonic()
    gate(node, "pause")
    elapsed = time.monotonic() - start
    armed = admin.command({"eloqFaultInject": "cluster_gate_intent_conflict_once",
                           "query": True})["armed"]
    assert not armed, "the forced conflict never fired"
    # The back-off is 1 to 3 seconds before the second attempt.
    assert elapsed >= 1, elapsed
    check_paused(node, "ix")
    gate(node, "resume")
    check_open(node, "ix")
    print(f"PASS (ix) a pause retried after a write intent conflict ({elapsed:.1f} s)",
          flush=True)


def scenario_privileges(node):
    """(x) The eloqClusterGate action: clusterManager holds it, database roles do not."""
    node.stop()
    config = node.root / "eloqdoc.yaml"
    settings = json.loads(config.read_text(encoding="utf-8"))
    settings["security"] = {"authorization": "enabled"}
    config.write_text(json.dumps(settings), encoding="utf-8")
    node.start()

    def login(user):
        return MongoClient("127.0.0.1", node.port, directConnection=True, username=user,
                           password="pw", authSource="admin", serverSelectionTimeoutMS=1000)

    # The localhost exception admits the first user.
    node.client.admin.command("createUser", "root", pwd="pw", roles=["root"])
    root = login("root")
    root.admin.command("createUser", "ops", pwd="pw", roles=["clusterManager"])
    root.admin.command("createUser", "app", pwd="pw",
                       roles=["readWriteAnyDatabase", "dbAdminAnyDatabase"])
    ops, app = login("ops"), login("app")
    app.cluster_gate.create_collection("anchor")
    try:
        app.admin.command({"eloqClusterGate": {"op": "pause", "mode": "ddl"}})
        raise AssertionError("a user without the eloqClusterGate action paused the gate")
    except OperationFailure as exc:
        assert exc.code == UNAUTHORIZED, exc
    pause_ts = ops.admin.command({"eloqClusterGate": {"op": "pause"}})["pauseTs"]
    refused(lambda: app.cluster_gate.create_collection("refused_x"), "a create")
    try:
        app.admin.command({"eloqClusterGate": {"op": "resume"}})
        raise AssertionError("a user without the eloqClusterGate action resumed the gate")
    except OperationFailure as exc:
        assert exc.code == UNAUTHORIZED, exc
    assert root.admin.command({"eloqClusterGate": {"op": "resume"}})["resumeTs"] > pause_ts
    app.cluster_gate.create_collection("open_x")
    for client in (root, ops, app):
        client.close()
    print("PASS (x) clusterManager and root run eloqClusterGate, database roles cannot",
          flush=True)


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
    parser.add_argument("--scenario", action="append",
                        help="Run only this scenario (its label, e.g. pause-after-log); "
                             "may be repeated")
    args = parser.parse_args()
    # Keep the server and its helpers within the user's CPU limit.
    if hasattr(os, "sched_getaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])

    for label, scenario in (("pause-resume", scenario_pause_resume),
                            ("pause-waits", scenario_pause_waits),
                            ("restart", scenario_restart),
                            ("pause-after-log", scenario_pause_after_log),
                            ("resume-after-log", scenario_resume_after_log),
                            ("pause-in-install", scenario_pause_in_install),
                            ("ddl-recovered-after-gate", scenario_ddl_recovered_after_gate),
                            ("concurrent-pauses", scenario_concurrent_pauses),
                            ("intent-conflict", scenario_intent_conflict_retried),
                            ("privileges", scenario_privileges)):
        if args.scenario and label not in args.scenario:
            continue
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
