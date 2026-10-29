#!/usr/bin/env python3
"""The reference consumer end to end, from one EloqDoc node into another (docs/cdc-change-stream.md
§6, §10.11; plan E5).

Each scenario starts a fresh source node and a fresh sink node and runs the consumer
(tools/cdc_consumer/eloq_cdc_consumer.py) in this process:

  (i)   converge: writes keep running on the source during the bootstrap and after it; once they
        stop, the sink holds the source's documents, collections, options and indexes (without
        unique and expireAfterSeconds), and its non-unique copy of a unique index holds no
        duplicate once the source is quiescent and the stream drained (§6.3); writes, a create
        and a drop in a database outside the consumer's scope never reach the sink; every DDL the
        consumer sends the sink carries the sink client's write concern; views (defined before the
        copy, then created, redefined and dropped while streaming) and stored JavaScript
        (system.js) match the source;
  (ii)  create index i on {x:1}, drop it, create i on {y:1}, all tried during the copy: with the
        gate the first is refused with ClusterGatePaused and the three wait out the copy, and
        replay converges on {y:1}; without it they run during the copy, which then holds {y:1},
        and replay halts on {x:1} (§6.2);
  (iii) a consumer whose copy keeps failing starts the bootstrap over until its retries run out,
        then halts with the gate resumed; one whose copy fails once starts over, the gate open
        between the attempts (DDL runs there), and converges; one that stops between the copy and
        publishing T0 leaves no usable checkpoint; the next run bootstraps again and converges;
  (iv)  views a and b on orders, then a on b, a back on orders, and b on a, all tried during the
        copy: with the gate the three view changes are refused until the copy ends and replay
        converges; without it the copy holds the last definitions, and replaying the first change
        over them would make a cycle, which the sink refuses, so the consumer halts;
  (v)   documents just under 16 MB, whose events would pass the limit and so arrive without them
        (fullDocumentOmitted): the consumer reads them from the source, and skips one deleted
        since.

The source needs the change stream, so a WAL; a node built with fault points is not required.
Requires pymongo.
"""

import argparse
import json
import os
import random
import shutil
import sys
import threading
import time
from pathlib import Path

from pymongo import MongoClient, monitoring
from pymongo.errors import OperationFailure, PyMongoError

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "tools" / "cdc_consumer"))
from ddl_recovery import Node  # noqa: E402
import eloq_cdc_consumer as cdc  # noqa: E402

CLUSTER_GATE_PAUSED = 5001
GRAPH_CONTAINS_CYCLE = 5
CONVERGE_TIMEOUT_S = 180
DB = "shop"
OTHER_DB = "elsewhere"  # on the source and the sink, outside the consumer's scope


def node_args(args, config):
    """Node's arguments with its own substrate config template."""
    node = argparse.Namespace(**vars(args))
    node.substrate_config = config
    return node


def snapshot(client):
    """Every in-scope collection's documents, options and indexes, and every view's definition,
    comparable across nodes."""
    state = {}
    for info in client[DB].list_collections():
        name = info["name"]
        if info.get("type") == "view":
            state[name] = {"view": cdc.view_definition(info.get("options", {}))}
            continue
        coll = client[DB][name]
        options = cdc.strip_collection_options(info.get("options", {}))
        indexes = sorted((spec["name"], json.dumps(spec["key"], sort_keys=True))
                         for spec in coll.list_indexes())
        docs = sorted((json.dumps(d, sort_keys=True, default=str) for d in coll.find()))
        state[name] = {"options": options, "indexes": indexes, "docs": docs}
    return state


def wait_converged(source, sink, what):
    """Waits until the sink streams and holds what the source holds; returns that state."""
    deadline = time.monotonic() + CONVERGE_TIMEOUT_S
    want, got = {}, {}
    while True:
        try:
            # A bootstrapping consumer drops and refills the sink under any read of it.
            progress = sink.admin[cdc.PROGRESS_COLLECTION].find_one({"_id": "progress"})
            if progress is not None and progress["state"] == "streaming":
                want, got = snapshot(source), snapshot(sink)
                if want == got:
                    return want
        except OperationFailure:
            pass
        if time.monotonic() > deadline:
            missing = {k: (len(want[k]["docs"]), len(got.get(k, {}).get("docs", [])))
                       for k in want}
            raise AssertionError(f"{what}: the sink never converged: {missing}")
        time.sleep(1)


class ConsumerThread:
    """The consumer running in this process, so a scenario can stop it or read its error."""

    def __init__(self, source, sink, **kwargs):
        self.consumer = cdc.Consumer(source, sink, databases=[DB], **kwargs)
        self.error = None
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            self.consumer.run()
        except cdc.ConsumerHalted as exc:
            self.error = exc

    def stop(self):
        self.consumer.stop.set()
        self.thread.join(timeout=60)
        assert not self.thread.is_alive(), "the consumer did not stop"


def retry_ddl(fn, refused=None):
    """Runs a DDL, retrying while the cluster gate refuses it, as a client would; sets the event
    refused, if given, at the first refusal."""
    while True:
        try:
            return fn()
        except OperationFailure as exc:
            if exc.code != CLUSTER_GATE_PAUSED:
                raise
            if refused is not None:
                refused.set()
            time.sleep(0.2)


class DdlRecorder(monitoring.CommandListener):
    """Records the DDL commands a client sends, with their write concern, as they go out."""

    NAMES = ("create", "drop", "createIndexes", "dropIndexes", "dropDatabase")

    def __init__(self):
        self.commands = []

    def started(self, event):
        if event.command_name in self.NAMES:
            self.commands.append((event.command_name, event.command.get("writeConcern")))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


def setup_source(source):
    db = source[DB]
    db.create_collection("orders", validator={"qty": {"$gte": 0}})
    db.orders.create_index("customer")
    db.orders.create_index("placed", expireAfterSeconds=3600 * 24 * 365)
    db.create_collection("users")
    db.users.create_index("email", unique=True, sparse=True)
    db.create_collection("empty")
    db.orders.insert_many([{"_id": i, "customer": i % 7, "qty": i, "placed": None}
                           for i in range(200)])
    db.users.insert_many([{"_id": i, "email": f"u{i}@x"} for i in range(50)])
    db.command({"create": "big_orders", "viewOn": "orders",
                "pipeline": [{"$match": {"qty": {"$gte": 100}}}]})
    db.system.js.insert_one({"_id": "double", "value": "function(x) { return 2 * x; }"})


class Writer:
    """Inserts, replaces and deletes orders, and moves unique emails between users."""

    def __init__(self, source):
        self.source = source
        self.stop = threading.Event()
        self.error = None
        self.ops = 0
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        rng = random.Random(7)
        db = self.source[DB]
        next_id = 1000
        try:
            while not self.stop.is_set():
                choice = rng.random()
                if choice < 0.4:
                    db.orders.insert_one({"_id": next_id, "customer": next_id % 7,
                                          "qty": rng.randint(0, 9), "placed": None})
                    next_id += 1
                elif choice < 0.7:
                    k = rng.randint(0, next_id)
                    db.orders.replace_one({"_id": k}, {"customer": k % 7,
                                                       "qty": rng.randint(0, 9),
                                                       "placed": None})
                elif choice < 0.85:
                    db.orders.delete_one({"_id": rng.randint(0, next_id)})
                else:
                    # Move an email from one user to another: unset, then set.
                    a, b = rng.sample(range(50), 2)
                    owner = db.users.find_one({"_id": a})
                    if owner and owner.get("email"):
                        db.users.update_one({"_id": a}, {"$unset": {"email": ""}})
                        db.users.update_one({"_id": b}, {"$set": {"email": owner["email"]}})
                self.ops += 1
        except PyMongoError as exc:
            self.error = exc

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=60)
        assert self.error is None, self.error


def check_sink_definitions(sink):
    orders = {spec["name"]: spec for spec in sink[DB].orders.list_indexes()}
    assert "expireAfterSeconds" not in orders["placed_1"], orders["placed_1"]
    users = {spec["name"]: spec for spec in sink[DB].users.list_indexes()}
    assert "unique" not in users["email_1"], users["email_1"]
    progress = sink.admin[cdc.PROGRESS_COLLECTION].find_one({"_id": "progress"})
    assert progress["state"] == "streaming", progress


def no_duplicate_emails(sink):
    dups = list(sink[DB].users.aggregate([
        {"$match": {"email": {"$exists": True}}},
        {"$group": {"_id": "$email", "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}}]))
    assert not dups, dups


def scenario_converge(source, sink):
    """(i) Converge under writes during and after the bootstrap; other databases stay out."""
    setup_source(source.client)
    # A sink collection the consumer never copied, in a database outside its scope, and the same
    # database on the source, with a collection of that name to drop later.
    sink.client[OTHER_DB].keep.insert_one({"_id": "sink-only"})
    source.client[OTHER_DB].keep.insert_one({"_id": "source"})
    # State a bootstrap discards: step 1 drops the in-scope database on the sink.
    sink.client[DB].stale.insert_one({"_id": "stale"})
    # The consumer's own sink client, with a write concern other than the server's default.
    recorder = DdlRecorder()
    sink_client = MongoClient("127.0.0.1", sink.port, directConnection=True, w=1,
                              event_listeners=[recorder])
    writer = Writer(source.client)
    consumer = ConsumerThread(source.client, sink_client)
    try:
        time.sleep(20)
        writer.finish()
        # Out of scope, all after the bootstrap: a create, writes and a drop of the collection
        # the sink holds. The marker after them reaches the sink only once the consumer has
        # passed them in the stream.
        source.client[OTHER_DB].create_collection("made")
        source.client[OTHER_DB].made.insert_one({"_id": 1})
        source.client[OTHER_DB].keep.drop()
        # Views and stored JavaScript while streaming.
        db = source.client[DB]
        db.command({"create": "small_orders", "viewOn": "orders",
                    "pipeline": [{"$match": {"qty": {"$lt": 3}}}]})
        db.command({"collMod": "big_orders", "viewOn": "orders",
                    "pipeline": [{"$match": {"qty": {"$gte": 5}}}]})
        db.command({"create": "doomed", "viewOn": "users", "pipeline": []})
        db.command({"drop": "doomed"})
        db.system.js.insert_one({"_id": "triple", "value": "function(x) { return 3 * x; }"})
        db.system.js.replace_one({"_id": "double"}, {"value": "function(x) { return x + x; }"})
        db.orders.insert_one({"_id": "after-other-db", "qty": 0})
        wait_converged(source.client, sink.client, "under writes")
        assert consumer.error is None, consumer.error
        check_sink_definitions(sink.client)
        assert sink.client[OTHER_DB].list_collection_names() == ["keep"], \
            sink.client[OTHER_DB].list_collection_names()
        assert list(sink.client[OTHER_DB].keep.find()) == [{"_id": "sink-only"}]
        # Quiescent source, drained stream, idle slots: the unique index's copy is clean.
        no_duplicate_emails(sink.client)
        views = sorted(doc["_id"] for doc in sink.client[DB].system.views.find())
        assert views == ["shop.big_orders", "shop.small_orders"], views
        # The sink's views work, on a connection other than the consumer's.
        for view in ("big_orders", "small_orders"):
            assert sink.client[DB][view].count_documents({}) == \
                source.client[DB][view].count_documents({}), view
        sent = {name for name, _ in recorder.commands}
        assert {"dropDatabase", "create", "createIndexes"} <= sent, recorder.commands
        assert all(wc == {"w": 1} for _, wc in recorder.commands), recorder.commands
    finally:
        consumer.stop()
        sink_client.close()
    print(f"PASS (i) converged after {writer.ops} writes during and after the bootstrap",
          flush=True)


INDEX_STEPS = (lambda db: db.orders.create_index([("x", 1)], name="i"),
               lambda db: db.orders.drop_index("i"),
               lambda db: db.orders.create_index([("y", 1)], name="i"))

VIEW_STEPS = (lambda db: db.command({"collMod": "a", "viewOn": "b", "pipeline": []}),
              lambda db: db.command({"collMod": "a", "viewOn": "orders", "pipeline": []}),
              lambda db: db.command({"collMod": "b", "viewOn": "a", "pipeline": []}))


def make_views(db):
    """Views a and b, both on orders, for the view race."""
    db.command({"create": "a", "viewOn": "orders", "pipeline": []})
    db.command({"create": "b", "viewOn": "orders", "pipeline": []})


def ddl_race(source, sink, gate, steps, prepare=None):
    """Runs the DDL steps, in order, while the copy of 'orders' is held, and returns the
    consumer; prepare(db) runs on the source before the consumer starts."""
    setup_source(source.client)
    db = source.client[DB]
    if prepare is not None:
        prepare(db)
    copy_held = threading.Event()
    release = threading.Event()
    refused = threading.Event()
    ddl_done = threading.Event()

    def hold(dbname, coll):
        if coll == "orders" and not copy_held.is_set():
            copy_held.set()
            # With the gate the DDLs are refused and retry, so wait a while for them to try; without
            # it they finish before the copy reads the catalog.
            release.wait(timeout=60)

    def ddls():
        copy_held.wait(timeout=120)
        for step in steps:
            retry_ddl(lambda: step(db), refused)
        ddl_done.set()

    ddl_thread = threading.Thread(target=ddls, daemon=True)
    ddl_thread.start()
    consumer = ConsumerThread(source.client, sink.client, before_copy_collection=hold,
                              unsafe_skip_gate=not gate)
    assert copy_held.wait(timeout=120), "the copy never reached orders"
    if gate:
        assert refused.wait(timeout=60), "no DDL was refused while the gate was paused"
        assert not ddl_done.is_set(), "the DDLs finished while the gate was paused"
    else:
        assert ddl_done.wait(timeout=120), "the DDLs did not run without the gate"
        assert not refused.is_set(), "a DDL was refused without the gate"
    release.set()
    return consumer, ddl_thread


def scenario_index_race(source, sink, gate):
    """(ii) create i on {x:1}, drop i, create i on {y:1} during the copy."""
    consumer, ddl_thread = ddl_race(source, sink, gate, INDEX_STEPS)
    try:
        if gate:
            ddl_thread.join(timeout=120)
            state = wait_converged(source.client, sink.client, "after the DDLs")
            assert ("i", json.dumps({"y": 1})) in state["orders"]["indexes"], state
            assert consumer.error is None, consumer.error
            print("PASS (ii) with the gate, the DDLs waited out the copy and replay converged",
                  flush=True)
        else:
            consumer.thread.join(timeout=120)
            assert isinstance(consumer.error, cdc.ConsumerHalted), consumer.error
            assert "gate was bypassed" in str(consumer.error), consumer.error
            print("PASS (ii) without the gate, replay halted on the conflicting index",
                  flush=True)
    finally:
        consumer.stop()


def scenario_view_race(source, sink, gate):
    """(iv) a on b, a back on orders, b on a, during the copy."""
    consumer, ddl_thread = ddl_race(source, sink, gate, VIEW_STEPS, make_views)
    try:
        if gate:
            ddl_thread.join(timeout=120)
            state = wait_converged(source.client, sink.client, "after the view changes")
            assert state["a"]["view"]["viewOn"] == "orders", state["a"]
            assert state["b"]["view"]["viewOn"] == "a", state["b"]
            assert consumer.error is None, consumer.error
            print("PASS (iv) with the gate, the view changes waited out the copy and replay "
                  "converged", flush=True)
        else:
            consumer.thread.join(timeout=120)
            assert isinstance(consumer.error, cdc.ConsumerHalted), consumer.error
            cause = consumer.error.__cause__
            assert getattr(cause, "code", None) == GRAPH_CONTAINS_CYCLE, consumer.error
            print("PASS (iv) without the gate, replaying the first view change made a cycle and "
                  "the consumer halted", flush=True)
    finally:
        consumer.stop()


def scenario_oversized(source, sink):
    """(v) Events over 16 MB arrive without their documents; the consumer reads them."""
    setup_source(source.client)
    consumer = ConsumerThread(source.client, sink.client)
    try:
        wait_converged(source.client, sink.client, "before the large documents")
        db = source.client[DB]
        watcher = db.watch(max_await_time_ms=1000)
        watcher.try_next()
        # About 200 bytes under BSON's 16 MB: insertable, but its event, which adds the token,
        # namespace and key, would not be.
        pad = 16 * 1024 * 1024 - 200
        db.blobs.insert_one({"_id": "kept", "pad": "a" * pad})
        db.blobs.replace_one({"_id": "kept"}, {"pad": "b" * pad})
        db.blobs.insert_one({"_id": "gone", "pad": "c" * pad})
        db.blobs.delete_one({"_id": "gone"})
        db.orders.insert_one({"_id": "after-blobs", "qty": 0})

        omitted = []
        deadline = time.monotonic() + CONVERGE_TIMEOUT_S
        while len(omitted) < 3 and time.monotonic() < deadline:
            event = watcher.try_next()
            if event and event["ns"].get("coll") == "blobs" and \
                    event["operationType"] in ("insert", "replace"):
                assert event.get("fullDocumentOmitted") is True, event.keys()
                assert "fullDocument" not in event, event.keys()
                omitted.append(event["documentKey"]["_id"])
        watcher.close()
        assert omitted == ["kept", "kept", "gone"], omitted

        wait_converged(source.client, sink.client, "after the large documents")
        assert sink.client[DB].blobs.find_one({"_id": "kept"})["pad"][:1] == "b"
        assert sink.client[DB].blobs.find_one({"_id": "gone"}) is None
        assert consumer.error is None, consumer.error
    finally:
        consumer.stop()
    print("PASS (v) events over 16 MB were applied from documents read on the source", flush=True)


def scenario_crash_before_publish(source, sink):
    """(iii) A copy that keeps failing gives up after its retries; one that fails once starts over,
    with the gate open in between; stopping between the copy and publishing T0 makes the next run
    bootstrap again."""
    setup_source(source.client)

    def fail_copy(dbname, coll):
        raise cdc.ConsumerHalted("test hook: the copy failed")

    failed = ConsumerThread(source.client, sink.client, before_copy_collection=fail_copy,
                            max_copy_retries=1, copy_retry_delay_s=1)
    failed.thread.join(timeout=180)
    assert isinstance(failed.error, cdc.ConsumerHalted), failed.error
    assert "gave up after 2 attempts" in str(failed.error), failed.error
    # The halted consumer resumed the gate on its way out: DDL runs at once.
    source.client[DB].create_collection("after_failed_copy")
    source.client[DB].drop_collection("after_failed_copy")

    failed_once = threading.Event()
    ddl_ran = threading.Event()
    open_between = []

    def fail_first_copy(dbname, coll):
        if not failed_once.is_set():
            failed_once.set()
            raise cdc.ConsumerHalted("test hook: the first copy failed")
        if not open_between:
            open_between.append(ddl_ran.is_set())

    retried = ConsumerThread(source.client, sink.client, before_copy_collection=fail_first_copy)
    try:
        assert failed_once.wait(timeout=120), "the first copy never started"
        # Between the attempts the gate is open: DDL runs during the wait.
        retry_ddl(lambda: source.client[DB].create_collection("between_attempts"))
        retry_ddl(lambda: source.client[DB].drop_collection("between_attempts"))
        ddl_ran.set()
        wait_converged(source.client, sink.client, "after the copy started over")
        assert retried.error is None, retried.error
        assert open_between == [True], "the second attempt started before the DDL could run"
    finally:
        retried.stop()

    # A fresh bootstrap, as an operator forces one (README, "Forcing a fresh bootstrap").
    sink.client.admin[cdc.PROGRESS_COLLECTION].update_one(
        {"_id": "progress"}, {"$set": {"state": "bootstrapping"}})
    first = ConsumerThread(source.client, sink.client, fail_before_publish=True)
    first.thread.join(timeout=180)
    assert isinstance(first.error, cdc.ConsumerHalted), first.error
    progress = sink.client.admin[cdc.PROGRESS_COLLECTION].find_one({"_id": "progress"})
    assert progress["state"] == "bootstrapping", progress
    source.client[DB].orders.insert_many([{"_id": 10000 + i, "qty": 1} for i in range(10)])
    second = ConsumerThread(source.client, sink.client)
    try:
        wait_converged(source.client, sink.client, "after bootstrapping again")
        assert second.error is None, second.error
    finally:
        second.stop()
    print("PASS (iii) a copy that kept failing gave up and resumed the gate; one that failed once "
          "started over with the gate open in between; a run stopped before publishing T0 "
          "bootstrapped again and converged", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--data-store", default="ELOQDSS_ROCKSDB")
    parser.add_argument("--log-state", default="ROCKSDB")
    parser.add_argument("--source-config", type=Path,
                        help="The source's data substrate config template ('{root}' becomes its "
                             "directory); default a generated one")
    parser.add_argument("--sink-config", type=Path,
                        help="The sink's config template; it must not share the source's ports")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--diagnostics-dir", type=Path)
    parser.add_argument("--scenario", action="append",
                        help="Run only this scenario (converge, index-race-gate, "
                             "index-race-no-gate, crash-before-publish, view-race-gate, "
                             "view-race-no-gate, oversized); may be repeated")
    args = parser.parse_args()
    if hasattr(os, "sched_getaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])

    scenarios = (("converge", scenario_converge),
                 ("index-race-gate", lambda s, k: scenario_index_race(s, k, True)),
                 ("index-race-no-gate", lambda s, k: scenario_index_race(s, k, False)),
                 ("crash-before-publish", scenario_crash_before_publish),
                 ("view-race-gate", lambda s, k: scenario_view_race(s, k, True)),
                 ("view-race-no-gate", lambda s, k: scenario_view_race(s, k, False)),
                 ("oversized", scenario_oversized))
    for label, scenario in scenarios:
        if args.scenario and label not in args.scenario:
            continue
        source = Node(node_args(args, args.source_config), f"source-{label}")
        sink = Node(node_args(args, args.sink_config), f"sink-{label}")
        ok = False
        try:
            source.start()
            sink.start()
            scenario(source, sink)
            ok = True
        finally:
            for node in (source, sink):
                node.close(keep_logs=not ok)


if __name__ == "__main__":
    main()
