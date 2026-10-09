#!/usr/bin/env python3
"""Unit tests of the reference consumer's apply loop (plan E1), with recorded events and a fake sink.

Run: python3 -m unittest tools/cdc_consumer/test_eloq_cdc_consumer.py (needs pymongo for bson).
"""

import os
import sys
import threading
import unittest

from bson import Binary, Int64
from pymongo import WriteConcern
from pymongo.errors import AutoReconnect, OperationFailure, WriteConcernError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import eloq_cdc_consumer as consumer  # noqa: E402


def token(ts, kind=0):
    return {"_data": "02" + format(ts, "016x") + format(kind, "02x")}


def dml(op, coll, key, ts, doc=None, key_bytes=None, db="app"):
    event = {"_id": token(ts), "operationType": op, "ns": {"db": db, "coll": coll},
             "documentKey": {"_id": key},
             "eloq": {"keyBytes": Binary(key_bytes if key_bytes is not None else
                                         f"k{key}".encode())}}
    if op != "delete":
        event["fullDocument"] = dict({"_id": key}, **(doc or {}))
    return event


def ddl(op, coll, ts, desc=None, db="app"):
    event = {"_id": token(ts), "operationType": op, "ns": {"db": db, "coll": coll}}
    if desc is not None:
        event["operationDescription"] = desc
    return event


class FakeSink:
    """Records what the applier sends, in order, and can fail on demand."""

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = []
        self.fail = []  # exceptions raised, one per call, by bulk_write

    def bulk_write(self, db, coll, ops):
        with self.lock:
            if self.fail:
                raise self.fail.pop(0)
            self.calls.append(("bulk", db, coll, [(type(o).__name__, o._filter) for o in ops]))

    def create_collection(self, db, coll, options):
        with self.lock:
            self.calls.append(("create", db, coll, options))

    def drop_collection(self, db, coll):
        with self.lock:
            self.calls.append(("drop", db, coll))

    def create_indexes(self, db, coll, specs):
        with self.lock:
            self.calls.append(("createIndexes", db, coll, specs))

    def drop_index(self, db, coll, name):
        with self.lock:
            self.calls.append(("dropIndex", db, coll, name))

    def put_view(self, db, name, definition):
        with self.lock:
            self.calls.append(("putView", db, name, consumer.view_definition(definition)))

    def drop_view(self, db, name):
        with self.lock:
            self.calls.append(("dropView", db, name))

    def drop_views(self, db):
        with self.lock:
            self.calls.append(("dropViews", db))


class FakeProgress:
    def __init__(self):
        self.saved = []

    def save(self, tok):
        self.saved.append(tok)


class ApplierTest(unittest.TestCase):
    def setUp(self):
        self.sink = FakeSink()
        self.progress = FakeProgress()
        self.applier = consumer.Applier(self.sink, self.progress, slots=4)

    def tearDown(self):
        self.applier.close()

    def test_one_document_one_slot_whatever_its_id_type(self):
        # An insert's NumberLong(7) and a delete's int32 7 carry the same keyBytes.
        insert = dml("insert", "c", Int64(7), 1, key_bytes=b"\x2b\x0e\x04")
        delete = dml("delete", "c", 7, 2, key_bytes=b"\x2b\x0e\x04")
        self.assertEqual(consumer.slot_of(insert, 16), consumer.slot_of(delete, 16))
        spread = {consumer.slot_of(dml("insert", "c", i, i), 16) for i in range(200)}
        self.assertGreater(len(spread), 8)

    def test_operations_on_one_document_stay_in_order(self):
        events = [dml("insert", "c", 1, 1, {"v": 1}), dml("replace", "c", 1, 2, {"v": 2}),
                  dml("delete", "c", 1, 3), dml("insert", "c", 1, 4, {"v": 4})]
        self.applier.apply_batch(events, token(4))
        ops = [op for call in self.sink.calls for op in call[3]]
        self.assertEqual([name for name, _ in ops],
                         ["ReplaceOne", "ReplaceOne", "DeleteOne", "ReplaceOne"])
        self.assertEqual(self.progress.saved, [token(4)])

    def test_ddl_waits_for_the_dml_before_it_and_is_saved_alone(self):
        events = [dml("insert", "c", i, i) for i in range(1, 9)]
        events.append(ddl("drop", "c", 10))
        events += [dml("insert", "c", i, i) for i in range(11, 15)]
        self.applier.apply_batch(events, token(20, consumer.TOKEN_KIND_WATERMARK))
        drop_at = [i for i, c in enumerate(self.sink.calls) if c[0] == "drop"]
        self.assertEqual(len(drop_at), 1)
        before = [op for c in self.sink.calls[:drop_at[0]] for op in c[3]]
        after = [op for c in self.sink.calls[drop_at[0] + 1:] for op in c[3]]
        self.assertEqual(len(before), 8)
        self.assertEqual(len(after), 4)
        self.assertEqual(self.progress.saved,
                         [token(10), token(20, consumer.TOKEN_KIND_WATERMARK)])

    def test_index_ddl_goes_to_the_sink(self):
        spec = {"v": 2, "key": {"a": 1}, "name": "a_1", "unique": True}
        self.applier.apply_batch([ddl("createIndexes", "c", 1, {"indexes": [spec]}),
                                  ddl("dropIndexes", "c", 2, {"indexes": [spec]})], None)
        self.assertEqual(self.sink.calls[0], ("createIndexes", "app", "c", [spec]))
        self.assertEqual(self.sink.calls[1], ("dropIndex", "app", "c", "a_1"))

    def test_a_hard_error_halts_before_progress(self):
        self.sink.fail = [OperationFailure("bad document", 2)]
        with self.assertRaises(consumer.ConsumerHalted):
            self.applier.apply_batch([dml("insert", "c", 1, 1)], token(1))
        self.assertEqual(self.progress.saved, [])

    def test_a_transient_error_is_retried(self):
        self.sink.fail = [AutoReconnect("lost the sink")]
        self.applier.apply_batch([dml("insert", "c", 1, 1)], token(1))
        self.assertEqual(len(self.sink.calls), 1)
        self.assertEqual(self.progress.saved, [token(1)])

    def test_events_outside_the_scope_are_skipped(self):
        applier = consumer.Applier(self.sink, self.progress, slots=4,
                                   in_scope=lambda db: db == "app")
        try:
            applier.apply_batch([dml("insert", "c", 1, 1, db="other"),
                                 ddl("drop", "c", 2, db="other"),
                                 dml("insert", "c", 3, 3),
                                 ddl("create", "d", 4, {}, db="other")], token(5))
        finally:
            applier.close()
        self.assertEqual(self.sink.calls, [("bulk", "app", "c", [("ReplaceOne", {"_id": 3})])])
        self.assertEqual(self.progress.saved, [token(5)])

    def test_an_omitted_document_halts_before_anything_is_applied(self):
        oversized = dml("insert", "c", 2, 2)
        del oversized["fullDocument"]
        oversized["fullDocumentOmitted"] = True
        with self.assertRaises(consumer.ConsumerHalted):
            self.applier.apply_batch([dml("insert", "c", 1, 1), oversized], token(2))
        self.assertEqual(self.sink.calls, [])
        self.assertEqual(self.progress.saved, [])

    def test_an_omitted_document_is_read_from_the_source(self):
        reads = []

        def fetch(db, coll, key):
            reads.append((db, coll, key))
            return {"_id": key["_id"], "v": "as the source holds it now"} if key["_id"] == 2 else None

        applier = consumer.Applier(self.sink, self.progress, slots=4, fetch=fetch)
        try:
            present = dml("insert", "c", 2, 2)
            gone = dml("replace", "c", 3, 3)
            for event in (present, gone):
                del event["fullDocument"]
                event["fullDocumentOmitted"] = True
            applier.apply_batch([dml("insert", "c", 1, 1), present, gone,
                                 dml("delete", "c", 3, 4)], token(4))
        finally:
            applier.close()
        self.assertEqual(sorted(reads, key=repr),
                         [("app", "c", {"_id": 2}), ("app", "c", {"_id": 3})])
        ops = [op for call in self.sink.calls for op in call[3]]
        # The gone document's replace is skipped; its delete still applies.
        self.assertEqual(sorted(ops, key=repr),
                         sorted([("ReplaceOne", {"_id": 1}), ("ReplaceOne", {"_id": 2}),
                                 ("DeleteOne", {"_id": 3})], key=repr))
        self.assertEqual(self.progress.saved, [token(4)])

    def test_view_changes_become_view_commands(self):
        view = {"viewOn": "c", "pipeline": [{"$match": {"a": 1}}]}
        events = [dml("insert", "c", 1, 1),
                  ddl("create", "system.views", 2, {}),
                  dml("insert", "system.views", "app.v", 3, view),
                  dml("replace", "system.views", "app.v", 4, dict(view, pipeline=[])),
                  dml("delete", "system.views", "app.v", 5),
                  ddl("drop", "system.views", 6)]
        self.applier.apply_batch(events, token(6))
        self.assertEqual(self.sink.calls, [
            ("bulk", "app", "c", [("ReplaceOne", {"_id": 1})]),
            ("putView", "app", "v", view),
            ("putView", "app", "v", dict(view, pipeline=[])),
            ("dropView", "app", "v"),
            ("dropViews", "app")])
        # Each view change is saved like a DDL; system.views' own create changes nothing.
        self.assertEqual(self.progress.saved,
                         [token(2), token(3), token(4), token(5), token(6), token(6)])

    def test_system_collections_but_system_js_are_skipped(self):
        self.applier.apply_batch([ddl("create", "system.js", 1, {}),
                                  dml("insert", "system.js", "f", 2, {"value": "x"}),
                                  ddl("create", "system.profile", 3, {}),
                                  dml("insert", "system.profile", 1, 4, {"op": "query"})],
                                 token(4))
        self.assertEqual(self.sink.calls, [
            ("create", "app", "system.js", {}),
            ("bulk", "app", "system.js", [("ReplaceOne", {"_id": "f"})])])
        self.assertTrue(consumer.replicated_collection("orders"))
        self.assertTrue(consumer.replicated_collection("system.js"))
        self.assertFalse(consumer.replicated_collection("system.views"))
        self.assertFalse(consumer.replicated_collection("system.profile"))

    def test_invalidate_halts(self):
        with self.assertRaises(consumer.ConsumerHalted):
            self.applier.apply_batch([ddl("invalidate", "c", 1)], None)


class FakeDb:
    def __init__(self, fail=None, collections=None, write_concern=None, reply=None):
        self.fail = fail
        self.commands = []
        self.collections = collections or []
        self.write_concern = write_concern or WriteConcern()
        self.reply = reply or {"ok": 1}

    def command(self, cmd):
        self.commands.append(cmd)
        if self.fail is not None:
            raise self.fail
        return self.reply

    def list_collections(self, filter=None):
        return [c for c in self.collections if c["name"] == filter["name"]]


class FakeClient:
    def __init__(self, db):
        self.db = db

    def __getitem__(self, name):
        return self.db


class MongoSinkTest(unittest.TestCase):
    def sink(self, fail=None, collections=None, write_concern=None, reply=None):
        db = FakeDb(fail, collections, write_concern, reply)
        return consumer.MongoSink(FakeClient(db)), db

    def test_ddl_carries_the_sinks_write_concern(self):
        sink, db = self.sink(write_concern=WriteConcern(w="majority", j=True))
        sink.create_collection("app", "c", {})
        sink.drop_collection("app", "c")
        sink.create_indexes("app", "c", [{"v": 2, "key": {"a": 1}, "name": "a_1"}])
        sink.drop_index("app", "c", "a_1")
        self.assertEqual([cmd.get("writeConcern") for cmd in db.commands],
                         [{"w": "majority", "j": True}] * 4)
        # The server's default write concern is left to the server.
        sink, db = self.sink()
        sink.drop_collection("app", "c")
        self.assertNotIn("writeConcern", db.commands[0])

    def test_a_write_concern_error_is_raised_and_a_timeout_retried(self):
        timeout = {"ok": 1, "writeConcernError": {"code": 64, "errmsg": "waiting timed out"}}
        sink, _ = self.sink(reply=timeout)
        with self.assertRaises(WriteConcernError) as caught:
            sink.create_indexes("app", "c", [{"v": 2, "key": {"a": 1}, "name": "a_1"}])
        self.assertTrue(consumer.is_transient(caught.exception))
        unsatisfiable = {"ok": 1, "writeConcernError": {"code": 100, "errmsg": "no such tag"}}
        sink, _ = self.sink(reply=unsatisfiable)
        with self.assertRaises(consumer.ConsumerHalted):
            consumer.retry_transient("drop", lambda: sink.drop_collection("app", "c"))

    def test_index_specs_lose_unique_and_ttl(self):
        sink, db = self.sink()
        sink.create_indexes("app", "c", [{"v": 2, "key": {"a": 1}, "name": "a_1",
                                          "unique": True, "expireAfterSeconds": 60}])
        self.assertEqual(db.commands[0]["indexes"], [{"v": 2, "key": {"a": 1}, "name": "a_1"}])

    def test_ignorable_errors(self):
        sink, _ = self.sink(OperationFailure("ns not found", consumer.NAMESPACE_NOT_FOUND))
        sink.drop_collection("app", "c")
        sink, _ = self.sink(OperationFailure("index not found", consumer.INDEX_NOT_FOUND))
        sink.drop_index("app", "c", "a_1")

    def test_an_existing_collection_with_the_same_options_is_fine(self):
        exists = OperationFailure("exists", consumer.NAMESPACE_EXISTS)
        sink, _ = self.sink(exists, [{"name": "c", "options": {"validator": {"a": 1}}}])
        sink.create_collection("app", "c", {"validator": {"a": 1}})
        sink, _ = self.sink(exists, [{"name": "c", "options": {"validator": {"b": 1}}}])
        with self.assertRaises(consumer.ConsumerHalted):
            sink.create_collection("app", "c", {"validator": {"a": 1}})

    def test_a_view_is_created_or_redefined(self):
        sink, db = self.sink()
        sink.put_view("app", "v", {"_id": "app.v", "viewOn": "c", "pipeline": [],
                                   "collation": {"locale": "fr"}})
        self.assertEqual(db.commands, [{"create": "v", "viewOn": "c", "pipeline": [],
                                        "collation": {"locale": "fr"}}])
        exists = OperationFailure("exists", consumer.NAMESPACE_EXISTS)

        class ExistsOnce(FakeDb):
            def command(self, cmd):
                self.commands.append(cmd)
                if len(self.commands) == 1:
                    raise exists
                return self.reply

        db = ExistsOnce()
        consumer.MongoSink(FakeClient(db)).put_view("app", "v", {"viewOn": "d", "pipeline": []})
        self.assertEqual(db.commands[1], {"collMod": "v", "viewOn": "d", "pipeline": []})

    def test_conflicting_index_specs_halt(self):
        sink, _ = self.sink(OperationFailure("conflict", consumer.INDEX_KEY_SPECS_CONFLICT))
        with self.assertRaises(consumer.ConsumerHalted):
            sink.create_indexes("app", "c", [{"v": 2, "key": {"a": 1}, "name": "a_1"}])


class FakeSource:
    """A source whose watch() fails as told."""

    def __init__(self, fail):
        self.fail = fail

    def watch(self, **kwargs):
        raise self.fail


class HistoryLostTest(unittest.TestCase):
    def test_open_reports_a_lost_history(self):
        lost = OperationFailure("history lost", consumer.CHANGE_STREAM_HISTORY_LOST)
        with self.assertRaises(consumer.HistoryLost):
            consumer.open_stream(FakeSource(lost), resume_after=token(1))

    @staticmethod
    def stub_consumer(stream_from, bootstrap):
        """A consumer with a saved token, whose stream and bootstrap are the given functions."""
        class Loaded:
            def load(self):
                return token(1)

        class Sink:
            write_concern = WriteConcern()

        run = consumer.Consumer.__new__(consumer.Consumer)
        run.sink_client = Sink()
        run.progress = Loaded()
        run.max_rebootstraps = consumer.MAX_REBOOTSTRAPS
        run.caught_up = False
        run.stream_from = stream_from
        run.bootstrap = bootstrap
        return run

    def test_a_lost_history_bootstraps_again(self):
        calls = []

        def stream_from(tok):
            calls.append(("stream", tok))
            if len(calls) == 1:
                raise consumer.HistoryLost("the stream's history is gone")

        def bootstrap():
            calls.append(("bootstrap",))
            return token(9)

        self.stub_consumer(stream_from, bootstrap).run()
        self.assertEqual(calls, [("stream", token(1)), ("bootstrap",), ("stream", token(9))])

    def test_a_history_lost_after_every_bootstrap_halts(self):
        bootstraps = []

        def stream_from(tok):
            raise consumer.HistoryLost("the stream's history is gone")

        def bootstrap():
            bootstraps.append(1)
            return token(9)

        with self.assertRaises(consumer.ConsumerHalted) as caught:
            self.stub_consumer(stream_from, bootstrap).run()
        self.assertNotIsInstance(caught.exception, consumer.HistoryLost)
        self.assertIn("txlog_cdc_retention_seconds", str(caught.exception))
        self.assertEqual(len(bootstraps), consumer.MAX_REBOOTSTRAPS)

    def test_catching_up_restarts_the_count(self):
        streams, bootstraps = [], []
        run = None

        def stream_from(tok):
            streams.append(tok)
            # The third stream catches up with the source before it loses its history.
            if len(streams) == 3:
                run.caught_up = True
            raise consumer.HistoryLost("the stream's history is gone")

        def bootstrap():
            bootstraps.append(1)
            return token(9)

        run = self.stub_consumer(stream_from, bootstrap)
        with self.assertRaises(consumer.ConsumerHalted):
            run.run()
        # Two before the catch-up, then the count starts over: one for it and two more.
        self.assertEqual(len(bootstraps), 2 + consumer.MAX_REBOOTSTRAPS)


class CatchUpTest(unittest.TestCase):
    NOW = 1790000000000000
    WATERMARK = consumer.TOKEN_KIND_WATERMARK

    def test_an_unchanged_opening_watermark_proves_nothing(self):
        # What a poll that timed out returns while its scan is still pending.
        start = self.NOW - 1000
        self.assertFalse(consumer.shows_catch_up(token(start, self.WATERMARK), start, self.NOW))

    def test_an_advanced_watermark_far_behind_the_clock_is_not_caught_up(self):
        start = self.NOW - 3600 * 1000 * 1000
        old = start + 1000 * 1000
        self.assertFalse(consumer.shows_catch_up(token(old, self.WATERMARK), start, self.NOW))

    def test_an_advanced_watermark_at_the_tail_is(self):
        start = self.NOW - 3600 * 1000 * 1000
        self.assertTrue(consumer.shows_catch_up(token(self.NOW - 5 * 1000 * 1000, self.WATERMARK),
                                                start, self.NOW))

    def test_an_event_token_never_is(self):
        start = self.NOW - 3600 * 1000 * 1000
        self.assertFalse(consumer.shows_catch_up(token(self.NOW, consumer.TOKEN_KIND_EVENT),
                                                 start, self.NOW))
        self.assertFalse(consumer.shows_catch_up(None, start, self.NOW))


class ScriptedStream:
    """A change stream that plays a script: each step is a resume token, returned once by a
    try_next() that finds nothing, or an exception, raised by try_next()."""

    def __init__(self, script):
        self.script = list(script)
        self.resume_token = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def try_next(self):
        step = self.script.pop(0)
        if isinstance(step, Exception):
            raise step
        self.resume_token = step
        return None


class StreamFromTest(unittest.TestCase):
    def consumer_on(self, script):
        class Source:
            def watch(self, **kwargs):
                return ScriptedStream(script)

        run = consumer.Consumer.__new__(consumer.Consumer)
        run.source = Source()
        run.sink = FakeSink()
        run.progress = FakeProgress()
        run.slots = 2
        run.stop = threading.Event()
        run.batch = 10
        run.max_await_ms = 10
        run.last_token = None
        run.caught_up = False
        run.databases = None
        return run

    def test_a_timed_out_poll_is_no_catch_up(self):
        import time as clock
        start = int(clock.time() * 1e6)
        lost = OperationFailure("history lost", consumer.CHANGE_STREAM_HISTORY_LOST)
        # The poll times out with its scan still pending: the opening watermark, unchanged.
        run = self.consumer_on([token(start, consumer.TOKEN_KIND_WATERMARK), lost])
        with self.assertRaises(consumer.HistoryLost):
            run.stream_from(token(start, consumer.TOKEN_KIND_WATERMARK))
        self.assertFalse(run.caught_up)

    def test_a_watermark_advanced_to_the_tail_is(self):
        import time as clock
        now = int(clock.time() * 1e6)
        start = now - 3600 * 1000 * 1000
        lost = OperationFailure("history lost", consumer.CHANGE_STREAM_HISTORY_LOST)
        run = self.consumer_on([token(now, consumer.TOKEN_KIND_WATERMARK), lost])
        with self.assertRaises(consumer.HistoryLost):
            run.stream_from(token(start, consumer.TOKEN_KIND_WATERMARK))
        self.assertTrue(run.caught_up)


class TokenTest(unittest.TestCase):
    def test_timestamp_and_kind(self):
        tok = token(1790000000123456, consumer.TOKEN_KIND_WATERMARK)
        self.assertEqual(consumer.token_timestamp(tok), 1790000000123456)
        self.assertEqual(consumer.token_kind(tok), consumer.TOKEN_KIND_WATERMARK)

    def test_collection_options_never_capped(self):
        self.assertEqual(consumer.strip_collection_options(
            {"capped": True, "size": 100, "validator": {"a": 1}, "uuid": "x"}),
            {"validator": {"a": 1}})


if __name__ == "__main__":
    unittest.main()
