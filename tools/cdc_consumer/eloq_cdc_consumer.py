#!/usr/bin/env python3
"""Reference change data capture consumer for EloqDoc (docs/cdc-change-stream.md §6; plan E1, E2).

Mirrors an EloqDoc deployment into a MongoDB-compatible sink. It bootstraps the sink from a
snapshot taken under the cluster gate, then applies the deployment-level change stream from the
position the snapshot reflects. Progress lives in the sink, in admin.__eloq_cdc_progress, as
{state: "streaming", token} or {state: "bootstrapping"}; a restart resumes from a streaming token
and bootstraps again from anything else.

Apply loop (§6.1): each batch of events is cut at its DDL events. The DML between two DDL events is
spread over N slots by crc32(ns, eloq.keyBytes), so every operation on one document lands in one
slot, and each slot applies its share per collection as an ordered bulk write with
bypassDocumentValidation. A DDL waits for every slot (the barrier) and is applied alone. The token
is saved after each DDL and after each batch, once everything before it is applied. The stream is
deployment-wide; events outside the databases in scope (--databases, and never admin, local or
config) are skipped, as the bootstrap skips those databases.

Namespaces, the same in the copy and in the stream: collections and system.js are replicated as
documents; views as definitions, created, redefined (collMod) or dropped on the sink as the source's
system.views changes; every other system collection (system.profile, ...) belongs to its node and is
skipped.

Bootstrap (§6.2): invalidate the checkpoint, drop the in-scope databases on the sink, pause the
cluster gate (eloqClusterGate), pick T0 from a fresh stream (a watermark at or above the pause's
timestamp, or an event token above it), copy collection definitions (without unique and
expireAfterSeconds, §6.3, §6.4) and documents, resume the gate, publish {streaming, T0} and apply
the stream after T0.

Usage: eloq_cdc_consumer.py --source mongodb://eloqdoc:27017 --sink mongodb://sink:27017
"""

import argparse
import logging
import sys
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

import pymongo
from pymongo import DeleteOne, ReplaceOne
from pymongo.errors import (AutoReconnect, BulkWriteError, ConnectionFailure, OperationFailure,
                            PyMongoError, WriteConcernError)

LOG = logging.getLogger("eloq_cdc_consumer")

PROGRESS_COLLECTION = "__eloq_cdc_progress"
SYSTEM_DATABASES = ("admin", "local", "config")
# A database's view definitions, one document per view: {_id: "<db>.<view>", viewOn, pipeline,
# collation}. The source reports a view's create, collMod and drop as an insert, a replace and a
# delete there.
VIEWS_COLLECTION = "system.views"
# The only system collection whose documents are replicated: stored JavaScript is user data.
REPLICATED_SYSTEM_COLLECTIONS = ("system.js",)
VIEW_FIELDS = ("viewOn", "pipeline", "collation")

# Server error codes the consumer acts on.
NAMESPACE_NOT_FOUND = 26
INDEX_NOT_FOUND = 27
NAMESPACE_EXISTS = 48
INDEX_OPTIONS_CONFLICT = 85
INDEX_KEY_SPECS_CONFLICT = 86
CONFLICTING_OPERATION_IN_PROGRESS = 117  # eloqClusterGate: the gate is already paused
CHANGE_STREAM_HISTORY_LOST = 286
CHANGE_STREAM_START_NOT_READY = 5000

# Errors that clear up on their own: a retry with backoff, not a halt.
TRANSIENT_CODES = {
    6,      # HostUnreachable
    7,      # HostNotFound
    64,     # WriteConcernFailed: wtimeout; every sink write the consumer retries is idempotent
    89,     # NetworkTimeout
    91,     # ShutdownInProgress
    112,    # WriteConflict
    189,    # PrimarySteppedDown
    10107,  # NotMaster
    11600,  # InterruptedAtShutdown
    11602,  # InterruptedDueToReplStateChange
    13435,  # NotMasterNoSlaveOk
    13436,  # NotMasterOrSecondary
}

# Token layout (§5.6): hex of 0x02 | u64 timestamp | u8 kind | ...
TOKEN_KIND_EVENT = 0
TOKEN_KIND_WATERMARK = 1


class ConsumerHalted(Exception):
    """An error the consumer must not guess past: it stops with the saved token intact."""


def token_timestamp(token):
    """The timestamp, in microseconds, a resume token records."""
    return int(token["_data"][2:18], 16)


def token_kind(token):
    """The kind a resume token records: 0 event, 1 watermark, 2 invalidate."""
    return int(token["_data"][18:20], 16)


def is_transient(exc):
    """Whether an error clears up on its own and the work should be retried."""
    if isinstance(exc, (AutoReconnect, ConnectionFailure)):
        return True
    if isinstance(exc, BulkWriteError):
        errors = exc.details.get("writeErrors", []) + exc.details.get("writeConcernErrors", [])
        return bool(errors) and all(e.get("code") in TRANSIENT_CODES for e in errors)
    if isinstance(exc, OperationFailure):
        return exc.code in TRANSIENT_CODES
    return False


def retry_transient(what, fn, stop=None, first_delay=0.1, max_delay=5.0):
    """Runs fn until it succeeds, backing off on transient errors; any other error halts."""
    delay = first_delay
    while True:
        try:
            return fn()
        except PyMongoError as exc:
            if not is_transient(exc):
                raise ConsumerHalted(f"{what}: {exc}") from exc
            LOG.warning("%s: transient error, retrying in %.1f s: %s", what, delay, exc)
        if stop is not None and stop.is_set():
            raise ConsumerHalted(f"{what}: stopped while retrying")
        time.sleep(delay)
        delay = min(delay * 2, max_delay)


def replicated_collection(coll):
    """Whether a collection's documents are replicated, in the copy and in the stream alike."""
    return not coll.startswith("system.") or coll in REPLICATED_SYSTEM_COLLECTIONS


def view_definition(options):
    """A view's create options as the sink takes them: viewOn, pipeline and collation, if any."""
    return {k: options[k] for k in VIEW_FIELDS if k in options}


def strip_index_spec(spec):
    """An index spec as the sink builds it: unique and expireAfterSeconds removed (§6.3, §6.4)."""
    return {k: v for k, v in spec.items() if k not in ("unique", "expireAfterSeconds", "ns")}


def strip_collection_options(options):
    """Collection options as the sink creates them: never capped, no source identity (§6.4)."""
    drop = ("capped", "size", "max", "autoIndexId", "uuid", "idIndex")
    return {k: v for k, v in options.items() if k not in drop}


class MongoSink:
    """The sink's operations the apply loop and the bootstrap use, over a pymongo client.

    Every write is made under the client's write concern, so a checkpoint saved after it never
    gets ahead of what the sink has made durable."""

    def __init__(self, client):
        self.client = client

    def bulk_write(self, db, coll, ops):
        self.client[db][coll].bulk_write(ops, ordered=True, bypass_document_validation=True)

    def _ddl(self, db, cmd):
        """Runs a DDL command under the sink's write concern. Database.command() sends the command
        as given and ignores a writeConcernError in a successful reply, so both are done here."""
        database = self.client[db]
        write_concern = database.write_concern.document
        if write_concern:
            cmd = dict(cmd, writeConcern=write_concern)
        reply = database.command(cmd)
        error = reply.get("writeConcernError")
        if error:
            raise WriteConcernError(error.get("errmsg", "write concern error"), error.get("code"),
                                    reply)
        return reply

    def create_collection(self, db, coll, options):
        """Creates a collection; an existing one with the same options is fine."""
        try:
            self._ddl(db, dict({"create": coll}, **options))
        except OperationFailure as exc:
            if exc.code != NAMESPACE_EXISTS:
                raise
            existing = self.collection_options(db, coll)
            if strip_collection_options(existing) != strip_collection_options(options):
                raise ConsumerHalted(
                    f"create {db}.{coll}: the sink already holds it with other options "
                    f"({existing} != {options}); the cluster gate was bypassed (bootstrap, §6.2)")

    def collection_options(self, db, coll):
        for info in self.client[db].list_collections(filter={"name": coll}):
            return info.get("options", {})
        return {}

    def drop_collection(self, db, coll):
        try:
            self._ddl(db, {"drop": coll})
        except OperationFailure as exc:
            if exc.code != NAMESPACE_NOT_FOUND:
                raise

    def create_indexes(self, db, coll, specs):
        try:
            self._ddl(db, {"createIndexes": coll, "indexes": [strip_index_spec(s) for s in specs]})
        except OperationFailure as exc:
            if exc.code in (INDEX_KEY_SPECS_CONFLICT, INDEX_OPTIONS_CONFLICT):
                raise ConsumerHalted(
                    f"createIndexes on {db}.{coll}: {exc}; the cluster gate was bypassed "
                    "(bootstrap, §6.2)") from exc
            raise

    def drop_index(self, db, coll, name):
        try:
            self._ddl(db, {"dropIndexes": coll, "index": name})
        except OperationFailure as exc:
            if exc.code not in (INDEX_NOT_FOUND, NAMESPACE_NOT_FOUND):
                raise

    def put_view(self, db, name, definition):
        """Creates a view, or redefines the one already there; a view's collation is fixed when it
        is created, on the source too, so a redefinition never changes it."""
        try:
            self._ddl(db, dict({"create": name}, **view_definition(definition)))
        except OperationFailure as exc:
            if exc.code != NAMESPACE_EXISTS:
                raise
            self._ddl(db, {"collMod": name, "viewOn": definition["viewOn"],
                           "pipeline": definition["pipeline"]})

    def drop_view(self, db, name):
        self.drop_collection(db, name)

    def drop_views(self, db):
        """Drops every view of a database: its system.views was dropped on the source."""
        for info in list(self.client[db].list_collections(filter={"type": "view"})):
            self.drop_view(db, info["name"])

    def drop_database(self, db):
        self.client.drop_database(db)

    def database_names(self):
        return self.client.list_database_names()


class Progress:
    """The consumer's checkpoint in the sink: admin.__eloq_cdc_progress, one document."""

    def __init__(self, sink_client, name="progress"):
        self.coll = sink_client["admin"][PROGRESS_COLLECTION]
        self.name = name

    def load(self):
        """The saved token, or None when the consumer must bootstrap."""
        doc = self.coll.find_one({"_id": self.name})
        if doc is None or doc.get("state") != "streaming":
            return None
        return doc["token"]

    def save(self, token):
        self.coll.replace_one({"_id": self.name},
                              {"_id": self.name, "state": "streaming", "token": token},
                              upsert=True)

    def mark_bootstrapping(self):
        self.coll.replace_one({"_id": self.name}, {"_id": self.name, "state": "bootstrapping"},
                              upsert=True)


def slot_of(event, slots):
    """The slot of a DML event: every event of one document maps to the same slot (§6.1)."""
    ns = event["ns"]
    key = bytes(event["eloq"]["keyBytes"])
    return zlib.crc32(f'{ns["db"]}\0{ns["coll"]}\0'.encode() + key) % slots


def dml_op(event):
    """The sink write for a DML event."""
    key = {"_id": event["documentKey"]["_id"]}
    if event["operationType"] == "delete":
        return DeleteOne(key)
    return ReplaceOne(key, event["fullDocument"], upsert=True)


DDL_TYPES = ("create", "drop", "createIndexes", "dropIndexes")
DML_TYPES = ("insert", "replace", "delete")


class Applier:
    """Applies events to the sink in the order §6.1 requires, over a pool of slots."""

    def __init__(self, sink, progress, slots=8, stop=None, in_scope=None):
        """in_scope(db) says whether events in database db are applied; default all of them. The
        stream is deployment-wide, so the events of every other database are skipped here."""
        self.sink = sink
        self.progress = progress
        self.slots = slots
        self.stop = stop or threading.Event()
        self.in_scope = in_scope or (lambda db: True)
        self.pool = ThreadPoolExecutor(max_workers=slots, thread_name_prefix="slot")

    def close(self):
        self.pool.shutdown(wait=True)

    def apply_batch(self, events, batch_token):
        """Applies one batch, then saves batch_token: the last event's or the batch's watermark."""
        dml = []
        for event in events:
            kind = event["operationType"]
            if kind == "invalidate":
                raise ConsumerHalted("invalidate on a deployment-level stream (§5.4)")
            db, coll = event["ns"]["db"], event["ns"].get("coll", "")
            if not self.in_scope(db):
                continue
            if kind in DML_TYPES and kind != "delete" and "fullDocument" not in event:
                # Point-reading the document by its key (§5.8) is plan E3, not built yet.
                raise ConsumerHalted(
                    f"{kind} on {db}.{coll} arrived without its document (fullDocumentOmitted: an "
                    f"event over 16 MB), which this consumer cannot apply yet: "
                    f"{event['documentKey']}")
            if coll == VIEWS_COLLECTION:
                # A view's definition changed: a DDL on the sink, behind the barrier.
                self._apply_dml(dml)
                dml = []
                self._apply_view_event(event)
                self._save(event["_id"])
                continue
            if not replicated_collection(coll):
                continue
            if kind in DML_TYPES:
                dml.append(event)
            elif kind in DDL_TYPES:
                self._apply_dml(dml)
                dml = []
                self._apply_ddl(event)
                self._save(event["_id"])
            else:
                raise ConsumerHalted(f"unknown event type {kind!r}: {event}")
        self._apply_dml(dml)
        if batch_token is not None:
            self._save(batch_token)

    def _save(self, token):
        retry_transient("save progress", lambda: self.progress.save(token), self.stop)

    def _apply_dml(self, events):
        if not events:
            return
        per_slot = [[] for _ in range(self.slots)]
        for event in events:
            per_slot[slot_of(event, self.slots)].append(event)
        futures = [self.pool.submit(self._apply_slot, share) for share in per_slot if share]
        # The barrier: nothing after these events runs before all of them are applied.
        for future in futures:
            future.result()

    def _apply_slot(self, events):
        """One slot's share: per collection, in stream order, as ordered bulk writes."""
        groups = []
        for event in events:
            ns = (event["ns"]["db"], event["ns"]["coll"])
            if groups and groups[-1][0] == ns:
                groups[-1][1].append(dml_op(event))
            else:
                groups.append((ns, [dml_op(event)]))
        for (db, coll), ops in groups:
            retry_transient(f"bulkWrite on {db}.{coll}",
                            lambda: self.sink.bulk_write(db, coll, ops), self.stop)

    def _apply_view_event(self, event):
        """An event on system.views, as the view command it stands for."""
        db, kind = event["ns"]["db"], event["operationType"]
        if kind in ("insert", "replace"):
            name = event["fullDocument"]["_id"][len(db) + 1:]
            fn = lambda: self.sink.put_view(db, name, event["fullDocument"])
        elif kind == "delete":
            name = event["documentKey"]["_id"][len(db) + 1:]
            fn = lambda: self.sink.drop_view(db, name)
        elif kind == "drop":
            fn = lambda: self.sink.drop_views(db)
        else:
            # The collection's own create and index events: the sink keeps its system.views itself.
            return
        LOG.info("applying a view %s on %s", kind, db)
        retry_transient(f"view {kind} on {db}", fn, self.stop)

    def _apply_ddl(self, event):
        db, coll = event["ns"]["db"], event["ns"]["coll"]
        kind = event["operationType"]
        desc = event.get("operationDescription", {})
        if kind == "create":
            fn = lambda: self.sink.create_collection(db, coll, desc)
        elif kind == "drop":
            fn = lambda: self.sink.drop_collection(db, coll)
        elif kind == "createIndexes":
            fn = lambda: self.sink.create_indexes(db, coll, desc["indexes"])
        else:
            def fn():
                for spec in desc["indexes"]:
                    self.sink.drop_index(db, coll, spec["name"])
        LOG.info("applying %s on %s.%s", kind, db, coll)
        retry_transient(f"{kind} on {db}.{coll}", fn, self.stop)


def open_stream(source, resume_after=None, max_await_ms=1000, stop=None):
    """Opens the deployment-level stream, retrying ChangeStreamStartNotReady (§5.1)."""
    delay = 0.5
    while True:
        try:
            return source.watch(resume_after=resume_after, max_await_time_ms=max_await_ms)
        except OperationFailure as exc:
            if exc.code != CHANGE_STREAM_START_NOT_READY and not is_transient(exc):
                raise ConsumerHalted(f"opening the change stream: {exc}") from exc
            LOG.info("change stream not ready yet, retrying in %.1f s: %s", delay, exc)
        except PyMongoError as exc:
            if not is_transient(exc):
                raise ConsumerHalted(f"opening the change stream: {exc}") from exc
        if stop is not None and stop.is_set():
            raise ConsumerHalted("stopped while opening the change stream")
        time.sleep(delay)
        delay = min(delay * 2, 5.0)


def drain(stream, max_events):
    """Up to max_events events, and the token valid once all of them are applied."""
    events = []
    while len(events) < max_events:
        event = stream.try_next()
        if event is None:
            break
        events.append(event)
    return events, stream.resume_token


class Consumer:
    """Bootstraps the sink when it holds no streaming checkpoint, then applies the stream."""

    def __init__(self, source, sink_client, databases=None, slots=8, batch=1000,
                 max_await_ms=1000, progress_name="progress", fail_before_publish=False,
                 before_copy_collection=None, unsafe_skip_gate=False):
        """before_copy_collection(db, coll) and unsafe_skip_gate are test hooks: the first runs
        before each collection's copy, the second bootstraps without the cluster gate, to show
        what the gate prevents. Neither is reachable from the command line."""
        self.source = source
        self.sink_client = sink_client
        self.sink = MongoSink(sink_client)
        self.progress = Progress(sink_client, progress_name)
        self.databases = databases
        self.slots = slots
        self.batch = batch
        self.max_await_ms = max_await_ms
        self.fail_before_publish = fail_before_publish
        self.before_copy_collection = before_copy_collection
        self.unsafe_skip_gate = unsafe_skip_gate
        self.stop = threading.Event()
        self.last_token = None

    def run(self):
        """Runs until stop is set or an error halts it."""
        if not self.sink_client.write_concern.acknowledged:
            raise ConsumerHalted("the sink's write concern is unacknowledged (w: 0): a checkpoint "
                                 "could get ahead of the writes it covers")
        token = self.progress.load()
        if token is None:
            token = self.bootstrap()
        self.stream_from(token)

    def in_scope(self, db):
        if db in SYSTEM_DATABASES:
            return False
        return self.databases is None or db in self.databases

    def gate(self, op):
        if self.unsafe_skip_gate:
            if op == "pause":
                # Any timestamp T0 can stand on: the stream's first token qualifies.
                return 0
            return None
        reply = retry_transient(
            f"eloqClusterGate {op}",
            lambda: self.source.admin.command({"eloqClusterGate": {"op": op, "mode": "ddl"}}),
            self.stop)
        return reply["pauseTs" if op == "pause" else "resumeTs"]

    def bootstrap(self):
        """§6.2 steps 1 to 6; returns T0, already published."""
        # 1. Withdraw the checkpoint before touching the sink.
        retry_transient("mark bootstrapping", self.progress.mark_bootstrapping, self.stop)
        for db in self.sink.database_names():
            if self.in_scope(db):
                LOG.info("bootstrap: dropping %s on the sink", db)
                retry_transient(f"drop {db}", lambda db=db: self.sink.drop_database(db), self.stop)
        # 2. Pause the gate.
        try:
            pause_ts = self.gate("pause")
        except ConsumerHalted as exc:
            cause = exc.__cause__
            if isinstance(cause, OperationFailure) and \
                    cause.code == CONFLICTING_OPERATION_IN_PROGRESS:
                raise ConsumerHalted(
                    "the cluster gate is already paused: another bootstrap is running, an "
                    "earlier one was abandoned, or this consumer's own pause was completed after "
                    "its reply was lost. An operator must decide (runbook, §8)") from cause
            raise
        LOG.info("bootstrap: gate paused at %d", pause_ts)
        try:
            # 3. T0: a watermark at or above pause_ts, or an event token above it.
            t0 = self.pick_t0(pause_ts)
            LOG.info("bootstrap: T0 %s (ts %d)", t0["_data"][:20], token_timestamp(t0))
            # 4. Copy definitions and documents; no DDL runs meanwhile.
            for db in self.source.list_database_names():
                if self.in_scope(db):
                    self.copy_database(db)
        except BaseException:
            # The checkpoint says bootstrapping, so the next run copies again from step 1; the gate
            # need not wait for an operator meanwhile. Only a consumer that dies leaves it paused.
            self.resume_after_failure()
            raise
        # 5. Resume the gate.
        self.gate("resume")
        LOG.info("bootstrap: gate resumed")
        if self.fail_before_publish:
            raise ConsumerHalted("test hook: failing between the copy and publishing T0")
        # 6. Publish T0: the copy is acknowledged under the sink's write concern.
        retry_transient("publish T0", lambda: self.progress.save(t0), self.stop)
        return t0

    def resume_after_failure(self):
        """Resumes the gate for a bootstrap that failed after pausing it, as far as it can."""
        try:
            self.gate("resume")
            LOG.info("bootstrap failed; the gate is resumed")
        except Exception as exc:  # noqa: BLE001 - the original failure is what the caller reports
            LOG.error("bootstrap failed and the gate could not be resumed; it stays paused until "
                      "an operator runs eloqClusterGate {op: \"resume\"}: %s", exc)

    def pick_t0(self, pause_ts):
        with open_stream(self.source, None, self.max_await_ms, self.stop) as stream:
            while not self.stop.is_set():
                stream.try_next()
                token = stream.resume_token
                if token is None:
                    continue
                kind, ts = token_kind(token), token_timestamp(token)
                if (kind == TOKEN_KIND_WATERMARK and ts >= pause_ts) or \
                        (kind == TOKEN_KIND_EVENT and ts > pause_ts):
                    return token
        raise ConsumerHalted("stopped while picking T0")

    def copy_database(self, db):
        for info in list(self.source[db].list_collections()):
            if info.get("type", "collection") != "view" and replicated_collection(info["name"]):
                self.copy_collection(db, info["name"])
        # A view has no indexes or documents of its own; only its definition is copied, read from
        # system.views, the documents whose changes the stream reports, so the copy and the replay
        # start from the same records.
        for definition in list(self.source[db][VIEWS_COLLECTION].find()):
            name = definition["_id"][len(db) + 1:]
            retry_transient(f"create view {db}.{name}",
                            lambda: self.sink.put_view(db, name, definition), self.stop)

    def copy_collection(self, db, coll):
        if self.before_copy_collection is not None:
            self.before_copy_collection(db, coll)
        LOG.info("bootstrap: copying %s.%s", db, coll)
        options = {}
        for info in self.source[db].list_collections(filter={"name": coll}):
            options = strip_collection_options(info.get("options", {}))
        retry_transient(f"create {db}.{coll}",
                        lambda: self.sink.create_collection(db, coll, options), self.stop)
        specs = [spec for spec in self.source[db][coll].list_indexes()
                 if spec["name"] != "_id_"]
        if specs:
            specs = [dict(spec) for spec in specs]
            retry_transient(f"createIndexes on {db}.{coll}",
                            lambda: self.sink.create_indexes(db, coll, specs), self.stop)
        ops = []
        for doc in self.source[db][coll].find(batch_size=1000):
            ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            if len(ops) == 1000:
                self._copy_batch(db, coll, ops)
                ops = []
        if ops:
            self._copy_batch(db, coll, ops)

    def _copy_batch(self, db, coll, ops):
        retry_transient(f"copy into {db}.{coll}",
                        lambda: self.sink_client[db][coll].bulk_write(
                            ops, ordered=False, bypass_document_validation=True),
                        self.stop)

    def stream_from(self, token):
        applier = Applier(self.sink, self.progress, self.slots, self.stop, self.in_scope)
        try:
            with open_stream(self.source, token, self.max_await_ms, self.stop) as stream:
                while not self.stop.is_set():
                    try:
                        events, batch_token = drain(stream, self.batch)
                    except OperationFailure as exc:
                        if exc.code == CHANGE_STREAM_HISTORY_LOST:
                            raise ConsumerHalted(
                                "the stream's history is gone (ChangeStreamHistoryLost): the "
                                "sink must be bootstrapped again") from exc
                        raise ConsumerHalted(f"reading the change stream: {exc}") from exc
                    if events or batch_token != self.last_token:
                        applier.apply_batch(events, batch_token)
                        self.last_token = batch_token
        finally:
            applier.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="EloqDoc connection string")
    parser.add_argument("--sink", required=True, help="the sink's connection string; its write "
                        "concern is the copy's and the checkpoint's")
    parser.add_argument("--databases", nargs="*",
                        help="databases to mirror; default every one but admin, local, config")
    parser.add_argument("--slots", type=int, default=8)
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--max-await-ms", type=int, default=1000)
    parser.add_argument("--progress-name", default="progress",
                        help="the checkpoint document's _id, to run several consumers into one "
                        "sink")
    parser.add_argument("--fail-before-publish", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = Consumer(pymongo.MongoClient(args.source), pymongo.MongoClient(args.sink),
                        args.databases, args.slots, args.batch, args.max_await_ms,
                        args.progress_name, args.fail_before_publish)
    try:
        consumer.run()
    except ConsumerHalted as exc:
        LOG.error("halted: %s", exc)
        return 2
    except KeyboardInterrupt:
        consumer.stop.set()
    return 0


if __name__ == "__main__":
    sys.exit(main())
