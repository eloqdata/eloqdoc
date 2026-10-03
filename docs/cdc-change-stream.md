# EloqDoc Change Data Capture: log scan and change streams

**Status: design, implemented through milestone M2** (plan PR 9; the PR
table of [cdc-implementation-plan.md](cdc-implementation-plan.md) tracks what
remains, the cluster gate and the reference consumer among it). This document
specifies v1 of CDC for EloqDoc: a pull-model change stream served by the
EloqDoc runtime, sourced from the log service, and consumed by an external
process that writes into a destination MongoDB. The "Key files" section lists
where each piece lives or lands, and [cdc-operations.md](cdc-operations.md) is
the operator runbook. When implementation diverges from this document, fix the
document in the same change.

This is an EloqDoc design. It requires changes inside the data_substrate
submodule (the tx service's DDL state machine, the checkpoint watermark, and
the log service's RPC surface), which are specified here at the level of the
invariants they must establish; the submodule's own docs are the reference for
the surrounding mechanisms.

The design was settled against the code in this repo; every invariant it relies
on is cited. Section 2 is the part to re-verify if any cited code changes.

**Path conventions.** Paths starting with `tx_service/`, `eloq_log_service/`,
`store_handler/`, or `core/` are relative to
`src/mongo/db/modules/eloq/data_substrate/`, and the numbered docs
`NN-*.md` plus `standby_replication_protocol.md` live in that submodule's
`docs/`. Paths starting with `src/base/` or `src/eloq_*` are relative to
`src/mongo/db/modules/eloq/`. Paths starting with `src/mongo/` are relative to
the repository root.

## 1. Goal and scope

**Goal.** Deliver every committed document change and every collection-level
schema change of an EloqDoc cluster, in an order that is correct per document
and per collection, to a consumer that can apply it with plain replace and
delete operations. The v1 consumer copies data into a destination MongoDB.

**Shape.** MongoDB-style change stream: the client opens an `aggregate` with a
`$changeStream` stage, then pulls batches with `getMore`. The server keeps no
per-consumer state beyond a live cursor; the consumer persists its own resume
token. This is the same contract MongoDB gives its own consumers, so MongoDB
drivers and the MongoDB Kafka source connector work unchanged.

**Why not the alternatives considered.**

- Pushing from each logger into the sink on apply was rejected: it couples
  commit latency to the sink and needs a replicated per-sink watermark.
- Independent per-logger shippers with last-write-wins on the sink was rejected
  once the merge moved server-side: it needs a version field on every
  destination document and tombstones for deletes. The merge makes both
  unnecessary.
- The standby replication stream is memory-bounded with explicit re-bootstrap
  on overflow; the log service is durable and already timestamp-indexed.

**In v1.** Insert, replace and delete events with full documents; drop,
create, and index DDL (truncate is an EloqKV-only op, not issued by EloqDoc);
secondary indexes carried to the sink as **non-unique** (§6.3);
resume tokens and `postBatchResumeToken`;
collection-, database-, and deployment-level streams with the lifetime
contracts of §5.1 (collection-level `invalidate` included); a
reference consumer; snapshot-then-replay bootstrap; both the RocksDB and the
RocksDB-Cloud log states (§4.6).

**Out of v1.** Transaction atomicity on the sink (a source transaction's
events are applied one object at a time and can be observed partially,
§6.1); unique secondary indexes on the sink (§6.3); `updateDescription` (updates are reported as `replace`),
database-level invalidation (no `dropDatabase` in EloqDoc), pre-images,
`startAtOperationTime` other than via tokens, cursor
migration between EloqDoc nodes, rename collection, capped collections
(unsupported on the source outside `local`; never created on the sink,
§6.4), Kafka delivery.

## 2. Facts the design relies on

| # | Fact | Where |
|---|---|---|
| F1 | A tx's redo record is one `WriteLogRequest` with `commit_timestamp` and `node_txn_logs: map<ng_id, blob>`. Each blob is a sequence of per-table sections: `u8 name_len ‖ name ‖ u8 engine ‖ u8 type ‖ u32 len ‖ entries`, entry = serialized key ‖ `u8 op` ‖ serialized record unless `op == Delete`. Object-command sections (EloqKV) use the same header and can be skipped by `len`. | `tx_service/src/tx_execution.cpp` `FillDataLogRequest`, the format comment at the "construct one log_ng_blob per ng_id" loop |
| F2 | For EloqDoc the table name is the Mongo namespace with `TableType::Primary`, `TableEngine::EloqDoc`; secondary indexes are separate tables named namespace + prefix + index name; the `_id` index is the primary table itself. | `src/base/eloq_util.h` `MongoTableToTxServiceTableName`, `MongoIndexToTxServiceTableName`; `src/eloq_kv_engine.cpp` `getSortedDataInterface` |
| F3 | The key is `u16 len ‖ KeyString(_id value)`; the record is `size_t ‖ unpack_info ‖ size_t ‖ BSON`. Updates are whole-document replacements (`updateWithDamagesSupported` is false). | `src/base/eloq_key.h`, `src/base/eloq_record.h`, `src/eloq_record_store.cpp` |
| F4 | Commit timestamps are microseconds. For one key they strictly increase in serialization order: the candidate is forced above the previous version's commit ts of every key in the write set, and the write lock is held until the log write returns. | `tx_execution.cpp` `Process(SetCommitTsOperation&)`; `standby_replication_protocol.md` P2 |
| F5 | The log state stores data records under key `bswap64(commit_ts) ‖ bswap32(ng) ‖ bswap64(txn)`, so a range scan is timestamp-ordered. Arrival order is **not** timestamp order: a tx chooses its timestamp before the WriteLog RPC. | `10-log-service.md` §5 |
| F6 | `ckpt_ts` is strictly below the earliest commit-pending write of a node group: `ActiveTxMinTs = min(wlock_ts) − 1` over lock-holding txs, meta tables excluded; a tx publishes its write-lock ts before receiving a commit ts. Hence every data record with `commit_ts <= ckpt_ts` is already in the log. Each log group holds `last_ckpt_ts` per ng and `min_ckpt_ts_` across ngs, replicated and snapshotted. | `07-durability-and-recovery.md` §3.2, §9; `eloq_log_service/include/log_state.h` `UpdateCkptTs` |
| F7 | DDL ops differ in which phase holds the catalog **write lock**, the only lock that excludes DML (a write intent does not conflict with DML catalog read locks; `AcquireReadLock` ignores intents). *Create, drop, truncate* (`UpsertTableOp`): intent (`ReadForWrite`/OCC → `WriteIntent`) across the prepare log; write lock (`acquire_all_lock_op_`, `Write`/`Locking`) only before the commit log, released by `PostCommit`. *Index ops* (`UpsertTableIndexOp`): intent, then `upgrade_all_intent_to_lock_op_` to a write lock, then the prepare log; `D` is computed from the lock results; `PrepareCommit` downgrades back to an intent. In both groups the write lock drains DML read locks and refuses new ones; a DML that read the *old* schema can commit above the prepare-chosen `D` until that lock; a DML that reads the *new* schema commits above `D`, because a catalog read enters the read set at the catalog cc entry's commit ts and the entry carries `D` from then on (for index ops `PrepareCommit` installs it under the prepare-stage write lock, since PR 4; for create and drop `PostCommit` does); successive DDLs on one table get increasing `D`. The catalog map's reader-writer control block only rejects a second concurrent DDL (`AddReader` has no callers). | `tx_service/src/tx_upsert_table_operation.cpp` `UpsertTableOp` ctor, `RunUntilPrepareLog` and `AcquireWriteLocks`; `tx_service/src/tx_index_operation.cpp` ctor and `Forward()`; `tx_service/include/cc_protocol.h` `DeduceLockType`; `tx_service/src/cc/non_blocking_lock.cpp` `AcquireReadLock`, `AcquireWriteIntent`; `tx_service/include/cc/template_cc_map.h` `PrepareCommit` branch of `Execute(PostWriteAllCc&)` |
| F8 | After the prepare log is flushed a DDL only rolls forward, with two exceptions in the current code. `CreateTable` rolls back on a kv-store error before any DML could exist (a commit log marked rolled back at `commit_timestamp == 0`, which leaves no change event); §4.4(b) creates the kv tables after the prepare log, not retried, and writes the catalog row after the commit log, retried until it succeeds, so a create rolls back when its kv tables or its sequence table update fail, and drops what it created first. `AddIndex` rolls back after the prepare log only when the index build hits a constraint violation (`PACK_SK_ERR`, e.g. a key over the size limit; `UNIQUE_CONSTRAINT` does not arise from EloqDoc, which for now refuses to build a unique secondary index on a non-empty collection, because the build does not detect duplicate keys among existing documents): `commit_ts_ = tx_op_failed_ts_`, commit log with `commit_timestamp == 0`, then `kv_rollback_create_index_op_` (`tx_index_operation.cpp`, `generate_sk_parallel_op_` branch). Since PR 4 add index creates the index's kv table before the prepare log (a failure there fails the op with no log) and writes the kv catalog only after the commit log, so no kv failure rolls it back after the prepare log. For the stream it is a second event; its commit log carries a commit-stage timestamp and a rolled-back flag instead of `commit_timestamp == 0` (§4.3, §4.4(b)). `TruncateTable` is EloqKV-only (asserted in the catalog map) and never issued by EloqDoc. | `tx_service/include/type.h` `PostWriteType::PrepareCommit`; `tx_service/src/tx_upsert_table_operation.cpp` `UpsertTableOp::CreateUntilCommitLog` and `AbandonCreate`; `log_state.h` `UpsertSchemaOp`; `catalog_cc_map.h` `RollBackSchema` |
| F9 | Data log records carry no schema version (F1). Replay drops stale records two ways: (1) the table is resolved by name through `GetCcm`/`InitCcm`; a dropped table's catalog record is `Deleted`, so `InitCcm` fails and the record is skipped, and a dropped index table resolves to no cc map and is skipped; (2) if the table exists, a record whose `commit_ts` is below the map's `schema_ts_` is skipped as belonging to an earlier incarnation. (2) is exact because of two commit-path rules. A DML that read schema version `V` commits above `V`: the catalog entry's commit ts is the schema version (while an index op's dirty schema is installed, the index op's `D`, installed by its `PrepareCommit` since PR 4), the read-local stores it in the meta read set (`rw_set_.AddRead(addr, read_res.ts_, ...)`), and `Process(SetCommitTsOperation&)` raises the candidate above every meta read entry's `version_ts_`. And a DDL's `D` is raised above every DML whose post-read ran before the DDL's acquire-all: `PostReadCc` publishes the DML's commit ts into the shard's `LastReadTs` before releasing its catalog read lock, the acquire-all folds each core's `LastReadTs` into `last_vali_ts` (max across cores and node groups), and `D >= last_vali_ts + 1`. The second rule covers all earlier DML only when `D` is computed after a write lock that drained the read locks (index ops; and a create that follows a drop, since the drop's commit-stage lock drained the old incarnation). For the intent-only prepare of create and drop it covers only DML that had already post-read (F7). The stream cannot use (1), so it relies on (2)'s timestamp property alone (§5.5). | `tx_service/include/cc/cc_request.h` `ReplayLogCc::Execute`; `tx_service/src/cc/cc_shard.cpp` `InitCcm`; `tx_service/include/cc/template_cc_map.h` `Execute(ReplayLogCc&)`, `Execute(PostReadCc&)` |
| F10 | The 4.0 change-stream machinery exists in the EloqDoc tree: `$changeStream` parsing, the oplog-entry-to-event transform, `ResumeToken`, invalidate and close-cursor stages. It refuses non-replica-set deployments with error 40573. Its `ResumeToken` is keyed on the collection UUID; its transform classifies every command other than `applyOps`/`drop`/`renameCollection`/`dropDatabase` as `invalidate`; its check-invalidate stage ends collection-level streams on drop. None of that is reused beyond the stage parsing, the cursor plumbing, and `DocumentSourceCloseCursor` (§5.2). | `src/mongo/db/pipeline/document_source_change_stream.cpp` `buildPipeline`; `document_source_change_stream_transform.cpp` command branch; `document_source_check_invalidate.cpp`; `resume_token.h` |
| F11 | Every Mongo command in EloqDoc runs as a `boost::context` coroutine; blocking a worker thread is not acceptable. Async work yields with `yieldFuncPtr` and is resumed by a completion callback (the CDC path puts the wait object of §5.8 between callback and coroutine, because its callbacks can outlive the `getMore` that issued them). | `src/eloq_recovery_unit.cpp` (`coro.yieldFuncPtr` / `coro.resumeFuncPtr` uses) |
| F12 | The schema timestamp is the DDL's `D`, end to end. `D` is the tx's `commit_ts_` (chosen at prepare for `UpsertTableOp`, after the lock upgrade for `UpsertTableIndexOp`); every `PostWriteAll` carries it as `req.CommitTs()`. `PrepareCommit` installs the dirty schema with `dirty_schema_version_ = D` (`CreateDirtyCatalog` → `CatalogEntry::SetDirtySchema`); `PostCommit` promotes it at the op's commit timestamp (`CommitDirtySchema(commit_ts)`, `schema_version_ = P` for create/drop/truncate, `D` for index ops, §4.4(b)) and, for truncate, sets the cc map's `schema_ts_` to it via `UpdateCcmSchema`; the kv catalog is written with the same timestamp (`upsert_kv_table_op_.write_time_`, `UpsertCatalog(schema, commit_ts)`). `schema_ts_` is used by replay (F9) and by the runtime schema check on every DML request (`req.SchemaVersion() != schema_ts_` → rejected), which is what stops a DML that read the old schema from writing into the new incarnation after `PostCommit`. The prepare log's `SchemaOpMessage.catalog_ts` carries the *old* version. | `tx_service/src/tx_execution.cpp` `Process(PostWriteAllOp&)`; `tx_service/include/cc/catalog_cc_map.h` `PrepareCommit`/`PostCommit` branches; `tx_service/include/catalog_key_record.h` `SetDirtySchema`, `CommitDirtySchema`; `tx_service/src/cc/cc_shard.cpp` `UpdateCcmSchema`; `tx_service/include/cc/template_cc_map.h` `SchemaVersion()` checks; `FillPrepareLogRequest` (`tx_upsert_table_operation.cpp`, `tx_index_operation.cpp`) |
| F13 | Drivers implement the change-streams specification's opening rule: if the opening `aggregate` reply has an empty `firstBatch` and no `postBatchResumeToken`, and none of `resumeAfter`/`startAfter`/`startAtOperationTime` was given, the driver saves the reply's `operationTime` for a later `startAtOperationTime` resume, and **fails the open if it is absent** (PyMongo 4.8: `OperationFailure: Expected field 'operationTime' missing`). EloqDoc never emits `operationTime`: `appendClusterAndOperationTime` returns early unless the node runs in `modeReplSet` with the logical clock enabled. Hence the opening reply of a stream without a token **must carry a `postBatchResumeToken`**; later replies may omit it without breaking the driver, but every reply here carries one (§5.3 step 4). | `src/mongo/db/service_entry_point_common.cpp` `appendClusterAndOperationTime`, the `modeReplSet` test; the driver change-streams specification, "ChangeStream ... MUST save the operationTime" |

## 3. Architecture

```text
 EloqDoc node (mongod)                         log groups (braft, one leader each)
 ┌───────────────────────────────┐             ┌──────────────┐
 │ aggregate {$changeStream}     │  CdcScan    │ LogInstance  │ data CF  (ts,ng,txn) → blob
 │  ┌─────────────────────────┐  │────────────▶│   + LogState │ ddl history, in memory (ddl_ts,txn) → (D, op)
 │  │ internal stages §5.2    │  │◀────────────│              │ arrival_complete_ts = min_ckpt_ts_
 │  │  per-logger positions   │  │  records,   └──────────────┘
 │  │  pull lowest-ts head    │  │  arrival_complete_ts … one per log group …
 │  │  emit T once others     │  │
 │  │   scanned ≥ T−1  (§4.2) │  │
 │  └────────┬────────────────┘  │
 │  decoded events               │
 │  ┌────────▼────────────────┐  │
 │  │ event documents §5.4a   │  │   getMore (await, ≤ maxTimeMS)
 │  │ + tokens §5.6           │  │◀──────────────────────────────  consumer process
 │  └─────────────────────────┘  │──────────────────────────────▶  (driver watch())
 └───────────────────────────────┘   events + postBatchResumeToken     │ replaceOne / deleteOne
                                                                        │ token → sink admin.__eloq_cdc_progress
                                                                        ▼ destination MongoDB
```

Three components: the logger's scan RPC and DDL store (§4), the EloqDoc
internal change-stream stages and cursor (§5), and the consumer (§6). The scan RPC is the
same in every design variant discussed, so it can be built first.

## 4. Log service: `CdcScan` and the DDL store

### 4.1 RPC

Added to `service LogService` in `tx_service/tx-log-protos/log.proto`:

```protobuf
message CdcScanRequest {
  uint32 log_group_id = 1;
  // Where to start. Exactly one is set; neither set = start at the beginning
  // of complete history (retention_floor_ts, §4.5).
  oneof start {
    // Returned by the previous response for this group: the last returned
    // storage key of each stream, after which both resume. Its layout is
    // shared with the runtime, which embeds it in resume tokens (§5.6).
    bytes  continuation = 2;
    // First storage key with timestamp >= from_ts, in both streams. Used for a
    // fresh stream (§5.1) and for token resume (§5.6).
    uint64 from_ts      = 3;
  }
  uint32 max_bytes    = 4;      // soft cap on the response size
  uint32 wait_ms      = 5;      // long-poll: hold the request until something is emittable
}

message CdcDataRecord {
  uint64 commit_ts  = 1;
  uint32 cc_ng_id   = 2;
  uint64 txn_number = 3;
  bytes  blob       = 4;        // the per-ng blob exactly as stored (F1)
}

message CdcDdlRecord {
  uint64 ddl_ts     = 1;        // stream timestamp (§4.4); == catalog_ts for index ops
  uint64 catalog_ts = 2;        // D: the new catalog version
  uint64 txn_number = 3;
  SchemaOpMessage schema_op = 4;
}

message CdcScanResponse {
  enum Status { Success = 0; NotLeader = 1; PositionTruncated = 2; }
  Status status             = 1;
  string redirect           = 2;   // leader address on NotLeader
  repeated CdcDataRecord data = 3; // ascending (commit_ts, ng, txn); all commit_ts <= arrival_complete_ts
  repeated CdcDdlRecord  ddl  = 4; // ascending (ddl_ts, txn); all ddl_ts <= arrival_complete_ts
  uint64 arrival_complete_ts = 5;  // this group's min_ckpt_ts_, read before either scan
  uint64 retention_floor_ts  = 6;  // §4.5: history is complete above this; 0 = not tracked (v1)
  bytes  continuation        = 7;  // pass back to resume both streams without gaps or duplicates
  bool   has_more            = 8;  // true: the page was cut; records at or below arrival_complete_ts
                                   // remain beyond `continuation` (possibly at the page's last timestamp)
}

rpc CdcScan(CdcScanRequest) returns (CdcScanResponse);
```

### 4.2 Semantics

- **One timestamp order for both streams.** Data records are scanned from the
  timestamp-ordered data CF (key `(commit_ts, ng, txn)`); DDL records from
  the DDL history, ordered by `(ddl_ts, txn)` (§4.3). Both are
  returned ascending by timestamp and **only at or below `arrival_complete_ts`**, where
  `arrival_complete_ts` is the group's `min_ckpt_ts_`. By F6 no data record at or below
  `arrival_complete_ts` can still arrive; by the §4.4 invariant no DDL record can either.
  So a caller that has consumed everything through `arrival_complete_ts` from every
  group has a complete prefix. `arrival_complete_ts` is the upper end of the
  readable window; its lower end is `retention_floor_ts` (§4.5), above which
  nothing has been purged. A scan is correct only inside
  `(retention_floor_ts, arrival_complete_ts]`: the first moves with purges,
  the second with checkpoints, and neither is derivable from the other.
- **Read order inside the logger.** Read `min_ckpt_ts_` into `arrival_complete_ts`
  *before* scanning either store. For data this is immaterial (F6). For DDL it
  is required: a DDL is appended with `ddl_ts > min_ckpt_ts_` *at append time*,
  so one appended after a DDL scan but before a later `arrival_complete_ts` read could
  have `ddl_ts <= arrival_complete_ts` yet be missing. With `arrival_complete_ts` read first, any DDL
  appended afterwards has `ddl_ts` above the `min_ckpt_ts_` of that later
  moment, hence above `arrival_complete_ts`, and is not needed for this response.
- **`min_ckpt_ts_` never runs ahead of the stored data log.** A scan reads
  the store, not the raft log, so every data record at or below
  `min_ckpt_ts_` must already be readable there when that value is
  published. `on_apply` writes data records at the end of a batch but applies
  an `UpdateCkptTs` entry at once, so this holds only because a covering
  checkpoint entry is never in the same batch as the records it covers:
  - on the leader, a `WriteLog` is acknowledged only after its batch is
    written (`LogInstance::BatchAddLogItems` runs the closures after
    `FlushLogItemBatch`), and the transaction keeps its write locks until
    then, so no checkpoint reaches its commit timestamp before the record is
    readable (F6); the covering `UpdateCkptTs` is proposed afterwards;
  - a new leader answers scans only from `on_leader_start`, which braft runs
    once the leader's first configuration entry is applied, after every
    earlier entry has gone through `on_apply` (each call ends with a write);
  - followers do not serve scans.
  Acknowledging a `WriteLog` before its record is written would break this;
  `on_apply` would then have to write its pending data records to the store
  (`BatchAddLogItems`) before applying each `UpdateCkptTs` entry, as it
  already does before a `ReplayLog` entry.
- **Starting a scan.** `from_ts` positions both streams at the first storage
  key with timestamp `>= from_ts`; a `continuation` resumes exactly after the
  last returned keys; neither means the beginning of complete history. The
  cursor uses `from_ts` to open a fresh stream (§5.1) and to resume from a
  token (§5.6), and `continuation` for every page after that.
- **Paging.** The logger walks the data and DDL stores as one sequence
  merged by timestamp, with every data record ahead of every DDL record at
  one timestamp, and returns each page as a **prefix of that merged
  sequence**: if the page contains any record at timestamp `T`, it contains
  every record of both streams below `T`. The tie rule carries no meaning of
  its own (order within a timestamp is free, C2), but it is fixed, so that
  each stream can resume from its own last key. A page is cut at `max_bytes`,
  and the cut may fall anywhere, including inside one timestamp. The
  `continuation` encodes, per stream, the exact last storage key it returned,
  after which it resumes; passing it back resumes both exactly, so pages
  neither omit nor duplicate records. A stream that has returned nothing
  keeps where it started: empty for the beginning of complete history, or,
  after a start at `from_ts`, the eight-byte big-endian `from_ts` itself,
  which sorts before every storage key at that timestamp (each begins with
  its big-endian timestamp and is longer), so "after" it means "at
  `from_ts`". That form exists only in continuations: a token entry (§5.6)
  is built from records the runtime produced, so its keys are always real.
  Records are returned as stored blobs; the logger does not decode them.
  `arrival_complete_ts` is returned on every response, including empty ones.
- **Completeness boundary.** From each response the caller derives an
  *inclusive completeness boundary* for the group, `scanned_ts`: every
  record of both streams with timestamp `<= scanned_ts` has now been
  returned to it. Rule: if `has_more`, `scanned_ts = last_ts − 1`, where
  `last_ts` is the highest timestamp in the page, because the page may
  have been cut inside `last_ts` and more records at `last_ts` may follow;
  if `!has_more`, `scanned_ts = arrival_complete_ts`, since the scan is
  exhausted through the bound. An empty response with `!has_more` thus
  proves the whole interval up to `arrival_complete_ts` empty and advances
  the boundary across idle time. The prefix property is what makes a single
  boundary serve both streams; without it the caller would keep one
  boundary per stream and take their minimum, which is the same rule
  applied twice. `arrival_complete_ts` establishes that the records have
  arrived at the logger; `scanned_ts` establishes that the caller has
  consumed them. A timestamp `T` is complete for a group only when
  `T <= scanned_ts`.
- **Emission rule.** Records are emitted in the order they are pulled,
  merged across groups by timestamp; completeness *at* a timestamp is
  never required. A record at timestamp `T` from group `g` may be emitted
  as soon as **`scanned_ts_h >= T − 1` for every other group `h`**, i.e.
  no other group still holds an unfetched record strictly below `T`. Two
  facts make this sufficient. First, every record at or below `T` from `g`
  that the cursor has already pulled arrived in storage order, and every
  record at or below `T − 1` from other groups is in hand, so nothing
  emitted later can be *older* than this one; per-object order (C2) is the
  only cross-record order the stream promises, and one object never
  changes twice in one microsecond, so order *within* `T` is free. Second,
  `scanned_ts_h >= T − 1` implies `arrival_complete_ts_h >= T − 1`, so no
  record below `T` can still arrive at `h`; a record at exactly `T` may,
  and that is fine for the same reason. In particular
  **`scanned_ts < arrival_complete_ts` holds nothing back**: it is the
  normal state while a cursor pages through a backlog. Only records whose
  timestamp exceeds some other group's `scanned_ts + 1` wait, and they
  wait for that group's scan, not for the logger.
- **Long poll.** If nothing is emittable and `wait_ms > 0`, the request is
  parked and completed when `min_ckpt_ts_` advances (from `on_apply` of an
  `UpdateCkptTs` entry) or when the deadline fires. Nothing else can make a
  record emittable: a data record at or below the bound never arrives
  later (F6), and a DDL record is appended with `ddl_ts` above the bound
  (§4.4), so neither kind of append wakes a parked request. Parked
  closures live in a list under a plain mutex and are completed
  from a `bthread_timer_add` callback or from the apply path; no bthread
  condition variable is involved (see the threading gotcha in the
  data_substrate `CLAUDE.md`).
- **Leader only.** Followers answer `NotLeader` with `redirect`. Followers hold
  the same data and could serve later; not in v1.
- **History loss.** A `continuation` or `from_ts` whose timestamp is at or
  below the group's `retention_floor_ts` (§4.5) is answered with
  `PositionTruncated`; the runtime turns this into `ChangeStreamHistoryLost`
  (code 286) and the consumer must re-bootstrap (§6.2). A request with
  neither never produces `PositionTruncated`: it means "start at the
  beginning of complete history", and the logger positions the scan at the
  boundary itself, never below it. The check is the logger's responsibility;
  the runtime never inspects storage or infers retention. **In v1 the
  boundary is not tracked** (`retention_floor_ts = 0`, no
  `PositionTruncated` is ever returned) and correctness assumes the
  requested history is still retained; see §4.5.

### 4.3 DDL store

Schema ops today live in the in-memory `tx_catalog_ops_` map and are erased one
hour after `min_ckpt_ts_` passes them. That is the wrong lifetime for CDC, so
the logger keeps a separate DDL history, also in memory:

- **One record per schema op**: `(D, txn, SchemaOpMessage)`, with `D` the
  catalog version, at the record's stream timestamp `ddl_ts` (§4.4). An
  index op that adds or drops several indexes, as a `createIndexes` with
  several specs does (the tx service builds them in one scan), is one
  record and yields one event listing all of them (§5.4a).
- **Position `(ddl_ts, txn)`**. DDLs on one table are serialized by its
  catalog write lock (F7), so they have distinct timestamps; only DDLs on
  different tables, hence of different transactions, can share one, and
  the transaction number tells them apart. The two records of a
  rolled-back add index share a transaction but not a timestamp (`D` and
  `P`). The order among records at one timestamp carries no meaning; the
  transaction number only makes it stable. There is no separate storage
  key: the history is ordered by this position, and wherever the
  continuation (§4.2) and tokens (§5.6) record a key for the data stream,
  they record this position for the DDL stream, encoded
  `be64 ddl_ts ‖ be64 txn`. That begins with the big-endian timestamp and
  is longer than eight bytes, as the continuation's timestamp form
  requires, and the runtime rebuilds it from the record's `ddl_ts` and
  `txn_number`.
- **Kept in memory and saved in the raft snapshot**, as `tx_catalog_ops_`
  already is, so it survives a restart; DDLs are rare, so its size is
  negligible. Trimmed with the data records: each purge of the data log
  also drops the DDL records below its floor (§4.5), not an hour after the
  checkpoint.
- No sequence counter: append order carries no meaning the stream needs,
  and the §4.4 invariant guarantees every record's `ddl_ts` is above
  `arrival_complete_ts` at append time.
- **When a record is appended.** Depends on which phase of the DDL holds the
  catalog **write lock** (F7), because that is the phase whose timestamp is
  synchronized with the checkpoint (§4.4):
  - *Index ops* (`UpsertTableIndexOp`: add/drop index) hold the write lock
    across the prepare log. Appended at `PrepareSchema` apply with
    `ddl_ts = D`. Semantically the index becomes visible to the stream when it
    becomes visible to DML, which matches MongoDB's background index build.
    - Drop index cannot roll back after the prepare log.
    - **Add index can** (F8), when the build hits a constraint violation.
      Since PR 4 a kv failure happens only before the prepare log (the index's
      kv table is created there), so it fails the op with no log and no
      event. After a violation the stream has already delivered
      `createIndexes`, so
      the rollback is a second DDL event, a `dropIndexes` for the same indexes.
    - *Decided:* the rolled-back add index writes its commit log with
      (a) a new commit timestamp `P`, computed from the commit-stage write
      locks as for drop (§4.4(b)), and (b) a flag marking the index dropped.
      It no longer uses `commit_timestamp == 0`. `P` is above `D` and above
      every DML that maintained the index before the commit-stage lock.
    - The `dropIndexes` record is appended at that commit log's apply with
      `ddl_ts = P`.
  - *Create and drop* (`UpsertTableOp`) hold only an intent across the
    prepare log and take the write lock before the **commit** log. Appended at
    `CommitSchema` apply with `ddl_ts` = the commit log's timestamp, which the
    coordinator re-assigns under the write lock for create, drop, and
    truncate alike (§4.4(b)). **Rolled-back ops.** A commit log marked
    rolled back (`SchemaOpMessage.rolled_back`) discards the op's dirty
    schema; its timestamp decides the stream. At timestamp 0 the op's DDL
    record is dropped at once and there is no event: a create whose kv tables
    or sequence table update failed once, before its commit log (§4.4(b)), and
    a logical alter whose catalog write failed write one. At a timestamp above 0 the
    rollback is a separate event at that timestamp: the rolled-back add
    index's `dropIndexes` at `P` (above). For drop this places the record
    after the DML of the old incarnation, which is what the source did; for
    create no DML can precede it.
- **What is recorded.** Only `SchemaOpMessage`s with `table_engine == EloqDoc`
  whose `table_op.op_type` is `CreateTable`, `DropTable`, `AddIndex` or
  `DropIndex`. Index ops arrive as a `table_op` too, with their index names in
  `alter_table_info_blob`; the `index_op` field is never set. **Not recorded
  in v1:** `UpsertTableOp`s of
  `OperationType::Update`, which is how a `collMod` reaches the tx service
  (`src/eloq_recovery_unit.cpp`, the catalog-record update path; validator
  and TTL changes go through `KVCollectionCatalogEntry` and end there), and
  the logical-alter schema logs embedded in `DataLogMessage.schema_logs`
  (single-key to multi-key index changes). Their consequences for the sink
  are handled by §6.4 rather than by events; mirroring `collMod` as a
  `modify` event is a follow-up (§9), since that op runs the same
  kv-write-before-lock branch that create had before §4.4(b).

### 4.4 DDL timestamps and the checkpoint bound

The stream needs this invariant: **when a DDL record is appended to a log
group, its `ddl_ts` is strictly greater than that group's `min_ckpt_ts_`.** Then
`ddl_ts` is above any bound the cursor has emitted, the record is emitted exactly
at `ddl_ts`, and resume is exact. Without it a DDL could reach the logger after
the cursor passed `ddl_ts`; placing it at a synthesized later position is not
resumable exactly, and a re-delivered drop after a recreate would destroy data
on the sink.

Today `ActiveTxMinTs` skips every lock on a meta table
(`tx_service/include/cc/cc_shard.h`, the `!TableName::IsMeta(...)` test;
`IsMeta` includes `TableType::Catalog`, `tx_service/include/type.h`), so no
DDL lock holds `ckpt_ts` back. Two changes establish the invariant.

**(a) Lock registry: count catalog write locks while they are held.**

- *Audit result.* The `table_type_` overwrite (last registration wins) is a
  real pre-existing hole. A write lock followed by any later registration in
  the same shard on a range, bucket or cluster-config entry made
  `!IsMeta(table_type_)` false and dropped the tx out of the checkpoint. Data
  txs on range-partitioned tables take such registrations: range read locks
  through `lock_range_op_`. Debug builds add one per fast catalog read. The
  registry-level hole is reproduced by `CkptLockRegistry-Test`. Whether a
  production lock sequence hits it was not traced. Either way, one table type
  cannot describe a tx whose locks span many tables. The field is removed,
  and the decision is made per write lock, when the lock is taken.
- *Decided: `wlock_ts_` is the tx's latest write-lock grant in the shard.*
  `UpsertLockHoldingTx` moves it to `Now()` on every write lock, on any
  table; read locks and intents leave it alone.
  - The guarantee: while a tx holds a counting write lock, the checkpoint
    stays below its latest write-lock grant. Outside that exclusive section
    the checkpoint may pass the tx; for a DDL that is expected, and the
    stream only needs the log record that each DDL writes under its write
    lock (§4.3).
  - DML chooses its commit timestamp after its last write lock, and each
    grant forces the commit above the granting shard's clock
    (`last_vali_ts`). So for DML the latest grant is also the tightest lower
    bound on the commit, and the freshest sign that the tx is alive.
  - The checkpoint, the orphan-lock recovery log search (`write_lock_ts`, the
    scan's lower bound), the committed-write window and the stall warning
    all want exactly that. It used to be the first grant.
- *Which txs count:* `TxLockInfo::holds_ckpt_wlock_`, set when a write lock
  is taken on a table for which `WriteLockHoldsBackCheckpoint(table type)`
  holds.
  - That is true for data tables and `TableType::Catalog`.
  - It is false for range, bucket and cluster-config entries, whose write
    locks split and migration operations hold for long stretches.
  - `ActiveTxMinTs` / `ActiveTxMaxTs` read `wlock_ts_` of the entries with
    the flag.
- *Downgrade.* `CcMap::DowngradeCceKeyWriteLock`
  (`tx_service/src/cc/cc_map.cpp`) is the single downgrade site, reached from
  the `PrepareCommit`/`DowngradeLock` branch of `Execute(PostWriteAllCc&)`.
  - It calls `CcShard::OnWriteLockDowngraded`, which clears the flag unless
    another counting write lock of the tx remains in the shard.
  - `wlock_ts_` stays: a downgrade acquires nothing.
  - The commit-stage upgrade sets the flag again and moves `wlock_ts_` to
    the upgrade time.
  - Release already erases the registry entry.
- `table_type_` is removed. Its other reader, `CcShard::ClearTx`'s
  keep-catalog-lock-objects check under `FLAGS_cmd_read_catalog`, now reads
  each entry's own table, as orphan-lock recovery already did.
- Decision recorded in `03-concurrency-control.md` §10.

With (a), a checkpoint computed while the write lock is held is below the
tx's latest write-lock grant on that shard, which for a DDL is its catalog
write lock. `AcquireAllCc`, which grants a DDL's catalog locks, returns
`last_vali_ts = max(LastReadTs(), Now())` (`TemplateCcMap::Execute(AcquireAllCc&)`),
as a DML's single-key write lock does, so a timestamp derived from the lock
results is above the acquisition time. The last
read ts covers a catalog reader only once it publishes its commit timestamp:
a regular catalog read lock does on release (`PostReadCc`), and so does
EloqKV's fast catalog read (`LocalCcHandler::ReleaseCatalogRead`).

**(b) Position rule per DDL group.**

- *Index ops*: `ddl_ts = D`. `D` is computed from the `upgrade_all_intent_to_lock_op_`
  results, i.e. after the write lock, so it is above the lock time; every
  old-schema DML was drained before the lock; every new-schema DML reads
  the catalog cc entry at commit ts `D` and commits above it. Exact with (a) alone.
  - Until PR 4 the entry kept the old schema version through the whole build, since
    `PrepareCommit` left its commit ts alone. So only the node clocks ordered a DML
    after `D`. A DML on a node whose clock trailed the fastest node's by more than
    the time from the lock to the DML could commit below `D`, and replay then dropped
    its write to the new index as older than the index map's `schema_ts_` (F9).
    `PrepareCommit` now installs `D` on the entry under the write lock and marks it
    loaded, and replay of the prepare log does the same.
  - The record is appended at the prepare log's apply (§4.3). The prepare
    log is written while the prepare-stage write lock is held, so `D` is
    above `min_ckpt_ts_` at append time.
  - The checkpoint passing `D` later, during the build under the write
    intent, is expected and harmless. From the prepare log on, DML maintains
    the new index, and the log carries those writes.
  - *Rolled-back add index:* position `P`, re-assigned after the commit-stage
    `acquire_all_lock_op_` exactly as for drop:
    `P = max(D + 1, max over results of (last_vali_ts + 1, commit_ts + 1))`.
    - Only one failure path is left after the prepare log, a *constraint
      violation* during the build, after the dirty schema is installed. It has
      a commit phase: it takes the commit-stage write lock first
      (`acquire_all_lock_op_`), computes `P` as above, writes a commit log
      at `P` (it used to carry `commit_timestamp == 0`), then runs a
      `PostCommit` that releases the catalog lock.
    - *Index kv table before the prepare log (PR 4).* The op used to run
      `kv_create_index_op_` after the install: the index's initial range
      record and the new schema in the kv catalog, retried up to 100 times,
      with a rollback (`commit_timestamp == 0`) on failure.
      - Recovery returns a node to the state right after the prepare log:
        replay installs the dirty schema, and DML writes the new index at
        once (EloqDoc `_creatingIndexes`). The checkpoint may flush it once
        `ckpt_ts >= dirty_schema_version_`. So the index's kv table must exist
        in every state recovery can reach, and the old order had a window in
        which it did not.
      - The kv table is now created before the prepare log, under the
        prepare-stage write lock, as for any table (`CreateKvTable`, no
        catalog row). If the creation or the prepare log fails, the op drops
        it and fails with no log, so the stream never sees the index. If the
        coordinator fails before that cleanup, the table stays orphaned. That
        is accepted: no schema refers to it, and correctness is unaffected.
      - The new schema is written to the kv catalog once, after the commit
        log, at `D` (`kv_update_schema_image_op_`, catalog only, retried until
        it succeeds under the commit-stage write lock). The prepare log
        carries the dirty schema until then, and the single write keeps the
        kv catalog version equal to the in-memory one (F12). Recovery alone
        would allow it after `PostCommit` (the commit log stays in the log
        state until the clean log), but a later DDL on the table could then
        write its catalog row first: the DSS RocksDB backends apply writes in
        arrival order, and a drop table deletes the row, leaving no version
        to compare against. The lock orders the two, as it does for create
        and drop table.
      - **Cost, accepted.** The prepare-stage write lock spans the kv table
        creation and the prepare log.
        - Writes to the table wait for the store, since the lock refuses DML
          catalog read locks.
        - With (a), the lock pins the node group's `ckpt_ts`, and therefore
          `arrival_complete_ts` on every logger, for that long. This is the
          same trade-off B3 accepts for create, but short: the creation is
          tried once, with no retry, and a failure fails the command.
        - Downgrading before the creation was rejected. Without the
          dirty-schema install, DML reads the old schema and commits above
          `D` without maintaining the index, and the build's scan skips
          documents above `D`.
    - *Done in PR 7.* The commit log carries `P` and a rolled-back mark
      (`SchemaOpMessage.rolled_back`), and everything that read
      `commit_timestamp == 0` as "this op failed" reads the mark instead:
      the log state keeps it at `CommitSchema` (`LogState::UpsertSchemaOp`),
      schema-op recovery hands it to the recovered op, catalog replay of the
      commit log builds the table from the current schema at `P`, and
      `UpsertTableIndexOp::Forward` keeps it in `rolled_back_`.
    - After the commit log, under the write lock: the index's kv tables are
      dropped and the catalog row is written with the table's current schema
      at `P` (the kv rollback), retried until they succeed; then the
      sequence-table cleanup and `clean_ccm_op_`.
    - The second phase is a `Rollback` (`PostWriteType::Rollback`, also a
      remote commit type) instead of a `PostCommit`. It drops the index's cc
      maps, ranges and statistics, as the zero-timestamp commit did, and
      keeps the table's current schema instead of promoting the dirty one.
      The catalog entry's version and its cc entry's commit timestamp become
      `P`: a later DDL on the table computes its timestamp above `P`, as for
      drop, and memory and the kv catalog both hold version `P` (F12). No
      schema object is rebuilt and no other cc map changes, since no table's
      version does.
    - The tables keep their own versions (see *Table versions* below): only
      the schema record moves to `P`. Re-committing an image that still
      recorded a table without its timestamp would have moved that table to
      `P`, and replay after a restart would then have skipped the table's
      earlier, not yet checkpointed writes as belonging to an older
      incarnation; `ddl_recovery.py` (iv) is the regression test.
- *Table versions and the schema record's version.* A collection is one
  schema record, whose version is the timestamp of the last DDL that changed
  it (the catalog row's timestamp and the catalog entry's commit timestamp:
  `P` for create, drop and truncate, `D` for index ops), holding several
  tables (the base table and one per index), each with its own version, the
  timestamp it was created at. A table's cc map carries its table's version,
  requests carry it, and replay skips a log record below it as belonging to
  an older incarnation. The schema image records every table's version
  (EloqDoc's `TableKeySchemaTs`). An engine builds a DDL's image before the
  DDL has a timestamp and records a table it creates without one (0, the
  unknown timestamp, which reads as the version the image is built at), so
  the tx service
  resolves the images once the prepare stage has chosen `D`, before anything
  is logged or installed (`SchemaOp::ResolveSchemaImages`): it builds the new
  schema at `D` and the current schema at its version, and keeps the
  objects' images, in which the engine writes each table's real version
  (`MongoTableSchema::RecordKeySchemasTs`; other engines keep their images).
  No image with an unresolved table version is logged, stored or sent to
  another node, and an image loaded at any version gives every table its own
  version. A table create makes gets `D`; the one truncate makes gets `P`,
  which B4 installs; a drop's does not matter. A CDC DDL event carries the
  schema record's version, the op's commit timestamp.
- *Drop*: the prepare-chosen `D` cannot be the position. A drop only excludes
  DML at its commit-stage write lock, so writes to the old incarnation
  legitimately commit in `(D, lock time)`; a stream with the drop at `D`
  would show a collection receiving writes after it was dropped, and `D` can
  also be below a checkpoint taken between prepare and the lock. *Decided:*
  **the drop's commit timestamp is re-assigned at the commit stage.** After
  `acquire_all_lock_op_` succeeds, for `DropTable` (and, per the next
  bullet, `CreateTable` and `TruncateTable`), `UpsertTableOp::AssignCommitStageTs` sets
  `txm->commit_ts_ = max(commit_ts_, max over results of (last_vali_ts + 1, commit_ts + 1))`
  = `P`. Everything downstream already uses `commit_ts_`: the commit log
  (`FillCommitLogRequest`), the `PostCommit` post-write, and the DDL record
  appended at commit-log apply (`ddl_ts = P`). One log-state edit: `UpsertSchemaOp`
  at the `CommitSchema` stage must set the op's commit timestamp to the commit
  log's value (today it only zeroes it on failure), so an interrupted drop is
  recovered with `P`. The kv store is not touched; the table is being deleted.
  Why this is exact on every axis:
  - *Old incarnation below `P`.* Each core's write-lock acquisition returns
    `last_vali_ts = max(LastReadTs(), Now())` after the DML read locks
    drained, and every DML published its commit timestamp into `LastReadTs`
    at post-read before releasing its lock (`Execute(PostReadCc&)`,
    `UpdateLastReadTs(commit_ts)`); `SetLastValidTs` and the coordinator take
    the max across cores and node groups.
  - *Next incarnation above `P`.* `PostCommit` installs the request's
    timestamp on the catalog entry (`SetCommitTsPayloadStatus(commit_ts, Deleted)`
    for `DropTable`), so the entry carries `P`, and a subsequent create on the
    same name computes `D2` above the entry's commit timestamp
    (`acq_all_res.commit_ts_ + 1`). No clock bump needed.
  - *Never late.* With (a) the checkpoint stays below the lock time, which is
    below `P`, so `ddl_ts > min_ckpt_ts_` at append time and resume is exact.
  - The dropped table's `schema_ts_`/kv catalog still say `D`; irrelevant once
    the table is gone (replay drops its records by name, F9).
- *Create (and truncate, EloqKV-only)*: **same rule as drop**, the commit
  timestamp is re-assigned to `P` under the write lock, with two additions
  because the table lives on and its version must agree everywhere (F12):
  - **The kv catalog must carry `P`.** A node that loads the table from the
    store takes the catalog row's timestamp as the entry's commit timestamp,
    so a row at `D` would let that node's DML commit in `(D, P)`, before the
    create. Truncate writes its catalog row after the commit log, at
    `commit_ts_`, so it gets `P` for free. Create used to write
    the whole kv side before the lock (`upsert_kv_table_op_` right after the
    dirty-schema install, then the sequence-table step, then the lock).
    *Decided:* **split create's kv work around the two logs.**
    - **The kv tables after the prepare log** (`create_kv_table_op_`,
      `DataStoreHandler::CreateKvTable` for the base table and each index; on
      the data store service, each range-partitioned table's initial range
      record), at `D`, under the write intent. No log has decided the op
      yet, so a failure is not retried: the create **gives up**: it removes
      what it created (`UpsertTableOp::AbandonCreate`) through the store's
      drop-table path over the dirty schema at `D` (kv tables, ranges, the
      catalog key it never wrote), then writes the commit log
      marked rolled back at timestamp 0; the post write, with the rollback
      flag and timestamp 0, discards the dirty schema
      and releases the intents, and the command fails with
      `DATA_STORE_ERROR`. That is the failure path create had before, now
      with a cleanup, and it appends no DDL record (§4.3). If
      the cleanup fails too, the kv tables stay orphaned, which is accepted
      as for a failed add index: they hold no data, no schema refers to
      them, and a later create of the name writes them again
      (`CreateKvTable` is idempotent). A node that recovers the op from its
      prepare log tries the kv tables again, once. The sequence-table step
      follows, also before the commit log, so a failure there makes the
      create give up the same way; the sequence entries it wrote stay, and a
      later create of the name writes them again. Then the commit-stage
      lock.
    - **The catalog row after the commit log**, at `P`: a catalog-only write
      (`upsert_kv_table_op_` with op type `Update`), retried under the write
      lock.
    - The table's initial ranges stay at `D` in the store and in memory
      (`PostCommit` initializes them at the prepare-stage version), so a node
      that loads them from the store after a failover gets the same version.
      Only the schema version and the catalog row move to `P`.
    - A commit log that commits the create is written only once the kv
      tables exist, so every state recovery can reach from it has them.
      Replay of a committed create builds the table as committed, as before;
      on non-shared storage it writes only the catalog row, since the kv
      tables are there. Replay and recovery of a create that gave up (commit
      timestamp 0) reject the dirty schema and write nothing to the store.
    - Once the commit log commits a create, no data-store failure fails it.
      The general rule, for index ops too: a data-store failure before the
      op's point of no return (the commit log of create and drop; the
      prepare log of add and drop index) rolls the op back at once, and one
      after it is retried until the step succeeds. Truncate never rolls back
      after its prepare log. On shared storage it empties the table under the
      write lock before its commit log (the cc maps, the kv table, the
      sequences), each step retried, so that a participant that recovers from
      the commit log and takes writes into the emptied table finds no later
      step that wipes them; only its catalog row follows the commit log
      (`UpsertTableOp::TruncateUntilCommitLog`). Without shared storage each
      node group truncates its own store after the commit log, and one that
      recovers from the commit log truncates its store in the replay, before
      it takes writes.
      **Rule, for the data-store steps after the commit log of create, drop
      and truncate alike:**
      1. Write the commit log with timestamp `P` (the DDL record is
         appended at its apply, `ddl_ts = P`).
      2. Perform the data-store operation (create's and truncate's catalog
         row, drop's kv table and sequence table steps) while holding the
         write lock, retrying a failure indefinitely, 30 seconds apart by
         default (`ddl_kv_retry_interval_seconds`; the test fixtures use 1 s),
         with a warning in the log naming the
         table, the step and the error on each attempt. The command's client
         stays blocked; if it gives up, the op continues on the coordinator.
         Create's steps before the lock, the kv tables and the sequence
         table, are not retried: a failure fails the create (above).
      3. On success, run `PostCommit`, which installs the new schema and
         releases the lock.
      4. If the coordinator fails at any point after step 1, recovery rolls
         the committed operation forward from the commit log (F8), with the
         timestamp the commit log recorded (the log state keeps the commit
         log's timestamp, not the prepare log's). No path rolls back:
         reversing a durable commit decision would need its own protocol,
         including recovery behavior, and removing or altering the logger
         record is not that protocol.
      **Why the lock is not downgraded during the retry.** A downgrade to an
      intent was considered and rejected. `DowngradeLock` commits nothing
      (`catalog_cc_map.h`, the `DowngradeLock` case): for drop and truncate
      the table keeps its old, current schema until `PostCommit`, catalog
      read locks coexist with a write intent, and DML could therefore read
      the old table and commit **above `P`** while the store retries, which
      breaks the drop-order invariant that `P` establishes (no DML of the
      old incarnation above the drop's position). Publishing an explicit
      "inaccessible" state before releasing the lock would fix that, but it
      is a second mechanism for a rare path; retaining the lock is simpler
      and needs no new state. For create the downgrade would have been
      safe, since the table is not yet visible, but a uniform rule is worth
      more than the exception.
      **Cost, accepted for v1.** With (a) in place, a catalog write lock
      held through a prolonged store failure pins `ckpt_ts` on that node
      group and therefore `arrival_complete_ts` on every logger: every
      stream and log truncation stall until the store recovers or the
      operator intervenes. This is the same availability trade-off as the
      existing behavior for a stuck write lock (§5.7), surfaces through the
      same `checkpoint_stall_rounds` gauge plus a warning in the log for
      every retry, and buys a single, consistent failure path. For create it applies only
      to the catalog row, a single write; the kv tables are created before the
      lock.
    In the normal case the write lock is held across the commit log and one
    kv write, milliseconds to seconds; nothing can wait on a create's
    catalog lock except another DDL on the same name. The checkpoint pin
    for that duration is the same one drop already imposes; only a failing
    store extends it (see the cost above).
  - **`PostCommit` promotes `P`, not the prepare-stage version.** It used to
    promote `dirty_schema_version_`, which is `D`. The catalog entry now takes
    `req.CommitTs()` (`CommitDirtySchema(commit_ts)`) and so does its cc
    entry's record, both the schema record's version. The schema object is
    not rebuilt: the tables keep their own versions (see *Table versions*
    below), a create's at `D`. Truncate's table is a new one whose version is
    `P`, so core 0 rebuilds truncate's dirty schema at `P` before any core
    builds cc maps from it; a post-write visits the cores in order, core 0
    first.
  - **The second phase fills its record from the node's catalog entry.** A
    schema op ends with `PostCommit`, which promotes the dirty schema, or
    `Rollback` (`PostWriteType::Rollback`), which discards it and keeps the
    current schema. Core 0 of each node group sets the request's record
    accordingly: the requester's own record for a local request, a record the
    request owns for a remote one, which is sent none. The template installs
    it in every core's cc entry, copy-on-write
    (`VersionedPayload::SetCurrentPayload`: copied in place when the entry
    holds its record alone, into a new record otherwise), and releases the
    lock; `PostCommit` publishes the commit timestamp, and `Rollback` moves it
    only if it is above 0. An op that failed before its prepare log installed
    no dirty schema, so there is nothing to discard: its `Rollback` carries no
    record and only releases the lock. Such an op may hold its write intents
    on some cores only, core 0 or not, since it failed to acquire them; the
    node's catalog entry having no dirty schema is what tells. The write
    type, not the timestamp, says "discard":
    every op that fails before or after its prepare log, a DML transaction
    that aborts after taking catalog locks, the table-cache invalidation that
    only releases its locks, and the add index rolled back at `P` end with a
    `Rollback`. Its commit timestamp above 0 moves the catalog entry's version
    there (the add index, at `P`) and 0 leaves it alone; a `PostCommit`'s is
    always above 0. Every post-write at timestamp 0 is a `Rollback` (or a lock
    downgrade), a range split that fails before its prepare log included.
    The commit log carries the same mark (see
    *Rolled-back ops* in §4.3): with it, timestamp 0 means no change event
    and a timestamp above 0 a separate one.
  Result: the first insert into a created table, and the first write after a
  truncate, read version `P` and commit above it by the ordinary read-set
  rule. Exact, no clamp, no clock bump. For drop the same two steps are
  unnecessary because the table ceases to exist.

**(c) Fallback: logger-side fence.** The logger rejects a DDL WriteLog whose
`ddl_ts <= min_ckpt_ts_`. Only meaningful for prepare logs, where the tx side
treats the failure as a clean abort (`UPSERT_TABLE_PREPARE_FAIL`) and the
command layer retries. Not needed once (a) and (b) are in place.

§5 assumes the invariant and needs no late-arrival path.

### 4.5 Retention and the history boundary

The pull model gives the logger no knowledge of consumers; retention is the
only guarantee. Both log states remove checkpointed history from the live
store, whole L0 SSTs at a time:

- the RocksDB state deletes every SST whose keys all lie below
  `min_ckpt_ts − 1`, whenever total SST size exceeds
  `txlog_rocksdb_sst_files_size_limit` (default 500 MB, which a busy
  cluster fills in minutes; `LogStateRocksDBImpl::PurgingSstFiles`);
- the RocksDB-Cloud state moves every SST whose keys all lie below
  `min_ckpt_ts` out of the live DB into an archive path, every
  `archive_move_interval_seconds` (default 600 s;
  `LogStateRocksDBCloudImpl::MoveFilesToArchive`, §4.6).

Each purge takes *everything* checkpointed, not only history older than
some window: raising the size limit or the interval makes purges rarer, but
each one still leaves nothing behind the checkpoint. A consumer that is
behind the checkpoint when one runs loses records.

**The CDC retention floor.** Neither purge removes a file whose largest
timestamp is at or above

> `min_ckpt_ts − txlog_cdc_retention_seconds` (in commit-timestamp
> microseconds),

so every record within that window behind the checkpoint stays in the live
store, on both states. The floor needs no clock: commit timestamps track
time, and `min_ckpt_ts` advances with every checkpoint round whether or not
the cluster writes. The default, 0, keeps each purge exactly as it is, so
deployments without CDC are unaffected; CDC deployments set it above the
worst-case consumer lag. The size limit and the interval then decide only
how often a purge looks, not how much it keeps: the RocksDB state's disk
holds about the write rate times the retention, whatever the limit says.
The DDL store (§4.3) is trimmed by the same purges, by the same floor, when
they run, not at every checkpoint: a DDL record stays as long as the data
records around it, so a consumer loses DDL records only where it loses data.
Trimming at every checkpoint would drop each record as soon as a scan could
return it unless a window is set.

**What "history loss" means, precisely.** The smallest surviving key is
*not* a usable boundary. Records arrive out of timestamp order (F5) and
purging deletes whole files, so a surviving file can hold older records while
a file covering intervening timestamps is already gone; a scan from the
smallest surviving key would silently skip that gap. The boundary the
protocol needs is:

> `retention_floor_ts`: a per-group timestamp such that every data record,
> every DDL record, and every piece of metadata needed to decode them, with
> timestamp strictly above it, is still present in the group. History at or
> below it may be incomplete.

It must cover all three kinds of content, it must only move upward, it must
be persisted and replicated so that a leader change does not lower it or
lose it, and it must be advanced *before* the purge that makes it true is
executed, never after, so that a concurrent scan can never observe a gap the
boundary does not yet admit. The natural implementation is that the purge
step computes the highest timestamp any file it is about to delete could
contain, proposes the new boundary through raft, and deletes only once that
proposal has applied.

**v1 scope.** Defined and handled, not detected:

- v1 defines `PositionTruncated`, its translation to
  `ChangeStreamHistoryLost`, and the consumer's re-bootstrap (§6.2), so the
  failure mode has a complete path through the system.
- v1 loggers do **not** persist or advance the boundary; they report
  `retention_floor_ts = 0` and never return `PositionTruncated`. v1
  correctness therefore *assumes* that the history a scan requests is still
  retained, i.e. that `txlog_cdc_retention_seconds` is set above any
  consumer's worst-case lag. That assumption is the operator's, and the lag
  metric in §8 is how it is watched.
- A later version implements the boundary in the logger as defined above.
  The retention floor is where it advances: a purge already knows the
  highest timestamp it may remove, so it proposes that as the new boundary
  and removes files only once the proposal has applied. The runtime and
  consumer need no change when it arrives: they already handle the error.

### 4.6 RocksDB-Cloud log state

The RocksDB-Cloud variants (`WITH_LOG_STATE=ROCKSDB_CLOUD_S3/GCS`,
`10-log-service.md` §10) are supported in v1. From the scan's perspective
they are the same store: the same timestamp-keyed CF, the same
`min_ckpt_ts_`, the same DDL store, and the scan is built on the same
iterator the replay path uses (`GetLogReplayList`), so whatever recovery can
read, the scan can read. Three differences matter and are handled where
they arise:

- **Leader start opens the cloud DB asynchronously** (`AsyncStartCloudDB`),
  and until it is open, writes are buffered in memory; once open, the
  buffered records are written into the DB and the buffer is cleared, and
  only then is the state marked in sync (`mem_db_in_sync_`). A scan that
  arrives on a leader not yet in sync is answered `NotLeader` with the
  leader's own address as `redirect`, so the cursor retries after its usual
  backoff instead of reading a partial view. From then on the DB alone holds
  the whole log, and the scan reads only the DB, as replay does
  (`GetLogReplayList` waits for the same flag). Every leadership term
  opens its own DB container, and the flag speaks only for the current
  term's, so the scan reads the leader term before checking the flag and
  again once its page is built, and discards the page with `NotLeader` if
  the term moved: otherwise a stop and re-election in between could pair
  the old term's flag with a new container that is open but not yet
  holding the buffered records. With the term unchanged, the container it
  captured is that term's in-sync one, and its own reference keeps it open
  to the end even if leadership is lost and the container replaced.
- **Retention runs in two stages, and only the first matters to CDC.**
  Stage 1 (`MoveFilesToArchive`, every `archive_move_interval_seconds`)
  moves checkpointed SSTs out of the live DB into an archive path in the
  same bucket; Stage 2 (`PurgeArchiveFiles`, daily at
  `log_purger_schedule`) deletes archived objects older than
  `log_retention_days`. Nothing reads the archive, neither replay nor the
  scan, so `log_retention_days` bounds only the archive, and for CDC the
  window is what Stage 1 leaves in the live DB: it obeys the retention floor
  of §4.5 like the RocksDB purge. The in-memory queue only triggers braft
  snapshots. When `retention_floor_ts` is implemented, the cloud variant
  advances it from Stage 1's cut-off.
- **Attach/detach** (`AttachLog`/`DetachLog`) stops and starts log instances.
  A scan against a detached instance fails like a leader loss; the cursor
  retries through the leader cache and resumes where it was once the group
  is attached again. Nothing is lost: detach does not purge.

## 5. EloqDoc runtime: the change-stream cursor

### 5.1 Wire surface and stream scopes

`aggregate` with `$changeStream` on a collection, a database, or the whole
deployment (`allChangesForCluster`), with `fullDocument`, `resumeAfter`,
`startAfter`; `getMore` with `maxTimeMS` on the resulting tailable await-data
cursor; `killCursors`. Batches carry `postBatchResumeToken`. The replica-set
assertion in `DocumentSourceChangeStream::createFromBson` (F10, error 40573)
is bypassed for EloqDoc. Driver-side `watch()` sends nothing else, so `hello`
need not change. What drivers *do* require of the reply is F13: the opening
reply must carry a `postBatchResumeToken`, which the opening protocol below
guarantees; §10.2 lists the driver scenarios to verify.

**A MongoDB change stream is not the same thing as CDC**, and the three
scopes carry different lifetime contracts:

| Scope | Events | Lifetime |
|---|---|---|
| Deployment (`allChangesForCluster`) — **the CDC mode** | every collection except `local`, `admin`, `config` | never invalidated; collections come and go as `create`/`drop` events |
| Database | collections of that database | MongoDB invalidates on `dropDatabase`; EloqDoc has no such operation (a database is dropped as a sequence of collection drops), so a database-level stream is **not invalidated in v1**, a documented deviation |
| Collection | that one namespace | MongoDB semantics: after the watched collection's `drop` event the stream emits `invalidate` and the cursor is closed; resuming from the drop's own token, at any later time and even if the name exists again, yields that `invalidate` and closes (§5.6); the client reopens with `startAfter` on the `invalidate` token (`resumeAfter` on it is rejected with `InvalidResumeToken`, as in MongoDB); a later collection with the same name is a new incarnation and a new stream |

The CDC consumer (§6) always opens a deployment-level stream, optionally
narrowed with its own `$match` on `ns`.

**Opening a stream.**

- *Supported configurations, checked at open.* `$changeStream` is rejected
  with a clear error when the node cannot provide the guarantees the stream
  rests on: no log service configured, or `txservice_skip_wal` set (no redo
  records exist, F1); the `MEMORY` log state (not durable, no retention;
  RocksDB and RocksDB-Cloud are both supported, §4.6). These are static
  properties of the deployment, so the check is once per open. Checkpoint
  rounds, which advance `arrival_complete_ts` (F6), need no check at open,
  because the data substrate guarantees them on any node that can serve a
  stream: it refuses to start with WAL enabled and no data store (the only
  setup without a checkpointer), rejects a negative `checkpointer_interval`
  at start-up (stored unsigned, it would become an interval of about 136
  years), and raises an interval below 5 s to 5 s.
- *Starting position without a token.* MongoDB semantics: a stream opened
  with neither `resumeAfter` nor `startAfter` starts "now" and reports only
  later changes. Here "now" is the arrival-complete bound at open: the
  cursor scans every group once with `wait_ms = 0`, takes
  `S = min over groups of arrival_complete_ts`, starts every group with
  `from_ts = S + 1` (§4.2), and issues the watermark
  for `S` as its first `postBatchResumeToken`. Events with timestamp `<= S`
  are never delivered to this stream; the first event delivered is the
  lowest position above `S`. This is the `T0` that bootstrap records (§6.2
  step 2), and it is consistent: everything at or below `S` had already
  arrived at every logger when the stream was opened, so no event is
  straddled.
- *Opening protocol: the opening reply always carries a token.* A driver
  that receives an empty opening reply with no `postBatchResumeToken`
  fails the open (F13), so the `aggregate` command does not return until
  it can fix `S`. At cluster bring-up, until every node group has completed
  one checkpoint round, some group reports `arrival_complete_ts = 0` and no
  start can be fixed; the open therefore **waits inside the command**,
  re-scanning every group with `wait_ms > 0` (yielding on the cursor's wait
  object, §5.8, never blocking the worker) until all groups report a
  non-zero bound, then fixes `S` as above and returns an empty `firstBatch`
  whose `postBatchResumeToken` is the watermark for `S`. The wait is
  bounded by the command's `maxTimeMS` if given, else by
  `cdc_open_wait_ms` (default 30 s, i.e. at least two
  `checkpointer_interval` rounds at the default 10 s); on expiry the open
  fails with `ChangeStreamStartNotReady` (new EloqDoc error code 5000, in EloqDoc's own range above upstream's sequential codes) and a
  message giving how long it waited and the configured round interval. If
  the wait was not longer than one round, the message names the value to
  raise (`maxTimeMS` or `cdc_open_wait_ms`); otherwise it says the delay
  is expected only right after cluster start-up and points at the server
  log if it persists. The client retries the open. Drivers do
  not retry a failed `aggregate` on their own, so the consumer treats this
  code as retriable (§6.1). A token-based open never waits: its opening
  reply carries the supplied token as `postBatchResumeToken` (the
  "previous batch's token" rule of §5.3 step 4) and, before the first
  checkpoint, behaves as normal resume, nothing being emittable until the
  bound passes the token. Because the opening reply always carries a
  token, a driver that loses its cursor before the first event resumes
  with that watermark, which positions every group at `S + 1` (§5.6);
  there is no window in which a reconnect lacks a token.
- *Log-group membership is fixed for the cluster's lifetime.* The number of
  log groups is set once by `LogAgent::Init` and the tx-to-group mapping is
  `tx_number % LogGroupCount()` (`eloq_log_wrapper.h`), so the group set
  cannot change without invalidating that mapping. What does change is
  membership and leadership *within* a group (`AddPeer`/`RemovePeer`,
  `UpdateLogGroupConfig`, raft elections), and the scan already handles
  that through `NotLeader`/`redirect` and the leader cache (§5.3). A cursor
  therefore captures the group list at open and keeps it, **in v1**. The
  token format (§5.6) is a variable-width vector keyed by `log_group_id`,
  so a later version that adds groups needs no token migration: open
  streams are reopened, a group absent from a token holds nothing at or
  below the token's timestamp and starts at `from_ts = timestamp + 1`, and
  a group named in a token but gone from the cluster is ignored.

### 5.2 Pipeline

The `$changeStream` stage expands into EloqDoc's own chain of internal
stages, which fetch, decode, and format the events, followed by the
user's stages:

```text
[EloqDoc internal stages: scan → decode → format (→ invalidate → close)]  →  user stages
```

**What is reused from the 4.0 machinery (F10):** the `$changeStream` stage
registration and spec parsing, and the tailable await-data cursor plumbing
behind `aggregate`/`getMore`. The spec parser is reused **with one
addition**: the 4.0 IDL (`src/mongo/db/pipeline/document_sources.idl`,
`DocumentSourceChangeStreamSpec`) defines `fullDocument`, `resumeAfter`,
`allChangesForCluster`, `startAtOperationTime` and the deprecated
`$_resumeAfterClusterTime`, but **not `startAfter`**, which MongoDB added
in 4.2. `startAfter` is added to the IDL as an optional `object` field with
the same mutual-exclusion rule as `resumeAfter` (at most one of the resume
options may be given), and the token it carries is decoded by the EloqDoc
decoder of §5.6. Drivers send `startAfter` verbatim, so nothing else
changes on the wire.

**What is not reused, and why.** In MongoDB a change stream is an ordinary
aggregation whose `$changeStream` stage the server expands, in
`buildPipeline` (`document_source_change_stream.cpp`), into a chain of
*internal* stages that read the oplog collection: `DocumentSourceOplogMatch`
(scan the oplog with a namespace/time filter),
`DocumentSourceChangeStreamTransform` (oplog entry → event document),
`DocumentSourceCheckInvalidate` (inject `invalidate` after a drop/rename of
the watched collection), a resume stage
(`DocumentSourceEnsureResumeTokenPresent` /
`DocumentSourceShardCheckResumability`, verify the token's event exists
before emitting what follows), `DocumentSourceCloseCursor` (close after
`invalidate`), and optionally `DocumentSourceLookupChangePostImage`
(`updateLookup`). None of these internal stages is used here: EloqDoc's own
internal stages (below) do the fetching, decoding, merging, and formatting,
and the only stages after them are the user's own (`$match`, `$project`, …
passed to `watch()`). Each of the internal stages encodes a MongoDB assumption this
design replaces:

- The transform's command classification recognizes only `applyOps`,
  `drop`, `renameCollection`, and `dropDatabase`; **every other command,
  including `create`, `createIndexes`, and `dropIndexes`, becomes an
  `invalidate` event** (`document_source_change_stream_transform.cpp`, the
  final `else` of the command branch). Feeding it richer oplog-shaped input
  does not help; there is no branch that copies collection options or index
  specs into a DDL event. The 6.0-style expanded events this design emits
  postdate that code.
- `DocumentSourceCheckInvalidate` invalidates a collection-level stream
  after a drop of that collection and a database-level stream after
  `dropDatabase` (`document_source_check_invalidate.cpp`); `CloseCursor`
  then closes the cursor. This design keeps deployment- and database-level
  streams open across drops and implements collection-level invalidation
  in its own stage with its own token (§5.1, §5.6).
- The transform's token construction and the resume stages' token
  comparison use the UUID-keyed 4.0 token layout (§5.6).
- `LookupChangePostImage` implements `updateLookup` by reading the current
  collection; every EloqDoc event already carries the full document.

What would remain reusable after removing all of that is the shaping of
`insert`/`replace`/`delete` documents, a few dozen lines. Extending the old
formatter is therefore not worth the coupling. EloqDoc's `$changeStream`
expands into **one pipeline stage of its own**, `DocumentSourceEloqChangeStream`,
followed only by MongoDB's `DocumentSourceCloseCursor` and the user's stages.
Inside that stage the work is decomposed the way MongoDB's chain is, so that
each piece is testable on its own, but the pieces are plain C++ components
the stage drives, not pipeline stages:

```text
DocumentSourceEloqChangeStream{ CdcScanSource → decode → format [→ invalidate] }  →  CloseCursor  →  user stages
```

One stage rather than a chain because the pieces share state that pipeline
stages cannot: every event's token (§5.6) describes every log group's emit
cursor, which the decode step advances but the format step writes, and the
watermark the batch loop asks for at `IS_EOF` needs both the scan source's
`emit_bound` and those cursors. A `DocumentSource` holds only its upstream
neighbour and could reach that state only through a side channel. As one
stage it is both the pipeline's first stage and the last one that writes an
`_id`, so it answers both of the batch loops' token questions itself
(`PostBatchResumeTokenProvider`): the watermark or opening token, and the
`_id` it last emitted, which `PipelineProxyStage` compares with the delivered
one (§5.3 step 4). No data is copied between the pieces; the one copy is a
logged document's BSON, from the scan page into the event.

| Component | Owns | Input → output | Detail |
|---|---|---|---|
| `CdcScanSource` (`src/cdc/cdc_scan_source.*`) | per-logger continuation, buffers, `arrival_complete_ts`, completeness boundary `scanned_ts`; `emit_bound`; `drained` (§5.3); the scan client's in-flight RPC state and the wait object of §5.8 | — → *records*, one at a time, in pull order merged across groups by timestamp | issues `CdcScan` asynchronously and yields on the wait object (F11, §5.8); long-poll clamped to the batch's deadline; releases a record at `T` only when every other group's boundary is at least `T − 1` (§4.2 emission rule, §5.3 step 2) |
| decode (`CdcEventStream`, `src/cdc/cdc_event_stream.*`) | the current record and the position `(section, entry)` reached in it; per group, the emit cursor and the set of entries produced at the group's current timestamp (§5.5); the resume skip (§5.6) | record → changes, in decode order | decodes one record at a time in storage order, skipping sections outside the stream's scope by their length (§5.4 step 1), suppresses forward-write duplicate entries (§5.5), consumes without producing what a resume token covers (§5.6), applies the canonical delete-key decoder (§5.4 step 3); no catalog access |
| format (`CdcEventStream`) | nothing | change → event document (§5.4a) with `_id` token (§5.6) | sets each document's `_id` to the *progress token* valid once that document is delivered; the `_id` is what the batch loop records at append time (§5.3 step 4), since nothing outside the BSON document survives to the batch builder; it does **not** decide what has been delivered |
| invalidate (`CdcEventStream`) | whether the watched collection has been dropped | events → events, plus an `invalidate` after the watched collection's `drop` | **collection-level streams only**, with the DDL events (plan C10); the `invalidate` token is the vector recorded after the drop, kind 2, set as the synthesized document's `_id` exactly as format does for events; on resume from the drop's token it re-reads the referenced drop record and emits the pending `invalidate` before anything else (§5.6) |
| `CloseCursor` (a stage) | — | closes the cursor once `invalidate` has been delivered | MongoDB's `DocumentSourceCloseCursor`, reused as is: it only checks `operationType == "invalidate"` |

**How it is wired.** The storage engine installs its expansion at startup
(`DocumentSourceChangeStream::setStageFactory`, called when the eloq storage
engine is registered), so the pipeline library does not depend on the eloq
module and the oplog-based chain stays intact where no factory is installed.
With a factory installed, `run_aggregate` keeps a change stream on its own
namespace and read concern, since only the oplog-based chain reads the oplog
and needs a majority snapshot, and the stage declares
`requiresInputDocSource = false`, so no oplog cursor is put in front of it.
The stage opens the stream when it is created, which is before the cursor is
registered, as the registration's token check requires (§5.1 opening
protocol). Its batch deadline follows §5.3: an awaitData `getMore` has no
operation deadline, its `maxTimeMS` (default 1 s) being in the operation's
awaitData state, and the `aggregate` never sets that state, so the stage
waits until the `getMore`'s deadline and does not wait at all for the opening
batch. It serializes as `{$changeStream: …}` with its opening token as the
resume point, so a re-parsed pipeline (debug builds round-trip every one)
reopens at the same place without waiting. `startAtOperationTime` and
`$_resumeAfterClusterTime` are refused: an EloqDoc stream starts fresh or
from a token.

The user's own stages (`$match`, `$project`, …) follow. Pushing a user
`$match` on `ns` down into the decode step's section skip is an optimization
for later.

### 5.3 Cursor state and the per-`getMore` algorithm

The state below is split across the components of §5.2 (`CdcScanSource`
holds the fetch state, the decode step the emit cursors); it is listed together because the algorithm is easiest to read
as one loop. All of it lives in memory, owned by the cursor manager between
`getMore`s:

- per log group `g`:
  - the **fetch cursor**: the scan `continuation` (exact storage keys of
    both streams, §4.2), last `arrival_complete_ts`, and the inclusive
    completeness boundary `scanned_ts` (§4.2: every record of `g` at or
    below it has been returned to this cursor);
  - a buffer of fetched, not yet emitted records, in storage order;
  - the **emit cursor**: the storage key of the record currently being
    decoded and the position `(section, entry)` of the last entry produced
    from it, or the key of the last record fully produced. Fetch and emit
    cursors differ whenever `g` has fetched ahead of what the merge has
    released;
- `emit_bound = min over groups of scanned_ts` (each `scanned_ts` is by
  construction `<= arrival_complete_ts`): the drained watermark value. It
  advances whether or not any event exists in the interval, since an
  exhausted scan proves the interval up to its bound empty (§4.2);
- `last_produced`: the token (§5.6) describing every group's emit cursor
  (a cursor the token's own timestamp covers is left implicit) after the
  last event document the source handed downstream; it is the
  value the format step writes into that document as its `_id`, and the batch
  loop never reads it from the source directly (step 4).
  `drained`: true when the source has been asked for a next event and no
  group has an emittable one, false again as soon as it produces one. The
  source knows what it has *produced*; it does not know what has been
  *delivered*, because a produced document can still be filtered by a
  user stage or stashed by the batch loop (step 4). Delivery is the batch
  loop's knowledge.

Each `getMore`:

1. Issue `CdcScan` to every group that has no scan in flight and whose
   buffer is empty or whose `scanned_ts` is below `T_min − 1`, where
   `T_min` is the lowest timestamp of any unemitted record anywhere, subject
   to the buffer cap (§5.8: buffered bytes plus `max_bytes` per scan
   already in flight must be below the cap, except for the group whose
   lag blocks `T_min`): **a non-empty buffer does not stop
   fetching**, because a group that is behind blocks the release of every
   other group's records above its boundary. Use `wait_ms` clamped to the
   remaining `maxTimeMS`. Yield on the cursor's wait object; a response or
   the deadline timer resumes the coroutine through that object, never a
   callback directly (F11, §5.8). `NotLeader` refreshes the leader from
   `redirect` and retries; the tx side's `LogAgent` leader cache can be
   reused. A `NotLeader` whose `redirect` names the node just asked (a
   leader not yet ready, §4.6) or is empty is retried only after a backoff,
   never at once. So is an empty page with `!has_more` that came back
   before its `wait_ms` elapsed: the logger may answer early (a full parked
   list, §5.8, or a logger without the long poll), and re-issuing at once
   would turn an idle stream into a tight loop against the logger. A page
   counts as held only if it took at least half its `wait_ms`, and a
   `wait_ms` under 20 ms never does, since timer granularity makes such a
   wait indistinguishable from an early answer and back-to-back short scans
   are a loop either way; for the same reason no scan is issued with less
   than 20 ms of `maxTimeMS` left, except the first scans of a pull. A
   redirect to another node is followed at once only if the previous
   redirect was not followed within one backoff period; otherwise it is
   followed after the backoff. Two nodes that name each other during an
   election match neither test above, and would otherwise be bounced
   between for as long as the election lasts. The backoff is bounded by the remaining
   `maxTimeMS`, like the wait itself.
   From each response record `arrival_complete_ts`, append the
   records to the group's buffer, store the continuation, and set the
   completeness boundary per §4.2: `scanned_ts = last_ts − 1` if
   `has_more`, else `arrival_complete_ts`. **An empty response with
   `!has_more`** therefore advances `scanned_ts`, and hence `emit_bound`,
   across idle time exactly as data does, one checkpoint round at a time;
   that is what lets an idle stream keep producing watermark tokens (step
   4, `IS_EOF`) and lets §5.1 fix a start position on a cluster with no
   traffic.
2. **Pick the next record.** Among groups with an unemitted buffered
   record, take the one whose next record has the lowest timestamp `T`
   (ties by lowest `log_group_id`; the choice carries no meaning). It is
   emittable when `scanned_ts_h >= T − 1` for every other group `h` (§4.2,
   emission rule). If it is not, or no group has a buffered record, and
   every group's fetch is either in flight or exhausted at its bound, set
   `drained = true` (nothing can be released until a scan returns).
3. **Decode and produce.** Decode the chosen record on its own (§5.4),
   suppress duplicate entries (§5.5), and produce the
   rest **one per pull, in decode order**, advancing the group's emit
   cursor (the `(section, entry)` position) per event; the token
   describing the cursors after each event becomes `last_produced` and,
   in the format step, that event's `_id`. No sorting happens anywhere: order within a timestamp
   is whatever the pulls produced, and it is not stable across runs, which
   is why the token records positions rather than an order (§5.6).
4. **Batch construction owns delivery.** MongoDB's `getMore` loop
   (`getmore_cmd.cpp`, `generateBatch`) and the `aggregate` first-batch
   loop (`run_aggregate.cpp`, `handleCursorCommand`), which has the same
   shape and is changed identically, pull one document at a time
   through the whole pipeline, user stages included, and for each document
   either **appends** it to the reply or, if it would exceed the reply size
   budget, **stashes** it for the next batch (`exec->enqueue`) and stops.
   Formatting an event is therefore not delivering it: a produced document
   may be filtered by a user `$match` (never delivered, but consumed) or
   stashed (delivered only next time). The rule that keeps
   `postBatchResumeToken` honest follows the loop's own structure:
   - Every event document carries its own progress token **as its `_id`**
     (§5.4a, §5.6): the format step sets it on each event, and the invalidate step
     sets the kind-2 token on the `invalidate` document it synthesizes.
     The token travels *inside* the document, not beside it: the
     pipeline-to-BSON conversion in front of the batch builder discards
     `Document` metadata, so nothing attached as metadata would survive to
     the point where progress is recorded. The loop records progress at
     two points only: **when appending an event to the reply, it records
     that event's `_id`** as the batch's resume token; **at `IS_EOF`, it
     records the source's watermark for `emit_bound`**, obtained through
     the executor (`getPostBatchResumeToken()`), which is valid because at
     `IS_EOF` the source has reported itself drained.
   - The append-time read is placed **only after a document has passed
     the size check and is being appended**, exactly where 4.0 calls
     `setLatestOplogTimestamp`. Reading the token from the document rather
     than from the source is what makes the value correct when the
     appended document is not the one the source last produced: the
     `invalidate` document is produced downstream of the source, after the
     drop, with a different token; and a stashed document is appended in
     a later `getMore` than the one in which it was produced. A document's
     token covers every event produced before it, including any a user
     stage filtered out, because it records each group's emit cursor.
     Because the loops trust the delivered `_id`, a user stage that
     rewrites it (`$project`/`$unset` removing it, `$addFields`/`$set`
     replacing it or a nested `_id._data`, `$replaceRoot`/`$replaceWith`,
     a computed projection) would corrupt the resume position, and no
     open-time inspection can rule all of those out. The guarantee is an
     **end-of-pipeline integrity check**: every stage allowed after
     `$changeStream` is streaming and yields at most one output per
     input, so the document leaving the pipeline always derives from the
     last document the internal stages emitted; `PipelineProxyStage`
     compares the delivered `_id` byte for byte with the token those
     stages assigned — the stage keeps the `_id` its format step wrote, or
     its invalidate step synthesized, in itself, where no
     user stage can reach it — and fails the
     command with `ChangeStreamFatalError` (code 280, which drivers treat
     as non-resumable) on any difference. Rejecting an obviously broken
     `$project` at open is a courtesy on top of that, not the mechanism.
   - A stashed document never reaches the append-time read (the loop
     breaks before it), so the recorded token stays at the previously
     appended document; **its `_id` stays with it**, and progress advances
     only when it is appended. On the next `getMore` the executor first
     returns the stashed document, which is appended and recorded from its
     `_id`.
   - At `IS_EOF` nothing is stashed (a stash ends the loop as `ADVANCED`,
     not `IS_EOF`), the source is drained, and the recorded token is the
     watermark for `emit_bound`, which covers events a user stage filtered
     at the end of the batch.
   - When `CloseCursor` ends the pipeline after `invalidate` (by
     `CloseChangeStream`, not `IS_EOF`), the loop records the source's
     `invalidate` token, whether the `invalidate` was appended or a user
     stage filtered it out. That token is the one `startAfter` needs
     (§5.6); with the `invalidate` filtered out, the last token appended
     would be the drop's, and a stream started after it would resume at
     its own drop and close again, never reaching the next incarnation.
   The token recorded by the last such call is the batch's
   `postBatchResumeToken`. If no call happened in a batch (every pulled
   document was stashed, i.e. the first one already exceeded the budget,
   which the size rule of §5.8 prevents by always allowing the first
   document), it is the previous batch's token. Drivers adopt
   `postBatchResumeToken` as the resume point once the batch is consumed,
   so a token ahead of what was appended would permanently skip the gap on
   reconnect. Every reply carries a `postBatchResumeToken`, the opening
   `aggregate` reply included: the opening protocol of §5.1 does not return
   until `S` is fixed or a token was supplied, so there is always either a
   watermark or the supplied token to report (F13).

Blocking never happens on the worker: the only wait is the parked RPC, and the
logger returns at `wait_ms` regardless.

### 5.4 Decoding

**Unit of decoding: the record, in storage order.** A transaction's
`WriteLogRequest` carries one blob per node group, and the logger stores
them as separate records `(commit_ts, ng, txn)` (F1, F5). Those records
share the transaction's timestamp but are **not adjacent** in storage
order: another transaction that committed in the same microsecond and
maps to the same log group sorts between them by `ng`. The decode step
therefore never assembles a transaction; it decodes each record on its
own, in the order the group returns them, which keeps the emit cursor a
single storage key plus a position inside one record (§5.6). The one
cross-record concern, a key that is double-written during bucket
migration and so appears in two records of one transaction, is handled by
the per-timestamp memory of §5.5, not by assembly. Every record of one
transaction is in one logger (one `WriteLog`, one group), which is why
that memory is kept per group.

Per data record within a transaction (F1–F3):

1. Walk the blob's sections. Keep sections whose `engine == EloqDoc` and
   `type == Primary` (documents); skip every other section, secondary-index
   tables included, using its `u32 len`. Skip namespaces in `local`,
   `admin`, `config`, and anything not matched by the stream's namespace
   filter.
2. Per entry: key = `u16 len ‖ KeyString bytes`; op = `OperationType`
   (`Update = 1, Delete = 2, Insert = 3, Upsert = 4`); record present unless
   `Delete`.
3. Map: `Insert → insert`, `Update`/`Upsert → replace`, `Delete → delete`.
   `fullDocument` is the record's BSON blob. `documentKey._id` comes from the
   document for inserts and replaces, and from decoding the KeyString for
   deletes. **A delete carries no type bits, and the stock KeyString decoder
   is not usable without them** (`src/mongo/db/storage/key_string.cpp`,
   the numeric branch of `toBson`): with empty type bits it reads every
   numeric as `kInt`, so an integer-valued key is narrowed with
   `int(integerPart)`, silently corrupting any int64 `_id` above 32 bits, and
   a key with a fractional part or a decimal hits `uassert(50810)`. The CDC
   path therefore uses its own **canonical value-preserving decoder**. It
   consumes exactly the bytes the stock decoder would, because in the
   KeyString V1 format (EloqDoc encodes with `KeyString::kLatestVersion`)
   the byte layout of every branch is fixed by the CType byte and by
   marker bits inside the key, never by type bits; type bits only select
   the *BSON type and representation* of the output. Only the numeric,
   string-like and zero readers consult type bits at all, so those are the
   only places the canonical decoder differs. Its outputs, by encoder
   branch (`key_string.cpp`, `_appendDoubleWithoutTypeBits`,
   `_appendNumberDecimal`, and the `toBsonValue` numeric cases):

   | CType / marker | What the key holds | Canonical output |
   |---|---|---|
   | `kNumericNaN` | nothing | double `NaN` (double and decimal NaN compare equal) |
   | `kNumericZero` | nothing | int32 `0` (`0`, `-0.0`, `0L`, and every decimal zero compare equal) |
   | 1..8-byte int, fraction bit clear | integer part, `1 <= |x| < 2^63` | int32 if in range, else int64; exact for int, long, and integer-valued double or decimal in this range |
   | 1..8-byte int, fraction bit set, `fracBytes = 8 − intBytes > 0`, DCM `= 0` (`kDCMEqualToDouble`) | 8 bytes = integer part ‖ fraction with the 2 low bits as DCM | double: `bin = (encoded & ~3) · 256^−fracBytes`, exact for a double original, and equal in value for a decimal that is exactly a double |
   | same, DCM `= 2` (`kDCMEqualToDoubleRoundedUpTo15Digits`) | as above, **no continuation follows** (the encoder returns after writing the double) | `Decimal128(bin, round to 15 digits, toward +∞ on the magnitude)`; the original decimal had at most 15 significant digits and equals this |
   | same, DCM `= 1` or `3` | as above, then an 8-byte decimal continuation | base = `integerPart > 2^53 ? Decimal128(integerPart) : Decimal128(bin, 34 digits, toward +∞)`; result = base normalized + continuation as a coefficient at base's exponent (`readDecimalContinuation`) |
   | 8-byte int, fraction bit set (`fracBytes = 0`) | integer part `>= 2^55` with fraction; DCM implied `3` | as the previous row: a decimal, continuation follows |
   | `kNumeric{Pos,Neg}LargeMagnitude`, 8 bytes `== ~0` | ±∞ | double ±Infinity (decimal infinities compare equal) |
   | `…LargeMagnitude`, top bit set, not `~0` | a normalized Decimal128 magnitude: high 63 bits then 8 more bytes | `Decimal128{low, high}`, negated for the negative CType; exact (the exponent bits in type bits only pick a cohort) |
   | `…LargeMagnitude`, top bit clear | double `>= 2^63`: bits `>> 1` with an implied leading exponent bit; low bit = "has decimal continuation" | no continuation: double `bin` (this also covers `LLONG_MIN`, whose double is exact); continuation: `Decimal128(bin, 34 digits, away from zero)` + continuation |
   | `kNumeric{Pos,Neg}SmallMagnitude`, selector (top 2 bits) `= 0` | a raw Decimal128 magnitude `< 2^−1074`: 8 + 8 bytes | `Decimal128{low, high}`, negated as needed; exact |
   | `…SmallMagnitude`, selector `= 1` or `2` | a double with exponent shifted by 256 (`2^−1074 <= |x| < 2^−255`), low bit = "has continuation" | no continuation: double `scaledBin · 2^−256`; continuation: the subnormal-aware Decimal128 reconstruction of the stock decoder, then the continuation |
   | `…SmallMagnitude`, selector `= 3` | a double (`2^−255 <= |x| < 1`) with a 2-bit DCM | DCM 0: double; DCM 2: `Decimal128(bin, 15 digits, toward +∞)`, no continuation; DCM 1 or 3: `Decimal128(bin, 34 digits, toward +∞)` + continuation |
   | `kStringLike` | the string | BSON String (a Symbol original compares equal to a String) |
   | every other CType | unchanged | as the stock decoder; no type bits are involved |

   Nested `_id` documents and arrays apply the same table element by
   element. Negative numeric CTypes invert the bytes and negate the
   result, exactly as the stock decoder does. Value preservation holds
   because MongoDB compares numbers across int, long, double and
   Decimal128 exactly by value, treats every zero and every NaN as equal
   within its kind, and ignores a decimal's exponent (cohort) for
   equality; the only information the type bits carry that this decoder
   cannot reproduce is that representation, never the value.
   The result compares equal to the original `_id` under MongoDB's
   comparison for every supported type, so `deleteOne({_id})` matches, while
   possibly differing in BSON numeric type or decimal exponent. This decoder
   ships with the round-trip test described in §10 item 7, which covers
   every row of the table above. Logging the
   delete key's type bits instead would be exact but is a persistent log
   format change (no record follows a `Delete` op today, F1, and replay's
   parser assumes so). Consumers that need a stable per-document key, for
   routing or partitioning, do not hash `documentKey`; they use the
   `eloq.keyBytes` field (§5.4a), which is the logged KeyString itself.
4. `ns` is the table name from the log record, verbatim. **No catalog lookup
   of any kind happens during decoding**, and no collection UUID is
   attached to events, positions or tokens. A UUID would be resolved
   against the *current* catalog, so a record of `db.c` written under one
   incarnation would receive the UUID of a later incarnation once the
   collection was dropped and recreated, and the same record would sort and
   resume differently depending on when it was read. Everything an event
   needs is immutable log content: the namespace, the key bytes, the value,
   the timestamp, and for DDL the schema-op message.

Per DDL record (`decodeCdcDdl`, `src/cdc/cdc_ddl_decoder.*`): decode
`SchemaOpMessage`; the namespace is `table_name_str`, verbatim, as for data.
A schema image begins with the collection's catalog entry, a BSON object
`{md: {ns, options, indexes: [{spec, ready, …}]}, idxIdent, …}`, behind a
native-order `size_t` length (`EloqDS::SerializeSchemaImage`), and the
decoder reads only that object, bounded by the image, so it needs neither
`MongoTableSchema` (which wants the store handler for the kv part) nor a
catalog. For `create`, the options are `md.options` of `new_catalog_blob`
without the UUID and EloqDoc's catalog version, which is what
`listCollections` reports. An index op's indexes are those whose names one
image has and the other lacks, which is the same set
`alter_table_info_blob` names: for `createIndexes`, the specs added in
`new_catalog_blob`, without the `ns` field; for `dropIndexes`, `{v, key,
name}` of each index `old_catalog_blob` has and `new_catalog_blob` lacks,
or, for the second record of an add index rolled back after its build
(B8), of each index it had added. An image that cannot be decoded fails
the stream with `ChangeStreamFatalError`; a record of another engine or op
type, a rolled-back create or drop, or the server's feature document
(`featureDocument`, no collection namespace) yields nothing, which the log
service never records anyway. Otherwise one DDL record yields exactly one
event.

### 5.4a Event documents

The format step emits these documents. Field names follow MongoDB's change
events; the DDL shapes follow the 6.0 expanded events, which is where
`create`, `createIndexes`, and `dropIndexes` are defined. Common to every
event:

| Field | Value |
|---|---|
| `_id` | the resume token (§5.6) |
| `operationType` | one of the strings below |
| `clusterTime` | `Timestamp(secs = ts / 1e6, inc = ts % 1e6)` of the event's timestamp |
| `ns` | `{db, coll}` split from the logged namespace |
| `eloq.keyBytes` | **DML events only.** BinData: the logged KeyString of `_id` (F3), verbatim. It is the document's stable routing key: byte-identical for every insert, replace and delete of one document, independent of the BSON representation of `_id`. `documentKey` is not usable for that purpose, since an insert's `_id` may be `NumberLong(7)` while the canonical delete decoder (§5.4 step 3) yields int32 `7`, equal in value but different bytes. An EloqDoc extension field; standard consumers ignore it |

Per operation type:

| `operationType` | Source | Additional fields |
|---|---|---|
| `insert` | DML `Insert` | `documentKey: {_id}`, `fullDocument` |
| `replace` | DML `Update`/`Upsert` | `documentKey: {_id}`, `fullDocument` |
| `delete` | DML `Delete` | `documentKey: {_id}` (canonical decoder, §5.4 step 3) |
| `create` | collection DDL, `CreateTable` | `operationDescription: <collection options>` as `listCollections` would report them (`collation`, `validator`, …; never `capped`, which EloqDoc rejects outside `local`, §6.4); `idIndex` omitted |
| `drop` | collection DDL, `DropTable` | none |
| `createIndexes` | index DDL, `AddIndex` | `operationDescription: {indexes: [<index spec>, …]}`, one spec per index the op added (a `createIndexes` with several specs is one op): `{v, key, name, unique?, sparse?, partialFilterExpression?, collation?, expireAfterSeconds?, …}` as `listIndexes` would report it. The stream reports the source's spec faithfully, `unique` and `expireAfterSeconds` included; the consumer drops both when applying (§6.3, §6.4) |
| `dropIndexes` | index DDL, `DropIndex` | `operationDescription: {indexes: [{v, key, name}, …]}`, one entry per index the op dropped |
| `invalidate` | the invalidate step, collection-level streams only, after the watched collection's `drop` | none; `_id` is the drop's position with token kind 2, usable with `startAfter` only |

Not emitted in v1: `modify` (`collMod`: validator, TTL and other option
changes on an existing collection or index; see §4.3 and §6.4), `rename`,
`dropDatabase` (EloqDoc drops a database as a sequence of collection drops;
the stream reports those, and database-level streams are not invalidated,
§5.1), `update` with `updateDescription`, `txnNumber`/`lsid`, `wallTime`.
Deployment-level streams, the CDC mode, are never invalidated.

### 5.5 Filters

- **Dedup happens on logged entries in the decode step.** During bucket
  migration a write-set entry with a forward address is appended to both
  node groups' blobs of the same tx (`FillDataLogRequest`, the
  `forward_addr_` loop), so the same `(txn, table, key)` entry, byte for
  byte, appears in two records of one transaction, both at the
  transaction's timestamp in the same group (§5.4). The decode step keeps,
  per group, the set of `(txn, table, key)` entries it has produced at the
  group's *current* timestamp, clears it when the group moves to the next
  timestamp, and suppresses an entry already in the set. The set can
  never suppress a distinct change: two changes to one key never share a
  timestamp (F4). On resume inside a timestamp the set is rebuilt from
  the records re-read below the token's key (§5.6). Nothing downstream
  deduplicates, and nothing downstream sorts (§5.6). The set is bounded
  (§5.8); when it overflows, a duplicate may be emitted twice, which the
  consumer absorbs as an idempotent re-application in the same slot.
- **No incarnation filter; the stream order is enough.** A data record names
  its table but not which incarnation of the name it belongs to (F9), and the
  stream never needs to know: DDL and DML are merged into one timestamp
  order, so a write belongs to the incarnation whose create precedes it and
  whose drop follows it. That order is true because of the drop's position
  rule, §4.4(b), which makes two guarantees: every write of the old
  incarnation is below `P_drop` (drained by the commit-stage lock and
  published into `LastReadTs`), and the next create's `ddl_ts` is above
  `P_drop` (its `D2` is forced above the catalog entry's commit timestamp,
  which `PostCommit` set to `P_drop`), with every write of the new
  incarnation above that. So no data entry falls in `(P_drop, pos_create)`.
  The prepare-stage `D` would not give this order: DML of the old
  incarnation legitimately commits between `D_drop` and `P_drop` (F7) and
  belongs *before* the drop, which is where `P` places it. The cursor keeps
  no per-table boundaries and drops nothing by incarnation. A runtime check
  that dropped an entry landing in the gap would only hide a broken position
  rule, so the rule is asserted by the fault tests instead (§10 item 6).
- **No pending state.** Create records exist only once committed (§4.3), so
  the cursor never withholds a DDL for its outcome.

### 5.6 Event identity and resume tokens

Two different things were conflated in earlier drafts and are kept apart
here: an event's **identity**, and the stream **position** a token records.

**Identity.** Every event is about one logical object at one timestamp:

| Event class | Logical object | `key` |
|---|---|---|
| 0 · Collection DDL (create, drop) | the collection | empty; the name is in `namespace` |
| 1 · Index DDL (create/drop indexes) | the indexes of the collection one op added or dropped | their names |
| 2 · DML (insert, replace, delete) | a document of the collection | the logged primary-key bytes (KeyString of `_id`) |

`(timestamp, namespace, class, key)` identifies an event uniquely, because
successive changes to one logical object always have distinct timestamps
(F4 for documents, F7/§4.4 for schema). Identity is what the event
document exposes (`ns`, `documentKey`, `clusterTime`, the index names) and
what the consumer (§6) keys on. It is
**not** an ordering: the stream promises order per object across
timestamps (C2) and promises nothing about the order of different objects
within one timestamp, which is whatever the pulls produced (§5.3 step 3)
and may differ between runs.

**Position.** Because order within a timestamp is not reproducible, a
resume point cannot be "after this identity". It is the state of every
group's emit cursor at the moment the event was produced: for each log
group that has produced anything, the storage key of the last record it
produced from, and for the group the event came from, the position inside
that record. Resuming from such a token restarts each group's scan at its
own key, so whatever order the pulls take after resume, nothing is skipped
and nothing is repeated except events after the token that the consumer
had not yet acknowledged. A **watermark** token is the same vector of emit
cursors, taken when the source is drained, with the timestamp equal to
`emit_bound`; it means "everything through this timestamp is consumed".
That is exact even when some group still holds buffered, unreleased
records: a record is unreleasable only if some other group's boundary is
below `T − 1`, so its `T` exceeds `emit_bound + 1`, and every record at or
below `emit_bound` has already been produced. A group with no emit cursor
(it has produced nothing since the stream started or resumed) has no
entry; the resume rule below positions it from the token's timestamp.

**Token encoding is EloqDoc's own.** MongoDB's 4.0 token is keyed on the
collection UUID, ruled out in §5.4, so `ResumeToken`'s layout is not
reused; only the wire shape that drivers expect is kept:
`_id: {_data: <hex string>}`, opaque to clients. `_data` is a versioned,
variable-width byte string built only from immutable log information:

```text
_data = 0x02                          // format version
      ‖ u64 timestamp                 // the event's timestamp; for a watermark, emit_bound
      ‖ u8  kind: 0 event, 1 watermark, 2 invalidate (startAfter only)
      ‖ u16 entry count
      ‖ entries, one per log group that has produced at least one record:
          u32 log_group_id
          u8  form: 0 = resume AFTER this key; 1 = resume AT this key, after position (section, entry)
          u16 len ‖ data-stream key   (commit_ts, ng, txn)             — the data-CF key
          u16 len ‖ ddl-stream key    be64 ddl_ts ‖ be64 txn           — the DDL record's position (§4.3)
          u32 section ‖ u32 entry      // form 1 only: last produced entry of this record
```

**Byte order: every multi-byte integer above is big-endian**, including the
timestamp, the entry count, `log_group_id`, the two `u16` key lengths and
the form-1 `section`/`entry`. Nothing compares token bytes, so the choice is
arbitrary; it is fixed here so that encoder and decoder cannot disagree and
so that a token dumped as hex reads in field order.

The two per-group keys are exactly the `continuation` the logger returns
(§4.2, whose layout is shared for this purpose), so a token entry is a
continuation with a form and a position. For the group the event came
from, the entry is form 1 with the key of the record being decoded and the
position of the entry that produced the event: the ordinal of its section
in the blob and of the entry within that section (F1). The position is a
property of the record alone, independent of every filter, so a token is
valid for any stream scope. Every other group gets a form-0 entry with the
last key whose entries were all produced, but only if that key lies beyond
what the token's own timestamp covers: at or above an event's timestamp, or
above a watermark's. A group whose keys are covered is left out and resumes
by the rule for a group without an entry, which is exact for it. A
watermark is issued only once the source drained, when every group's
records at or below it have been produced, and an event at `T` only after
every group's records below `T`, since the merge releases the lowest
timestamp first (§4.2). Carrying such a key would send the resume below the
token, into history the log may have purged (§4.5) although the token covers
it. A DDL record yields one
event, so a DDL event's own group uses form 0. The byte layout is not
order-preserving and nothing compares token bytes; tokens are decoded and
interpreted per group. Because entries are keyed by `log_group_id` and
counted, the format survives a later change in the number of log groups
(§5.1). The stream's `clusterTime` field, which drivers display and use
for `startAtOperationTime`, is `Timestamp(secs = ts / 1e6, inc = ts % 1e6)`,
lossless for microsecond timestamps.

`resumeAfter`/`startAfter` with token `R`, per group:

- **With an entry.** Let `T_K` be the higher of the two keys' timestamps.
  A group produces its records in the merged order of §4.2, so once it has
  produced a record at `T_K` in one stream, every record of the other
  stream below `T_K` came before it and was produced. At `T_K` itself the
  tie rule of §4.2 decides: if the other stream is the data stream, all of
  its records at `T_K` came first and were produced too; if it is the DDL
  stream, none of them was, and since its key lies below `T_K`, each of
  them is above that key and is produced by the rule below. Either way the
  lower-timestamped stream has no record between its key and `T_K` that the
  resume could skip. Scan with `from_ts = T_K`, always
  through `from_ts` rather than a continuation, because the logger's
  continuation only resumes *after* a key and a form-1 entry needs the
  record *at* it. Records at `T_K` whose storage key is at or below the
  entry's key for their stream are not produced; data records among them
  are decoded only to rebuild the group's per-timestamp dedup set (§5.5).
  For form 1 the record at the data key is decoded and its entries through
  `(section, entry)` are likewise consumed without producing. Everything
  after that is produced normally. Re-reading the head of one timestamp is
  the whole cost of a resume.
- **Without an entry**, including a group absent from `R` because it did
  not exist when `R` was issued: start at `from_ts = R.timestamp` for an
  event or invalidate token (kind 0 or 2), since records at the event's
  timestamp from other groups may not have been produced yet, and at
  `from_ts = R.timestamp + 1` for a watermark (kind 1), since a watermark
  certifies its timestamp consumed. An entry whose `T_K` is below that
  start is treated the same way, for the reason given above; a token this
  server issues never carries one, but a token written otherwise may.
  Ignore an entry for a group that no longer exists.

The first watermark of a fresh stream (§5.1) has no entries, so it starts
every group at `S + 1`, as stated there.

**Resuming a collection-level stream at its own drop.** §5.1 promises that
a collection-level stream ends with `invalidate` after the watched
collection's `drop`. A consumer that applied the `drop`, saved its token,
and reconnects later must still receive that `invalidate`, even if the
source has since recreated a collection of the same name, and the resumed
stream must neither re-emit nor re-apply the drop. Checking whether the
name exists is therefore the wrong test; the token identifies the drop
record itself, and the cursor recognizes it:

1. If the token is an event token (kind 0) and some form-0 entry's
   DDL-stream key has `ddl_ts` equal to the token's timestamp, that record
   is re-read: the resume scans the group with `from_ts = ddl_ts` anyway
   (that is its `T_K`), and the record is consumed without producing, as
   every record at or below the entry's key is. When the stream reaches
   it, if its `schema_op` is a `DropTable` of the watched collection, the
   token is the drop's token.
   (A kind-2 token is never checked: it already denotes the position
   after the `invalidate`.)
2. In that case emit a single `invalidate` event whose `_id` is the same
   vector with kind 2, and close the cursor. Nothing in scope precedes it:
   every group restarts at the drop's timestamp, and no write of the
   collection and no other DDL on it shares that timestamp (§4.4(b), F7).
   The drop is not re-emitted, because the entry is form 0 ("after this
   key"), and the consumer already applied it.
3. Otherwise the stream goes on as a normal resume.

This holds for `resumeAfter` and `startAfter` alike when the token is the
drop's. A later incarnation of the same name is a different collection
and is reached only by opening a new stream. Deployment- and
database-level streams skip step 1: for them the drop is an ordinary event
and the recreated collection's events follow it. If the referenced record
has been purged, the scan reports `PositionTruncated` and the usual
history-loss handling applies (§4.5).

An `invalidate` token (kind 2) is accepted by `startAfter` only and is the
vector recorded after the watched collection's `drop` event: the new
stream starts with whatever follows it, which for a recreated collection
is its `create` and then its data.
`postBatchResumeToken` follows §5.3 step 4: it is recorded by the batch
loop at append time, so it is the token of the last document actually
appended to a reply, or the watermark for `emit_bound` when the source
reported itself drained at `IS_EOF`, or the `invalidate` token when the
stream closes after it, even if a user stage filtered the `invalidate` out;
a stashed or user-filtered document can never move it ahead of what was
appended.

Multi-document transactions: all writes of one tx share its commit
timestamp and all of its records live in one log group, but they are
decoded record by record (§5.4), and two transactions that committed in
the same microsecond interleave in storage order and hence in pull order,
as do records from different groups at one timestamp. The stream carries
no transaction identity in v1
(`txnNumber`/`lsid` are not set), and consumers must not infer transaction
boundaries from adjacency; §6 applies events per object, which never
depends on them.

### 5.7 Latency

`arrival_complete_ts` advances once per checkpoint round (`checkpointer_interval`, default
10 s, `core/src/tx_service_init.cpp`), so end-to-end latency is roughly one to
two intervals plus the consumer's poll. Deployments that need less lower the
interval. A stuck write lock pins `ckpt_ts` and therefore the stream, exactly
as a lagging shard pins a sharded MongoDB change stream; the existing
`checkpoint_stall_rounds` gauge already alerts on it.

**Tail records and idle periods.** The last records written before a quiet
period sit at some timestamp `T`, and nothing after them will ever push the
bound past `T`; they can be emitted only if the checkpointer keeps reporting
during idle time. It does, and the stream depends on it:

- With no active transaction, `ActiveTxMinTs` returns `clock − 1`
  (`cc_shard.h`), so each round's `ckpt_ts` rises with the clock and the
  round is not skipped by the `ckpt_ts <= last_ckpt_ts` test
  (`checkpointer.cpp`, `Ckpt()`).
- A round that finds no modified table still reports: `truncate_log_ts_` is
  zero, is set to the round's `ckpt_ts`, and `NotifyLogOfCkptTs` runs
  because it exceeds the last reported value (`checkpointer.cpp`, the
  `truncate_log_ts_ == 0` branch, whose comment says it exists to mark the
  completion of an empty round). `need_truncate_log_` is true by default and
  is cleared only by data-sync tasks that skipped entries, which an idle
  round does not run. Standby nodes never notify; leaders do.
- Under MVCC the reported timestamp is lowered by `ckpt_delay_time_`, or
  held at the oldest snapshot transaction's start (`GetNewCheckpointTs`);
  that delays the tail by a constant, or until that transaction ends, but
  never prevents it.

So within about one `checkpointer_interval` after the last transaction
finishes (plus the MVCC delay if enabled), every logger's
`arrival_complete_ts` passes `T`, each scan exhausts with `!has_more`,
`scanned_ts` reaches the bound (§4.2), and the tail records are emitted,
after which the idle stream keeps producing watermark tokens. If a future
change made idle rounds stop notifying the log service, the tail of every
quiet period would be stuck until the next write; §10 lists the test that
guards this.

### 5.8 Resource limits and cancellation

**Size limits, from the store outward.** Every bound below is either a hard
cap that already exists in the system or a soft cap with an explicit rule
for exceeding it, so the stream can always make progress.

| Object | Bound | Rule when exceeded |
|---|---|---|
| One stored record (a tx's per-ng blob) | it was written through one `WriteLog` RPC, so it fits brpc's `max_body_size` (data_substrate sets it to 512 MB) | cannot happen |
| One `CdcScan` page | soft: `max_bytes`, from the server parameter `cdc_scan_max_bytes` (default 4 MB, at most 32 MB, the logger's own cap), read when a stream opens; hard: brpc `max_body_size`. A group is scanned again only once its buffered records are consumed, and a `getMore` stops waiting at its first result, so one reply holds at most about one page per log group | the logger always returns **at least one record** even if it alone exceeds `max_bytes`; the hard cap cannot be hit by one record (row above), and the page is cut before it would be by more |
| Per-cursor buffered records, all groups | soft: `cdc_cursor_buffer_bytes` (default 64 MB), counted as **received-and-unconsumed bytes plus `max_bytes` reserved per scan in flight** | a new `CdcScan` is issued only while that count is below the cap, **except** the scans needed to release the lowest unemitted record: a group whose boundary is behind is always fetched (§5.3 step 1), because its lag blocks every other group. Every response that arrives is admitted in full; nothing is dropped. The peak is therefore not the cap: with `G` log groups the cursor can hold the cap **plus one page per group**, and a page is `max_bytes` unless a single record exceeds it (row above), so **peak ≤ `cdc_cursor_buffer_bytes` + `G` × max(`max_bytes`, largest record)**, plus the dedup sets (next row). With the defaults and four groups that is 64 + 4 × 4 = 80 MB for ordinary records, and 64 + 4 × 40 = 224 MB if every group happens to return a 40 MB record at once. Decoding one record at a time bounds decode-side memory only; it does not shrink responses already received, and the size-check at issue time is what keeps the `G` term from compounding |
| Per-group dedup set (§5.5): the `(txn, table, key)` entries produced at the group's current timestamp | soft: `cdc_dedup_set_bytes` per group (default 4 MB); the set is cleared whenever the group moves to the next timestamp, so it holds at most the keys of one timestamp | when a group's set is full, further entries at that timestamp are produced without being recorded, so a forward-write duplicate among them may be emitted twice. That is harmless by construction: the two events are byte-identical, at the same timestamp, for the same key, and the consumer applies them in the same slot in order (§6.1), so the second is an idempotent re-application; the set exists to avoid the redundant event, not to protect correctness. The occurrence is counted (`cdc_dedup_set_overflow`) |
| Long-poll wait | `wait_ms <= min(remaining maxTimeMS, cdc_scan_max_wait_ms (default 30 s))` | the logger completes the request at its deadline regardless |
| Parked long-poll requests per log group | `cdc_scan_max_parked` (default 1024) | beyond it the logger answers immediately with an empty page and `arrival_complete_ts`; the caller polls |
| In-flight `CdcScan` per (cursor, group) | exactly one | a new scan is issued only after the previous completed or its cancellation callback ran (see below) |
| One event document | `BSONObjMaxUserSize` (16 MB) | see "oversized events" |
| One `getMore` reply | MongoDB's batch budget (`FindCommon::kMaxBytesToReturnToClientAtOnce` = 16 MB, first document always allowed, `find_common.cpp`) and `batchSize` | the batch is cut before the event that would exceed it; `postBatchResumeToken` per §5.3 step 4 |

**Oversized events.** A document at the 16 MB limit plus the event envelope
(`_id` token, `ns`, `documentKey`, `clusterTime`) exceeds
`BSONObjMaxUserSize`. MongoDB 4.0 fails such change events with
`BSONObjectTooLarge`, which would wedge a stream permanently at that event,
and the split mechanism MongoDB added later (`$changeStreamSplitLargeEvent`)
is not in this tree. Rule for v1: if the serialized event exceeds
`BSONObjMaxUserSize`, the format step emits it **without** `fullDocument` and
with the EloqDoc extension field `fullDocumentOmitted: true`; everything
else, including the token, is unchanged. The consumer treats such an event
as MongoDB's `updateLookup` would: read the current document from EloqDoc by
`documentKey` and apply that. If the read finds a newer version, applying it
early is harmless because the later event for the same key re-applies it
in order (per-object order, C2); if the read finds nothing, the document was
deleted since and the delete event follows, so the event is skipped. This
triggers only for documents within about a kilobyte of the limit.

**Cancellation ownership.** Three things can end a `getMore` while scans
are in flight: the `maxTimeMS` deadline, `killCursors` (or the cursor's
idle timeout, or the close after `invalidate`), and a logger leader change.
The rules that keep them from racing:

- **In-flight RPCs are owned by the cursor, not by the `getMore` call.**
  Each group's scan state (controller, response, continuation, flags) lives
  in a `shared_ptr` that the RPC completion callback also holds. The
  callback never holds or resumes a coroutine handle: a `getMore` coroutine
  may already have returned on its deadline, and resuming a finished
  coroutine is a use-after-free. Instead the callback deposits the response
  (or error) into the shared state under a mutex and notifies a per-cursor
  wait object. Only that object resumes anything: a `getMore` coroutine
  registers itself as the waiter before yielding and deregisters before
  returning, and `notify` atomically resumes the registered waiter if there
  is one or just records that data arrived if there is none. The deadline
  timer uses the same object. A response that lands after the `getMore`
  returned is simply there for the next one. This is what makes a deadline
  safe: returning a batch never abandons an RPC, and no RPC ever touches a
  coroutine that is gone.
- **Deadline.** On `maxTimeMS` the coroutine returns the batch it has; it
  does **not** cancel outstanding scans, since they complete at their own
  `wait_ms` (clamped to the same deadline) and their results are wanted.
- **Cursor destruction** (`killCursors`, idle timeout, close after
  `invalidate`, session end). The cursor manager already pins a cursor for
  the duration of a `getMore`, so destruction happens between calls. The
  destructor sets a `cancelled` flag on every group's shared state and
  calls `brpc::StartCancel` on each in-flight controller; it does not wait
  (F11 forbids blocking the worker). Each callback checks the flag under
  the mutex, drops the response, and releases its `shared_ptr`; the last
  release frees the state. No callback ever touches a destroyed cursor.
- **Log-group leader change** (the braft leader of a log group, which is
  the only replica that serves `CdcScan`; unrelated to EloqDoc node-group
  leadership). A scan that fails with `NotLeader`, a connection error,
  or a cancellation is recorded as a failure in the group's state; the next
  `getMore` refreshes the leader from `redirect` or the leader cache and
  reissues from the **same continuation**, which only ever advances on a
  successful response. Because there is at most one in-flight scan per
  group and a superseded response is dropped, a stale page from an old
  leader can never be applied after a newer one.
- **Logger side.** A parked long-poll request is completed exactly once by
  whichever of several sources, running on different threads, comes first;
  each parked entry carries a `std::atomic<bool> completed`, every source
  does `compare_exchange(false → true)`, and only the winner runs the brpc
  `done` closure (running it twice is undefined behavior, never running it
  leaks the request until the client's timeout). The sources: its deadline timer,
  an `arrival_complete_ts` advance (the only append that can make a record
  emittable, §4.2), the client's cancellation
  (`Controller::NotifyOnCancel` removes it from the parked list), or
  `on_leader_stop`/shutdown, which complete every parked request with
  `NotLeader`. The parked list holds only the closure and the request's
  bounds; no buffers are allocated until the request is actually served.

## 6. Consumer

### 6.1 Apply loop

A standalone process using a MongoDB driver. The stream guarantees order per
logical object only (C2), so the consumer may parallelize across objects but
must never reorder operations on one object. **MongoDB's unordered bulk
writes give no such guarantee**: with `ordered: false` the server "can execute
the operations in parallel, as well as in a nondeterministic order", so
`replace(k, A), replace(k, B)` in one unordered batch may leave `A`, and a
replace followed by a delete may resurrect the document. Ordered bulk writes
(`ordered: true`) execute serially in list order.

The consumer therefore uses a **fixed pool of slots**:

```text
stream = source.watch(pipeline, resumeAfter = load_token())      // full deployment or filtered
slots  = N worker queues; slot(e) = hash(e.ns, e.eloq.keyBytes) mod N   // one document → one slot, always
loop:
  batch = drain(stream, up to M events or maxAwaitTimeMS)
  for e in batch, in stream order:
    if e is DDL:
      barrier()                            // wait until every slot's queue is empty
      apply e alone:
        drop           → collection.drop()          (ignore NamespaceNotFound)
        create         → createCollection(options)  (ignore NamespaceExists)
        createIndexes  → createIndexes(specs minus unique, expireAfterSeconds)  (ignore IndexAlreadyExists)
        dropIndexes    → dropIndex(name) per entry  (ignore IndexNotFound)
      save_token(e.token)
    else:
      slots[slot(e)].enqueue(e)            // insert/replace → replaceOne({_id}, fullDocument, upsert=true)
                                           // delete         → deleteOne({_id})
  barrier()                                // all slots drained for this batch
  save_token(batch.last or postBatchResumeToken)

slot worker: take its queue, group by collection keeping relative order,
             send each group as an ORDERED bulkWrite with
             bypassDocumentValidation: true; retry the slot with
             backoff on transient errors; any other error halts the consumer
             with an alert and the saved token intact
```

Opening the stream can fail with `ChangeStreamStartNotReady` when the
cluster has not completed a checkpoint round within `cdc_open_wait_ms`
(§5.1). Drivers do not retry a failed `aggregate`, so the consumer retries
the open itself with backoff; any other open error halts it.

**Namespaces.** The stream is deployment-wide; the consumer applies the
databases in its scope only, never `admin`, `local` or `config`, the same
ones bootstrap copies (§6.2). In them it replicates collections and
`system.js` (stored JavaScript, user data) as documents, and views as
definitions: the source reports a view's create, `collMod` and drop as an
insert, a replace and a delete on `system.views`, which the consumer
applies as the view command, behind the barrier like a DDL (create, or
`collMod` when the view exists; drop), rather than writing the sink's
`system.views`, which a sink may refuse. Every other system collection
(`system.profile`, ...) belongs to its node and is skipped, in the copy and
the stream alike.

Why this is correct:

- **Every write bypasses document validation.** Bootstrap copies collection
  options, validators included (§6.2), and MongoDB's write commands default
  to `bypassDocumentValidation: false` (`write_ops.idl`). The source may
  have accepted a document with `bypassDocumentValidation: true`, or under
  a validator that was later tightened; its post-image is committed source
  state, and the sink's job is to hold that state, not to re-judge it. A
  validator rejection on the sink would be a non-transient error and would
  halt the consumer permanently. So every `bulkWrite` in the apply loop and
  every `replaceOne` in the bootstrap copy sets
  `bypassDocumentValidation: true`; the validator remains on the sink for
  anything written to it outside CDC. This requires the consumer's user to
  hold the `bypassDocumentValidation` action on the target namespaces.
- **Per-object order.** Every operation on one document lands in the same
  slot and is applied by that slot in stream order with ordered writes. The
  routing key is `(ns, eloq.keyBytes)`, the logged KeyString of `_id`
  (§5.4a), never `documentKey`: two events for one document can carry
  `_id` in different BSON representations (an insert's `NumberLong(7)`
  versus a decoded delete's int32 `7`), and hashing those would split the
  document across slots and reorder it. The KeyString is the same bytes for
  both. Different documents in one slot are serialized too, which costs
  parallelism, not correctness. Order across slots is never observed by any
  document.
- **DDL is serialized against everything.** The barrier before a DDL drains
  all pending DML, the DDL is applied alone, and dispatch resumes afterwards,
  so a drop follows every write of the incarnation it ends and a create
  precedes every write of the one it starts, as the stream positions require.
- **Progress is a barrier.** Slots finish out of order, so the token cannot
  advance per slot. It is persisted only after a whole batch (or a DDL) has
  been applied by every slot. Nothing from batch N+1 is applied before batch N
  completes, so a crash replays from the previous token and re-applies batch N
  in full. That re-application is safe **because the sink imposes no
  constraint that an already-applied operation could violate** (§6.3):
  replace and delete carry whole state and per-object order is intact, so
  re-applying a prefix leaves the same final state. Per-slot low watermarks
  would allow finer progress; not needed for v1. "Applied" means
  acknowledged under the sink's write concern, the same one the token's
  own write uses: every sink write carries it, DDL included (a driver's
  generic command helper adds none, so the consumer adds it and fails on
  a write concern error), or a sink failover could keep a token past
  writes it lost.
- **Coalescing (optional).** Within one batch a slot may keep only the last
  operation per key, since replace and delete carry whole state.

No version comparison is needed on the sink: re-delivery replays an ordered
prefix whose final state is unchanged. Multi-document transaction atomicity is
not preserved, as stated in §1.

The token is stored in the sink, in `admin.__eloq_cdc_progress`, after each
successful batch. **The checkpoint namespace must be disjoint from the
mirrored namespaces**, and `admin` is the natural home: a deployment-level
stream excludes only `local`, `admin` and `config` (§5.1), so a source
collection in any other database, `__eloq_cdc.progress` included, is a
mirrored collection whose `drop` or writes the consumer would apply on the
sink, on top of its own checkpoint. Under `admin` no source event can name
it, and the consumer never mirrors into `admin`. Storing the checkpoint
elsewhere is fine as long as it is outside every mirrored namespace and is
written only after the batch is durable on the sink. The progress document has two states,
`{state: "streaming", token}` and `{state: "bootstrapping"}`, and
`load_token()` returns a token only in the first: a saved token is a
claim that the sink holds every event at or before it, and bootstrap
(§6.2) withdraws that claim durably before it touches the sink and
restores it, with `T0`, only once the copy is durable. On startup the
consumer either resumes from a `streaming` token or, finding
`bootstrapping` or nothing, runs bootstrap from its first step. The
principle: **whenever the consumer decides to discard the sink's data,
the token is re-acquired**, never carried over.

### 6.2 Bootstrap (snapshot then replay)

**Contract.** Bootstrap requires a fresh, empty destination for the
namespaces in scope. The snapshot copies collection definitions, including
empty collections and their options, index definitions, and documents.
Restarting bootstrap with a new `T0` discards the previous attempt's partial
destination state before copying again. The stream cannot repair a sink that
is stale for any other reason: a document the source deleted before `T0` is
never mentioned after `T0`, and the DDL that created a collection or index
before `T0` is never replayed, so copy-only onto an existing sink leaves
deleted documents alive forever and empty collections, options and indexes
missing. Every path that ends in "re-bootstrap", including recovery from
`ChangeStreamHistoryLost`, therefore starts by invalidating the saved
checkpoint and then dropping the in-scope namespaces on the sink, and
ends by publishing `T0` as the new checkpoint only after the copy is
durable (steps 1 and 6). Between those two writes the sink has no usable
checkpoint, by design: the decision to discard the sink's data is also
the decision to re-acquire the token.

**DDL is gated during the copy; DML is not.** (Cluster gate, mode `ddl`.) Documents tolerate a fuzzy
copy because replay from `T0` re-applies every later change per object.
Definitions do not: the copy reads the *current* catalog, and replay then
re-applies every DDL after `T0` against it. Source history after `T0` such
as `create index i on {x:1}`, `drop index i`, `create index i on {y:1}`
makes the copy hold `{y:1}` and replay attempt `{x:1}` first, which MongoDB
rejects with `IndexKeySpecsConflict`, not the ignorable
`IndexAlreadyExists` (`index_catalog_impl.cpp`, the spec-conflict check),
so the consumer halts before ever reaching the resolving drop. The same
shape exists for collection options. Rather than reconciling conflicting
definitions during replay, which converges but rebuilds indexes and
collections for a race that exists only during bootstrap, bootstrap holds
DDL still while the catalog is copied:

- **Cluster gate (source side).** The requirement is only that **no DDL
  runs while the catalog is being copied**, so v1 uses an explicit pause:
  an admin command, `eloqClusterGate: {op: "pause" | "resume", mode: "ddl" | "ddl_dml"}`,
  sets or clears a cluster-wide flag that, in mode `ddl`, makes every new
  `UpsertTableTxRequest` (create, drop, index ops) fail fast with a
  retriable error. View changes are held still the same way, though they
  are not schema ops: a view's create, `collMod` and drop are writes to
  its database's `system.views`, and EloqDoc's record store admits every
  write there by first reading the cluster config record locally in the
  writing transaction (`EloqRecoveryUnit::checkClusterGate`). A local
  read takes a read lock whatever the transaction's protocol and keeps it
  until the transaction ends, as a schema op's lock step does, so a pause
  waits for a view change already admitted, the release publishes its
  commit timestamp below `pause_ts`, and a view change that reads a paused
  gate is refused. The command needs the `eloqClusterGate` action on the
  cluster resource, which `clusterManager` holds (and so `clusterAdmin`
  and `root`). **The pause stays in force until it is explicitly
  resumed.** There is no lease, no renewal, no clock comparison, no
  expiry and nothing to validate at the end of the copy: the flag either
  is set or is not, and only `resume` clears it. That holds for every
  copy of the flag, the loggers' included (durability bullet below), so
  the pause is durable across recovery for as long as it lasts and the
  copy has no deadline. The price is that a consumer that dies mid-copy
  leaves DDL paused until an operator clears the pause and abandons
  that attempt (§8); that is accepted for v1 in exchange for a
  mechanism with two states. CDC bootstrap uses mode `ddl` and nothing
  else in v1; the mechanism is named and shaped for reuse, since a
  consistent backup or snapshot needs the same window with DML excluded
  as well (mode `ddl_dml`, specified below, not implemented for CDC). The
  request itself is short and one-shot; **the exclusive window is the
  interval between `pause` and `resume`**, not the request: pause sets the
  flag and returns once no DDL is in flight, the copy runs inside the
  window, and resume clears the flag. No lock or transaction is held open
  across the copy, so DML and the copy's own catalog reads are unaffected;
  a lock-held window would block both. Mechanism, built on what DDL
  already does:
  - *Carrier.* Every `UpsertTableOp` and `UpsertTableIndexOp` begins with
    `lock_cluster_config_op_`, a read-local of the cluster config record
    under a read lock, before touching any catalog entry
    (`tx_operation.cpp`, the op constructors and the first `Forward()`
    step). What that read returns today is an **empty record**: the
    cluster-config cc map holds one entry, `neg_inf_`, whose payload is a
    default `ClusterConfigRecord` used only for concurrency control; the
    real topology lives in `Sharder` and the host manager, and the map's
    `PostWriteAll` deliberately never stores the payload it is handed
    (`cluster_config_cc_map.h`, the constructor and the comment before
    the lock release in `Execute(PostWriteAllCc&)`: "the record ... is
    always empty"). The gate therefore needs three adaptations of that
    map, each small:
    - `ClusterConfigRecord` gains an **optional**
      `cluster_gate {mode, paused}`, carried through the copy
      constructor, assignment, `Serialize`, and `Deserialize` with a
      has-bit, exactly as the record already serializes its node-group
      configs behind `has_ng_config`. `mode` is `ddl` or `ddl_dml`.
      Absent and "present with `paused = false`" are different
      values on purpose: the first says nothing, the second is a resume.
      The record carries no pause time, and the map notes none: the
      only thing that would want one, the alert below, measures the
      pause's duration itself.
    - `Execute(PostWriteAllCc&)` gets a **gate-only path that bypasses
      the topology code**. After decoding the incoming record, the handler
      branches on whether it carries a gate: if it does, it skips
      straight to the existing lock-release block **without entering the
      `Sharder::UpdateClusterConfig` branch**, and stores the gate into
      `neg_inf_`'s payload **inside that block's ownership check**, in
      the branch that already runs only when the lock's write holder is
      the request's `txn` (`cluster_config_cc_map.h`, the
      `write_lk_txn == txn` test before `SetCommitTsPayloadStatus`,
      `OnCommittedUpdate` and `ReleaseCceLock`). A request whose
      transaction no longer owns the lock stores nothing and just
      finishes, exactly as the handler already commits nothing for it.
      The placement matters: a post-write can arrive late, retried after
      its transaction has completed elsewhere, and by then a newer gate
      transaction may have taken the lock, installed its value and
      released. An older `resume` delivered after a newer `pause` would,
      if stored unconditionally, overwrite the pause and admit DDL
      during the new bootstrap; gated on ownership it is a no-op, since
      the lock now belongs to the newer transaction or to nobody. The
      same rule is what the recovery path relies on when it re-runs the
      install on surviving groups (durability bullet below). That bypass is mandatory,
      not an optimization: the topology branch runs whenever the request's
      commit timestamp exceeds `Sharder::ClusterConfigVersion()`, which a
      gate op's always does (its `commit_ts_` is raised above the
      acquire-all `MaxTs()`), and it calls `GetNodeGroupConfigs()` on the
      record (`cluster_config_cc_map.h`, the `UpdateClusterConfig` call),
      which dereferences the record's config pointer
      (`cluster_config_record.h`, `GetNodeGroupConfigs`). A gate record
      carries no node-group configs, so that call would dereference null.
      A record without a gate takes the existing path unchanged:
      cluster-scale post-writes never carry one, so an ordinary topology
      update passes through the handler without clearing a pause, and
      the stored gate is left untouched because the handler never
      overwrites the payload on that path. The node-group-config part of
      the payload stays as it is today (unused, `Sharder` is the source
      of truth).
    - With the gate in the payload, the DDL's existing read-local gets it
      for free: `Execute(ReadCc&)` copies the entry payload into the
      op's `cluster_conf_rec_` (`template_cc_map.h`, the payload copy in
      `Execute(ReadCc&)`), so the check below reads
      `cluster_conf_rec_.cluster_gate` and nothing else changes on the
      DDL side.
  - *Check: new admissions only.* A schema op takes the cluster config
    read lock once, at its first step (`lock_cluster_config_op_`), holds it
    through its last post write all and releases it right after
    (`unlock_cluster_config_op_`, before the clean log;
    `tx_upsert_table_operation.cpp` `RunEnding`, `tx_index_operation.cpp`
    the `post_all_lock_op_` branch), so every step it sends to all node
    groups reaches the same node groups. A recovered op resumes under a lock
    its recovery tx took outside the op
    (`LocalCcShards::CreateSchemaRecoveryTx`). The lock step is therefore
    exactly where an op is admitted, and the gate check lives **only
    there**, immediately before `acquire_all_intent_op_`
    (`UpsertTableOp::RunUntilPrepareLog`, `UpsertTableIndexOp::Forward`).
    There the op reads `paused` from the record it just locked and, if it
    is set, fails with a new `TxErrorCode::CLUSTER_GATED` through the
    existing error path; nothing has been acquired yet, so nothing is
    undone. An op past its lock step, or a recovered one, is never gated:
    those operations are exactly what pause's drain (below) is waiting to
    finish, and
    rejecting them there would make the drain wait on operations the gate
    itself blocks, and would turn a roll-forward that F8 guarantees into a
    stall. EloqDoc maps `CLUSTER_GATED` to its own error
    `ClusterGatePaused` (5001), which a client retries once the gate
    resumes: a create (an insert's implicit one too), a drop or
    `dropDatabase`, an index build or drop, `collMod`, and a view's
    create, `collMod` or drop each fail at once and change nothing. The refusal leaves EloqDoc's catalog writes
    as a status, never as a write conflict, so neither
    `writeConflictRetry` nor the drop's own retry loop spins on it. A
    refused index build's cleanup writes back the catalog entry it
    started from, which differs from the stored one only in its
    `catalogVersion`; EloqDoc skips that write instead of issuing a DDL
    the gate would refuse as well, since the cleanup treats any failure
    but a write conflict as fatal (`fassert 18644`).
  - *Mode `ddl_dml` (future, for backup/snapshot).* DML never reads the
    cluster config, so its check point is different: when
    `install_cluster_config_op_` runs on every core, each shard caches the
    gate (`mode`, `paused`) locally, and `TransactionExecution` consults
    the cache on the first write request of a transaction
    (`TxUpsert`/delete paths), failing with `CLUSTER_GATED` while a
    `ddl_dml` pause is set; reads are never gated. The drain for DML reuses
    the checkpointer's machinery: a `CkptTsCc` round on every node group
    reports `ActiveTxMinTs`, and the drain is complete when every group's
    value equals a freshly synced clock, i.e. no transaction holds a write
    lock anywhere. That state is exactly what a consistent snapshot needs;
    CDC does not use it.
  - *Pause = drain.* The command runs a small multi-stage op modeled on
    cluster scale's `install_cluster_config_op_`: acquire the **write**
    lock on the cluster config record on every node group, post-write the
    record with `paused` set, release. That write lock conflicts with the
    read lock each DDL holds from its first step until after its last post
    write (F7; since PR 7 a create, drop, truncate or index op never
    releases it in between, and the recovery transaction that resumes a
    logged op holds it too), so the acquire waits for every DDL already
    admitted to install its schema and let go, and once the lock is held no
    new DDL passes the check. The lock is the drain; nothing is polled.
    What an admitted DDL may still have left when the lock is granted is
    its clean log, which prunes the log service's replay set and touches
    neither the catalog nor the stream. The command returns the pause's own
    commit timestamp, `pause_ts`, which stage (1) below raises above its
    acquire-all results. A DDL releasing its cluster-config read lock
    publishes its commit timestamp into that shard's `LastReadTs` (the
    `PostReadCc` release carries the tx's `commit_ts_`, and the entry's
    version, the key timestamp it checks, is never 0, so the publication
    always happens), and the cluster config map's acquire-all reports
    `max(LastReadTs, Now())` as `last_vali_ts`, read once every earlier DDL
    on that node group has released its read lock; `AcquireAllOp::MaxTs`
    adds one.
    So `pause_ts` is above every DDL committed before the gate closed, and
    bootstrap uses it to pick `T0` (below).
  - *One request, two ops.* Pause and resume are the same
    `ClusterGateTxRequest` and the same `ClusterGateOp`; `op` selects the
    precondition, the stages and the installed value. The precondition is
    evaluated by the read for write (stage (1) below), against the
    record's state at install time:

    | `op` | Precondition on the record | On failure | Stages | Installed value |
    |---|---|---|---|---|
    | `pause` | not paused | already paused: `CLUSTER_GATE_HELD` (another bootstrap or an abandoned one; the operator decides) | lock, log, install (mode `ddl_dml`, reserved, would add a DML drain) | `mode`, `paused = true`; returns `pause_ts` |
    | `resume` | none (clearing a cleared gate is a no-op) | — | lock, log, install | `paused = false` |

    Resume needs no drain: while the gate is paused no DDL can have
    started. Both ops take the config write lock, so a pause and a resume
    are serialized against each other and against in-flight DDL, and both
    write the log entry, so a leader elected afterwards does not resurrect
    an older state. `pause` refuses to stack: a second bootstrap cannot
    silently share a window whose end the first will trigger.
  - *Observability.* Every node exports a gauge `cluster_gate_paused`:
    1 while its cluster-config map holds a paused value, 0 otherwise. It
    reads the value the map already holds and needs no timestamp. The
    gate-age alert of §8 is a duration rule in the monitoring system: it
    fires when the gauge, taken as the maximum over the cluster's nodes,
    has stayed at 1 for longer than the expected copy duration, which is
    how an abandoned pause is noticed. Nothing else ends a forgotten
    pause, so this alert is the only thing that surfaces one. Because the
    monitor, not the server, measures the duration, a gap in scraping
    the whole cluster restarts the count and can delay the alert by up
    to one threshold (one node restarting does not, since the maximum
    still reads 1 from the others). For a pause abandoned for hours that
    delay is acceptable, and it is why the server keeps no pause time.
  - *Translation into the tx service.* A new
    `ClusterGateTxRequest : TemplateTxRequest<ClusterGateTxRequest, Void>` with
    `op` and `mode` fields, submitted by the command handler through a
    `TransactionExecution` as `UpsertTableTxRequest` is. **Every gate
    command is its own short transaction, pinned to log group 0.**
    `InitTxRequest` already takes a `log_group_id` (`tx_request.h`), the
    tx service assigns a transaction number that maps to that group, and
    the cluster-scale command handler already uses it to send every
    cluster-scale log to group 0 (`remote/cc_node_service.cpp`, the
    `init_req.log_group_id_ = 0` before the scale transaction is
    started). The gate's command handler does the same for `pause`, for
    `resume`, and for an operator-issued `resume` alike: init a
    transaction with `log_group_id_ = 0`, submit the one
    `ClusterGateTxRequest`, finish. No transaction outlives its command,
    nothing is held across the copy, and every `cluster_gate_log` entry
    ever written lands in group 0 regardless of who wrote it. Each op
    takes its own `commit_ts_` from its own acquire-all, so successive
    entries have strictly increasing versions, which is all the log
    state's "keep the latest" rule needs.
    `ProcessTxRequest(ClusterGateTxRequest&)` takes a pooled `ClusterGateOp`
    and pushes it, exactly as `ClusterScaleTxRequest` → `ClusterScaleOp`
    (`tx_execution.cpp`); a finished op goes back to the pool rather than
    being freed, because a remote response that arrives after the op
    finished still reads its stages' cc handler results. `ClusterGateOp::Forward` runs four child stages;
    the read, lock, log and install stages follow `ClusterScaleOp`'s
    config update (`tx_operation.h`, `acquire_cluster_config_write_op_` /
    `update_cluster_config_log_op_` / `install_cluster_config_op_`):
    (1) a **read for write**: a `ReadLocalOperation` on the
    cluster-config map on the coordinator's own node group, the op the
    DDLs use as `lock_cluster_config_op_`, but taking a write intent
    rather than a read lock, since the write lock follows. Schema ops'
    read locks pass a write intent, so this stage blocks no DDL; another
    gate op coordinated by the same node group waits for it, so two of
    them cannot each hold a read lock and deadlock upgrading. Its record
    carries the stored gate, and the op's precondition (table above) is
    evaluated on it, so a `pause` of a paused gate fails before any write
    lock is taken; the transaction's abort releases the intent. The value
    is authoritative because every node group's copy was installed by the
    same replicated post-write (stage 4) or by the same replay (below),
    and no other gate op can install anywhere while the intent is held:
    it needs the write lock on every node group, this one included. When stage (2) has upgraded the intent to
    the write lock, `AcquireAllOp` drops the read from the transaction's
    read set
    (`DedupRead`), as it does for any key a transaction reads before
    writing; otherwise the release of meta reads at commit would compare
    the version read with the entry's commit timestamp, which stage (4)
    moves to the gate's. That match needs the cluster-config map's
    acquire-all to report the entry's lock address, which it reported
    as 0 on a first execution until PR 11, so cluster scale's reads were
    never dropped either. On a precondition failure the read stays, and
    the abort releases it with the version unchanged.
    (2) the write lock on every node group, in two steps, as a schema op
    takes its catalog locks (`UpsertTableIndexOp::Forward`: "Acquire
    write intent, and then upgrade to write lock(Locking), rather than
    acquire write lock(OCC) directly"). (2a) an `AcquireAllOp` on
    `cluster_config_ccm_name`, `ReadForWrite`/`OCC`: write intents on
    every node group. Intents pass DDLs' read locks, so only another
    gate op or a cluster scale makes it fail, and OCC fails rather than
    waits. The op then releases everything through a `PostWriteAllOp`
    with `commit_ts_ = tx_op_failed_ts_` (what the map treats as "release
    the lock, commit nothing"), backs off 1 to 3 seconds with jitter and
    starts again from stage (1), re-checking the precondition; the caller
    sees one request. Never waiting while holding the entry anywhere is
    what keeps gate ops, and a gate op and a cluster scale, from
    deadlocking: deadlock detection runs only when a waiter requests it,
    and `AcquireAllOp` never does. (2b) an `AcquireAllOp` `Write`/`Locking`
    that upgrades the intents. With every intent held, no other gate op
    or cluster scale holds the entry anywhere, so it waits only for DDLs
    holding the config read lock; that wait is the drain, and the queued
    upgrade stops new readers. `commit_ts_` is raised above its results'
    `MaxTs()`. A failure of (2b) is not a conflict but a node group
    without a leader, and starts over the same way. **This stage yields
    no record**: on the cluster-config map `Execute(AcquireAllCc&)`
    fills `AcquireAllResult` with `last_vali_ts_`, the lock address, the
    entry's `commit_ts_` and the term, nothing more
    (`cluster_config_cc_map.h`), an inherent property of acquire-all,
    which locks rather than reads.
    (3) a `WriteToLogOp` whose `WriteLogRequest` carries a **new log
    content kind**, not a new RPC: `LogContentMessage.content` gains
    `cluster_gate_log = 6` of type
    `ClusterGateLogMessage {mode, paused}`
    (`tx_service/tx-log-protos/log.proto`, next to `cluster_scale_log`),
    written through the same `WriteLog` call every other log goes
    through, to the transaction's own log group and to no other: a
    transaction writes its log to exactly one group, and the gate op is
    not an exception. That group is always **log group 0**, because every
    gate transaction is initialized with `log_group_id_ = 0` (translation
    bullet below), which makes its transaction number map there. The
    entry carries, as `node_terms`, the term of every node group that
    granted the write lock in stage (1), as a schema op's prepare log
    does: cluster scale writes a prepare log before it locks, so a
    participant's replay can restore a lock it lost by failing over, but
    the gate writes nothing before, so a group that failed over between
    granting the lock and the entry would come back with the old value
    and no lock, and the install would skip it. With the terms, the log
    service refuses that entry and the op releases its locks and fails;
    the caller retries. The log
    service applies it with a new
    `case kClusterGateLog` in the content switch of `LogInstance::on_apply`
    (`eloq_log_service/src/log_instance.cpp`, the switch that today
    dispatches `kSchemaLog` to `UpsertSchemaOp` and `kClusterScaleLog` to
    `UpdateClusterScaleOp`), calling `LogState::UpdateClusterGate(commit_ts, msg)`.
    That is what makes the gate durable; how the log state holds it is
    the next bullet.
    (4) a `PostWriteAllOp` with `Commit`, whose record is a
    `ClusterConfigRecord` carrying the new gate, version `commit_ts_`, on
    every node group; the record carries no node-group configs, and the
    handler's gate-only path (carrier bullet above) stores the gate and
    releases the lock without touching `Sharder` or calling
    `GetNodeGroupConfigs()`.
    There is no drain stage: stage (1)'s write lock is the drain (the
    *Pause = drain* bullet above), and a `pause` reports the op's
    `commit_ts_` as `pause_ts`.
  - *Durability and replay.* Cluster-scale logs are kept in the log
    service's in-memory `tx_cluster_scale_ops_` map, snapshotted with
    braft, shipped to a recovering node group by `GetClusterScaleOpList`
    in **no particular order** (it walks an `unordered_map`), and erased
    one hour after a `CleanScale`-stage entry falls below `min_ckpt_ts_`
    (`eloq_log_service/include/log_state.h`). Neither property suits a
    value whose *latest* state is all that matters: two gate entries
    replayed in arbitrary order could resurrect a pause, and an erased
    entry would lose it. So the gate is **not** kept as a cluster-scale
    op. Each log group's `LogState` holds one replicated, snapshotted
    `cluster_gate {value, version, txn}` next to `min_ckpt_ts_`;
    `UpdateClusterGate` (the apply of a `cluster_gate_log` entry, stage
    (3)) sets it when `version = commit_ts` is higher than the stored
    one, and the entry itself is not retained: like `UpdateCkptTs`, it
    is a log entry that updates state rather than one that is kept. On
    recovery, the replay list for a node group includes one item per
    log group carrying that group's `cluster_gate` (a new
    `LogItemType::ClusterGateLog`, added beside the schema, split-range
    and cluster-scale items that `GetLogReplayList` already assembles),
    replayed into the cluster-config cc map by a new branch of
    `Execute(ReplayLogCc&)`. That branch does two different things,
    depending on whether the recovering node group **coordinated** the
    gate transaction, decided as cluster scale decides it, from the node
    group id embedded in `txn` (`cluster_config_cc_map.h`, the
    `is_coordinator` test in `Execute(ReplayLogCc&)`):
    - On every node group it installs the slot's value into `neg_inf_`'s
      payload, touching neither `Sharder` nor the migration flags. The
      slot is never older than what the map holds, since nothing installs
      a value it has not logged first. The value is set in place, as the
      gate's post-write sets it, because cluster scale's replay, which
      `RecoveryService` runs and waits for first, caches node group
      configs in the same payload; and the entry's commit timestamp only
      grows, since scale's replay may leave it at a config version above
      the gate's. The gate item replays before any schema op, so the node
      group enforces the gate before it serves DDL. A corrupt slot fails
      the replay rather than bring the group back unpaused.
    - On the coordinator's node group it additionally recovers the
      coordinator, under the general rule for multi-stage ops: once an
      op's log is flushed the op is guaranteed to complete, so on failure
      coordinator and participants are recovered to keep moving forward,
      and **the stage at which the coordinator resumes is dictated by
      the log**, not chosen by the recovery code. For cluster scale that
      means reading the logged stage (`PrepareScale` versus
      `ConfigUpdate`) and the migration progress to decide which child
      op to resume at. The gate op is simpler because it writes exactly
      one log entry, at stage (3), after the precondition passed and
      while the write lock was held: the only recoverable state is
      "gate decided and logged, install pending", and a coordinator lost
      before that entry left nothing to recover. Nothing in the op follows
      its log but the install, so recovery takes **no lock or intent**: the
      replay has already installed the value on the recovering node group,
      and a recovery transaction (`TransactionExecution::RecoverClusterGate`,
      modeled on `RecoverClusterScale`, `tx_execution.cpp`) runs a pooled
      `ClusterGateOp` positioned with its log stage marked finished, so
      `Forward` runs the install stage (4) alone. Re-running stage (4) is
      what **completes a partially installed update**: the install is a
      `PostWriteAllOp` under the *same* `txn`, and on every surviving node
      group that still holds the config write lock for that `txn` the
      map's handler stores the gate and releases the lock
      (`Execute(PostWriteAllCc&)`, the `write_lk_txn == txn` branch); on a
      node group that failed over after the log entry, the coordinator's
      own included, the lock is gone and the value was installed by that
      group's own replay, so the request just finishes. (A group that
      failed over between granting the lock and the log entry never gets
      that far: the entry carries the granting terms and the log service
      refuses it, stage (3).) Re-taking the write lock on the recovering
      group, as the `locked` path of cluster-scale replay does, would
      deadlock: the schema-op recovery that replays next reads the
      cluster config under a read lock, and replay waits for that read,
      while the install that would release the lock is refused until the
      group leads, which it does only once replay ends. The install
      retries a second apart until the group leads. A slot whose op
      finished long ago is re-installed the same way at every recovery of
      its coordinator's group, finding the lock nowhere: one extra
      cluster-wide post-write per such recovery.
      This path is required, not optional: the cluster-config write lock
      is a meta lock that orphan-lock recovery does not clear on its own,
      its release depends on the coordinator (`tx_operation.cpp`, the
      comment at the catalog post-all timeout: "the orphan lock recovery
      mechanism doesn't handle catalog orphan wlock; it depends on the
      coordinator"), and `07-durability-and-recovery.md` §5.4 hands all
      meta write locks to the log-consulting path, which for this log
      kind is exactly the replay described here. Until the coordinator's
      node group recovers, a surviving group is in one of two states: its
      post-write already ran, so it holds the new value with the lock
      released, or it did not, so it holds the old value with the lock
      still held. For a half-installed `pause` that admits no DDL either
      way (the new value rejects, the held lock blocks). For a
      half-installed `resume` some groups admit DDL and others block on
      the lock, which is harmless: the resume is decided and logged, and
      is guaranteed to complete.
    Because every gate transaction is pinned to log group 0 (translation
    bullet below), **group 0 is the only logger that ever holds a
    `cluster_gate`**, and its slot is the single, ordered history of the
    gate: pause, resume, an operator's resume after the consumer died,
    all apply there in commit order, and the slot always reads the
    latest. No other group carries a copy, so there is no cross-group
    reconciliation, no stale value to outvote, and the highest-version
    rule only orders successive entries in that one group. The replay
    list of every node group therefore contains at most one gate item,
    from group 0. **The slot is never aged out.** Schema ops and
    cluster-scale ops are purged an hour after they finish (`log_state.h`,
    the loop that erases a `CleanSchema` or `CleanScale` entry once
    `min_ckpt_ts_` passes its commit timestamp by an hour) because each
    is a per-operation record, and without the purge they would
    accumulate. The gate is not: it is one slot, overwritten in place by
    every entry, so there is nothing to collect. A resume is its cleanup
    — "not paused" is simply the latest value — and a resumed slot costs
    one small record for as long as it sits there. Expiring it would buy
    nothing and would break the rule that only a resume ends a pause.
    Two alternatives were considered and
    rejected: a per-group write-to-all fan-out, because a transaction
    writes its log to one group, atomically, and the recovery,
    deduplication and truncation rules of the log service rest on that;
    and one long-lived transaction spanning pause and resume so that
    both would map to the same group, because pinning each short
    transaction to group 0 gives the same single-group history with
    nothing held open and covers operator-issued resumes as well.
    Because the slot never expires, a node group that recovers at any
    point during a pause comes back paused, like every other group:
    there is no age past which the loggers have forgotten a pause that
    the cc maps still enforce, and so no window in which a recovered
    group admits DDL while the rest refuse it. That is also why the copy
    has no deadline (step 4 below). If the consumer dies mid-copy, the
    pause holds everywhere until the operator's `resume` (§8) clears the
    cc maps and group 0's slot through this same path. The cc map's
    constructor still starts from an empty payload; a node that restarts
    learns the gate from this replay before it can serve DDL, as it
    learns everything else.
    Coordinator recovery therefore follows cluster scale's rule: a
    coordinator lost after (3) is rolled forward by the coordinator-side
    replay above, which re-runs the install on every node group and
    releases the lock everywhere; one lost before (3) wrote nothing, and
    the write lock it held on other node groups is an ordinary orphan
    meta lock whose log consultation finds no `cluster_gate` at that
    `txn` and clears it. The consumer sees the coordinator's death as a
    transport error on its `pause` or `resume`; it retries, and a retry
    that reports `CLUSTER_GATE_HELD` after a `pause` cannot distinguish
    its own recovered pause from another bootstrap's, so it stops and
    the operator decides (§8), as for any held gate. The recovered
    transaction is a new `TransactionExecution` on the new leader and
    finishes on its own; the consumer's later `resume` is, as always, a
    fresh short transaction pinned to group 0.
- **Letting prior DDL pass `T0`.** Every DDL that ran before the gate
  closed committed below `pause_ts`, the pause's commit timestamp (the
  *Pause = drain* bullet above), so `pause_ts` bounds them all, and their
  records sit at or below it in the DDL stores (index ops append at
  prepare, create and drop at commit, §4.3). Two kinds of token prove that
  every such DDL lies at or before the token, and either serves as `T0`:
  - a **watermark** with timestamp `>= pause_ts`: it is issued only when
    every group's boundary covers its timestamp and every record at or
    below it has been produced (§5.6);
  - an **event token** with timestamp `T > pause_ts`: the event was
    released only when every other group's boundary was at least
    `T − 1 >= pause_ts` (§4.2 emission rule), so every record with timestamp
    below `T`, every DDL below `pause_ts` included, had been
    fetched and, being below `T`, produced before it; the token's vector
    therefore stands after all of them, and a group without an entry
    resumes at `from_ts = T`, also above them. Strictly greater keeps the
    rule safe for any bound a record could sit on, although none sits on
    `pause_ts`.
  The event-token case matters because watermarks are issued only when the
  source drains (§5.3 step 4): under continuous DML every batch can end
  with an event token, and a rule that accepted watermarks alone could
  wait indefinitely while checkpoints passed `pause_ts` unobserved. No
  clock is compared across nodes and no interval is guessed; the wait is
  at most about one `checkpointer_interval`, whether the cluster is idle
  or busy.

No version field is needed because the snapshot is taken after the stream
position is recorded:

1. **Invalidate the checkpoint, then destroy.** Durably write
   `admin.__eloq_cdc_progress = {state: "bootstrapping"}` (§6.1), replacing any
   saved token, and only then drop the in-scope namespaces on the sink
   (a no-op on a fresh destination; on a restart this discards the
   partial state). The order matters: a crash after the drop but before
   the invalidation would leave a token that claims completeness over a
   sink that has just lost a collection, and a restart through
   `load_token()` would stream into it. With the invalidation first, a
   crash anywhere in steps 1 to 5 leaves no usable checkpoint, and the
   next start runs bootstrap again from this step.
2. `pause` the cluster gate in mode `ddl`; the command returns once its
   write lock has waited out every admitted DDL and the gate is installed,
   with `pause_ts`. The command runs as its own
   short transaction pinned to log group 0 (§6.2) and holds nothing
   afterwards. A transport error is retried. If the command reports `CLUSTER_GATE_HELD`, a previous
   attempt was abandoned without a `resume`, another bootstrap is
   running, or this consumer's own earlier `pause` was completed by
   coordinator recovery after its reply was lost (§6.2, durability
   bullet); the operator resolves that (§8), the consumer does not.
3. Open a fresh stream and read its `postBatchResumeToken`s, applying
   nothing, until one qualifies under the rule above: a watermark with
   timestamp `>= pause_ts`, or an event token with timestamp
   `> pause_ts`. That exact token is `T0`. Close the stream. Every DDL
   committed so far lies at or before `T0`; none can commit until step 5.
   Events at or before `T0` are never replayed, and that is correct: they
   are committed state at the time of the copy, which step 4 reads.
4. For each collection in scope, in this order: create the collection on
   the sink with the options read from EloqDoc's catalog (`listCollections`),
   create its indexes from `listIndexes` **with `unique` and
   `expireAfterSeconds` removed** (§6.3, §6.4),
   then `find()` on EloqDoc and `replaceOne(upsert)` every document into
   the sink with `bypassDocumentValidation: true` (§6.1: the copy holds
   committed source state and must not be re-validated). Empty collections are thereby created too. Every copied
   definition equals the source's definition at `T0`, since no DDL can run;
   every copied document reflects some state at or after `T0`. The copy
   has **no deadline**: the pause cannot lapse on its own (§6.2,
   durability bullet), so however long the copy takes, no DDL runs
   during it and a node-group recovery in the meantime comes back
   paused. How long DDL stays unavailable is the copy's duration, which
   the operator sees in advance and the gate-age alert watches.
   Which namespaces the copy takes follows the apply loop's rule (§6.1):
   collections and `system.js` with their documents; a view by its
   definition alone, a `create` with its `viewOn`, `pipeline` and
   `collation`, since it has no indexes or documents of its own; no other
   system collection. The definitions are read from `system.views`, the
   documents whose changes the stream reports, so the copy and the replay
   start from the same records. View changes are held still too (the
   gate bullet above), so the copy reads the views exactly as at `T0`,
   even through a cursor that spans several transactions, and replay
   applies only changes made after the resume, each over a set of views
   that existed on the source. Replaying view changes over a later copy,
   as documents are replayed, would not be safe: the sink checks every
   view against the others (no cycle, a bounded depth, matching
   collations), and an older definition beside newer copied ones can form
   a cycle the source never had, which the sink refuses on every retry. A
   consumer that halts on an error after step 2 resumes the gate before it
   stops; its checkpoint still says `bootstrapping`, so the next run starts
   again from step 1. Only a consumer that dies leaves the gate paused
   (§8).
5. `resume` the cluster gate, another short transaction pinned to log
   group 0. Every DDL from here on has `ddl_ts > T0` and
   reaches the sink through the stream, applied against a catalog that
   was exact at `T0`. Nothing needs verifying here: the gate cannot
   lapse on its own, so if step 2 succeeded and no one resumed the gate
   in between, no DDL ran during step 4. An operator who resumes the gate
   while a copy is running has abandoned that copy; the consumer treats a
   `resume` it did not issue as a failed bootstrap only if it learns of
   it, which is why the runbook (§8) says to stop the consumer first.
6. **Publish the checkpoint.** With every copied collection and index
   durable on the sink (the copy writes with the sink's durable write
   concern, and this step waits for them), write
   `admin.__eloq_cdc_progress = {state: "streaming", token: T0}`. `T0` was
   chosen in step 3 but is usable only now; until this write, a restart
   repeats bootstrap. Then open `resumeAfter: T0` and run the apply
   loop. Every document change after `T0` is replayed after the copy
   landed, so the final state per object is the last event. Creating
   the (non-unique) indexes before the documents means they are built
   incrementally rather than in one pass afterwards; either order is
   correct.

The copy must finish for a collection before that collection's events are
applied; running the copy collection by collection while the stream stays
unopened satisfies this trivially. Because no DDL runs during the copy, no
collection can be created or dropped while it is being copied; the first
DDL replay sees the catalog exactly as it stood at `T0`, and every later
`create` either targets a name that does not exist on the sink or follows
its own `drop` in the stream. If replay nevertheless meets
`IndexKeySpecsConflict`, `IndexOptionsConflict`, or a `NamespaceExists`
whose options differ, the gate was bypassed; the consumer halts with an
error naming the bootstrap protocol rather than guessing at a repair.
Retention must cover the paused window plus the copy duration;
`PositionTruncated` during step 6 means restarting from step 1. In v1 that
error is never raised (§4.5), so the copy duration must be kept well inside
`txlog_cdc_retention_seconds`. DDL is unavailable for the copy duration, which
the operator sees in advance, plus however long an abandoned pause goes
unnoticed, which the gate-age alert bounds in practice.

### 6.3 Index uniqueness at the sink

**Rule.** Uniqueness of secondary indexes is enforced at the source only. The
consumer creates every secondary index on the sink as **non-unique**, both
during bootstrap and when applying `createIndexes` events, by dropping the
`unique` option from the spec the stream reports. The `_id` index is
unaffected: every DML is applied by `_id`, so it can never conflict on it.

**Why.** The consumer and the sink are separate systems. Applying an event
and persisting the progress token cannot be one ACID transaction, so after
any failure the token and the sink's state disagree, and the consumer
necessarily re-applies operations the sink has already absorbed. Replace and
delete tolerate that: they carry whole state and per-object order is
preserved, so a re-applied prefix converges to the same result. A unique
constraint does not tolerate it: it judges each operation against the sink's
*current* state, which may already reflect later events, and rejects a
re-applied older insert whose value has since moved to another document.
That failure is permanent under retry. Earlier drafts of this design tried
to keep the sink's unique indexes by ordering events around ownership
transfers, detecting transfers from the index log, adding a deletion phase
to the stream, barriers and extra token writes in the consumer, and a
special duplicate-key policy. Each closed one case and the next review found
another, because the underlying requirement, exactly-once application
against a constraint-checking sink without sink-side versioning, is not
achievable across two systems. Dropping the constraint on the sink removes
the whole class.

**Consequences.**

- The stream itself is unchanged and faithful: `createIndexes` events carry
  the source's spec including `unique: true`; only the consumer's
  application differs.
- The source enforced uniqueness on every committed state, so the sink's
  non-unique indexes are free of duplicates exactly when the sink holds a
  committed source state: every event of every transaction the sink has
  applied any event of has been applied, and nothing else. **A persisted
  token does not mark such a point.** The stream carries no transaction
  boundaries (§5.6), a batch can end between two events of one source
  transaction, and the token saved after that batch (§6.1) is a fully
  applied prefix of the *stream* while the sink holds half of a
  transaction: a value the transaction moved from document A to document
  B may sit on both, or on neither, until the rest arrives. So
  **duplicates are normal, not exceptional**, at any token, and the sink
  must tolerate them, which is precisely why the indexes are non-unique.
  They also arise inside a batch during ordinary parallel application
  (the two documents of a transfer are applied by two slots and B's new
  image can land before A's), during bootstrap (`find()` reflects
  different instants for different documents, so a value transferred on
  the source mid-copy can appear twice until replay from `T0` reconciles
  it), and after a crash (the replay of a partially applied batch passes
  through the same intermediate states again). All of these converge once
  every event of the transactions involved has been applied. The
  consumer cannot recognize such a point from the stream in v1; the only
  point it can name is a **quiescent source**: no writes committing on
  the source, the stream drained (empty batches with advancing watermark
  tokens, §5.7) and every slot empty. Readers of the sink that need
  uniqueness read only at such a point.
- Sink-side unique indexes remain possible in a later version only with
  sink-side versioning, i.e. a per-document version field and conditional
  writes, the approach set aside in §1; it is listed in §9.

### 6.4 Collection and index options on the sink

The stream carries no `collMod` (§4.3), so an option changed on an existing
collection or index after bootstrap is not mirrored in v1. The scope is
stated per option so that nothing is left to inference:

| Option | Copied at bootstrap? | Mirrored after? | Effect of divergence, and rule |
|---|---|---|---|
| Collection validator (`validator`, `validationLevel`, `validationAction`) | yes, from `listCollections` | no | none on replication: every CDC write bypasses validation (§6.1); a stale validator affects only writes made to the sink outside CDC. Left as copied. |
| Index `expireAfterSeconds` (TTL) | **no: stripped** | not applicable | a sink-side TTL deletes on its own clock; if its setting ever differed from the source's, the sink would delete documents the source still holds. The source's TTL deletions already arrive as ordinary `delete` events, so expiry, like uniqueness (§6.3), is enforced at the source only. The consumer removes `expireAfterSeconds` from every index it creates, at bootstrap and from `createIndexes` events. |
| Index `unique` | no: stripped (§6.3) | not applicable | as §6.3. |
| Other index options (`sparse`, `partialFilterExpression`, `collation`, `key`) | yes | via `dropIndexes`/`createIndexes` events only, since they cannot change in place | none: an index with these options changed is dropped and recreated on the source and the stream reports both. |
| Capped (`capped`, `size`, `max`) | **no: never created capped** | not applicable | a capped sink collection evicts on its own, by *its* insertion order across documents, which the consumer does not preserve (it preserves order per document only, §6.1); a document evicted on the sink but not on the source has no later event to restore it. This is the third sink-side deletion mechanism, after uniqueness and TTL, and it is excluded the same way. EloqDoc does not support capped collections outside `local` (`src/mongo/db/catalog/database_impl.cpp`, the `collectionOptions.capped` rejection; `EloqKVEngine::supportsCappedCollections` is false and `EloqRecordStore` has no eviction path), and `local` is outside every stream, so no `create` event and no `listCollections` result in scope carries `capped: true`. The consumer still enforces the rule: if it ever meets `capped: true`, it creates an ordinary collection, drops `size` and `max`, and logs it, so that the sink never gains a deletion mechanism the source does not log. |
| Collation and other create-time collection options | yes | via `drop`/`create` events only | none: they cannot change in place. |

If EloqDoc gains capped collections, the stream must first
carry each eviction as a `delete` event before the consumer may create
capped collections on the sink (§9).

## 7. Correctness argument

- **C1 No event is ever older than one already emitted, per object.**
  A record at `T` from group `g` is emitted only when every other group's
  completeness boundary is at least `T − 1` (§4.2 emission rule), so every
  record strictly below `T` from every group is already in hand and, being
  below `T`, was emitted earlier by the same rule; `g`'s own records arrive
  in storage order. By F6 no data record at or below `T − 1` arrives at any
  logger later; by the §4.4 invariant and the logger's read order no DDL
  record does either. The boundary rule (a page cut at `last_ts` certifies
  only `last_ts − 1`; an exhausted scan certifies the bound; pages are
  prefixes of the merged timestamp order) is what makes "in hand" exact
  for a timestamp split across pages. A watermark for `emit_bound` is
  issued only when every group's boundary covers it and every record at
  or below it has been produced (§5.6).
- **C2 Per-object order.** Events for one logical object (a document, an
  index, a collection) are emitted in timestamp order, which is
  serialization order (F4, F7), by C1. Order among different objects within
  one timestamp is pull order, carries no semantic claim, and may differ
  between runs (§5.6); the sink imposes no cross-object constraint that
  could depend on it (§6.3).
- **C3 DDL order.** A DDL at `ddl_ts` precedes every DML that observed its
  schema (F7; for drop, the next create's `D2` is forced above the drop's
  re-assigned `P` through the catalog entry's commit timestamp, §4.4(b)) and
  follows every DML of the incarnation it ends, because `P` is derived from
  the commit-stage lock that drained them (§4.4(b)). No DML lies between a
  drop's `ddl_ts` and the next create's `ddl_ts`, so the stream order alone
  places every write in its incarnation (§5.5); the fault tests of §10
  item 6 assert it, and nothing at runtime filters by incarnation.
- **C4 Resumability.** A token records every group's emit cursor, as a
  storage key plus, for one group, the position of the last produced entry
  in the current record (§5.6); the keys are immutable and the entry order
  within a record is fixed, so resuming restarts each group exactly where
  it stood, regardless of the pull order before or after the resume. Every
  token the server hands out, per-event or `postBatchResumeToken`, is the
  token of a document appended to a reply, or the watermark of a bound the
  source had drained when the batch loop reached `IS_EOF` (§5.3 step 4);
  the batch loop, not the formatting stage, records it, and it records only
  at append time, so no token ever points past a stashed or otherwise
  undelivered event. Duplicates are limited to two kinds, both
  idempotent: the events after the token that the consumer had applied
  but not acknowledged, and a forward-write entry emitted twice when a
  group's dedup set has overflowed (§5.5, §5.8), which is the same
  change at the same timestamp for the same key, applied in the same
  slot in order.
- **C5 No blocking.** Neither the EloqDoc worker nor the logger's apply path
  waits on a condition variable or blocks on an RPC (§4.2, §5.3, §5.8).

## 8. Failure modes and operations

| Event | Effect | Handling |
|---|---|---|
| Log group leader change | `NotLeader` on scan | Refresh leader, retry from the same continuation, which advances only on success (§5.8); nothing is lost or duplicated |
| EloqDoc node serving the cursor dies | Cursor gone | Driver surfaces the error; consumer reopens with its token on any node |
| Checkpoint stalls (stuck lock) | `arrival_complete_ts` stops, stream pauses | `checkpoint_stall_rounds` gauge; orphan-lock recovery unblocks it |
| A create's kv tables fail (before its commit log) | the step is not retried: the create gives up at once, under its write intent, which pins nothing: it drops what it created and fails with `DATA_STORE_ERROR`; no event, no stall | the error log names the table; retry the create once the store is back; kv tables whose drop also failed stay orphaned, empty, and a later create of the name writes them again |
| DDL kv write keeps failing after the commit log (create, drop) | the catalog write lock is retained through the retry (§4.4(b)), so `ckpt_ts` of that node group and `arrival_complete_ts` on every logger stop: **all streams stall** and log truncation pauses until the store recovers; the table itself is unavailable | `checkpoint_stall_rounds` alert and the retry warnings in the log, which name the table; fix the store; the op rolls forward on its own, or via recovery if the coordinator fails, which restores a write intent on the coordinator's node group: the op keeps retrying under it, holding back neither the checkpoint nor DML, and no other DDL runs on the table; no rollback path exists. Runbook: [cdc-operations.md](cdc-operations.md) §1 |
| Consumer down longer than retention | `PositionTruncated` → `ChangeStreamHistoryLost` once loggers track the history boundary (§4.5); **in v1 undetected**, the scan silently resumes at the smallest surviving key | Re-bootstrap (§6.2): drop the in-scope namespaces on the sink, then snapshot and replay from a new `T0`. In v1, prevent rather than detect: set `txlog_cdc_retention_seconds` (§4.5) above the worst-case consumer lag and alert on the lag metric |
| Sink unreachable | Consumer stops acknowledging | Nothing happens on the source; retention is the budget |
| DDL attempted while the bootstrap cluster gate is paused (mode `ddl`) | the DDL fails fast with a retriable error (§6.2) | expected; the operator scheduled the bootstrap window; retry after the copy |
| Consumer dies mid-copy with the gate paused | **DDL stays paused until an operator resumes it**, on every node group alike: the pause never expires, in the cc maps or the loggers (§6.2), so a node group that recovers meanwhile comes back paused too. The sink holds no usable checkpoint (step 1 wrote `bootstrapping` before anything was dropped), so a restarted consumer cannot resume into the partial copy; it runs bootstrap again, whose step 1 discards the partial state. A consumer that halts on an error instead of dying resumes the gate on its way out (§6.2 step 4) | gate-age alert when the window outlives the expected copy time — the only thing that surfaces a forgotten pause, since nothing ends one on its own. Runbook: confirm the consumer is stopped, `eloqClusterGate {op: "resume"}`, then restart bootstrap from step 1. Accepted v1 trade-off: DDL availability is recovered by hand, not by a timer |
| Consumer crashes between the copy finishing and the checkpoint being published (step 6) | the sink is complete but `admin.__eloq_cdc_progress` still says `bootstrapping` | the consumer cannot tell a complete copy from a partial one without the checkpoint, so it bootstraps again; the redundant copy is the price of a checkpoint that is only ever written after what it claims is durable |
| `pause` reports `CLUSTER_GATE_HELD` | a previous attempt was abandoned without a `resume`, or another bootstrap is running | operator decides: if no consumer is copying, `resume` and retry; if one is, wait for it. Two bootstraps at once is an operator error |
| Gate resumed by an operator while a copy is running | DDL may run against the catalog being copied; that copy is not trustworthy | the runbook says to stop the consumer before resuming; if it was not stopped, stop it and restart bootstrap from step 1 |
| DDL prepare log fenced (only if §4.4 fallback (c) is used) | `UPSERT_TABLE_PREPARE_FAIL` | Command layer retries; expected to be rare, counted by a metric |
| Pages that take the cursor over its buffer cap | every in-flight response is admitted in full, prefetch pauses (§5.8) | memory peak is the cap plus one page per log group, where a page is `max_bytes` or one oversized record; a single record is bounded by the source's transaction limits (§5.8 gives the formula) |
| Event document over 16 MB | delivered with `fullDocumentOmitted: true` (§5.8) | consumer point-reads the document from the source |
| `killCursors` / idle timeout with scans in flight | in-flight RPCs cancelled, responses dropped, no blocking (§5.8) | none |
| Stream opened before the first checkpoint round completes, and the bound is still zero after `cdc_open_wait_ms` | open fails with `ChangeStreamStartNotReady` (§5.1) | consumer retries the open; the message says whether the wait was shorter than one checkpoint round (raise `cdc_open_wait_ms` or `maxTimeMS`) or rounds are not completing (`checkpoint_stall_rounds`, server log) |

Metrics to add: per-group `cdc_arrival_complete_ts`, per-cursor lag
`(arrival_complete_ts − last emitted ts)`, fence rejections, parked-request count.

## 9. Follow-ups after v1

**History-boundary tracking in the logger** (§4.5: persisted, replicated,
advanced before each purge, covering data, DDL and decode metadata; turns
`PositionTruncated` from defined-but-never-raised into a real detection);
unique secondary indexes on the sink, which needs sink-side versioning (§6.3);
`modify` events for `collMod` (validator, TTL and other in-place option
changes), which needs the `OperationType::Update` branch of `UpsertTableOp`
to get the commit-stage timestamp and post-commit kv write that create got
in §4.4(b), plus a `modify` event shape and its application (§4.3, §6.4);
`updateDescription` from before-images or a document diff; database-level
invalidation if EloqDoc gains a `dropDatabase` operation; `txnNumber`/`lsid`
on multi-document transactions; type bits on delete entries; serving scans
from followers; time-based log retention for the RocksDB log state; rename
collection if EloqDoc adds it; capped collections if EloqDoc adds them,
which requires logging every eviction as a `delete` so the sink can be
created capped without evicting on its own (§6.4); a Kafka Connect source
connector over the same scan RPC.

## 10. Items to confirm during implementation

1. The `$changeStream` stage registration and spec parsing (F10) are compiled
   into the CMake build, not only the SCons one; of the 4.0 internal stages
   only `DocumentSourceCloseCursor` is linked into the EloqDoc expansion
   (§5.2).
2. Drivers in use do not require replica-set topology from `hello` for
   `watch()`; the 40573 check is server-side only. Verify the opening
   protocol (§5.1, F13) with PyMongo, the Java driver and the Node driver,
   each in three scenarios: (i) `watch()` on a cluster that has not yet
   completed a checkpoint round, asserting the open returns within
   `cdc_open_wait_ms` with a `postBatchResumeToken` and no
   `operationTime`-related error; (ii) `watch()`, then the cursor is
   killed before any event is delivered, asserting the driver resumes with
   the opening watermark and later events arrive exactly once; (iii) the
   open exceeding `cdc_open_wait_ms` (checkpointer stopped by fault
   injection), asserting `ChangeStreamStartNotReady` surfaces and a retry
   after the checkpointer resumes succeeds. Also confirm the drivers accept
   a `getMore` reply without `operationTime` when it carries a
   `postBatchResumeToken`.
3. *Done in PR 7.* Create's kv tables are created after the prepare log and
   its catalog row is written after the commit log. A data-store failure
   before the commit log (the kv tables, the sequence table) is not retried:
   the create gives up, drops what it created and fails. The catalog row,
   and drop's and truncate's kv and sequence table steps, come after the
   commit log and retry until the store answers, 30 seconds apart by default, with a
   warning in the log on each attempt; index ops apply the same rule with
   the prepare log as the point of no return. The retry after the commit log
   keeps the catalog write lock; it is **not** downgraded (§4.4(b)). The
   operator runbook for a prolonged store failure, which stalls every
   stream, is [cdc-operations.md](cdc-operations.md) §1 (B7, PR 9).
4. *Confirmed in PR 9.* `SchemaOpMessage.new_catalog_blob` for EloqDoc
   carries the collection options needed for `create` events (collation,
   validator and similar; capped collections cannot exist in scope, §6.4):
   its catalog entry's `md.options`, from which `create` reports what
   `listCollections` does (§5.4; `eloq_cdc/ddl_events.js` compares them).
5. Rename collection is unsupported in EloqDoc (no `rename` path in
   `eloq_kv_engine.cpp`); confirm and document the user-facing behavior.
6. Implement §4.4(a) (registry: reset `wlock_ts_` on downgrade, count catalog
   write locks) and §4.4(b): re-assign `commit_ts_` after
   `acquire_all_lock_op_` in `UpsertTableOp` (`AssignCommitStageTs`) for create, drop, and
   truncate; move create's `upsert_kv_table_op_` into the commit-log success
   branch and rebuild the coordinator's dirty schema with `P` before it;
   promote `req.CommitTs()` in the catalog map's `PostCommit`; update the
   op's commit timestamp at the `CommitSchema` stage in
   `LogState::UpsertSchemaOp`.
   Recovery from a commit log uses the recorded value; recovery from only a
   prepare log re-runs the lock stage and re-derives it. Add a fault-injection test that
   runs a checkpoint round while the DDL's write lock is held and asserts
   `ckpt_ts < ddl_ts`, and one that commits DML between a drop's prepare and its
   commit-stage lock and asserts the drop is emitted after every such DML,
   that a following create is emitted after the drop, and that no write to
   the table is emitted between the drop and the create. The stream has no
   runtime incarnation check behind these tests (§5.5). *Both exist:* the
   checkpoint test is `CkptLockRegistry-Test`; the drop test is
   `eloq_fault/ddl_drop_order.js` (PR 9), which holds a drop between its
   prepare log and its commit-stage write locks with the data substrate's
   `upsert_table_hold_before_commit_lock` fault point and inserts on the same
   node meanwhile. The drop's MongoDB database lock does not stop them:
   every EloqDoc operation runs with `EloqLockerNoop`, whose locks exclude
   nothing, so only the data substrate's catalog locks order a drop against
   DML, and a write on the drop's own node is placed like one on another
   node.
7. The canonical delete-key decoder (§5.4 step 3) and its round-trip test
   against the real `KeyString` encoder exist before any delete event is
   emitted; the stock `KeyString::toBson` must not be called with empty
   type bits anywhere in the CDC path. The test enumerates every row of
   the §5.4 table: int32 and int64 at their limits and beyond 2^53,
   `LLONG_MIN`, doubles with fractions in every `fracBytes` width, doubles
   and decimals at and beyond 2^63, ±∞, NaN, all zeros including `-0.0`
   and decimal zeros of several exponents, decimals hitting each DCM
   value (0, 1, 2, 3), decimals above 2^53 with and without fraction,
   tiny doubles and decimals below 2^−255 and below 2^−1074, huge decimals
   beyond the double range, symbols, and nested documents and arrays. Each
   decoded value is checked against the original with a MongoDB numeric
   comparison and against a real `deleteOne`, and the test also asserts
   the decoder consumed exactly the bytes the stock decoder consumes.
8. Reply-size behavior of the 4.0 cursor path for the oversized-event rule
   (§5.8): confirm whether a single event document between
   `BSONObjMaxUserSize` and `BSONObjMaxInternalSize` can be returned in a
   `getMore` reply at all, and set the `fullDocumentOmitted` threshold at
   whichever limit the reply path actually enforces. Verify the brpc
   `StartCancel`/`NotifyOnCancel` pairing with the log service's parked
   requests under a forced leader step-down.
9. The batch loops' call placement (§5.3 step 4). *Resolved against the
   tree:* there are **two** loops, `generateBatch` in `getmore_cmd.cpp`
   and `handleCursorCommand` in `run_aggregate.cpp`, both recording at
   append (after `haveSpaceForNext`) and at `IS_EOF` and nothing on
   stash; the aggregate loop pulls nothing when `batchSize` is 0. The
   tree predates 4.0.7, so **no `postBatchResumeToken` exists at any
   layer**: `CursorResponseBuilder::done` emits only the top-level
   `$_internalLatestOplogTimestamp`, and the only executor query is
   `getLatestOplogTimestamp()`. The implementation adds the field at
   every layer: a `postBatchResumeToken()` on the scan source, reached
   through `PipelineD` → `PipelineProxyStage` → a new
   `PlanExecutor::getPostBatchResumeToken()`; a per-`ClientCursor`
   "previous batch's token", initialized at registration from the opening
   token so a `batchSize: 0` aggregate still returns one; and
   `CursorResponseBuilder::setPostBatchResumeToken`, emitted **inside the
   `cursor` sub-object** as drivers expect. The mongo shell likewise has
   no `postBatchResumeToken` handling and gets the 4.0.7 behavior
   (`DBCommandCursor` stores it; `getResumeToken()` returns it when the
   batch is exhausted). Delivery tracking stays in the loops, never in a
   stage. Confirm that the `Document`-to-BSON conversion ahead of the
   builder is the reason the token must live in `_id` rather than in
   metadata. *Resolved:* `_id` integrity is enforced at the end of the
   pipeline (§5.3 step 4), not at open; tested for removal (`$project`,
   `$unset`), replacement (`$addFields: {_id: "$documentKey"}`), nested
   modification (`$addFields: {"_id._data": …}`), `$replaceRoot`, and
   the pass-through cases (`$project` keeping `_id`, `$match` filtering
   events), through the real `PipelineProxyStage`.
   Test, in **both** loops: `batchSize: 0` returns the opening or supplied
   token; a batch whose last pulled document is stashed returns the
   previous document's token; a batch whose events are all removed by a
   user `$match` returns the drained watermark; an idle `getMore` returns
   an advancing watermark; the batch that delivers `invalidate` returns
   the kind-2 token, in the first batch (resume at own drop) and in a
   later one; a non-change-stream cursor carries no such field.
10. Idle-tail emission (§5.7): a test that writes one transaction, then
    stops all writes, and asserts that a deployment-level stream delivers
    its events within `checkpointer_interval` plus `ckpt_delay_time_` and
    then keeps advancing `postBatchResumeToken` every round with no
    further events. Confirm the value and meaning of `ckpt_delay_time_`
    in the EloqDoc configuration, since it adds directly to tail latency.
11. The cluster gate used by bootstrap (§6.2): the
    optional `cluster_gate {mode, paused}` field on
    `ClusterConfigRecord`, carried through copy, assignment and
    serialization with a has-bit; the gate-only path in
    `ClusterConfigCcMap::Execute(PostWriteAllCc&)` that stores the gate
    into `neg_inf_`'s payload and releases the lock **without** entering
    the `Sharder::UpdateClusterConfig` branch or calling
    `GetNodeGroupConfigs()` on a record that has no configs (today the
    handler stores nothing and would dereference null on such a record),
    leaving the gate untouched for records without one; a unit test that
    a gate-only post-write with a commit timestamp above the topology
    version neither crashes nor changes `Sharder`'s configuration; a
    unit test that a gate post-write whose `txn` no longer holds the
    write lock stores nothing: deliver an old `resume`'s post-write
    after a newer `pause` has taken the lock, installed and released,
    and assert the entry still reads paused and the newer transaction's
    lock state is untouched; the `paused` check
    in `UpsertTableOp`'s `lock_cluster_config_op_` step and the unprepared
    branch of `UpsertTableIndexOp::Forward`, reading `cluster_conf_rec_` filled by
    the existing read-local; the pause/resume op that takes the
    cluster-config write lock on every node group, then reads the record
    under that lock (acquire-all returns no record) and evaluates its
    precondition on it; the `cluster_gate_log` content kind in
    `LogContentMessage` and its `kClusterGateLog` apply case; the per-group
    `LogState::cluster_gate {value, version, txn}` set on apply, snapshotted
    with `min_ckpt_ts_`, never purged, shipped as one replay item per
    group while present, and found by its `txn` when orphan lock recovery
    consults the log (`RecoverTx` answers `Committed` for the gate tx that
    set the slot, so that tx's write lock waits for its coordinator's
    replay); the new `Execute(ReplayLogCc&)` branch that installs the
    slot's value into the payload on every node group and, on the
    coordinator's, restarts the op through `RecoverClusterGate` at its
    install, with no lock, re-running the install on all node groups; the `cluster_gate_paused` gauge; and
    the consumer
    resuming the gate at the end of the copy. Test: a pause survives a
    log-group leader change, a node-group leader change, and a full
    restart of a node, and is still enforced afterwards; every gate
    command, including an operator's `resume` issued from a different
    node after the consumer died, writes its `cluster_gate_log` to log
    group 0 and no other group ever holds a `cluster_gate`, so a pause
    followed by a resume leaves group 0 at "not paused" and every node
    group's replay list carries at most one gate item; a pause stays in
    the replay list however far `min_ckpt_ts_` advances past it, and a
    node group recovering hours into a pause comes back paused, so no
    recovered group ever admits DDL the others refuse; the
    `cluster_gate_paused` gauge reads 1 on every node while paused, a
    node that restarted into the pause included, and 0 on every node
    after the resume; a cluster-scale operation (add node group)
    completed while paused leaves the pause in force on every node group,
    the new one included; a second `pause` while paused fails with
    `CLUSTER_GATE_HELD`. Test, with fault injection at
    `cluster_config_PostWriteAll_local`/`_remote` (the existing points in
    `Execute(PostWriteAllCc&)`): kill the coordinator node **after the
    `cluster_gate_log` write and before `PostWriteAll` has completed on
    every node group**, keeping the other node groups running, for a
    `pause` and separately for a `resume`; before recovery make no
    assertion about which surviving groups still hold the lock, since a
    participant whose post-write already ran has installed the value and
    released its lock while the others have not; assert that after
    recovery every node group holds the new gate value, no node group
    holds a lock for the old `txn`, a DDL is admitted (after `resume`)
    or rejected (after `pause`) on every node group, and for `pause`
    that the drain stage ran; repeat with one non-coordinator node group
    also failing over during the window. Test: an index
    build whose last batch finishes, and a create whose kv write returns,
    after the pause was requested must both complete (their re-lock of
    the cluster config takes the ungated prepared branch) and pause's
    drain must return only after they do; the same for recovery of a
    prepared DDL from its prepare log on a paused cluster. Confirm every DDL kind takes
    the cluster-config read lock as its first step (split-flush and
    cluster-scale ops do too, and must keep working under the gate, which
    targets `UpsertTableTxRequest` only). Test: the
    `{x:1}` → drop → `{y:1}` history from §6.2 executed with the gate
    disabled must halt the consumer with the protocol error, and executed
    under the protocol must converge with no error.

## Key files

| Piece | Location |
|---|---|
| `CdcScan` RPC, `CdcDdlRecord`, `CdcScanResponse`; `cluster_gate_log` content kind and `ClusterGateLogMessage` (§6.2) | `tx_service/tx-log-protos/log.proto` |
| Scan handler, DDL store, long-poll parking, fence (fallback, §4.4(c)) | `eloq_log_service/` (RPC boundary only; see the data_substrate `CLAUDE.md` on proprietary internals) |
| Leader cache reuse | `tx_service/tx-log-protos/log_agent.h/.cpp` |
| `DocumentSourceEloqChangeStream` and its components `CdcScanSource`, `CdcScanClient`, `CdcWaitObject`, `CdcEventStream` (§5.2) | `src/mongo/db/modules/eloq/src/cdc/` in the EloqDoc tree |
| `$changeStream` stage factory, `startAfter`, `run_aggregate`'s oplog and majority bypass | `src/mongo/db/pipeline/document_source_change_stream.*`, `document_sources.idl`, `src/mongo/db/commands/run_aggregate.cpp` |
| Blob, key and resume-token codecs (`CdcBlobDecoder`, `CdcKeyDecoder`, `CdcResumeToken`) | `src/mongo/db/modules/eloq/src/base/cdc_*` |
| DDL record decoder (`decodeCdcDdl`, §5.4) | `src/mongo/db/modules/eloq/src/cdc/cdc_ddl_decoder.*` |
| Reference consumer | new tool under the EloqDoc repo |
| Facts F1–F12 | files cited in §2 |
