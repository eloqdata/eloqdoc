# EloqDoc CDC: implementation plan

**Status: draft plan for [cdc-change-stream.md](cdc-change-stream.md).** This
document sequences that design into workstreams, milestones and PRs. It does
not restate the design; every task points at the section that specifies it
(`§n` refers to the design doc), and the §10 checklist items are assigned to
the task that closes them. Where implementation diverges from the design, the
design doc is fixed in the same change, per its own rule.

Path conventions follow the design doc. `DS/` below is short for
`src/mongo/db/modules/eloq/data_substrate/`.

## 0. Ground rules

1. **Two repositories.** `DS/` is the `eloqdata/tx_service` submodule. Every
   change under `tx_service/`, `eloq_log_service/` or `core/` is a PR there,
   merged first; the EloqDoc PR that needs it bumps the submodule pointer.
   Proto changes (`tx_service/tx-log-protos/log.proto`) therefore gate
   everything and land first (M0).
2. **Proprietary boundary.** `eloq_log_service/` and
   `tx_service/raft_host_manager/` are documented only at their RPC boundary
   (`DS/CLAUDE.md`). The scan handler, DDL store and long-poll parking are
   implemented there; `10-log-service.md` gets the RPC contract, nothing
   about internals.
3. **Threading.** No `bthread::ConditionVariable` waited on from CC-request
   context; atomics plus `bthread_usleep` polling, or a plain mutex with
   timer/apply-path completion (`DS/CLAUDE.md`, "Threading Model"). The
   design already shapes §4.2 long-poll and §5.8 cancellation around this;
   reviewers check every new wait against that rule.
4. **Build.** The CMake source manifest is derived from the SConscripts
   (`cmake/generate_eloqdoc_sources.py`), so every new `.cpp` under
   `src/mongo/` is registered in its directory's `SConscript`, not only in
   CMake. `DS/` unit tests build with `-DWITH_TESTS=ON` (Catch2,
   `tx_service/tests/CMakeLists.txt`) and run under `ctest`; jstests run
   through `.github/scripts/cmake_ci_runtime.sh <build> jstests`.
5. **Definition of done per task:** code, the tests named in the task, the
   design-doc edit if behavior differs, and the metric/log lines §8 names.

## 1. Workstreams and dependencies

| WS | What | Repo | Design |
|---|---|---|---|
| **A** | `CdcScan` RPC, DDL store, long-poll, RocksDB-Cloud parity | `DS/eloq_log_service`, `log.proto` | §4.1–4.3, §4.6 |
| **B** | DDL timestamp invariant: lock registry, commit-stage `P`, create's kv write after commit log, `PostCommit` promotes `P` | `DS/tx_service`, `DS/eloq_log_service/log_state` | §4.4 |
| **C** | EloqDoc change-stream cursor: stages, decoders, tokens, batch loop, resume, invalidate, limits | `src/mongo/db/pipeline`, `src/mongo/db/modules/eloq/src/base`, `getmore_cmd.cpp` | §5 |
| **D** | Cluster gate: record field, cc-map path, tx op, log kind, log-state slot, replay, admin command, drain RPC | `DS/tx_service`, `DS/eloq_log_service`, EloqDoc command | §6.2 (gate bullets), §10.11 |
| **E** | Reference consumer: apply loop, bootstrap, checkpoint | new tool in EloqDoc repo | §6.1–6.4 |
| **F** | Cross-cutting verification, metrics, ops docs, CI | both | §7, §8, §10 |

```text
            M0: proto + interfaces frozen
             │
   ┌─────────┼──────────────┬──────────────┐
   ▼         ▼              ▼              ▼
  WS-A      WS-B           WS-D          WS-C (against a fake logger
 (scan)   (ddl_ts)        (gate)          until A lands)
   │         │              │              │
   └────┬────┘              │              │
        ▼                   │              │
  A+B: DDL store holds      │              │
  correct `P`               │              │
        └──────────┬────────┴──────────────┘
                   ▼
              WS-E consumer (needs C tokens + D gate)
                   ▼
              WS-F hardening, CI, docs
```

A, B, C and D are independent enough to run in parallel after M0, with one
hard edge: A's DDL-store append (A3) merges only after B2–B5, so the store
never holds a create/drop positioned at the prepare-stage `D`. C's scan source
is written against the proto and tested with a fake `LogService` until A is
real. E needs C's tokens and D's gate command.

## 2. Milestones

| M | Exit criterion | Workstreams |
|---|---|---|
| **M0** | `log.proto` merged with `CdcScan*`, `cluster_gate_log = 6`, `ClusterGateLogMessage`, `CheckSchemaOpsInFlight`; token byte layout (§5.6) and `ClusterGateTxRequest` shape agreed in writing. The EloqDoc-side codecs (PR 2) do **not** wait on this and do not bump the submodule: keys are opaque strings to them. The submodule pointer is bumped by the first EloqDoc PR that actually calls a new RPC (PR 5) | all |
| **M1 — data flows** | Deployment-level stream on a single-log-group cluster delivers insert/replace/delete with correct `fullDocument`, `documentKey`, `eloq.keyBytes`, `clusterTime` — on the RocksDB-Cloud (S3) log state that CI builds and on plain RocksDB locally; idle stream advances `postBatchResumeToken` (F3); **the opening reply always carries a `postBatchResumeToken`** (§5.1 opening protocol, F13) and PyMongo, Java and Node `watch()` pass the three §10.2 scenarios (C13); the `eloq_cdc` jstest suite runs in CI with the DML cases (F2); no DDL yet | A (A1, A2, A6, A7 on both log states), C (C1, C2, C4, C5, C7 DML, C8a, C9, C13, C14), F (F1, F2, F3; items 10.1, 10.2, 10.10) |
| **M2 — order and resume** | Multi-group; DDL events; `resumeAfter`/`startAfter` exact across pages, timestamps and leader changes; collection-level `invalidate`; no write to a table is emitted between its drop and the next create, under the §10.6 fault tests; `ddl_kv_retry_exhausted` alert and its runbook exist (B7), since B3's retry-under-lock stalls every stream; cloud-specific scan behaviors (A8); `eloq_cdc` extended with DDL/resume/invalidate cases | A (A3, A4, A5, A8), B (B1–B7), C (C7 DDL, C8, C10), F (10.3–10.7, 10.9) |
| **M3 — bootstrap** | Consumer bootstraps an empty sink and converges under concurrent DML; gate survives leader change and node restart; `{x:1}`→drop→`{y:1}` history converges under the protocol and halts without it | D, E, F (10.11) |
| **M4 — hardening** | §5.8 limits enforced and tested; cancellation races fixed; oversized-event rule; §8 metrics exported; retention and consumer runbooks written; §2 facts re-verified | A (A9), C (C11, C12, C7 oversized), E (E3, E4, E6), F (10.8, F4–F6) |

## 3. Workstream A — log service scan

All in `DS/eloq_log_service/` unless noted. Verified anchors:
`LogContentMessage` kinds 1–5 exist, `6` is free
(`log.proto:237`); `LogState::UpsertSchemaOp`, `tx_catalog_ops_`,
`GetLogReplayList`, `last_ckpt_ts_` exist (`log_state.h:111–320`); three
log-state impls (`log_state_memory_impl.cpp`, `log_state_rocksdb_impl.cpp`,
`log_state_rocksdb_cloud_impl.cpp`); `CheckMigrationIsFinished` is the model
for a read-only polling RPC (`log.proto:459`).

| # | Task | Notes / tests |
|---|---|---|
| A1 | **Proto** (M0): `CdcScanRequest/Response`, `CdcDataRecord`, `CdcDdlRecord`, `rpc CdcScan`; `CheckSchemaOpsInFlightRequest/Response` (count of ops below `CleanSchema`, `lg_highest_ddl_ts`); `cluster_gate_log = 6` + `ClusterGateLogMessage` for WS-D | §4.1, §6.2. Reserve the `continuation` byte layout as a documented struct shared with C's token entries (§5.6). |
| A2 | **Data scan** over the timestamp-keyed data CF, `from_ts` / `continuation` positioning, `max_bytes` cut with the "always at least one record" rule, `arrival_complete_ts = min_ckpt_ts_` read **before** either scan — on **both** the RocksDB and the RocksDB-Cloud log states from the start, built on the iterator `GetLogReplayList` already uses for both (§4.6), including the cloud state's DB-plus-in-memory-queue merge. CI builds only `WITH_LOG_STATE=ROCKSDB_CLOUD_S3` (`.github/workflows/ci.yml`), so a RocksDB-only scan would leave CDC untested in CI | §4.2, §4.6. Unit tests in `eloq_log_service/test/` on both states: page boundaries inside one timestamp, `has_more` semantics, empty response carries the bound. |
| A3 | **DDL store**: an in-memory DDL history in the log state, one record per schema op, `(D, txn, SchemaOpMessage)` at position `(ddl_ts, txn)` (`be64 ddl_ts ‖ be64 txn` in the continuation), saved in the raft snapshot beside `tx_catalog_ops_` and trimmed at the A11 retention floor, not an hour after the checkpoint; no RocksDB column family and no separate storage key (§4.3). Append at `PrepareSchema` apply for index ops (`ddl_ts = D`) and at `CommitSchema` apply for create/drop (`ddl_ts =` commit log ts, which is `P` once B2 is in); filter `table_engine == EloqDoc` and `table_op.op_type` in `CreateTable`, `DropTable`, `AddIndex`, `DropIndex` (index ops are `table_op`s whose index names are in `alter_table_info_blob`; `SchemaOpMessage.index_op` is never set); skip `commit_timestamp == 0` commit logs; for an add-index commit log with the rolled-back flag (B8), append one `dropIndexes` record at `ddl_ts = P` covering every index the op added. Also brings `log.proto`'s continuation comment and `cdc_scan.h`, which still describe a DDL storage key, in line with the position | §4.3. Tests: one record per schema op, including an index op that adds two indexes, correct position, `Update` ops not recorded, a rolled-back add index yields `createIndexes` at `D` then `dropIndexes` at `P`; and the end-to-end half of B4's recovery test — the same restart scenario additionally asserts the DDL record's `ddl_ts == P` and `ddl_ts ==` the recovered `schema_ts_`. **Merges after B2–B5** (PR 7 before PR 8), which is why that assertion lives here and not in B4. The scan side is ready for it: A3 supplies a `CdcStreamCursor` over the in-memory history in place of the empty one in `CdcScanRocksDB`, taking its view after the bound is read (the read-order rule of `RunCdcScan`); its positions begin with the big-endian `ddl_ts` and are longer than eight bytes, which the continuation's timestamp form relies on (§4.2 "Paging"). On the EloqDoc side, `CdcSourceRecord::ddlKey()` rebuilds the position from the record, as `dataKey()` does for data (needed by C8b's token entries). |
| A4 | **Merged paging**: single timestamp-ordered walk over both stores, page is a prefix of the merged sequence, `continuation` = exact last key of each stream | §4.2 "Paging". Test: cut inside a timestamp with records of both streams; resume from continuation yields no gap and no duplicate. |
| A5 | **Long poll**: parked list under a plain mutex, completed by `UpdateCkptTs` apply, `bthread_timer_add` deadline, `NotifyOnCancel`, or leader stop; `std::atomic<bool> completed` CAS so `done` runs once; `cdc_scan_max_parked` overflow → immediate empty page | §4.2 "Long poll", §5.8 "Logger side". Test under forced step-down (§10.8). |
| A6 | **Leader only / NotLeader + redirect**; **PositionTruncated** defined, never returned in v1, `retention_floor_ts = 0` | §4.2, §4.5. |
| A7 | **Memory log state**: `CdcScan` returns an error; runtime rejects `$changeStream` at open (C1) | §5.1. |
| A8 | **RocksDB-Cloud specifics** (the basic scan on the cloud state is A2's): `NotLeader` with own address while `AsyncStartCloudDB` is pending; DDL store (A3) in the same DB; attach/detach behaves as leader loss; document that retention is `log_retention_days` | §4.6. Test in `log_server_rocksdb_cloud_tests.cpp`. M2, with A3. |
| A9 | Metrics: `cdc_arrival_complete_ts` per group, parked-request count | §8. |
| A10 | `10-log-service.md`: RPC contract for `CdcScan` and `CheckSchemaOpsInFlight` (boundary only) | rule 0.2. |
| A11 | **CDC retention floor** in both purges: `LogStateRocksDBImpl::PurgingSstFiles` and `LogStateRocksDBCloudImpl::MoveFilesToArchive` never remove a file whose largest timestamp is at or above `min_ckpt_ts − txlog_cdc_retention_seconds`; the setting (gflag and ini key, default 0 = today's behavior) is read in `core/src/log_init.cpp` like the other `txlog_*` settings and passed down to both log states; A3's DDL store purges by the same floor | §4.5. Tests: the RocksDB purge over its size limit keeps every file inside the window and removes those below it, and removes everything checkpointed with retention 0; the cloud mover likewise against MinIO. M1: every consumer that ever lags depends on it. |

Not in v1, tracked: retention-floor tracking (§4.5), follower scans. The
§4.4(c) logger-side fence is not implemented: (a)+(b) establish the
DDL timestamp invariant by construction, and the fence would only cover
prepare-stage records anyway.

## 4. Workstream B — DDL timestamp invariant

All in `DS/tx_service/` unless noted. Verified anchors: `ActiveTxMinTs` at
`cc_shard.h:674` with the `!TableName::IsMeta` skip at `:709`; `wlock_ts_`
at `:170`, `UpsertLockHoldingTx` at `:578`; `DowngradeCceKeyWriteLock` at
`cc_map.cpp:359`, called from `template_cc_map.h:1198` and
`cluster_config_cc_map.h:213`.

| # | Task | Notes / tests |
|---|---|---|
| B1 | **Lock registry** §4.4(a): `TxLockInfo::wlock_ts_` becomes the latest write-lock grant (was the first); new `holds_ckpt_wlock_`, set when a write lock is taken on a data table or catalog entry, cleared by `CcShard::OnWriteLockDowngraded` from `DowngradeCceKeyWriteLock`; `ActiveTxMinTs`/`ActiveTxMaxTs` read `wlock_ts_` of flagged entries (range/bucket/cluster-config write locks do not count; decision recorded in `03-concurrency-control.md` §10). | The `table_type_` overwrite audit found a real hole (a later meta registration hid a data or catalog write lock). One per-tx table type cannot describe locks on many tables, so the field is removed. `ClearTx`, its other reader, now checks each entry's own table. Test `tx_service/tests/CkptLockRegistry-Test.cpp`: `ckpt_ts < lock time` for data and catalog write locks; meta write locks, intents and read locks not counted; a later meta registration does not hide the lock; downgrade uncounts, upgrade re-counts from the upgrade time. |
| B2 | **Commit-stage `P`** for `DropTable`/`CreateTable`/`TruncateTable` in `UpsertTableOp::Forward` after `acquire_all_lock_op_`: `commit_ts_ = max(commit_ts_, max(last_vali_ts + 1, commit_ts + 1))` | §4.4(b). `FillCommitLogRequest`, `PostCommit` and the DDL record all read `commit_ts_`, so no further plumbing. |
| B3 | **Create's kv write after the commit log**: move `upsert_kv_table_op_` into the commit-log success branch; `update_sequence_table_op_` runs before it; rebuild the coordinator's dirty schema with `P` first; add `CreateTable` to the roll-forward retry branch (fast backoff ~1 min, then slow unbounded with error log + `ddl_kv_retry_exhausted`); lock is **not** downgraded during retry | §4.4(b), §10.3. This removes create's zero-timestamp commit log; update F8 in the design and `07-durability-and-recovery.md`. Test: kv failure injected after commit log → create completes after store recovers; no rollback path reachable. |
| B4 | **`PostCommit` promotes `req.CommitTs()`** (not `dirty_schema_version_`) for create/truncate in `catalog_cc_map.h`; re-install dirty schema with `P` before promotion; **`LogState::UpsertSchemaOp` at `CommitSchema`** stores the commit log's timestamp so an interrupted drop/create recovers with `P` | §4.4(b), §10.6, F12. Test (PR 7, tx service only): node restarts after the commit log and before the kv write → the recovered table's `schema_ts_` and kv catalog both equal `P`. The matching DDL-store assertion belongs to A3 (PR 8), which merges after this. |
| B5 | **Recovery paths**: from a commit log use the recorded `P`; from a prepare log only, re-run the lock stage and re-derive it | §10.6. |
| B6 | **Fault-injection tests** (Catch2, `tx_service/tests/` with `cluster_harness`): (i) checkpoint during held write lock ⇒ `ckpt_ts < ddl_ts`; (ii) DML committed between a drop's prepare and its commit-stage lock ⇒ every such DML has `commit_ts < P_drop`, and a following create has `ddl_ts > P_drop` | §10.6. These tests are the only check of the drop's position rule: the stream relies on the merged order alone and has no runtime incarnation filter (§5.5). |
| B8 | **Rolled-back add index** §4.3/§4.4(b): the constraint-violation path takes the commit-stage write lock (it already does), then `commit_ts_ = P` as in B2 (the only add-index failure after the prepare log since PR 4, which creates the index's kv table before the prepare log and writes the kv catalog after the commit log); commit log carries `P` and a new rolled-back flag in `SchemaOpMessage`; `LogState::UpsertSchemaOp`, schema-op recovery and `UpsertTableIndexOp::Forward` read the flag instead of `commit_timestamp == 0`; `PostCommit` installs `P` | Test: an index build that meets a key over the size limit (`PACK_SK_ERR`; EloqDoc no longer builds unique indexes over existing documents) rolls back with a commit log at `P > D` and the flag set; recovery from that commit log completes the rollback. A3 appends `dropIndexes` at `ddl_ts = P`. |
| B7 | Metrics/alerts: `ddl_kv_retry_exhausted` per table; runbook entry for a prolonged store failure (stalls every stream) | §8, §10.3. |

**Risk.** DDL *requests* are issued per product (EloqDoc from
`eloq_recovery_unit.cpp`, EloqKV/EloqSQL from their trees), but they all run
the one `UpsertTableOp::Forward` state machine in `tx_operation.cpp`, which
has no engine branch; `catalog_cc_map.h`'s `PostCommit`, `cc_shard.h`'s
`ActiveTxMinTs` and `LogState::UpsertSchemaOp` are likewise single, shared
implementations. B1–B4 edit exactly those, so every product's create, drop
and (for EloqKV) truncate change timestamp, kv-write order and rollback
behavior at once. **That is intended, with no flag:** the new behavior is
correct for every product (commit-stage `P` and a post-commit-log kv write
are what the DDL protocol should have done regardless of CDC), and a
divergent path would have to be maintained and tested twice. B's tests
(B6) therefore run against the `tx_service` cluster harness, not an
EloqDoc-only setup, and the `tx_service` PR notes the behavior change for
EloqKV/EloqSQL reviewers: create no longer rolls back on a kv error, and a
DDL's catalog write lock now pins the checkpoint. There is no fallback if B
slips: A3 waits for it (§1), so M2 slips with it rather than shipping a DDL
store whose drop positions mean something else.

## 5. Workstream C — EloqDoc change-stream cursor

Verified anchors: `buildPipeline` at
`src/mongo/db/pipeline/document_source_change_stream.cpp:333`,
`createFromBson` at `:423`, the 40573 replica-set assert at `:391`,
`DocumentSourceCloseCursor` lives in
`document_source_change_stream_close_cursor.{h,cpp}` (the design's
`document_source_close_cursor.cpp` is that file); the 4.0 IDL has
`resumeAfter`/`allChangesForCluster`/`$_resumeAfterClusterTime` and no
`startAfter` (`document_sources.idl:75–103`); `generateBatch` at
`getmore_cmd.cpp:521`, `haveSpaceForNext`/`enqueue` at `:538–539`,
`setLatestOplogTimestamp` at `:546` and `:571`. The 4.0 change-stream
sources (`_change_stream.cpp`, `_transform.cpp`, `_close_cursor.cpp`,
`lookup_change_post_image.cpp`, `resume_token.cpp`) are already in the
CMake manifest, which settles the first half of §10.1.

Proposed new files (names are proposals):

```text
src/mongo/db/modules/eloq/src/base/cdc_blob_decoder.{h,cpp}     F1 sections/entries, scope skip
src/mongo/db/modules/eloq/src/base/cdc_key_decoder.{h,cpp}      canonical delete-key decoder §5.4 step 3
src/mongo/db/modules/eloq/src/base/cdc_resume_token.{h,cpp}     §5.6 encode/decode
src/mongo/db/modules/eloq/src/cdc_scan_client.{h,cpp}           per-group RPC state, wait object §5.8
src/mongo/db/pipeline/document_source_cdc_scan_source.{h,cpp}
src/mongo/db/pipeline/document_source_cdc_decode.{h,cpp}
src/mongo/db/pipeline/document_source_cdc_order.{h,cpp}
src/mongo/db/pipeline/document_source_cdc_format.{h,cpp}
src/mongo/db/pipeline/document_source_cdc_invalidate.{h,cpp}
```

| # | Task | Notes / tests |
|---|---|---|
| C1 | **Open path** — **done in PR 6**, in the shape of design §5.2: `$changeStream` expands into one stage, `DocumentSourceEloqChangeStream` (`src/cdc/document_source_eloq_change_stream.*`), installed as `DocumentSourceChangeStream::setStageFactory` when the eloq storage engine registers, so `createFromBson` never reaches 40573 and `run_aggregate` keeps the stream off the oplog and its majority wait; `startAtOperationTime` is refused; the checkpointer-interval check is not made, since the configuration accepts no interval below 10 s, and a MEMORY log state is caught by its first scan (`ENOMETHOD` → `CommandNotSupported`); the optional `$project` rejection is not done. Planned: `createFromBson` bypasses 40573 for EloqDoc; supported-configuration check (log service present, not `txservice_skip_wal`, not `MEMORY` log state, checkpointer enabled); optionally reject an obviously broken `$project` at open as early feedback — the *guarantee* that `_id` survives user stages is the end-of-pipeline integrity check in C9 (implemented, inside `PipelineProxyStage::getNextBson`), since `$addFields`/`$set`, computed projections and `$replaceRoot` can all rewrite `_id` and cannot be ruled out statically; add `startAfter` to `DocumentSourceChangeStreamSpec` with the mutual-exclusion rule; **opening protocol** (§5.1, F13): a token-less open waits inside the command, yielding on the wait object, until every group reports a non-zero bound, then returns an empty `firstBatch` with the watermark for `S` as `postBatchResumeToken`; bounded by `maxTimeMS` else `cdc_open_wait_ms`; on expiry fail with new error code `ChangeStreamStartNotReady` (`src/mongo/base/error_codes.err`); a token-based open returns immediately with the supplied token as `postBatchResumeToken` | §5.1, §5.2. Replace `buildPipeline`'s stage list with the CDC chain; keep `DocumentSourceCloseCursor`. Unit test on spec parsing; test that the opening reply never lacks a token (fake logger reporting bound 0 for N rounds, then non-zero; and never). |
| C2 | **Blob decoder** (F1–F3), **implemented**: `src/base/cdc_blob_decoder.{h,cpp}` — `CdcBlobDecoder::next()` walks sections, skips non-`EloqDoc`/non-`Primary` and out-of-scope sections by their length, and yields `{section, entry, ns, op, keyString, record}`; `CdcStreamScope` carries the three §5.1 scopes and the `local`/`admin`/`config` exclusion. Format verified against the writer (`tx_service/src/tx_execution.cpp`, `TransactionExecution::FillDataLogRequest`) *and* the reader recovery uses (`ReplayLogCc`: the section walk in `tx_service/include/cc/cc_request.h`, the entry loop in `tx_service/include/cc/template_cc_map.h`), which settles two things the design stated loosely. **"Record present unless `Delete`" is a format contract, not a convenience**: the writer's condition is `op != Delete && rec_ != nullptr`, but nothing delimits an entry, and replay reads a record for every non-`Delete` op unconditionally — so a non-`Delete` entry without a record would desynchronise the rest of the section for replay too, and the decoder matches replay rather than the writer's guard. The same loop asserts the op is `Insert`, `Update` or `Delete`, which is the evidence that **`Upsert` never reaches a data log** — the write set resolves it earlier — so the decoder accepts the value but no event can come from one. **Positions count skipped sections**: `section`/`entry` are ordinals over *all* sections and entries, because §5.6 requires a token to mean the same thing at any stream scope; tested by decoding one blob at three scopes and asserting the shared entries are numbered identically | **19 cases passing** (`cdc_blob_decoder_test.cpp`). Blobs are built in-test by a `BlobBuilder` that mirrors `FillDataLogRequest` field for field (same order, same native-endian integers, same record rule), which keeps this a unit test; an end-to-end check against blobs from a real `DS/` harness is still owed and belongs with F1. Covered: mixed ops with deletes between records (the case that desynchronises a naive decoder), binary keys and documents, non-empty unpack-info, EloqKV object-command sections skipped whole, secondary and unique-secondary index tables skipped, excluded databases at every scope including `admin.__eloq_cdc_progress` asked for by name, database/collection scope prefix confusion, empty sections and empty keys, and rejection of every truncation prefix, an unknown op byte, and a section length past the end of the blob. Entry-level corruption is covered separately, because truncating a blob makes `kv_len` overrun first and the walk then fails before any entry is parsed: four cases keep the section length *consistent* and corrupt a key length, a record document length, an unpack-info length and an entry header, and each asserts the failure came from the entry parser rather than the section walk, so a test cannot silently stop covering its check. |
| C3 | **Canonical delete-key decoder** with the §5.4 table (**implemented**: `src/base/cdc_key_decoder.{h,cpp}`, `CdcKeyDecoder::{toBsonCanonical,decodeIdKey,decodeLoggedIdKey}`; `decodeLoggedIdKey` also hands back the raw KeyString for `eloq.keyBytes`); round-trip test enumerating every row against the real `KeyString` encoder | §10.7. **Gate for any delete event**: no `KeyString::toBson` with empty type bits anywhere on the CDC path (grep-enforced in review). Tests (**32 cases, passing**, `cdc_key_decoder_test.cpp`): every value is encoded with the real encoder followed by a **sentinel field**, which is what proves byte-consumption equality — a decoder off by a byte corrupts the sentinel. **Equality with the original and agreement with the stock decoder are both asserted unconditionally**, with no "the stock decoder disagrees too" escape hatch — that is how a bug in this decoder would hide, and one did. Finding it required fixing the shared decoder as well (see the storage-layer note under the PR sequence): `key_string.cpp` selected the exact-integer branch with `integerPart > kMaxIntForDouble` where the constant is a `double`, so the `int64` integer part was promoted and 2^53+1 rounded to 2^53, skipping the branch that exists for exactly that value and returning a decimal one unit low. The encoding was never lossy. Guarded by `TwoToThe53Plus1WithAFractionSurvivesBothDecoders` and `NeighboursOfTwoToThe53PlusOne`. Coverage added for the previously untested §5.4 rows: 8-byte integer part with a fraction (`fracBytes == 0`, implied DCM), negative small-magnitude decimals, `CodeWScope`, and BinData at 254/255/256/70000 bytes (the `u32` length path). The `deleteOne` match itself is asserted by the `eloq_cdc` jstests (F2), not here: it is the same value comparison, and a unit test has no server. |
| C4 | **Scan client + wait object**: one in-flight scan per (cursor, group) in a `shared_ptr` state; callback deposits under mutex and notifies; coroutine registers/deregisters as waiter; deadline timer through the same object; destructor sets `cancelled`, `StartCancel`, no wait; leader cache reuse from `LogAgent`; **backoff before retrying a `NotLeader` whose `redirect` is the node just asked or empty** (a cloud-state leader not yet in sync answers with its own address, §4.6; a node without a replica of the group answers with none), bounded by the remaining `maxTimeMS` | §5.3 step 1, §5.8 "Cancellation ownership", F11. Tests with a fake `LogService`: callbacks completed after the coroutine returned; a group answering `NotLeader` with its own address for N rounds is retried at the backoff rate, not in a loop. |
| C5 | **`CdcScanSource`**. Three constraints, all load-bearing. Two the C9 review pinned down: it must be able to **name its opening token at construction**, because `run_aggregate` now asserts a non-empty token when registering a token-providing cursor (C1's opening wait therefore runs before `registerCursor`, not after); and its long poll must **return EOF at the `opCtx` deadline rather than throw**, since a pipeline executor never runs the awaitData wait and an error reply would carry no token. The third, from PR 3: **an empty page with `!has_more` that returns before its `wait_ms` elapsed is followed by a client-side backoff before that group is scanned again** (§5.3 step 1). Until A5 the logger ignores `wait_ms` and answers every scan at once, so without it an idle source re-issues scans in a tight loop; after A5 a full parked list still answers early. Then: per-group fetch cursor, buffer, `scanned_ts` rule (`last_ts − 1` if `has_more`, else bound), emission rule, `emit_bound`, `drained`, `last_produced`, buffer cap with the "always fetch the lagging group" exception, `wait_ms` clamped to `maxTimeMS`; fresh-stream start `S = min arrival_complete_ts`, `from_ts = S + 1`, first watermark; a "fix `S`" primitive that C1's opening protocol drives until all bounds are non-zero (no token-less empty batches ever leave the server) | §4.2, §5.1, §5.3 steps 1–2, §5.8. Unit tests with a scripted fake logger: multi-group merge, page cut inside a timestamp, idle advance, buffer cap, bound-zero groups delaying `S`, and an idle group whose fake logger answers every scan at once: the scan rate stays at the backoff rate while `postBatchResumeToken` still advances with the bound. |
| C6 | Decode **done in PR 6** as `CdcEventStream`'s decode step (`src/cdc/cdc_event_stream.*`), with the dedup set's `cdc_dedup_set_bytes` limit and overflow count as options/counters (not yet a server parameter or metric); nothing of C6 remains, since the design has no order step: the merged timestamp order already places every write in its incarnation (§5.5). Planned: **`CdcDecode`** (record at a time, `(section, entry)` position, per-group per-timestamp dedup set with `cdc_dedup_set_bytes` overflow counter) | §5.4, §5.5. |
| C7 | DML part **done in PR 6** (`CdcEventStream`'s format step): `insert`/`replace`/`delete` with every §5.4a field and the oversized rule, the threshold being the event's estimated size against `BSONObjMaxUserSize`; the DDL events wait for A3. Planned: **`CdcFormat`**: event documents per §5.4a (`insert`/`replace`/`delete`/`create`/`drop`/`createIndexes`/`dropIndexes`), `eloq.keyBytes`, `clusterTime`, DDL `operationDescription` decoded via `MongoTableSchema` from `new_catalog_blob`/`old_catalog_blob`; oversized rule `fullDocumentOmitted: true` | §5.4a, §5.8, §10.4, §10.8. Confirm which limit the reply path enforces before fixing the threshold. |
| C8a | **Token codec and watermark resume** (M1) — resume **done in PR 6**.  Codec **implemented**: `src/base/cdc_resume_token.{h,cpp}`, `CdcResumeToken` / `CdcResumeTokenEntry` with `encode`/`decode`/`toBSON`/`parse`, the `{_data: <hex>}` wire shape, `entryFor()`, and `startTimestampForAbsentGroup()` (which is the watermark-vs-event rule: `ts + 1` for a watermark, `ts` otherwise). **23 cases passing** (`cdc_resume_token_test.cpp`): round trips for every kind and both entry forms, binary keys full of NULs and 0xFF, empty keys, 64 groups, and rejection of a wrong version, unknown kind or form, duplicate log group, trailing bytes, every truncation prefix, and a malformed `{_data}` wrapper. Two hardening rules on the parse path, which takes client bytes: duplicate log-group ids are tracked in a hash set, not by rescanning the entries so far — the count is client-supplied and reaches 65535, where a pairwise scan is ~2.1 billion comparisons for a ~1 MB token and ties up a worker for seconds; and a **watermark at the maximum timestamp is rejected**, because `startTimestampForAbsentGroup()` would compute a successor that wraps to 0 and restart the stream at the beginning of history rather than past its end (the accessor `uassert`s on the same condition for a token built in process). Restart behavior is tested at the boundary, not only serialization. Still to wire: `_data` v2 encode/decode for all kinds; resume from a **watermark** token (kind 1, no entries): every group starts at `from_ts = ts + 1`. This is what a driver's automatic resume needs before the first event, so it is part of M1 (C13 scenario ii) | §5.6. Test: open, take the opening watermark `S`, commit event `E` after `S`, kill the cursor, reopen with `resumeAfter: S` → `E` arrives exactly once. A fresh-stream reopen would fix a new `S' > E` and skip it, so this cannot pass by accident. |
| C8b | Event resume **done in PR 6**, moved up from M2 because drivers resume from event tokens and from watermarks with entries as soon as a stream has delivered anything: form 0 and form 1 entries, `from_ts = T_K`, consume-without-producing through the entry key and `(section, entry)`, the data-before-DDL rule at `T_K`, and the dedup set rebuilt from the consumed records; tested by resuming from every event token of a three-group script with cut pages (`cdc_stream_test`) and by `eloq_cdc/resume.js`. Invalidate resume stays with C10. Planned (M2): form 0/1 entries; per-group `from_ts = T_K`, consume-without-producing through the entry key / `(section, entry)`, rebuild the dedup set; absent groups at `ts` for kind 0/2 | §5.6. Property test: for random pull orders and cut points, resume from every issued token yields the same multiset of events after the token, none before. |
| C9 | **`postBatchResumeToken` plumbing and both batch loops** — see "C9 in detail" below. This tree predates 4.0.7: no `postBatchResumeToken` exists at any layer (`CursorResponseBuilder` emits only the top-level `$_internalLatestOplogTimestamp`; `PlanExecutor` has only `getLatestOplogTimestamp()`; `ClientCursor` holds no token; the shell has no `_postBatchResumeToken`), and `aggregate` builds `firstBatch` in its own loop (`handleCursorCommand`, `run_aggregate.cpp:98`), so the token is introduced at every layer and both loops change identically | §5.3 step 4, §10.9. Test matrix in the detail section. |
| C10 | **`CdcInvalidate`** for collection-level streams; resume-at-own-drop check (re-read the referenced DDL record, emit `invalidate`, close); `startAfter` accepts kind 2, `resumeAfter` rejects it | §5.1 table, §5.6. |
| C11 | `cdc_open_wait_ms` and `cdc_scan_max_bytes` (1 byte to 32 MB, read when a stream opens; brought forward so C9 row 4 is testable) **done in PR 6**, runtime server parameters (`setParameter`), documented in `configuration-description.md`; the others are still the components' defaults. Planned: **Config**: `cdc_cursor_buffer_bytes`, `cdc_dedup_set_bytes`, `cdc_scan_max_wait_ms`, `cdc_scan_max_bytes`, `cdc_open_wait_ms` (default 30 s); document in `configuration-description.md` | §5.1, §5.8. `cdc_open_wait_ms` is needed at M1 with C1; the rest at M4. |
| C12 | **Metrics**: per-cursor lag `(arrival_complete_ts − last emitted ts)`, `cdc_dedup_set_overflow` | §8. |
| C13 | **Done in PR 6** for PyMongo, Node.js and Java: `cmake/tests/change_stream_drivers.py` runs PyMongo's scenarios and the clients in `cmake/tests/change_stream_clients/` (Node.js `change_stream_check.js`, Java `ChangeStreamCheck.java`), each driver on fresh servers; the CMake target `eloqdoc-change-stream-drivers` and a CI step after the smoke test (with `setup-node` and `setup-java`); all nine scenario runs pass locally (iii through the `checkpointer_skip_ckpt` fault point). Each driver is pinned to the last release that still accepts a MongoDB 4.0 server (wire version 7): Node.js 6.16.0 and Java 5.4.0; PyMongo is pinned to 4.8.0 in CI, and PyMongo 4.14, Node.js 6.17 and Java 5.5 onward require 4.2 and refuse to connect to EloqDoc. The local build is Debug, which reparses every aggregate, so the stream that runs there is the one reopened at the opening watermark (`openAt(S)`); the Release path, where the `openFresh` stream itself runs, is covered end to end only by CI's RelWithDebInfo job. Planned: **Driver verification, M1 gate** (§10.2, F13): PyMongo, Java and Node `watch()` against standalone EloqDoc in three scenarios each — (i) open on a cluster that has not completed a checkpoint round: returns within `cdc_open_wait_ms` with a `postBatchResumeToken`, no `operationTime` error; (ii) cursor killed before the first event, with an event `E` committed after the opening watermark `S` and before the reconnect: the driver resumes from `S` (C8a) and `E` arrives exactly once — a fresh reopen would start past `E`, so the assertion is on `E`, not merely on "later events"; (iii) open past `cdc_open_wait_ms` with the checkpointer stopped by fault injection: `ChangeStreamStartNotReady` surfaces, retry after restart succeeds; plus `getMore` replies without `operationTime` accepted. Results recorded in the design doc | Scripted as jstests + a `pytests` case so they stay in CI; the PyMongo case is the one already reproduced failing against the previous design. |
| C14 | **Shell support**, prerequisite for every `eloq_cdc` jstest that reads a token (implemented, own rewrite): `DBCommandCursor` (`src/mongo/shell/query.js`) stores `cursor.postBatchResumeToken` from the opening reply and every getMore reply, and keeps the **consumed position separate from the fetched batch's token**: `getResumeToken()` returns the batch token only once that batch is empty, otherwise the `_id` of the last document `next()` returned, otherwise the position from *before* the current batch was fetched (snapshotted in `_runGetMoreCommand`). The `watch()` helpers in `collection.js`/`db.js`/`mongo.js` forward `startAfter` and require no `operationTime` | M1, lands with C9 (PR 6). Tests (`tests/jstests/eloq_cdc/shell_resume_token.js`, shell-only with a stubbed `db`): the **prefetch case** — open with an empty batch and watermark `S`, `hasNext()` fetches `A`,`B`, `getResumeToken()` must still be `S`, then `A._id` after one `next()`, then the batch token after both; an empty getMore advances to its watermark; an unread non-empty opening batch reports no token rather than a fabricated one; non-change-stream cursors report nothing. Plus, once a stream exists: a shell `watch()` on an idle stream reports a non-null, advancing `getResumeToken()`. Status: the five stubbed cases pass against the real `query.js` (run under a node `vm` harness with shell-global shims, since the CMake build produces no shell; CI's SCons `install-shell` runs them for real once F2 wires the suite), and the prefetch case fails against the previous logic. The **`watch()` helpers** have their own test, `tests/jstests/eloq_cdc/shell_watch_helpers.js`, with each helper's aggregate call stubbed — they reach it three different ways (`this.aggregate`, `this._runAggregate`, `getDB("admin")._runAggregate`), so each gets its own stub. Per helper: `startAfter` lands in the `$changeStream` stage and is **removed from the options passed down** (left there, the server would reject or ignore it and silently open a fresh stream), other options still reach aggregate, user stages follow the stage, `resumeAfter` still works, and given both resume options the helper forwards both so the server can report the conflict rather than the helper choosing; plus `Mongo.watch` targets `admin` with `allChangesForCluster`. Verified against the three `watch` bodies lifted verbatim from the source files; deleting the `startAfter` branch from any one helper alone makes the test fail on that helper. |

### C9 in detail — `postBatchResumeToken` plumbing

Ground truth in this tree (verified; the pre-4.0.7 layout):

- `CursorResponseBuilder::done` ([cursor_response.cpp:59](../src/mongo/db/query/cursor_response.cpp#L59)) closes the `cursor` sub-object with only `id` and `ns`; the only progress field it knows is the **top-level** `$_internalLatestOplogTimestamp`.
- The only executor-to-stage query chain is `PlanExecutor::getLatestOplogTimestamp()` (`plan_executor.h:520`) → `PipelineProxyStage::getLatestOplogTimestamp` (`exec/pipeline_proxy.cpp:129`) → `PipelineD::getLatestOplogTimestamp` (`pipeline_d.cpp:553`) → `DocumentSourceCursor`. There is no `getPostBatchResumeToken()`.
- Two batch loops of the same shape: `handleCursorCommand` (`run_aggregate.cpp:98–155`) and `generateBatch` (`getmore_cmd.cpp:521–575`). Both record at append (after `haveSpaceForNext`) and at `IS_EOF`, nothing on stash. Differences: the aggregate loop **never calls `getNext` when `batchSize == 0`**, and on `CloseChangeStream` nulls the cursor and breaks; the getMore loop turns `CloseChangeStream` into `FAILURE` and keeps the batch.
- `ClientCursor` has no per-cursor token state; `src/mongo/shell/*.js` has no `postBatchResumeToken` handling.

Assignment by layer. **Status:** every row except the scan source itself is
implemented (own rewrite, no upstream code: `post_batch_resume_token_provider.h`,
`pipeline_d`, `pipeline_proxy`, `plan_executor`, `clientcursor.h`,
`cursor_response`, both batch loops, shell `query.js`/`watch` helpers, and
`cursor_response_test.cpp` rows for the response layer; all touched TUs
compile warning-free under g++-13 and g++-16, and the
`command_request_response_test` unit passes, 115 tests including the six
new ones). The **end-of-pipeline `_id` integrity check** is implemented too:
`getLastEmittedResumeToken()` on the provider, compared inside
`PipelineProxyStage::getNextBson()`, error code `ChangeStreamFatalError`
(280) in `error_codes.err`, and the new `resume_token_integrity_test` unit
(registered in the pipeline `SConscript`, selected into the CMake manifest;
14 cases, all passing, every one of them driven through a real
`PipelineProxyStage`, since that stage is the only place the check runs and
simulating it in the test would let the two drift apart).
The stages implementing the interface arrive in C5 and C7 — and are the
reason it must be split first, see below; until then every cursor carries an
empty token, the new code is inert, and the batch-loop rows of the matrix
below cannot run.

| Layer | Change | Files |
|---|---|---|
| Pipeline | New interface `PostBatchResumeTokenProvider`, with `getPostBatchResumeToken()` (the watermark for `emit_bound` once drained; before the first event, the opening token) and `getLastEmittedResumeToken()` (the `_id` the stream's own stages put on the document they last emitted). `PipelineD::getPostBatchResumeTokenProvider(const Pipeline*)` returns the first stage when it implements the interface, else `nullptr`; `PipelineProxyStage` resolves it **once in its constructor**, since the first stage is fixed by then and a per-document `dynamic_cast` would be paid by every pipeline on the server. **Split into two interfaces before C7** — the two methods belong to opposite ends of the chain; see below | `post_batch_resume_token_provider.h`, `document_source_cdc_scan_source.*`, `pipeline_d.{h,cpp}` |
| Executor | `PipelineProxyStage::getPostBatchResumeToken()`; `PlanExecutor::getPostBatchResumeToken()` beside `getLatestOplogTimestamp()`, reaching the root `PipelineProxyStage` the same way | `exec/pipeline_proxy.{h,cpp}`, `query/plan_executor.{h,cpp}` |
| Cursor state | `ClientCursor::{get,set}PostBatchResumeToken()` holds the **previous batch's token** between calls; initialized at registration in `run_aggregate.cpp` from `exec->getPostBatchResumeToken()`. Because the batch loops infer "this cursor reports tokens" from the token being non-empty, registration also **asserts** it: `exec->tracksResumeToken()` says whether the pipeline supplies tokens at all, and when it does an empty opening token fails with `ChangeStreamFatalError` rather than registering a cursor whose every reply would silently omit the field. That pins an ordering requirement on C1 — the opening wait must fix `S` *before* `registerCursor` — in the one place it can be seen | `clientcursor.h`, `run_aggregate.cpp`, `plan_executor.{h,cpp}` |
| Response | `CursorResponseBuilder::setPostBatchResumeToken(BSONObj)`; `done()` appends `postBatchResumeToken` **inside the `cursor` sub-object before `_cursorObject.doneFast()`** (the driver-facing layout, unlike the internal timestamp); omitted when empty. `CursorResponse` and `parseFromBSON` gain the optional field so C++ tests can assert on it | `query/cursor_response.{h,cpp}` |
| Both batch loops | one rule, applied verbatim in each: `token = cursor->getPostBatchResumeToken()`; on append `token = next["_id"].Obj()`; on stash nothing; on `IS_EOF` `token = exec->getPostBatchResumeToken()`; on `CloseChangeStream` keep (the `invalidate` was already appended); then `builder.setPostBatchResumeToken(token)` and, if the cursor survives, `cursor->setPostBatchResumeToken(token)`. An aggregate with `batchSize: 0` thus returns the opening token without pulling. Non-CDC cursors carry an empty token and are untouched | `run_aggregate.cpp` `handleCursorCommand`; `getmore_cmd.cpp` `generateBatch` and its caller, which owns the builder |
| awaitData | **The idle wait is the source's, entirely.** `PlanExecutor::shouldListenForInserts()` begins with `_cq &&`, and a pipeline executor has no `CanonicalQuery`, so the executor's awaitData wait never runs for a change stream — there is no capped-insert notifier to fall back on. Nothing in `generateBatch` catches `MaxTimeMSExpired` either, and an error reply carries no `postBatchResumeToken`, so an idle `getMore` that let the deadline throw would return no token and a driver would stop advancing. C4/C5 must therefore **return EOF at the `opCtx` deadline**, never let `checkForInterrupt` throw: the long poll is clamped to the deadline *and* resolves as end-of-stream, which is what lets the `IS_EOF` branch report the watermark (matrix row 6) | `getmore_cmd.cpp`, C4, C5 |

**Resolved in PR 6 without a split.** With the chain folded into one stage (design §5.2), that stage is both the first and the last server stage that writes an `_id`, so it answers both questions and the single interface stays. The analysis below applies again only if the chain is ever split into several stages. **Originally: split the provider interface by position.** `PostBatchResumeTokenProvider`
currently declares two methods that describe **two different points in the chain**:

| method | the value | who holds it |
|---|---|---|
| `getPostBatchResumeToken()` | the watermark for `emit_bound`, or the opening token before any event | `CdcScanSource`, the **first** stage — it alone knows `emit_bound` |
| `getLastEmittedResumeToken()` | the `_id` the stream's own stages last wrote onto a document | `CdcFormat`, or `CdcInvalidate` for the synthesized invalidate — the **last** server stage |

Both are looked up on `_sources.front()`, so the front stage is asked a question about the back.
A `DocumentSource` holds only `pSource`, its *upstream* neighbour, and has no downstream pointer
and no handle on the `Pipeline`, so it cannot answer such a question except through a side
channel. Resolving the *last* implementing stage instead would simply break the other method,
since no downstream stage can see `emit_bound`. Conflating two positions in one interface is the
defect; any single-stage lookup is wrong for one of them.

Split them, and look each up where it lives:

```cpp
// The pipeline's FIRST stage: how far the stream has consumed.
class ResumeTokenSource {
    virtual BSONObj getPostBatchResumeToken() const = 0;
};

// The LAST server stage: the _id it last wrote.
class EmittedTokenStamp {
    virtual BSONObj getLastEmittedToken() const = 0;
};
```

`PipelineD` resolves the first from `_sources.front()` and the second by finding the last stage
implementing it; `PipelineProxyStage` caches **both** pointers at construction, as it now caches
one. No shared state and no back-channel: each question reaches the stage that holds the answer.

A note for whoever writes `CdcFormat`, since the storage choice is not arbitrary. The stamp is
kept as the `_id` **`BSONObj`**, not as a decoded token: the value's only use is a per-document
`binaryEqual` against the outgoing `_id`, so any other representation buys a conversion on the
hot path, and comparing the whole `_id` rather than its `_data` field is what catches a user
stage *adding* to `_id` (`{$addFields: {"_id.extra": 1}}`), which a token-only comparison would
pass. It costs nothing to keep: the stage builds that `BSONObj` anyway to put in the document,
and `BSONObj` is a handle over a refcounted buffer, so the stamp shares it.

Nothing is broken today — the only implementation is a test mock playing both roles — so this
lands when `CdcFormat` becomes the first real implementation, in C7.

Test matrix — C++ unit tests driving **both** loops against the fake logger (F1), plus one jstest per row once C14 lands:

| # | Scenario | Expected `postBatchResumeToken` |
|---|---|---|
| 1 | aggregate, `batchSize: 0`, fresh stream | opening watermark for `S`; the driver needs no `operationTime` |
| 2 | aggregate, `batchSize: 0`, with `resumeAfter` / `startAfter` | the supplied token |
| 3 | aggregate, first batch with events | last appended event's `_id` — **cannot occur on a real server**: the first batch has no time left, so its scan pass only issues the scans and the first getMore collects them |
| 4 | last pulled doc stashed (`haveSpaceForNext` false), either loop | previous appended doc's `_id`; the next getMore appends the stashed doc first and reports its `_id`, or the drained watermark (at or above it) when nothing follows — **getMore loop tested** (`post_batch_resume_token_stash.js`, with `cdc_scan_max_bytes` at 32 MB so one page holds more than a reply); the aggregate loop cannot reach it, see row 3 |
| 5 | every event removed by a user `$match`, source drains | watermark for `emit_bound`, at or above the filtered events' positions |
| 6 | idle getMore, `maxTimeMS` expires, nothing appended | watermark, advancing one checkpoint round per call |
| 7 | `invalidate` in the first batch (resume at own drop, §5.6) | kind-2 token; cursor id 0 |
| 8 | `invalidate` in a getMore batch | kind-2 token; `FAILURE` closes the cursor, batch kept |
| 9 | non-CDC aggregate / getMore | field absent |
| 10 | user stage removes `_id` (`$project: {_id: 0}`) | `ChangeStreamFatalError` (280) from `PipelineProxyStage`; cursor closed — **implemented, tested** |
| 11 | user stage replaces `_id` (`$addFields: {_id: "$documentKey"}`, computed `$project`, `$replaceRoot`) | same — **implemented, tested**. `$set`/`$unset`/`$replaceWith` are 4.2 aliases not registered in this tree; if they are ever added they are the same stages and the same check applies, so add them to this row's test then |
| 12 | user stage modifies nested `_id._data` | same — **implemented, tested** |
| 13 | user stages keep `_id` (`$project: {_id: 1, ns: 1}`, `$match` filtering some events) | delivered unchanged; the check tracks the *last emitted* token so filtered events never trip it — **implemented, tested** |

Order inside C: C1–C3 and C4 are independent; C5 needs C4; C6–C7 need C2/C3;
C8a needs C5; C8b needs C5–C6; C9 needs C7–C8a; C10 last. M1 needs C1
(including the opening protocol), C2, C4, C5, C7 (DML part), C8a, C9 (every
layer; test rows 1–6 and 9), C11's `cdc_open_wait_ms`, C14, and C13 as its
exit gate; rows 7–8 come with C10 at M2; the rest is M2/M4.

## 6. Workstream D — cluster gate

Verified anchors: `cluster_config_PostWriteAll_local/_remote` fault points
at `cluster_config_cc_map.h:226/239`; `DowngradeCceKeyWriteLock(&neg_inf_, …)`
at `:213`; `ClusterConfigRecord` in `cluster_config_record.h`;
`tx_cluster_scale_ops_` and its purge in `log_state.h:455–800`.

| # | Task | Notes / tests |
|---|---|---|
| D1 | `ClusterConfigRecord.cluster_gate {mode, paused}` optional with has-bit through copy/assign/`Serialize`/`Deserialize`. No pause time, in the record or noted by the map: the D8 alert measures the pause's duration in the monitoring system | §6.2 carrier. |
| D2 | `ClusterConfigCcMap::Execute(PostWriteAllCc&)` gate-only path: skip `Sharder::UpdateClusterConfig`, store the gate inside the `write_lk_txn == txn` block; records without a gate unchanged | §6.2 carrier, §10.11. Tests: gate-only post-write above the topology version neither crashes nor changes `Sharder`; stale `resume` post-write after a newer `pause` stores nothing. |
| D3 | Gate check in the **unprepared** branch of `UpsertTableOp::Forward` and `UpsertTableIndexOp::Forward`, before `acquire_all_intent_op_`, new `TxErrorCode::CLUSTER_GATED`; EloqDoc maps it to a retriable error naming CDC bootstrap | §6.2 check. Test: prepared re-lock branches (index build tail, create after kv write, recovery from prepare log) are **not** gated. |
| D4 | `ClusterGateTxRequest` + `ClusterGateOp` (lock → read-under-lock precondition → `WriteToLogOp` with `cluster_gate_log` → `PostWriteAllOp` commit → drain `AsyncOp` for pause), pinned to log group 0 via `InitTxRequest.log_group_id_ = 0`; `CLUSTER_GATE_HELD` on stacked pause | §6.2 translation, "one request, two ops". |
| D5 | Log side: `kClusterGateLog` case in `LogInstance::on_apply` → `LogState::UpdateClusterGate(commit_ts, msg)`; replicated, snapshotted `cluster_gate {value, version, txn}` next to `min_ckpt_ts_`, **never purged** — it is one slot overwritten in place, not a per-operation record, so unlike schema and cluster-scale ops there is nothing to collect, and expiring it would let a recovered group come up unpaused while the rest refuse DDL; one `LogItemType::ClusterGateLog` replay item per group while present | §6.2 durability. |
| D6 | `Execute(ReplayLogCc&)` branch: install highest-version value into `neg_inf_` everywhere; on the coordinator's node group re-acquire the config write lock and `TransactionExecution::RecoverClusterGate` → resume at install (and drain for pause) | §6.2 durability. Fault tests at `cluster_config_PostWriteAll_*`: coordinator killed after the log write, before all post-writes, for pause and for resume, with and without a second node group failing over. |
| D7 | `CheckSchemaOpsInFlight` RPC (A1) served from `tx_catalog_ops_` + DDL store; drain poll in D4 uses it and returns `max_ddl_ts` | §6.2 pause = drain. |
| D8 | EloqDoc admin command `eloqClusterGate {op, mode}`; per-node gauge `cluster_gate_paused` (1 while the cc map holds a paused value, else 0) and the gate-age alert, a monitoring-side duration rule on its cluster-wide maximum — the only thing that surfaces a forgotten pause, since none ends on its own | §6.2, §8. Mode `ddl_dml` is parsed and rejected as unimplemented. The alert rule ships with the E6 runbook. |
| D9 | Tests from §10.11 not covered above: pause survives log-group and node-group leader change and a node restart; every gate log lands in group 0 and replay lists carry at most one gate item; a pause is reinstated on recovery however old it is, so no recovered group admits DDL the others refuse; `cluster_gate_paused` reads 1 on every node while paused, a node restarted into the pause included, and 0 everywhere after resume; cluster scale completed while paused leaves the pause on the new group; second pause → `CLUSTER_GATE_HELD`; split-flush and cluster-scale ops keep working under the gate | Catch2 with `cluster_harness`. |

## 7. Workstream E — reference consumer

New tool under `tools/cdc_consumer/` (Python + `pymongo` for the v1
reference; the design fixes the protocol, not the language — revisit if
throughput needs a compiled consumer).

| # | Task | Notes / tests |
|---|---|---|
| E1 | Apply loop: slot pool keyed by `hash(ns, eloq.keyBytes)`, ordered `bulkWrite` per collection with `bypassDocumentValidation: true`, DDL barrier, ignorable error codes, halt-on-other-error, token in `admin.__eloq_cdc_progress` after each batch | §6.1. Unit tests with a recorded event stream and a local MongoDB sink. |
| E2 | Bootstrap: invalidate checkpoint → drop in-scope namespaces → `pause` → pick `T0` (watermark `>= max_ddl_ts` or event token `> max_ddl_ts`) → copy definitions (strip `unique`, `expireAfterSeconds`, never capped) and documents, with no deadline since the pause cannot lapse → `resume` → publish `{streaming, T0}` → `resumeAfter: T0` | §6.2 steps 1–6, §6.3, §6.4. |
| E3 | Oversized-event handling: point-read by `documentKey`, skip if gone | §5.8. |
| E4 | `ChangeStreamHistoryLost` → re-bootstrap path (exercised by a fake logger returning `PositionTruncated`, since v1 loggers never do) | §4.5, §8. |
| E5 | Integration tests: converge under concurrent DML during bootstrap; `{x:1}`→drop→`{y:1}` with and without the gate; crash between copy end and checkpoint publish re-bootstraps; quiescent-source uniqueness check as documented | §10.11 last item, §6.3. |
| E6 | Operator docs: runbook for abandoned pause, `CLUSTER_GATE_HELD`, retention sizing, lag alert; the gate-age alert rule (`max(cluster_gate_paused) == 1` held longer than the expected copy duration) | §8. |

## 8. Workstream F — verification, CI, docs

| # | Task |
|---|---|
| F1 | **Fake `LogService`** (C++ test double implementing `CdcScan` from an in-memory script of records, bounds and failures) shared by C's unit tests; lives beside `eloq_log_service/test/replay_service_for_test.h` or in the EloqDoc test tree. Needed at M1, so it is the first F task. |
| F2 | M1 part **done in PR 6**: `eloq_cdc.yml` and seven server tests (`dml_events`, `scopes`, `resume`, `post_batch_resume_token`, `id_integrity`, `open_errors`, `idle_tail`) beside the two shell tests, helpers in `tests/jstests/libs/eloq_cdc_util.js`, in both CI suite lists; the server tests drive raw `aggregate`/`getMore` so they check the wire contract whatever the shell. Planned: **jstests**: new `tests/jstests/eloq_cdc/` suite (the 22 tests in `tests/jstests/change_streams/` assume replica-set/oplog semantics; keep them excluded, port the ones about `resumeAfter`/`startAfter`/`invalidate`/`_id` integrity to EloqDoc semantics). **Lands at M1** with the DML cases and the two shell-only tests, `shell_resume_token.js` and `shell_watch_helpers.js`, as a resmoke suite `scripts/buildscripts/resmokeconfig/suites/eloq_cdc.yml` modeled on `eloq_basic.yml`. CI has **two** jstest entry points with separate suite lists, and both get `eloq_cdc`: the CMake job's `cmake_ci_runtime.sh` (`--suites=eloq_basic,eloq_core,eloq_cdc`) and the SCons job's `run_jstests()` in `common.sh` (line 449, same list). Every CI configuration uses the RocksDB-Cloud S3 log state. Grows at M2 (DDL, resume, invalidate) and M3 (bootstrap). |
| F3 | **Done in PR 6** as `eloq_cdc/idle_tail.js`; `ckpt_delay_time_` left at its default. Planned: **Idle-tail test** (§10.10), **M1**: one transaction, then silence; events within `checkpointer_interval + ckpt_delay_time_`; watermark keeps advancing. Also settle `ckpt_delay_time_`'s value in the EloqDoc config. |
| F4 | **Retention guidance**: `txlog_cdc_retention_seconds` (A11) on both log states, set above the worst-case consumer lag; what it costs in disk on the RocksDB state (write rate × retention, beyond `txlog_rocksdb_sst_files_size_limit`); that `log_retention_days` bounds only the cloud archive, which CDC does not read; documented in `configuration-description.md` with the lag metric to watch. |
| F5 | **Design-doc upkeep**: after M2 re-verify §2 facts against the changed code (F7, F8, F12 change under B); mark §10 items closed with the PR that closed them. |
| F6 | **Docs in `DS/`**: `03-concurrency-control.md` (registry change), `04-transaction-execution.md` (gate op, DDL commit-stage `P`), `07-durability-and-recovery.md` (create roll-forward, gate replay), `10-log-service.md` (RPC boundary only). |

## 9. PR sequence

Small, reviewable PRs; each row is one PR unless noted. `DS` PRs precede the
EloqDoc PR that bumps to them.

| Order | Repo | PR | Milestone |
|---|---|---|---|
| 1 | DS | A1 proto (all new messages/RPCs, no handlers). **`log.proto` only** — the submodule working tree may carry unrelated fixes (an `#include <cstdint>` in `tx_service/include/circular_queue.h`, say); they are not part of this PR. Nothing in the EloqDoc tree may reference the new messages until this has landed, which is why the codecs of PR 2 are written against opaque byte strings | M0 |
| 2 | EloqDoc | C8a token codec + C3 key decoder with tests (pure code, no wiring) — **done**: both codecs and 52 passing unit tests in a new dependency-light `cdc_codec` library (eloq `SConscript`), picked up by the CMake manifest as `mongo/db/modules/eloq/cdc_codec_test`. **No submodule bump**: the codecs depend on nothing from `log.proto` — a scan continuation and the two storage keys are opaque byte strings to them — so this PR must leave `src/mongo/db/modules/eloq/data_substrate` untouched, and A1 lands independently in the submodule repo | M0 |
| 3 | DS | A2, A4, A6 data scan + paging + NotLeader on both the RocksDB and RocksDB-Cloud log states, A7 memory rejection, A11 retention floor — **committed in the submodule** (`7d62181`, bumped by PR 5): `eloq_log_service/{include,src}/cdc_scan.*` (continuation codec, the merge, the RocksDB data cursor shared by both RocksDB states), `LogState::CdcScan` with a RocksDB and a cloud override (the memory state inherits the rejection), `LogInstance`/`LogServiceImpl::CdcScan`. The DDL stream is an empty cursor until A3, and `wait_ms` is ignored until A5. Tests: `eloq_log_service/test/CdcScan-Test.cpp`, 15 cases, built both by the data_substrate suite (`tx_service/tests`) and by the standalone log-service project; its real-store cases need `LOG_STATE_TYPE_RKDB`, so they run in the substrate's RocksDB suite and only the in-memory cases run in cloud builds. The cloud override has its own case, `test_cdc_scan_cloud_state` in `log_server_rocksdb_cloud_tests.cpp`, added to the log-service workflow's filter that runs against MinIO: refused before the DB is in sync and after it stops, a record buffered in memory before the open read back from the DB, checkpoint bound, paging, `from_ts`. It drives `LogStateRocksDBCloudImpl` directly, since fault injection is not compiled into `logservice` and the not-in-sync window cannot be held open through a whole log server. The memory variant is compile-checked. A11: `LogState::CdcRetentionFloor` applied in `PurgingSstFiles` and at the `MoveFilesToArchive` call, the setting `txlog_cdc_retention_seconds` read in `core/src/log_init.cpp`, `launch_sv.cpp` and `AttachLog` and handed to the log state by `LogInstance`; tested by `CdcScan-Test` ("The RocksDB purge keeps the CDC retention window", files flushed through `WriteSnapshot`) and by `test_cdc_retention_floor_cloud_state` against MinIO (added to the workflow filter); both fail with the floor removed. The scan depends on `WriteLog` being acknowledged only after its record is written, which already holds and is now stated at `BatchAddLogItems` and in §4.2. The RocksDB state drains CDC scans before it closes `db_` (a scan begun while the node led can outlast a step-down into a snapshot install): scans register on an atomic counter and check a flag, and the close clears the flag and polls with `bthread_usleep` until the counter drains, so the braft FSM bthread never blocks a worker; `CdcScan-Test` reinstalls a snapshot 30 times under three scanning threads, and segfaults without the drain | M1 |
| 4 | DS | B1 registry — **committed in the submodule** (`bb69dfa`, bumped by PR 5; the range-split fix below is its own commit, `b4bbf5d`): latest-grant `TxLockInfo::wlock_ts_`, `holds_ckpt_wlock_` + `CcShard::OnWriteLockDowngraded` (`cc_shard.{h,cpp}`, `cc_map.cpp`), `tx_service/tests/CkptLockRegistry-Test.cpp`, `03-concurrency-control.md` §10 and `07-durability-and-recovery.md` §3.2; plus a tx-service fix found on the way: add index creates the index's kv table before its prepare log, under the prepare-stage write lock, through the new generic `DataStoreHandler::CreateKvTable` (the counterpart of `DropKvTable`, no catalog row; the DSS client writes the initial range record). Recovery returns to the state right after the prepare log, where DML writes the index, so the table must exist by then; the old order created it after the install. A failure of the creation or the prepare log drops the tables and fails with no log; an orphan after a coordinator failure is accepted. The new schema reaches the kv catalog once, after the commit log, at `D` (`kv_update_schema_image_op_`, now for every add index, under the commit-stage write lock, which orders it before any later DDL's catalog write), so a build constraint violation is the only add-index rollback after the prepare log. The write lock therefore spans the creation, blocking DML and the checkpoint on a slow store; the creation is tried once, with no retry (`tx_index_operation.{h,cpp}`, `data_store_handler.h`, DSS client, `08-range-and-bucket-management.md` §6, `09-store-handler.md`, design §4.4(b)); and a second one: the index op's `PrepareCommit` installs `D` as the catalog cc entry's commit ts under the prepare-stage write lock, so every DML that sees the dirty schema commits above `D` on every node; before, only the node clocks ordered it, and a DML below `D` lost its index write on replay (`template_cc_map.h` `PrepareCommit` branch, `catalog_cc_map.h` replay, `05-data-model-and-catalog.md`, design F7/F9/§4.4(b)) | M1 |
| 5 | EloqDoc | F1 fake logger; C4 scan client; C5 scan source — **done**, with 30 passing unit tests in `cdc_scan_test`. Against PR 3's logger, which answers at once (`wait_ms` ignored until A5): client-side backoff on early empty pages (C5) and on `NotLeader` with the node's own or no address (C4). **Preceded by a small DS commit** (`6080145`) that gives the scan path a way to the log groups through the tx service's `TxLog`, whose `LogAgent` already keeps the leader cache: `TxLog::CdcScan` (asynchronous, a `LogService_Stub` call to the cached leader, reporting the node ID it sends to; the base class fails the call) and `TxLog::LogNodeByAddress` (the node ID configured with the `host:port` a `NotLeader` redirect names; following it is the existing `UpdateLeaderCache`), forwarded by `EloqLogAgent` (`eloq_log_wrapper.h`), with `tx_service/tests/LogAgentCdc-Test.cpp` and a line in `10-log-service.md` §9. **This PR bumps the submodule pointer**, past PR 1 (`8b8016d`), PR 3, PR 4, the range-split fix and that commit. EloqDoc side, in a new `cdc_scan` library under `src/cdc/`: `CdcWaitObject` (§5.8; a coroutine yields and a bthread timer bounds it, a plain thread blocks on a condition variable), `CdcScanTransport` with its `TxLog` implementation, `CdcScanClient` (C4) and `CdcScanSource` (C5); a `NotLeader` redirect is compared with the call's node ID, never as a string, since the log service may name a node by hostname or by IP address. A position below the retention floor raises the new `ChangeStreamHistoryLost` (286). F1 is `src/cdc/cdc_fake_log_service.{h,cpp}`, in the EloqDoc test tree, compiled only into `cdc_scan_test` | M1 |
| 6 | EloqDoc | C2 blob decoder; C6 decode (DML only); C7 format (DML); C1 open path + opening protocol + `ChangeStreamStartNotReady`; C9 token plumbing at every layer + both batch loops; **C8a watermark-token resume**; C14 shell; `cdc_open_wait_ms`; **F2: `eloq_cdc` suite (yaml + DML cases + `shell_resume_token.js` + `shell_watch_helpers.js`), added to both CI suite lists — `cmake_ci_runtime.sh` and `common.sh` `run_jstests()`**; **F3 idle-tail test**; C13 driver scenarios in CI **Done**: the single stage and its event stream (design §5.2; C1, C6 decode, C7 DML, C8a, C8b event resume, C9's provider answered by that stage, `cdc_open_wait_ms`), `startAfter`, error code 5000 (`ChangeStreamStartNotReady`, EloqDoc's own range); unit tests `cdc_stream_test` (12) and `cdc_scan_test` (32); the `eloq_cdc` suite (F2, F3) and C13's PyMongo scenarios, all passing locally against a Debug server with the RocksDB log state. Companion commits: the data substrate's `checkpointer_interval` rules and `Checkpointer::CheckpointInterval()` (submodule `3e7a1e4`), which the `ChangeStreamStartNotReady` message names, and the `_id` point-lookup type-bits fix (`8b0bf5df2a`), found by `dml_events.js`. `eloq_cdc/missing_namespace.js` opens streams on a missing collection and database (the null-collection path in `run_aggregate`). `eloq_cdc/post_batch_resume_token_stash.js` covers C9 row 4 in the getMore loop, using the new `cdc_scan_max_bytes` server parameter (default 4 MB, at most 32 MB) to put more than one reply's worth in a page. Not reachable on a real server, so not tested: row 3 and row 4 in the aggregate loop. An aggregate's first batch has no time left, so its first scan pass only issues the scans and the first getMore collects them; the first batch holds no events. | **M1 exit** |
| 7 | DS | B2–B5 commit-stage `P` + create kv move + PostCommit + log state; B8 rolled-back add index; B6 tests (recovery asserts `schema_ts_` and kv catalog `== P`); **B7 `ddl_kv_retry_exhausted` metric** | M2 |
| 8 | DS | A3 DDL store + append hooks, including the end-to-end recovery assertion `ddl_ts == P`; A5 long poll; **A8 cloud-specific behaviors** | M2 |
| 9 | EloqDoc | C7 DDL events; C8b invalidate resume (event resume done in PR 6); C10 invalidate; `eloq_cdc` DDL/resume/invalidate cases; **B7 runbook entry** for a prolonged store failure | **M2 exit** |
| 10 | DS | D1–D3, D5, D7 (record, cc map, tx-op check, log slot, drain RPC) | M3 |
| 11 | DS | D4, D6 gate op + replay + fault tests; D9 | M3 |
| 12 | EloqDoc | D8 admin command; E1–E2 consumer + E5 integration tests; `eloq_cdc` bootstrap cases | **M3 exit** |
| 13 | DS | A9 metrics | M4 |
| 14 | EloqDoc | C11–C12 config/metrics; C7 oversized rule; E3–E4, E6; F4–F6 docs | **M4 exit** |

Parallelism: after PR 2, one engineer each on A, B, C, D keeps every lane
busy through M2; D is the longest lane and can start at PR 1. E starts when
PR 9 and PR 11 are in review.

**One change outside this sequence.** Writing C3's round-trip test surfaced a
decode bug in shared storage code, `src/mongo/db/storage/key_string.cpp`: the
exact-integer branch for a decimal with a continuation was selected by
comparing the `int64` integer part against a `double`-typed `kMaxIntForDouble`,
which converts 2^53+1 to 2^53 and so skips that branch for the first value
that needs it, returning a value one unit low. Every caller of
`KeyString::toBson` inherited it, including `EloqIndexCursor::_curr` when a
plan requests `kWantKey`. Fixed by comparing against an `int64`-typed bound;
`mongo/db/storage/storage_key_string_test` (38 tests, including its
randomized round-trip fuzzing) passes. It is a decode-only change — no
storage format moves — but it is not CDC work and belongs in its own
storage-layer PR ahead of PR 2.

## 10. Risks and decisions to make early

| Risk / decision | Impact | Plan |
|---|---|---|
| B edits the shared `UpsertTableOp` state machine, `PostCommit`, `ActiveTxMinTs` and `UpsertSchemaOp`, which every `tx_service` product's create/drop/truncate runs through | cross-product behavior change, no flag | Land once for all products (§4 risk). B6 tests run in the `tx_service` harness; the PR calls out the two observable changes (no create rollback on kv error; catalog write lock pins the checkpoint) for EloqKV/EloqSQL reviewers. |
| DDL kv retry pins checkpoints cluster-wide (§4.4(b) cost) | availability | Accepted in the design; B7 alert + runbook are part of M2, not M4. |
| `ckpt_delay_time_` under MVCC adds directly to tail latency (§5.7) | latency SLO | Measure in F3; document; consider lowering `checkpointer_interval` in CDC deployments. |
| Proto `continuation` layout is shared with tokens (§4.2, §5.6) | token compatibility | Freeze at M0 as a documented struct; version byte in `_data` already covers later changes. |
| `eloq_log_service/` proprietary: fewer reviewers, docs limited to boundary | review bandwidth | A's tests carry the spec; `10-log-service.md` documents the RPC contract precisely. |
| 4.0 jstests for change streams do not apply | CI coverage | F2 builds `eloq_cdc/` from scratch; do not try to make `change_streams/` pass. |
| CI builds only the RocksDB-Cloud S3 log state (`WITH_LOG_STATE=ROCKSDB_CLOUD_S3` for both engine entries) | a RocksDB-only scan would leave CDC untested in CI until cloud parity | A2 implements the scan on both log states from M1; A8 keeps only the cloud-specific behaviors and lands at M2; F2 wires `eloq_cdc` into CI at M1. |
| Both log states purge everything checkpointed: the RocksDB state deletes every SST below `min_ckpt_ts − 1` once over its size limit, and the cloud state's Stage-1 archive mover (`MoveFilesToArchive`, every 600 s by default) moves every SST below `min_ckpt_ts` out of the live DB, into an archive nothing reads | without a floor, a consumer behind the checkpoint when a purge runs loses records, silently in v1; §4.6 had claimed the cloud window was `log_retention_days` | **Decided:** the CDC retention floor (§4.5, A11) in both purges, in PR 3. |
| Consumer language | maintainability vs throughput | Python reference in v1; the protocol is the deliverable. Revisit after M3 measurements. |
| Oversized-event threshold (§10.8) | wedged streams | Resolve in C7 by testing the actual reply path before choosing the limit. |
| Driver opening rule (F13): an empty opening reply without a token makes spec-conforming drivers demand `operationTime`, which standalone EloqDoc never sends | every `watch()` on a fresh cluster fails | Design changed (§5.1 opening protocol): the open waits for a fixable `S`, bounded by `cdc_open_wait_ms`, and always returns a token or `ChangeStreamStartNotReady`. C13 is the M1 gate so the three driver scenarios are proven before anything is built on the wire contract. |

## 11. Out of scope for this plan (design §9)

Retention-floor tracking, sink-side unique indexes, `modify` events,
`updateDescription`, `txnNumber`/`lsid`, follower scans, time-based RocksDB
retention, rename, capped collections, Kafka connector, mode `ddl_dml` of the
gate beyond parsing.

**Unique secondary index builds over existing documents** are deferred to the
next project. The tx-service build does not detect duplicate keys among existing
documents, so for now EloqDoc refuses `createIndexes` with `unique: true` on a
non-empty collection (`CannotCreateIndex`, `MultiIndexBlockImpl::init`). Once
DML sees the build (dirty schema installed), inserts and updates are refused
until it finishes. A document inserted between the emptiness check and that
point is still built without duplicate detection. The window spans the DDL's
lock stages, prepare log and kv index creation; closing it would need the check
inside the tx service's exclusive section. The CI core suites skip the three tests that build a unique
index over documents (`apply_ops_dups.js`, `covered_index_simple_2.js`,
`covered_index_simple_3.js`).
