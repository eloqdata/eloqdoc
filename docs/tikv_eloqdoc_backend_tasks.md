# TiKV Backend for EloqDoc: Implementation Tasks

## Goal

Implement an `ELOQDSS_TIKV` data-store backend for EloqDoc while preserving Eloq's existing MVCC semantics.

The backend must keep Eloq logical MVCC/archive encoding unchanged:

- Base values store Eloq `commit_ts` in the value prefix.
- Archive records are stored in the logical `mvcc_archives` table.
- Archive keys include big-endian Eloq `commit_ts` for ordered scans.
- TiKV MVCC is used only as the underlying transactional KV mechanism, not as the source of Eloq historical versions.

## Non-Goals

- Do not redesign `tx_service` MVCC.
- Do not replace `DataStoreServiceClient` archive logic.
- Do not depend on TiKV internal historical versions to implement Eloq snapshot reads.
- Do not expose TiKV physical MVCC key/value encoding to EloqDoc.

## Design Invariants

- `DataStoreServiceClient` remains the owner of Eloq archive writes and archive reads.
- The new TiKV backend implements the existing `EloqDS::DataStore` interface.
- The new TiKV backend must return `record`, `ts`, and `ttl` exactly like `RocksDBDataStoreCommon`.
- Snapshot reads must continue to work through `FetchArchives` and `FetchVisibleArchive`.
- A single DSS batch write should commit atomically in TiKV.

## Task 0: Extend `/home/lance/Projects/tikv-client-c`

This task must be completed first. EloqDoc should not start integrating TiKV until these client capabilities exist.

### 0.1 Add Transactional Delete Support

Current state:

- `include/pingcap/kv/Txn.h` only exposes `set`.
- `src/kv/2pc.cc` builds prewrite mutations as key/value puts.
- `kvrpcpb::Mutation` already supports `Op::Put` and `Op::Del`.

Required work:

- Replace the transaction buffer value type with a mutation-aware structure.
- Add `Txn::del(const std::string &key)` or equivalent.
- Ensure `TwoPhaseCommitter::prewriteSingleBatch` sets:
  - `mutation.op = kvrpcpb::Op::Put` for puts.
  - `mutation.op = kvrpcpb::Op::Del` for deletes.
  - `mutation.value` only when needed.
- Ensure transaction size accounting includes delete keys correctly.
- Preserve existing `Txn::set` behavior.

Acceptance criteria:

- A transaction can put a key, commit, then delete the key in another transaction.
- A snapshot after the delete no longer sees the key.
- Existing 2PC tests still pass.

### 0.2 Optional: Add Explicit Get Result

Current state:

- `Snapshot::Get` returns `std::string`.
- Empty value and not-found can be confused by callers.

This is optional for the first EloqDoc TiKV backend because Eloq encoded values are never empty:

- Base/archive values always include at least the 8-byte Eloq `ts` prefix.
- Even an empty logical record body still produces a non-empty stored value.

Optional work:

- Add an API such as:

```cpp
struct GetResult {
    bool found;
    std::string value;
};

GetResult Snapshot::GetResult(const std::string &key);
GetResult Snapshot::GetResult(Backoffer &bo, const std::string &key);
```

- Keep the existing `Snapshot::Get` API for compatibility.
- Use TiKV `GetResponse.not_found` instead of inferring not-found from an empty value.

Minimum EloqDoc requirement:

- The EloqDoc TiKV adapter must still distinguish not-found from found.
- This can be done privately in the adapter by sending `GetRequest` directly and checking `response.not_found()`.
- It is not required to change the public `tikv-client-c::Snapshot::Get` API for the first implementation.

Acceptance criteria:

- A stored empty value is distinguishable from a missing key.
- Existing callers using `Snapshot::Get` continue to compile.

### 0.3 Add Bounded Forward and Reverse Scan API

Current state:

- `Snapshot::Scan(begin, end)` uses `Scanner`.
- `Scanner` sends `kvrpcpb::ScanRequest` with forward scan only.
- TiKV proto supports `ScanRequest.reverse`.

Required work:

- Add an API suitable for Eloq DSS scan requests, for example:

```cpp
struct ScanOptions {
    std::string start_key;
    std::string end_key;
    uint32_t limit;
    bool reverse;
    bool key_only;
    uint64_t version;
};

struct ScanResult {
    std::vector<kvrpcpb::KvPair> pairs;
    bool has_more;
    std::string next_start_key;
};

ScanResult Snapshot::ScanOnce(const ScanOptions &options);
ScanResult Snapshot::ScanOnce(Backoffer &bo, const ScanOptions &options);
```

- Forward scan range semantics must match TiKV `[start_key, end_key)`.
- Reverse scan must use TiKV reverse scan semantics:
  - scan `[end_key, start_key)` in descending order.
  - `start_key` is the upper bound cursor for reverse scans.
- Resolve locks using the same lock resolver behavior as existing scan/get.
- Preserve existing `Scanner` API for compatibility.

Acceptance criteria:

- Forward scan returns keys in ascending order.
- Reverse scan returns keys in descending order.
- Reverse scan can find the latest key at or before a timestamp-encoded suffix.
- Region split boundaries are handled through the existing region cache/backoff path.

### 0.4 Optional: Expose Commit Timestamp

This is optional for the first EloqDoc backend because Eloq commit timestamps are stored in values.

Required only if needed by tests or diagnostics:

- Add a way to inspect the commit timestamp selected by `TwoPhaseCommitter`.
- Do not use this timestamp as Eloq `commit_ts`.

Acceptance criteria:

- No EloqDoc logic depends on this API.

## Task 1: Add TiKV Client Adapter in EloqDoc

Repository: `/home/lance/Projects/eloqdoc`

Create a small adapter layer so `TikvDataStore` does not directly depend on low-level `RegionClient` details.

Suggested files:

- `src/mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service/tikv_config.h`
- `src/mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service/tikv_kv_client.h`
- `src/mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service/tikv_kv_client.cpp`

Required adapter operations:

```cpp
struct KvGetResult {
    bool found;
    std::string value;
};

struct KvScanItem {
    std::string key;
    std::string value;
};

struct KvScanResult {
    std::vector<KvScanItem> items;
    bool has_more;
    std::string next_cursor;
};

class TikvKvClient {
public:
    bool Initialize(const TikvConfig &config);
    KvGetResult Get(const std::string &key);
    bool CommitBatch(const std::vector<Mutation> &mutations);
    KvScanResult Scan(const ScanOptions &options);
    bool DeleteRange(const std::string &start_key, const std::string &end_key);
};
```

Acceptance criteria:

- Adapter hides TiKV backoff, region retry, transaction commit, and scan details.
- Adapter returns explicit not-found information.
- Adapter supports reverse scan.

## Task 2: Implement Eloq Value Codec for TiKV Backend

Create shared codec helpers for the TiKV backend. Do not copy ad hoc parsing into every method.

Required encoding:

```text
value = uint64_t ts_with_ttl_flag
      + optional uint64_t ttl
      + record bytes
```

Required helpers:

- `EncodeValue(record_parts, ts, ttl)`.
- `DecodeValue(value) -> record, ts, ttl`.
- `EncodeHasTTLIntoTs`.
- `DecodeHasTTLFromTs`.

Important compatibility note:

- Match `RocksDBDataStoreCommon::TransformRecordToValueSlices`.
- Do not use TiKV commit timestamp as Eloq `ts`.

Acceptance criteria:

- Values written by TiKV backend decode identically to RocksDB backend values.
- `ts` MSB is reserved for TTL flag.
- `ttl == 0` means no TTL.

## Task 3: Implement `TikvDataStore`

Suggested files:

- `tikv_data_store.h`
- `tikv_data_store.cpp`
- `tikv_data_store_factory.h`

`TikvDataStore` must implement `EloqDS::DataStore`.

### 3.1 Physical Key Format

Use:

```text
[cluster_prefix] + kv_table_name + "/" + partition_id_decimal + "/" + logical_key
```

Rules:

- Keep the same table/partition/user-key shape as RocksDB DSS.
- Add `cluster_prefix` only if configured.
- Do not put Eloq base `commit_ts` into the physical base key.
- Archive keys already contain the big-endian Eloq `commit_ts` in the logical key.

Acceptance criteria:

- `Read` and `ScanNext` can strip the physical prefix and return logical keys.
- `DropTable` can delete all keys for a logical table without affecting other tables.

### 3.2 `Read`

Required behavior:

- Build physical key.
- Read latest committed TiKV value.
- Decode Eloq value into `record`, `ts`, `ttl`.
- If key is not found, return `KEY_NOT_FOUND`, `record=""`, `ts=0`, `ttl=0`.
- If TTL is expired, return `KEY_NOT_FOUND` or logically deleted behavior consistent with RocksDB callbacks.

Acceptance criteria:

- `DataStoreServiceClient` receives the same values as with RocksDB.
- Expired records are not returned as live records.

### 3.3 `BatchWriteRecords`

Required behavior:

- Reject writes when shard status is not read-write.
- For each item:
  - `PUT`: encode `[ts|ttl|record]` and add TiKV put mutation.
  - `DELETE`: add TiKV delete mutation.
- Commit all mutations in one TiKV transaction.
- Return `NO_ERROR` only after TiKV commit succeeds.

Acceptance criteria:

- Base table writes are atomic per DSS batch.
- Archive table writes are atomic per DSS batch.
- Delete operations remove object table keys correctly.
- MVCC range deletes that are converted to PUT tombstones by `DataStoreServiceClient` remain PUTs.

### 3.4 `ScanNext`

Required behavior:

- Build physical start/end keys using the table partition prefix.
- Support both forward and reverse scans.
- Apply inclusive/exclusive start/end semantics expected by DSS requests.
- Decode every value to `record`, `ts`, `ttl`.
- Return logical keys without the physical table/partition prefix.
- Apply search conditions currently supported by RocksDB, especially object `type` filtering.
- Support session IDs if required, or implement stateless cursor continuation with equivalent behavior.

Acceptance criteria:

- Forward collection/index scans work.
- Archive reverse scan finds the visible historical version.
- `FetchArchives` and `FetchVisibleArchive` work unchanged.

### 3.5 `DeleteRange`

Required behavior:

- Convert DSS logical range to physical TiKV key range.
- Delete the range.
- If TiKV transactional delete range is not available, scan-and-delete in bounded batches.

Acceptance criteria:

- Range cleanup works without deleting adjacent partitions or tables.

### 3.6 `CreateTable`, `DropTable`, `FlushData`, `ScanClose`

Required behavior:

- `CreateTable`: no-op success.
- `DropTable`: delete all keys under `kv_table_name + "/"`.
- `FlushData`: no-op success after committed writes.
- `ScanClose`: no-op success if stateless scan is used.

Acceptance criteria:

- `DataStoreServiceClient::PersistKV` succeeds.
- Table drop removes base and index table data for that table.

### 3.7 Backup and Standby Snapshot

First implementation:

- Return explicit unsupported status for `CreateSnapshotForBackup` unless backup is required.
- Document that TiKV backend does not produce RocksDB backup files.

Acceptance criteria:

- Normal CRUD/MVCC tests do not depend on backup.
- Backup commands fail clearly instead of silently succeeding with invalid files.

## Task 4: Add Build and Configuration Wiring

Required files to update:

- `src/mongo/db/modules/eloq/data_substrate/CMakeLists.txt`
- `src/mongo/db/modules/eloq/SConscript`
- `src/mongo/db/modules/eloq/data_substrate/core/src/storage_init.cpp`
- `src/mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service/data_store_factory.h`

Required build option:

```text
WITH_DATA_STORE=ELOQDSS_TIKV
DATA_STORE_TYPE_ELOQDSS_TIKV
```

Required configuration:

- TiKV PD endpoints.
- Optional keyspace or key prefix.
- Optional request timeout.
- Optional scan batch size.

Acceptance criteria:

- EloqDoc builds with `WITH_DATA_STORE=ELOQDSS_TIKV`.
- Existing RocksDB builds still compile.
- `storage_init.cpp` creates `TikvDataStoreFactory` for `ELOQDSS_TIKV`.

## Task 5: Align MVCC Configuration

Required behavior:

- `storage.eloq.enableMVCC` and tx_service `enable_mvcc` must be consistent.
- If one is enabled and the other is disabled, startup should fail with a clear error.

Reason:

- MongoDB readConcern mapping depends on `storageGlobalParams.enableMVCC`.
- tx_service archive flush depends on `LocalCcShards::EnableMvcc()`.

Acceptance criteria:

- Snapshot isolation cannot be advertised while archive flush is disabled.
- Misconfiguration is caught at startup.

## Task 6: Tests

### 6.1 tikv-client-c Tests

Required tests:

- Put/get.
- Put/delete/get missing.
- Empty value round trip.
- Forward scan.
- Reverse scan.
- Scan across region splits if existing test infrastructure supports it.

### 6.2 Eloq Value Codec Tests

Required tests:

- Encode/decode without TTL.
- Encode/decode with TTL.
- MSB TTL flag does not corrupt timestamp.
- Record body is preserved byte-for-byte.

### 6.3 EloqDoc TiKV Backend Tests

Required tests:

- Base table put/read.
- Object table delete.
- Range table MVCC delete marker.
- `mvcc_archives` write/read.
- Archive reverse scan for visible version.
- Snapshot read:
  - T1 starts.
  - T2 updates and commits.
  - T1 reads old value.
- Snapshot scan:
  - T1 starts scan.
  - T2 updates records.
  - T1 scan sees snapshot-consistent values.
- Restart/checkpoint:
  - Persist base and archive.
  - Restart.
  - Snapshot read can fetch archive from TiKV.
- Drop table removes all table keys.

## Task 7: Operational Follow-Up

Required follow-up before production use:

- Add a TiKV backend TTL/GC worker for expired archive and retired tombstone records.
- Define TiKV GC safepoint expectations. Eloq must not rely on TiKV historical versions.
- Define backup/restore behavior for TiKV backend.
- Add metrics for TiKV read/write/scan latency and retry counts.
- Add failure injection for partial write, TiKV unavailable, region error, and lock conflict.

### 7.1 Production Constraints

The first TiKV backend is functionally usable for CRUD, scan, range cleanup,
logical archive reads, and restart persistence, but it must not be treated as a
production-ready storage backend until the following constraints are explicit in
configuration, operations docs, and tests.

#### TiKV GC Safepoint

EloqDoc must not use TiKV MVCC history as its snapshot-read source.

Rules:

- Eloq historical versions are logical rows in `mvcc_archives`.
- Base and archive values carry Eloq logical `commit_ts` in the encoded value.
- TiKV commit timestamps are an implementation detail of the underlying KV
  transaction.
- TiKV GC safepoint may be advanced according to TiKV cluster policy, but it
  must never be used to decide whether an Eloq snapshot can still read an old
  version.
- Eloq archive retention must be controlled by an Eloq/TxService safe timestamp
  or by an explicit configured retention window, not by TiKV GC safepoint.

Operational expectation:

- Operators can use normal TiKV MVCC GC for raw TiKV versions because EloqDoc
  reads only latest committed raw TiKV values for each logical key.
- Operators must keep `mvcc_archives` logical rows until Eloq declares them
  safe to remove.
- If a future restore uses TiKV cluster-level backup, the backup must include
  both base table keys and `mvcc_archives` keys for the same logical cut.

Acceptance criteria before production:

- Startup or docs clearly state that TiKV GC safepoint is not an Eloq snapshot
  retention mechanism.
- Any archive cleanup code uses an Eloq safe archive watermark, not the TiKV GC
  safepoint.
- Tests prove snapshot read/scan still work after normal TiKV GC has advanced
  past old TiKV internal versions, because logical archive rows remain.

#### TTL and Logical GC Worker

Current behavior:

- `Read` treats expired TTL values as not found.
- Expired base values are not proactively removed.
- Archive rows and tombstone records are retained until table/range cleanup or
  drop-table cleanup.

Production rule:

- Do not run a single broad "delete old data" worker without a safe watermark.
- Cleanup must be bounded, prefix-scoped, idempotent, and safe to resume.
- Cleanup must never delete an archive/tombstone version that might still be
  visible to an active or recoverable Eloq snapshot.

Required cleanup classes:

1. **Expired base TTL cleanup**
   - Scope: normal base/index table prefixes, excluding `mvcc_archives`.
   - Eligibility: decoded value has `ttl > 0` and `ttl < now_ms`.
   - Safety: delete only the current logical key when it is still expired at
     delete time.
   - Batching: use bounded scan and bounded transaction/delete batches.

2. **Archive retention cleanup**
   - Scope: `mvcc_archives` table only.
   - Eligibility: archive Eloq `commit_ts` is older than the safe archive
     watermark.
   - Safety: safe watermark must come from TxService/archive retention policy or
     explicit configuration; it must not come from TiKV GC safepoint.

3. **Retired tombstone cleanup**
   - Scope: base tombstone records and archive tombstone rows.
   - Eligibility: tombstone is older than the same safe archive watermark and no
     live range/table metadata needs it for delete visibility.
   - Safety: if the watermark is unknown, skip tombstone cleanup.

Minimum implementation constraints:

- Worker disabled by default until a safe watermark source is wired.
- Every run is prefix-scoped by TiKV `tikv_key_prefix`.
- Every run logs scanned/deleted counts and stops at configured batch limits.
- Worker failure must leave data in a conservative state: undeleted is OK,
  deleting too much is not.

#### Backup and Restore

Current behavior:

- `CreateSnapshotForBackup` returns an explicit unsupported error for TiKV
  backend.
- `FlushData` is a no-op success because TiKV commits are durable when the
  underlying transaction commits.

Production rule:

- RocksDB backup files are not available for TiKV backend.
- A TiKV backend backup must preserve a consistent logical keyspace containing:
  - all base/index table keys,
  - all `mvcc_archives` keys needed by the chosen recovery point,
  - metadata/config needed to rebuild the same `tikv_key_prefix`,
  - the matching EloqDoc catalog state.
- Restore must not combine base keys from one cut with archive keys from another
  cut.

Supported production directions:

1. **TiKV cluster-level backup/restore**
   - Use TiKV/BR-style tooling or an operator-approved TiKV backup workflow.
   - Scope must include the full EloqDoc key prefix.
   - Restore must be validated against a fresh EloqDoc instance with the same
     prefix configuration.

2. **Eloq logical backup/restore**
   - Export/import logical base and archive rows through DataStore APIs.
   - Slower but prefix-scoped and backend-neutral.

Until one direction is implemented, backup commands for `ELOQDSS_TIKV` must fail
clearly instead of silently producing invalid RocksDB-style artifacts.

#### Metrics and Failure Injection

Current implemented coverage:

- Basic TiKV read/write/scan/range-delete total and duration metrics are wired
  through `metrics::kv_meter`.
- Failure smoke covers empty PD endpoints, transaction conflict, and failed
  multi-key batch cleanup without partial visibility.

Remaining production gaps:

- Retry count metrics require a client-c backoff/retry observer or a wrapper
  API. Do not fabricate retry counts by guessing from success/failure results.
- Additional region error injection should be added once there is a stable
  client-c test hook for region miss / epoch-not-match / unavailable store.

### 7.2 Follow-Up Task Split

Do not implement a monolithic TiKV cleanup subsystem in one change. Split the
remaining production work into small reviewable tasks:

#### Task 7A: Document and Enforce TiKV GC Safepoint Contract

Scope:

- Add operator docs explaining that TiKV GC safepoint is independent from Eloq
  archive retention.
- Add a startup log or configuration note for `ELOQDSS_TIKV` when MVCC is
  enabled.

Acceptance criteria:

- Docs explicitly say Eloq snapshot reads rely on `mvcc_archives`.
- No code path uses TiKV GC safepoint as archive cleanup watermark.

#### Task 7B: Define Safe Archive Cleanup Watermark

Scope:

- Identify or add the TxService-facing source of "oldest snapshot that may still
  need archive rows".
- Add config for a conservative fallback retention window.
- Expose the computed watermark to TiKV cleanup code without coupling cleanup to
  TiKV internals.

Acceptance criteria:

- Unknown watermark disables archive/tombstone cleanup.
- Tests cover "watermark not available" and "watermark available" decisions.

#### Task 7C: Implement Expired Base TTL Cleanup Worker

Scope:

- Scan normal table/index prefixes in bounded batches.
- Decode values and delete keys whose TTL is expired.
- Exclude `mvcc_archives`.

Acceptance criteria:

- Expired TTL keys are removed eventually.
- Live and non-TTL keys remain.
- Worker is idempotent and safe to interrupt/restart.

#### Task 7D: Implement Archive and Tombstone Cleanup Worker

Scope:

- Use the safe archive watermark from Task 7B.
- Clean `mvcc_archives` and retired tombstones in bounded batches.
- Keep conservative behavior when records are malformed or watermark is absent.

Acceptance criteria:

- Snapshot read/scan at or after the safe watermark still succeeds.
- Versions newer than the watermark are never removed.
- Cleanup cannot cross `tikv_key_prefix`, table, or partition boundaries.

#### Task 7E: Decide and Implement TiKV Backup/Restore Mode

Scope:

- Choose either TiKV cluster-level backup or Eloq logical backup as the first
  supported production path.
- Update `CreateSnapshotForBackup` behavior only after the chosen path is
  implemented end-to-end.

Acceptance criteria:

- Backup captures base and archive keys from a consistent logical cut.
- Restore is validated by CRUD, archive reverse scan, snapshot read/scan, and
  restart smoke.
- Unsupported modes continue to fail clearly.

#### Task 7F: Add Retry/Region Error Observability

Scope:

- Extend tikv-client-c or the adapter with a real retry/backoff observer.
- Record retry counts by operation type.
- Add region miss / epoch-not-match / unavailable-store smoke tests when a
  stable injection hook exists.

Acceptance criteria:

- Retry metrics are based on actual retry/backoff events.
- Metrics do not change operation behavior when disabled.

## Implementation Order

1. Modify `/home/lance/Projects/tikv-client-c`.
2. Add EloqDoc TiKV adapter and value codec.
3. Implement `TikvDataStore`.
4. Wire build/config.
5. Add MVCC config validation.
6. Run tikv-client-c tests.
7. Run EloqDoc TiKV backend tests.
8. Add TTL/GC and operational polish.

## Review Checklist

- Eloq `commit_ts` is never replaced by TiKV commit timestamp.
- `DataStoreServiceClient` archive code remains unchanged unless a bug is found.
- Archive key suffix remains big-endian Eloq `commit_ts`.
- Reverse scan is implemented before claiming snapshot read support.
- TiKV not-found and empty value are distinguishable.
- `FlushData` no-op is intentional and documented.
- Backup unsupported path is explicit.
- RocksDB backend behavior remains unchanged.
