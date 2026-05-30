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

Current call chain/source audit:

- Cluster backup RPC is exposed as `CreateClusterBackup` in
  `tx_service/include/proto/cc_request.proto` and implemented by
  `CcNodeService::CreateClusterBackup`. It calls `BackupUtil::CreateBackup`,
  which sends `CreateBackup` RPCs to each node-group leader.
- Each leader handles `CcNodeService::CreateBackup` by enqueuing
  `SnapshotManager::CreateBackup`; the worker path is
  `SnapshotManager::HandleBackupTask`. That path runs one checkpoint round,
  then calls `store_hd_->CreateSnapshotForBackup(backup_name, snapshot_files,
  last_ckpt_ts)`.
- When `store_hd_` is `DataStoreServiceClient`, the request fans out to every
  DSS shard through `DataStoreServiceClient::CreateSnapshotForBackup` /
  `CreateSnapshotForBackupInternal`, then enters `DataStoreService` and finally
  the concrete `DataStore::CreateSnapshotForBackup` implementation for the
  shard.
- Existing RocksDB-style backup implementations create local snapshot files
  under the configured backup path and can optionally rsync those files to
  `dest_path`. That file artifact contract does not map to TiKV. TiKV currently
  returns `CREATE_SNAPSHOT_ERROR` from `TikvDataStore::CreateSnapshotForBackup`
  with an explicit unsupported message, so the cluster backup path fails closed
  instead of reporting invalid RocksDB files.
- Source audit found only create/fetch/terminate backup RPCs for this path.
  There is no restore RPC or restore entry point wired to TiKV BR today; standby
  snapshot sync hooks such as `RequestStorageSnapshotSync`/`OnSnapshotReceived`
  are separate and must not be treated as TiKV backup restore support.

Chosen first production path: **TiKV cluster-level backup/restore**.

- Use TiKV BR or an operator-approved TiKV cluster backup workflow as the first
  supported path. This is the smallest safe production path because TiKV can
  capture the physical keyspace at one consistent cut without adding a new
  cross-table Eloq export protocol.
- Scope must include every key under the EloqDoc `tikv_key_prefix`, including
  base/index table keys, `mvcc_archives`, tombstones retained for snapshot
  visibility, and any TiKV-stored Eloq metadata that belongs to that prefix.
- The backup manifest must record enough EloqDoc configuration to restore with
  the same logical namespace: `data_store=ELOQDSS_TIKV`, `tikv_key_prefix`, MVCC
  archive settings, the EloqDoc build/config version, and the catalog/config
  cut that matches the TiKV backup.
- Restore must target a fresh TiKV/EloqDoc deployment or an otherwise empty
  prefix. Reusing a prefix with leftover keys is unsupported.
- Restore must not combine base keys from one cut with archive keys, tombstones,
  or catalog/config from another cut.

Non-selected path: **Eloq logical backup/restore**.

- Logical export/import through DataStore APIs remains a valid future fallback
  if prefix-level TiKV BR is not acceptable in an environment.
- It is not the first path because it needs a new consistent-cut export/import
  protocol across base keys, `mvcc_archives`, tombstones, and catalog state, and
  would be slower than TiKV-native backup for production data.

Until the TiKV BR path is implemented and validated end-to-end, backup commands
for `ELOQDSS_TIKV` must continue to fail clearly instead of silently producing
invalid RocksDB-style artifacts. `CreateSnapshotForBackup` must not return
RocksDB backup files for TiKV.

##### TiKV BR Operator Runbook and Support Contract

Support contract for the first production path:

- The supported backup primitive is an operator-approved TiKV cluster backup or
  storage snapshot that preserves the whole EloqDoc TiKV logical namespace at
  one consistent cut. It is an external operator workflow, not the current
  `CreateSnapshotForBackup` RPC.
- A dedicated TiKV cluster for one EloqDoc deployment is the preferred support
  shape. In that mode, take and restore a full TiKV cluster backup/snapshot; do
  not attempt to filter by prefix until a prefix-scoped BR flow is validated.
- A shared TiKV cluster is only supported after the operator workflow proves it
  can select every key for the full `tikv_key_prefix` and every TiKV storage
  component/column family required by the transaction API used by
  `TikvKvClient`. A RawKV-only/default-CF-only backup is not acceptable for the
  current transaction-client backend.
- Operators must fence EloqDoc writes before choosing the backup cut. Until a
  first-class read-only/fence entry point is added, the conservative procedure is
  to stop application traffic and stop or quiesce all EloqDoc/TxService/DSS
  writers before running the TiKV backup.
- `FlushData` does not create a backup cut for TiKV; it only remains a no-op
  durability acknowledgement. TiKV BR/snapshot metadata and the EloqDoc manifest
  define the backup cut.
- `mvcc_archives` and retained tombstones are part of the protected data set. Do
  not derive the retention point from TiKV GC safepoint and do not run cleanup
  that can remove versions needed by the selected recovery point.
- Restore must target a fresh TiKV cluster or an otherwise empty
  `tikv_key_prefix`. In-place restore over an existing prefix is unsupported.

Operator runbook skeleton:

1. **Preflight**
   - Verify all EloqDoc components are built/configured with
     `WITH_DATA_STORE=ELOQDSS_TIKV` / `DATA_STORE_TYPE_ELOQDSS_TIKV`.
   - Record the running config: `data_store=ELOQDSS_TIKV`, `tikv_pd_endpoints`,
     `tikv_key_prefix`, MVCC/archive settings, cleanup retention settings,
     EloqDoc commit/config version, TiKV/PD versions, and BR/operator tool
     version.
   - Confirm backup storage credentials and network access from every TiKV node
     or operator component that will participate in the backup.
   - If the TiKV cluster is shared, stop here unless the exact prefix-scoped
     backup command has been validated for this transaction-client keyspace.

2. **Fence writes and choose a cut**
   - Put the deployment into maintenance mode and stop client writes.
   - Stop or quiesce all EloqDoc/TxService/DSS writers. If a future read-only
     mode is wired, verify every shard has entered it before continuing.
   - Wait for in-flight operations to drain and record the wall-clock time, TiKV
     timestamp/backup timestamp if the operator workflow exposes one, and the
     latest EloqDoc catalog/config cut used for the manifest.

3. **Run the TiKV backup**
   - For the preferred dedicated-cluster support shape, run the validated full
     TiKV cluster BR/operator backup command against the recorded PD endpoints
     and backup storage URI.
   - For future prefix-scoped support, the start boundary is the exact
     `tikv_key_prefix` and the end boundary is its lexicographic upper bound;
     the workflow must include base/index keys, `mvcc_archives`, retained
     tombstones, and any TiKV-stored Eloq metadata under that prefix.
   - Treat any partial node/region/CF failure as a failed backup. Do not publish
     a manifest for a partial backup.

4. **Write the backup manifest**
   - Store a manifest next to the TiKV backup containing at least:
     `backup_name`, backup storage URI, BR/operator tool/version, TiKV/PD
     cluster identity/version, PD endpoints used, backup timestamp/cut if
     available, `tikv_key_prefix`, prefix upper bound when prefix-scoped, EloqDoc
     binary commit/config version, `data_store=ELOQDSS_TIKV`, MVCC/archive
     config, cleanup retention config, catalog/config snapshot identifier, and
     whether the backup is full-cluster or prefix-scoped.
   - Mark the manifest usable only after the backup command completes
     successfully and the backup artifact inventory is durable.

5. **Unfence**
   - Resume EloqDoc/TxService/DSS writers only after the backup and manifest are
     durable, or keep the deployment stopped if this was a migration backup.

6. **Restore**
   - Provision a fresh TiKV cluster or clear an unused `tikv_key_prefix`; never
     restore into a prefix containing old EloqDoc keys.
   - Validate the manifest before restore: the target config must use the same
     `tikv_key_prefix`, compatible TiKV/EloqDoc versions, and matching
     `data_store=ELOQDSS_TIKV`/MVCC settings.
   - Run the corresponding TiKV BR/operator restore workflow. Prefix rewrite,
     catalog rewrite, and mixed-cut base/archive restores are unsupported.
   - Start EloqDoc only after restore succeeds and the manifest check passes.

7. **Post-restore validation**
   - Run CRUD and restart smoke.
   - Verify base/index reads, reverse scan over `mvcc_archives`, snapshot
     read/scan at the restored cut, and tombstone-retention behavior.
   - Confirm cleanup workers still obey the restored safe archive watermark and
     do not use TiKV GC safepoint as an Eloq archive cleanup watermark.

Backup manifest schema, version 1:

```yaml
schema_version: 1
backup_name: operator-supplied-unique-name
created_at_utc: RFC3339-timestamp
backup_mode: full_cluster | prefix_scoped
backup_artifact:
  storage_uri: BR-or-operator-backup-location
  tool: br | tikv-operator | storage-snapshot | other-approved-tool
  tool_version: version-string
  command_digest: hash-or-archived-command-spec-reference
  artifact_inventory_digest: hash-of-files-or-objects-recorded-after-success
consistency_cut:
  write_fence_method: stopped-writers | read-only-mode | storage-snapshot-fence
  fence_started_at_utc: RFC3339-timestamp
  writers_drained_at_utc: RFC3339-timestamp
  backup_timestamp: TiKV-or-BR-timestamp-if-exposed-otherwise-null
  catalog_config_cut_id: catalog-config-snapshot-id-or-digest
tikv_source:
  pd_endpoints: [host:port, ...]
  cluster_id: TiKV-or-PD-cluster-id-if-available
  cluster_version: TiKV-version-set
  api_mode: txn
  keyspace_scope:
    tikv_key_prefix: exact-prefix-from-EloqDoc-config
    prefix_upper_bound: lexicographic-upper-bound-or-null-for-full-keyspace
    scope_mode: full_cluster | validated_prefix_range
  includes_transaction_storage: true
eloqdoc_source:
  data_store: ELOQDSS_TIKV
  binary_version: EloqDoc-build-or-version
  git_commit: EloqDoc-commit
  config_digest: combined-EloqDoc-TxService-DSS-config-digest
  enable_mvcc: true | false
  mvcc_archive_config_digest: archive-related-config-digest
  cleanup_retention_config_digest: cleanup-related-config-digest
  catalog_config_cut_id: same-value-as-consistency-cut-catalog-config-cut-id
protected_logical_sets:
  base_and_index_keys: true
  mvcc_archives: true
  retained_tombstones: true
  ti_kv_stored_eloq_metadata: true
validation:
  backup_completed: true
  manifest_completed_after_artifacts_durable: true
  rawkv_only_backup: false
  uses_tikv_gc_safepoint_as_archive_watermark: false
```

Restore pre-check rules:

- Reject any manifest with an unsupported `schema_version`, missing required
  fields, `backup_completed != true`, or no durable artifact inventory digest.
- Reject any manifest whose `eloqdoc_source.data_store` is not
  `ELOQDSS_TIKV` or whose target deployment is not configured for
  `ELOQDSS_TIKV`.
- Recompute the target `tikv_key_prefix` upper bound and require exact equality
  with the manifest. Empty prefix means full keyspace and is supported only for
  the dedicated full-cluster restore shape.
- Require a fresh TiKV cluster for `backup_mode=full_cluster`. For
  `backup_mode=prefix_scoped`, require an empty target prefix and a manifest
  that states `scope_mode=validated_prefix_range`; otherwise reject.
- Reject prefix rewrite, catalog rewrite, or restore into a different
  `tikv_key_prefix`. A future rewrite feature must be a separate design.
- Require target TiKV/EloqDoc versions and `api_mode` to be compatible with the
  manifest. The current backend requires transaction API storage; reject
  `rawkv_only_backup=true` or `includes_transaction_storage != true`.
- Require `consistency_cut.catalog_config_cut_id` to equal
  `eloqdoc_source.catalog_config_cut_id`; reject if the operator cannot restore
  the matching catalog/config cut with the TiKV artifact.
- Require MVCC/archive and cleanup-retention config digests to match the target
  or require the target to start with cleanup disabled until a human confirms
  equivalent conservative settings.
- Reject manifests that include TiKV GC safepoint as an Eloq archive cleanup
  watermark or as the source of snapshot visibility.
- Reject restore if any preflight smoke detects existing keys in the target
  prefix, unless the restore target is a newly provisioned cluster known to be
  empty.
- After restore, keep the deployment fenced until CRUD, archive reverse scan,
  snapshot read/scan, tombstone-retention, and restart smoke all pass.

##### Restore Validation Smoke Harness Design

The restore validation smoke harness is the mandatory post-restore gate before
any TiKV BR/operator backup is declared usable by EloqDoc. It validates that the
restored prefix is internally consistent; it is not a backup implementation and
must not make the current `CreateSnapshotForBackup` path appear supported.

Harness phases:

1. **Seed the source deployment before backup**
   - Write base table rows across at least two tables and partitions, including
     records that look like primary/base rows and index/secondary-table rows
     from the DataStore keyspace point of view.
   - Include normal CRUD state: live rows, deleted rows, range boundaries, and a
     dropped-table or deleted-range sentinel that proves restore does not revive
     data outside the selected cut.
   - Include scan data for forward scan, reverse scan, pagination cursor resume,
     and `type` filtering where non-matching rows appear between matching rows.
   - Include `mvcc_archives` rows for the same logical keys at multiple Eloq
     `commit_ts` values, including one archived delete/tombstone version.
   - Include base-current plus archive-older rows so snapshot read and snapshot
     scan at chosen read timestamps must read both base and archive versions.
   - Include retained tombstones, live TTL rows, and expired TTL rows. Cleanup
     must be disabled or run only through the conservative single-round helpers
     during validation; restore validation must not depend on TiKV GC safepoint.
   - Include a transaction/atomicity sentinel equivalent to the existing 2PC
     smoke: a failed multi-key write must leave no partial visible row, and the
     committed conflict winner must remain visible.

2. **Capture expected results before backup**
   - Record latest `Read` results, including not-found cases and record
     `ts`/`ttl` metadata when relevant.
   - Record forward and reverse `ScanNext` result pages, pagination cursors, and
     `type` filter output.
   - Record archive reverse-scan visible-version results for selected logical
     keys and read timestamps.
   - Record TxService-style snapshot read and snapshot scan results at the
     selected read timestamps.
   - Record tombstone-retention and cleanup-guard expectations: unknown safe
     archive watermark is no-op, candidates older than a known watermark are
     bounded, malformed/truncated rows are skipped conservatively, and
     `mvcc_archives` are never processed by base TTL cleanup.
   - Restart the source store once when practical and re-read a small baseline,
     so the harness has an expected persistence baseline independent from the
     restore target.

3. **Fence, backup, and restore**
   - Follow the TiKV BR operator runbook above: fence writers, capture the
     consistent cut, produce the manifest, and keep restore fenced until the
     harness passes.
   - Restore only to a fresh TiKV cluster or an empty target
     `tikv_key_prefix` that exactly matches the manifest. Prefix rewrite,
     catalog rewrite, mixed cuts, and in-place restore are outside this harness
     and must fail before validation starts.

4. **Validate the restored deployment**
   - Run the restore pre-check rules first and verify the target prefix was
     empty before restore.
   - Re-run latest CRUD reads, not-found reads, forward/reverse scans,
     pagination, and `type` filtering; compare with the captured expectations.
   - Re-run archive reverse scan, snapshot read, and snapshot scan; compare
     visible versions and Eloq `commit_ts` values with the captured
     expectations.
   - Re-run tombstone/TTL cleanup safety checks in validation mode: unknown
     watermark remains no-op, known-watermark candidates stay bounded and
     prefix-scoped, and no check uses TiKV GC safepoint as an Eloq archive
     watermark.
   - Restart the restored EloqDoc/TiKV-facing components and repeat a minimal
     read/scan/snapshot baseline before unfencing.
   - Verify intentionally bad manifests are rejected fail-closed: wrong
     `data_store`, mismatched `tikv_key_prefix`, incompatible `api_mode`, missing
     artifact digest, mismatched catalog/config cut, incompatible MVCC/archive or
     cleanup config, non-empty target prefix, RawKV-only backup, and
     `uses_tikv_gc_safepoint_as_archive_watermark=true`.

Test-case matrix and failure classification:

- **CRUD and base/index keyspace**
  - Seed: one base table and one index-like secondary table, each with live
    rows, overwritten rows, explicit deletes, and bounded/open-ended range
    delete sentinels across at least two partitions.
  - Restore assertions: latest `Read` and not-found results match the captured
    `ts`/`ttl`/payload expectations; deleted-range and dropped-table sentinels
    stay absent; other partitions remain present.
  - Failure class: manifest/config problem if the restored prefix or catalog cut
    differs from the manifest; restore artifact problem if keys are missing,
    extra, or from a mixed cut; EloqDoc backend bug if raw keys are present but
    `Read`/delete/drop semantics decode incorrectly.

- **Forward/reverse scan, pagination, and `type` filtering**
  - Seed: ordered keys with interleaved record types and enough rows to force at
    least two pages in both scan directions.
  - Restore assertions: page order, cursor resume, range boundaries, and type
    filter results match the pre-backup expectation, including continuing the
    scan across non-matching rows.
  - Failure class: restore artifact problem if restored key ordering or row set
    differs; EloqDoc backend bug if the row set is intact but cursor, reverse
    boundary, or type-filter semantics differ.

- **Archive reverse scan**
  - Seed: `mvcc_archives` rows for the same logical key at multiple Eloq
    `commit_ts` values, plus one archived tombstone/delete version.
  - Restore assertions: reverse scan from selected read timestamps returns the
    same visible archive version and the same big-endian Eloq `commit_ts`
    suffix; deleted archive versions are interpreted as deletes.
  - Failure class: restore artifact problem if archive rows are missing or mixed
    with another backup cut; EloqDoc backend bug if archive key decoding or
    reverse-scan visibility is wrong.

- **Snapshot read and snapshot scan**
  - Seed: current base rows whose older versions exist only in `mvcc_archives`,
    unchanged rows that should remain visible from base, and new rows that must
    be hidden at an older snapshot timestamp.
  - Restore assertions: snapshot read and snapshot scan at each captured read
    timestamp return the same visible payloads and Eloq `commit_ts` values before
    and after restore.
  - Failure class: restore artifact problem if base and archive rows come from
    different cuts; EloqDoc backend bug if intact rows produce wrong snapshot
    visibility or commit timestamp ordering.

- **Tombstone retention**
  - Seed: retained canonical tombstones below, equal to, and above a known safe
    archive watermark, plus live rows in the same partition.
  - Restore assertions: tombstones required for snapshot/delete visibility remain
    present until a valid cleanup watermark says they are candidates; equality
    with the watermark is retained conservatively.
  - Failure class: restore artifact problem if tombstones disappear during
    restore; cleanup safety bug if validation cleanup deletes tombstones without
    a safe watermark or deletes versions at/after the watermark.

- **Cleanup safety after restore**
  - Seed: expired base TTL rows, live TTL rows, malformed/truncated values,
    `mvcc_archives`, and retained tombstones.
  - Restore assertions: unknown watermark cleanup is no-op; expired-base cleanup
    skips `mvcc_archives`, tombstones, and malformed rows conservatively;
    archive/tombstone cleanup only reports/deletes bounded candidates older than
    a known safe watermark and never uses TiKV GC safepoint.
  - Failure class: cleanup safety bug for any unsafe delete, unbounded cursor,
    cross-prefix scan, or TiKV-GC-derived watermark; EloqDoc backend bug if
    candidate decoding counters are wrong without deleting data.

- **Restart persistence**
  - Seed: a small baseline from the CRUD, scan, and snapshot cases after source
    `FlushData` and before backup.
  - Restore assertions: after restoring and restarting EloqDoc/TiKV-facing
    components, the same baseline reads/scans/snapshot reads still match.
  - Failure class: restore artifact problem if restart exposes missing or stale
    TiKV data; EloqDoc backend bug if data remains present but adapter startup,
    prefix handling, or scan/read state is inconsistent after restart.

- **Negative manifest/preflight cases**
  - Seed: reuse the valid backup artifact, then mutate one manifest or target
    preflight condition at a time: wrong `data_store`, mismatched
    `tikv_key_prefix`, incompatible `api_mode`, missing artifact digest,
    mismatched catalog/config cut, incompatible MVCC/archive or cleanup config,
    non-empty target prefix, RawKV-only backup, and
    `uses_tikv_gc_safepoint_as_archive_watermark=true`.
  - Restore assertions: each case fails closed before restore or before
    unfencing, with an actionable error and no partial target-prefix mutation.
  - Failure class: manifest/config problem if the checker accepts an invalid
    manifest or target config; restore artifact problem if a rejected restore has
    already mutated the target prefix.

Exit criteria:

- All restored results match the pre-backup expectations and the restart
  baseline.
- No evidence exists of a mixed cut between base/index keys, `mvcc_archives`,
  tombstones, and catalog/config state.
- Failed negative cases fail before restore or before unfencing.
- `CreateSnapshotForBackup` remains unsupported for TiKV until a later task
  wires a user-visible BR/operator entry point or keeps that RPC fail-closed
  with actionable operator guidance.

Non-goals for the harness design:

- Do not implement the restore worker, BR command wrapper, or user-visible
  restore RPC in this step.
- Do not connect background cleanup workers to the restore path.
- Do not implement prefix rewrite or catalog rewrite.
- Do not use TiKV GC safepoint as an Eloq snapshot/archive cleanup watermark.

Explicitly unsupported until separate validation exists:

- Calling Eloq `CreateClusterBackup`/`CreateSnapshotForBackup` and expecting a
  TiKV backup artifact.
- In-place restore over an existing prefix or restoring into a different
  `tikv_key_prefix`.
- Mixing base/index keys from one backup with `mvcc_archives`, tombstones, or
  catalog/config from another backup.
- Prefix-scoped backup on a shared TiKV cluster without proof that the workflow
  includes all transaction-client storage needed by `TikvKvClient`.
- Any procedure that uses TiKV GC safepoint as the Eloq snapshot/archive
  retention watermark.

Reference operator material to validate future concrete commands:

- TiDB/TiKV BR overview: <https://docs.pingcap.com/tidb/stable/backup-and-restore-overview/>
- TiKV RawKV BR reference, useful as a contrast for why RawKV-only/default-CF
  backup is not automatically sufficient for this transaction-client backend:
  <https://docs.pingcap.com/tidb/stable/rawkv-backup-and-restore/>

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

#### Task 7E: TiKV Backup/Restore Implementation Split

Decision: implement the TiKV cluster-level backup/restore path first. Keep
`CreateSnapshotForBackup` fail-closed until the selected path is implemented and
validated end-to-end.

Follow-up implementation tasks:

1. **Operator runbook and support contract**
   - Initial runbook/support contract is documented in the Backup and Restore
     section above.
   - Future work should turn that contract into an operator-facing deployment
     page and validate the exact BR/operator command for the chosen support
     shape.

2. **Backup manifest capture and validation**
   - Initial manifest schema and restore pre-check rules are documented in the
     Backup and Restore section above.
   - Future work should implement manifest generation, digest capture, and a
     fail-closed restore preflight checker before wiring any user-visible
     restore entry point.

3. **Restore validation smoke harness**
   - Initial harness design and use-case list are documented in the Backup and
     Restore section above.
   - Future work should implement it as an automated gate that seeds source
     data, captures expectations, restores to a fresh same-prefix target, and
     validates the documented test-case matrix: CRUD/base-index, scans, archive
     reverse scan, snapshot read/scan, tombstone retention, cleanup safety,
     restart persistence, and fail-closed manifest mismatch cases before any
     backup is declared usable.

4. **User-visible backup entry point**
   - Keep current `CreateSnapshotForBackup` unsupported behavior until tasks 1-3
     are complete.
   - After validation exists, either wire a clear TiKV external-BR orchestration
     entry point or keep `CreateSnapshotForBackup` unsupported with an actionable
     message pointing operators to the TiKV BR runbook. Do not pretend to produce
     RocksDB-style files.

5. **Optional later logical-backup track**
   - If a backend-neutral path is still needed, add a separate design for a
     consistent logical export/import protocol across base keys, `mvcc_archives`,
     tombstones, and catalog/config state.

Acceptance criteria for the selected path:

- Backup captures base, archive, tombstone, and catalog/config state from one
  consistent logical cut under the configured `tikv_key_prefix`.
- Restore rejects mismatched prefix/config manifests and never mixes cuts.
- Restore is validated by CRUD, archive reverse scan, snapshot read/scan,
  tombstone-retention coverage, and restart smoke.
- Unsupported or not-yet-integrated modes continue to fail clearly.

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
- TiKV backup remains fail-closed until the BR-first path is implemented and
  validated end-to-end.
- BR operator runbook documents write fencing, manifest requirements, fresh
  restore targets, and unsupported restore modes.
- Backup manifest schema and restore pre-check rules are fail-closed and reject
  prefix/config/cut mismatches.
- Restore validation smoke harness covers CRUD, forward/reverse scan, archive
  reverse scan, snapshot read/scan, tombstone retention, cleanup safety,
  restart, and fail-closed manifest mismatch cases; each case documents seed
  data, restore assertions, and failure classification before backup artifacts
  are declared usable.
- RocksDB backend behavior remains unchanged.
