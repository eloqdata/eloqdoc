# Multi-node recovery tests

Tests that need several node groups, each node a separate EloqDoc process on one host.

- `cluster.py`: starts an N-node cluster (one node group per node, one log group replicated on
  every node), kills (`kill -9`) and restarts single nodes or the whole cluster. Node 0 first runs
  alone, which creates its data store shard with the system collections, and every other node
  starts from a copy of it. Options: `metrics` (each node exports metrics on its tx port + 8),
  `log_replicas` (1 makes a log group of each node), `ng_replicas` (more than 1 adds the next
  nodes to each node group as voters) and `cloud` (EloqStore's cloud mode, below). `stop()` shuts
  a node down cleanly with the `shutdown` command; `kill()` is `kill -9` of its process group.
- `smoke.py`: three nodes serve one collection, and a node killed and restarted recovers it.
- `ddl_split_recovery.py`: a schema op and a range split of the same table recovered together (data
  substrate `docs/range-split-schema-op-recovery.md`), the lock a participant restores after an
  add index's last prepare data log, with and without a split of its own, and controls for the
  split itself.
- `cluster_gate_multinode.py`: the cluster gate across node groups (`docs/cdc-change-stream.md`
  §6.2, §10 item 11; plan D9): pauses refused and admitted everywhere, the gauge on every node,
  log group and node failures, gate ops killed after their log or in their install with a second
  node group failing over, a range split under the gate, a DDL and an index build in flight
  finishing under a waiting pause, recovered or not, and the gauge's reset when a node stops
  leading its node group.
- `cdc_step_down.py`: change streams across a log group leader's step-down (§10 item 8; plan A5):
  a braft transfer, the loss of a majority, the leader killed, cancellations racing the step-down,
  and every replica's arrival gauge.
- `dss_starting.py`: a request meeting a data store shard while it opens waits and is then served
  (data substrate `DataStoreService::WaitWhileStarting`): node 1 restarts with its shard held in
  `Starting` (the `dss_hold_starting` fault point, armed from the start through
  `ELOQ_DSS_FAULT_INJECT`) while a listing through node 0 reads the catalog from it.
- `replay_rerequest.py`: a node asks for its node group's log replay once, at its leader start, and
  again only for a log group that goes silent (data substrate `Sharder::RequestLogReplay`,
  `RecoveryService`'s watchdog): node 1 restarts with its first replay stream dropped (the
  `replay_drop_stream_once` fault point, armed from the start through `ELOQ_TX_FAULT_INJECT`), and
  the log group replaces the idle shipping agent when node 1 asks again.

The server must be built with fault points, with either data store service: `smoke.py` passes with
the RocksDB and the EloqStore services alike (`-DWITH_DATA_STORE=ELOQDSS_ROCKSDB` or
`ELOQDSS_ELOQSTORE`). In a multi-node cluster no build creates a
node group's data store shard: the node opens it, when it first leads its node group, with
create_if_missing off (`DataStoreService::OpenDataStore`), and only a node alone in its cluster
creates one, shard 0. Hence the seed run of node 0 and the copies of its shard.

```sh
cmake -S . -B build-mn -G Ninja -DCMAKE_BUILD_TYPE=Debug -DWITH_DATA_STORE=ELOQDSS_ELOQSTORE \
    -DWITH_LOG_STATE=ROCKSDB -DWITH_FAULT_INJECT=ON -DELOQDOC_BUILD_TESTS=OFF \
    -DELOQ_THIRD_PARTY_PREFIX=<prefix> -DCMAKE_PREFIX_PATH=<prefix>
ninja -C build-mn eloqdoc host_manager
python3 cmake/tests/multinode/ddl_split_recovery.py --server build-mn/eloqdoc --root /tmp/mn
```

Each `ddl_split_recovery.py` scenario but (v) loads about 320 MB, past the 256 MB range size, so
that a checkpoint splits the range. Three nodes pinned to 8 CPUs each fit a 32-core host;
`--port-offset`, `--first-cpu` and `--cpus-per-node` run two clusters side by side.

With `--cloud <file>` (every test script takes it) the nodes run EloqStore's cloud mode: every
shard lives in one S3 bucket, under a prefix of the cluster's own, and each node's
`eloqstore_data` is a cache of it, so any node can lead any node group. Node 0's bootstrap writes
shard 0 to the bucket and the others start from it; nothing is copied. The file holds
`endpoint=`, `bucket=`, `access_key=` and `secret_key=` lines; keep it private. On the test VM an
S3 proxy (s3proxy) fronts an Azure storage account, and a relay in front of the proxy rewrites
one header: EloqStore uploads through presigned URLs that sign only `Host`, with curl's default
`Content-Type: application/x-www-form-urlencoded`, and the proxy's web server parses such a body
into request parameters, counts them in the signature and refuses the upload
(SignatureDoesNotMatch; EloqStore fails to start with `CloudErr`). Sending
`application/octet-stream` from EloqStore would remove the need for the relay.

Things the tests allow for:

- A failover is modelled by a crash: `kill()` is `kill -9` of the node's process group. `stop()`
  is for a planned restart and shuts the node down with the `shutdown` command: SIGTERM is no clean
  shutdown here (threads created before mongod masks its signals, brpc's `bvar_sampler`, leave it
  deliverable to a thread with no handler, so the process dies at once). The cluster stops all its
  nodes together: a node shut down after the others have gone waits on its log group, which has
  lost its majority.
- A restarted node serves before its node group has finished failing over, and requests through
  it, or to its data store shard, can fail meanwhile (seen: a create answered "Requested table
  schema mismatch"; a listing through another node failed with InternalError while brpc still
  held the restarted node's data store service down). `Cluster.restart` waits until the node has
  replayed its node group from every log group ("set recovering status to finished") and every
  node can list the databases.
- A node group's voters lead it while its candidate is down, and hand it back once the candidate
  is up; this needs shared storage (cloud mode), since with local shards a voter that leads opens
  its own stale copy. EloqStore keeps one instance per process by design (any tx service shard
  reads through the node's local instance; the object store is shared), but the data store service
  opens an instance per shard it leads, so a node that leads a second node group runs a second
  instance, and the nodes crashed. So `ng_replicas` above 1 is safe only until a node group's
  candidate fails.
- A node group's replay may be shipped twice in one term: its host manager asks the log groups for
  it at leader start, and its tx node's replay service asks again when no replay connection has
  come within its check interval; when the log group elects its leader slower than that, both
  requests wait and are both served, each with its own shipping agent. Replay is idempotent; the
  gate checks allow for a gate item replayed twice.
- The log group's preferred leader, its first member, may ask for the lead back 5 s after it starts
  following. The log service's own fault point that turns this off is not the tx service's, so
  `eloqFaultInject` cannot arm it; the step-down tests check against whichever node leads.
