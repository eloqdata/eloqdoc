# Configuration Parameters and Descriptions

## Core Parameters

### systemLog

#### systemLog.verbosity

Type: Number
Required: True
Accepted Values: 0 ~ 5.
Desc: Log level. Higher numbers indicate more detailed logging.

#### systemLog.destination

Type: Enum
Required: True
Accepted Values: file
Desc: Specify EloqDoc write log to a file.

#### systemLog.path

Type: String
Required: True
Accepted Values: Absolute file path.
Desc: Specify where should EloqDoc write log to.

#### systemLog.logAppend

Type: Boolean
Required: False
Accepted Value: true|false
Default: false
Desc: Append to the end of the existing log file or create a new log file every time EloqDoc process starts.

### net

#### net.bindIpAll

Type: Boolean
Required: False
Accepted Values: true|false
Default: false
Desc: Listen on all addresses or listen on [net.bindIp](#netbindip).

#### net.bindIp

Type: Comma seperated IP addresses
Required: False
Accepted Values: All available addresses.
Default: 127.0.0.1
Desc: Listen on specified addresses.

#### net.port

Type: Number
Required: False
Accepted Values: All available ports.
Default: 27017
Desc: Port to listen on.

#### net.serviceExecutor

Type: Enum
Required: True
Accepted Values: adaptive
Desc: Non-blocking and full asynchronous executor.

### storage

#### storage.dbPath

Type: String
Required: True
Accepted Values: Absolute directory path.
Desc: The default storage path. When deploying on cloud vm, it should be placed on a EBS disk for reliability.

#### storage.engine

Type: Enum
Required: True
Accepted Values: eloq
Desc: Specify eloq as storage engine.

#### storage.eloq.adaptiveThreadNum

Type: Number
Required: True
Accepted Values: Core count.
Desc: Network IO threads for handle client input requests. Its pressure is not very high, 2 is enough in most cases.

#### storage.eloq.reservedThreadNum

Type: Number
Required: True
Accepted Values: Core count.
Desc: Executor threads. The more executor threads, the better performance.

#### storage.eloq.txService.localIP

Type: IPv4:Port
Required: False
Accepted Values: Listenable endpoint.
Default: 127.0.0.1:8000
Desc: Used to contact between internal components and cooperative nodes. The specified endpoint, and endpoints with port up to `Port+10` are reserved for internal usage.

#### storage.eloq.txService.ipList

Type: Comma seperated IPv4:Port list.
Required:  Required for EloqDoc-Cluster.
Accepted Values: List of [localIP](#storageeloqtxservicelocalip) of all node.
Default: Value specified by localIP.
Desc: Used to contact between cooperative nodes.

#### storage.eloq.txService.checkpointerIntervalSec:

Type: Number
Required: False
Accepted Values: 10 - 86400
Default: 10
Desc: Checkpoint interval.

#### storage.eloq.txService.nodeMemoryLimitMB

Type: Number
Required: False
Accepted Values: 512 - 1000000
Default: 8000
Desc: Memory limit. Set it to no more than 80% memory of your hardware.

#### storage.eloq.txService.enableIOuring

Type: Boolean
Required: False
Accepted Value: true|false
Default: false
Desc: Enable io_uring. Available on high version kernel.

#### storage.eloq.txService.txlogAsyncFsync

Type: Boolean
Required: False
Accepted Value: true|false
Default: false
Desc: Enable async fsync with io_uring. Available when [enableIOuring](#storageeloqtxserviceenableiouring) is enabled.

## Change Stream Parameters

Server parameters of EloqDoc's change streams (`$changeStream`, see `docs/cdc-change-stream.md`),
set at startup with `--setParameter` or `setParameter:` in the configuration file, or at runtime
with the `setParameter` command.

#### cdc_open_wait_ms

Type: Number
Required: False
Accepted Values: a positive number of milliseconds
Default: 30000
Desc: How long a change stream opened without a resume token may wait, when its `aggregate` has no
`maxTimeMS`, for every log group to complete a checkpoint round. Until then no start position can
be fixed, which happens at cluster start-up; past the wait the open fails with
`ChangeStreamStartNotReady` (5000) and the client retries. Keep it at two or more checkpoint
rounds: `checkpointer_interval` in the data substrate configuration, default 10 s, never below
5 s. If an open fails without having waited longer than one round, the error says so and names
the value to raise.

#### cdc_scan_max_bytes

Type: Number
Required: False
Accepted Values: 1 - 33554432 (32 MB, the log service's own cap)
Default: 4194304 (4 MB)
Desc: The page size, in bytes, a change stream asks the log service for in each scan. The log
service stops filling a page before the record that would take it past this size; a page's first
record always goes in. Each stream also reserves this much of its read buffer
([cdc_cursor_buffer_bytes](#cdc_cursor_buffer_bytes)) per scan in flight. A `getMore` returns once it has an event, so one reply holds at most about one page per
log group: a larger page means fewer scans and larger replies, at the cost of memory per stream.
Read when a stream opens; a change applies to streams opened afterwards.

#### cdc_cursor_buffer_bytes

Type: Number
Required: False
Accepted Values: 1 - 4294967296 (4 GB)
Default: 67108864 (64 MB)
Desc: Each change stream's read buffer, in bytes: the records it has received and not yet returned,
plus `cdc_scan_max_bytes` reserved for each scan in flight. A new scan is issued only while that
count is below this value, except for the log group whose position holds the others back, which is
always read. A stream can therefore hold up to this value plus one page per log group. Read when a
stream opens; a change applies to streams opened afterwards.

#### cdc_dedup_set_bytes

Type: Number
Required: False
Accepted Values: 0 - 1073741824 (1 GB)
Default: 4194304 (4 MB)
Desc: The bytes of (transaction, namespace, key) entries each log group's dedup set holds at one
commit timestamp. The set suppresses the repeat of a change the log service stores twice; past
this size further entries go unrecorded, so such a repeat may be returned twice, as two identical
events at the same timestamp, which a consumer applies idempotently. 0 records nothing. How often
that happened is `dedupSetOverflows` in `serverStatus`'s `eloqChangeStreams` section; if it grows,
the workload's transactions are larger than the set. Read when a stream opens; a change applies to
streams opened afterwards.

#### cdc_scan_max_wait_ms

Type: Number
Required: False
Accepted Values: 1 - 600000 (10 minutes)
Default: 30000
Desc: The longest a change stream asks the log service to hold one scan open while nothing new has
arrived (the long poll). A `getMore`'s own remaining time bounds the wait too, and the log service
caps it with its own `cdc_scan_max_park_ms` (default 60 s). Shorter waits mean more scans on an idle
stream. Read when a stream opens; a change applies to streams opened afterwards.

#### History retention for change streams

`txlog_cdc_retention_seconds`, in the `[local]` section of the data substrate configuration (or the
`--txlog_cdc_retention_seconds` flag), is the setting that keeps change-stream history: no purge
of the log service removes a record from the last that many seconds before the checkpoint.

Type: Number
Required: False
Accepted Values: seconds, 0 or more
Default: 0 (no window: every checkpointed record may be purged)
Desc: Set it above the longest a change-stream consumer may stay down or behind. In this version a
stream whose position is older than the retained history is not told; it resumes at the oldest
record still kept and misses the changes in between (`docs/cdc-change-stream.md` §8), so retention
is the only protection. It applies to both log states. On the RocksDB log state it costs disk on
top of `txlog_rocksdb_sst_files_size_limit` (default 500 MB): roughly the log write rate times the
window. `txlog_rocksdb_cloud_log_retention_days` (RocksDB-Cloud) bounds only the cloud archive of
purged files, which change streams do not read. Watch the consumer's lag against it
(`docs/cdc-operations.md` §4).

## EloqDoc-RocksDBCloud Parameters

### storage.eloq.txService

#### storage.eloq.txService.txlogRocksDBStoragePath

Type: String
Required: False
Default: ""
Desc: The local filesystem path for RocksDB-Cloud to store wal. It is suggested to place the directory on a local NVME. If unset or empty, it will be placed under [dbPath](#storagedbpath).

#### storage.eloq.txService.txlogRocksDBCloudEndpointUrl

Type: String
Required: True
Desc: AWS S3 endpoint URL.

#### storage.eloq.txService.txlogRocksDBCloudRegion

Type: String
Required: False
Default: ""
Desc: AWS S3 region.

#### storage.eloq.txService.txlogRocksDBCloudBucketName

Type: String
Required: True
Desc: Postfix of bucket name.

#### storage.eloq.txService.txlogRocksDBCloudBucketPrefix

Type: String
Required: True
Desc: Prefix of bucket name.

#### storage.eloq.txService.txlogRocksDBCloudObjectPath

Type: String
Required: True
Desc: Object path in the bucket for wal.

#### storage.eloq.txService.txlogRocksDBCloudSstFileCacheSize

Type: String
Required: True
Accepted Values: nMB, nGB, nTB
Desc: Max disk space can be used for wal file.

### storage.eloq.storage

#### storage.eloq.storage.rocksdbCloudStoragePath

Type: String
Required: True
Desc: The local filesystem path for RocksDB-Cloud to store data. It is suggested to place the directory on a local NVME. If unset or empty, it will be placed under [dbPath](#storagedbpath).

#### storage.eloq.storage.rocksdbCloudEndpointUrl

Type: String
Required: False
Desc: AWS S3 endpoint URL.

#### storage.eloq.storage.awsAccessKeyId

Type: String
Required: False
Desc: AWS S3 accessKeyId.

#### storage.eloq.storage.awsSecretKey

Type: String
Required: False
Desc: AWS S3 secretKey.

#### storage.eloq.storage.rocksdbCloudRegion

Type: String
Required: True
Desc: AWS S3 region.

#### storage.eloq.storage.rocksdbCloudBucketName

Type: String
Required: True
Desc: Postfix of bucket name.

#### storage.eloq.storage.rocksdbCloudBucketPrefix

Type: String
Required: True
Desc: Prefix of bucket name.

#### storage.eloq.storage.rocksdbCloudObjectPath

Type: String
Required: True
Desc: Object path in the bucket for data.

## EloqDoc-RocksDBCloud-Cluster Parameters

### storage.eloq.txService

#### storage.eloq.txService.forkHostManager

Type: Boolean
Requried: False
Accepted Value: true|false
Default: false
Desc: Enable raft host manager.

#### storage.eloq.txService.nodeGroupReplicaNum

Type: Number
Required: False
Accepted Values: 1 ~ 9.
Default: 3
Desc: Replication number of node group.

#### storage.eloq.txService.txlogGroupReplicaNum

Type: Number
Required: False
Accepted Values: 1 ~ 9
Default: 3
Desc: Replicate number of tx log group.
