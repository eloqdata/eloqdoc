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
