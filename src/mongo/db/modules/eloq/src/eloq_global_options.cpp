/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the license:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <cstdint>
#include <limits>
#include <string>

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/db/modules/eloq/src/eloq_global_options.h"
#include "mongo/util/log.h"
#include "mongo/util/options_parser/constraints.h"
#include "mongo/util/options_parser/option_description.h"

namespace mongo {
EloqGlobalOptions eloqGlobalOptions;

Status EloqGlobalOptions::add(moe::OptionSection* options) {
    moe::OptionSection eloqOptions("Eloq options");

    // Eloq TxService Options
    eloqOptions
        .addOptionChaining("storage.eloq.txService.localIP",
                           "eloqLocalIP",
                           moe::String,
                           "IP address of the local node")
        .setDefault(moe::Value("127.0.0.1:8000"));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.ipList",
                           "eloqIPList",
                           moe::String,
                           "IP addresses of the nodes in the cluster")
        .setDefault(moe::Value(""));
    eloqOptions.addOptionChaining("storage.eloq.txService.txlogServiceList",
                                  "eloqTxlogServiceList",
                                  moe::StringVector,
                                  "IP address of the tx log service node");
    eloqOptions
        .addOptionChaining("storage.eloq.txService.forkHostManager",
                           "eloqForkHostManager",
                           moe::Bool,
                           "Enable forking host manager process.")
        .setDefault(moe::Value(false));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.hmIP",
                           "eloqHMIP",
                           moe::String,
                           "IP addresses of the host manager")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining(
            "storage.eloq.txService.hmPort", "eloqHMPort", moe::Int, "Port of the host manager")
        .validRange(1, std::numeric_limits<uint16_t>::max());
    eloqOptions
        .addOptionChaining("storage.eloq.txService.hmBinPath",
                           "eloqHMBinPath",
                           moe::String,
                           "Path to host manager binary path.")
        .setDefault(moe::Value(""));

    eloqOptions
        .addOptionChaining("storage.eloq.txService.rangeSplitWorkerNum",
                           "eloqRangeSplitWorkerNum",
                           moe::Int,
                           "Number of range split worker")
        .validRange(1, 1024)
        .setDefault(moe::Value(1));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.nodeMemoryLimitMB",
                           "eloqNodeMemoryLimitMB",
                           moe::Int,
                           "memory limit per node (MB)")
        .validRange(1, 1000000)
        .setDefault(moe::Value(8000));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.checkpointerIntervalSec",
                           "eloqCheckpointerIntervalSec",
                           moe::Int,
                           "Interval of checkpointer(s)")
        .validRange(1, 86400)
        .setDefault(moe::Value(1000));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.checkpointerDelaySec",
                           "eloqCheckpointerDelaySec",
                           moe::Int,
                           "The time(second) which ckpt_ts is less than min lock ts")
        .validRange(0, 86400)
        .setDefault(moe::Value(5));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.collectActiveTxTsIntervalSec",
                           "eloqCollectActiveTxTsIntervalSec",
                           moe::Int,
                           "Interval of collect active tx start timestamp(s)")
        .validRange(1, 86400)
        .setDefault(moe::Value(2));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.deadlockIntervalSec",
                           "eloqDeadlockIntervalSec",
                           moe::Int,
                           "Interval of dead lock check(s)")
        .validRange(1, 30)
        .setDefault(moe::Value(3));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.txlogGroupReplicaNum",
                           "eloqTxlogGroupReplicaNum",
                           moe::Int,
                           "Replicate number of tx log group")
        .validRange(1, 10)
        .setDefault(moe::Value(3));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.useKeyCache",
                           "eloqUseKeyCache",
                           moe::Bool,
                           "Use key cache in primary key to avoid kv read if key does not exists.")
        .setDefault(moe::Value(false));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.enableMVCC",
                           "eloqEnableMVCC",
                           moe::Bool,
                           "When enabled, use muliti-versions. Repeatable Read isolation level "
                           "will be converted to "
                           "Snapshot isolation level")
        .setDefault(moe::Value(true));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.ccProtocol",
                           "eloqCcProtocol",
                           moe::String,
                           "Concurrency control protocol.(OCC|OccRead|Locking)")
        .setDefault(moe::Value("OccRead"));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.skipRedoLog",
                           "eloqSkipRedoLog",
                           moe::Bool,
                           "Skip write redo log in tx_service")
        .setDefault(moe::Value(false));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.kickoutDataForTest",
                           "eloqKickoutDataForTest",
                           moe::Bool,
                           "Kickout data after checkpoint")
        .setDefault(moe::Value(false));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.realtimeSampling",
                           "eloqRealtimeSampling",
                           moe::Bool,
                           "Whether enable realtime sampling. If disable it, user may need execute "
                           "analyze command at some time.")
        .setDefault(moe::Value(true));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.enableHeapDefragment",
                           "eloqEnableHeapDefragment",
                           moe::Bool,
                           "Enable heap defragment.")
        .setDefault(moe::Value(false));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.nodeGroupReplicaNum",
                           "eloqNodeGroupReplicaNum",
                           moe::Int,
                           "Replicate number of node group(Max: 9)")
        .setDefault(moe::Value(3));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.bthreadWorkerNum",
                           "eloqNodeBthreadWorkerNum",
                           moe::Int,
                           "Number of bthread worker threads")
        .setDefault(moe::Value(0));

    // txlog
    eloqOptions
        .addOptionChaining("storage.eloq.txService.txlogRocksDBStoragePath",
                           "eloqTxlogRocksDBStoragePath",
                           moe::String,
                           "The path for tx log service rocksdb storage")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.txService.txlogRocksDBScanThreadNum",
                           "eloqTxlogRocksDBScanThreadNum",
                           moe::Int,
                           "Number of rocksdb scan threads")
        .setDefault(moe::Value(1));


    // Eloq Storage Options
    eloqOptions
        .addOptionChaining("storage.eloq.storage.keyspaceName",
                           "eloqKeyspaceName",
                           moe::String,
                           "Keyspace of KV Storage")
        .setDefault(moe::Value("mono_mongo"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.cassHosts",
                           "eloqCassHosts",
                           moe::String,
                           "Contact points of Cassandra")
        .setDefault(moe::Value("127.0.0.1"));
    eloqOptions
        .addOptionChaining(
            "storage.eloq.storage.cassPort", "eloqCassPort", moe::Int, "Port of Cassandra")
        .validRange(1, UINT16_MAX)
        .setDefault(moe::Value(9042));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.cassQueueSizeIO",
                           "eloqCassQueueSizeIO",
                           moe::Int,
                           "Queue_size_io of Cassandra client")
        .validRange(0, INT_MAX)
        .setDefault(moe::Value(300000));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.cassKeyspaceClass",
                           "eloqCassKeyspaceClass",
                           moe::String,
                           "Keyspace class of Cassandra")
        .setDefault(moe::Value("SimpleStrategy"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.cassReplicationFactor",
                           "eloqCassReplicationFactor",
                           moe::String,
                           "Keyspace replication factor of Cassandra")
        .setDefault(moe::Value("1"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.cassHighCompressionRatio",
                           "eloqCassHighCompressionRatio",
                           moe::Bool,
                           "Cassandra enable high compression ratio")
        .setDefault(moe::Value(false));
    eloqOptions
        .addOptionChaining(
            "storage.eloq.storage.cassUser", "eloqCassUser", moe::String, "Cassandra username")
        .setDefault(moe::Value("cassandra"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.cassPassword",
                           "eloqCassPassword",
                           moe::String,
                           "Cassandra password")
        .setDefault(moe::Value("cassandra"));
    // Eloq DataStoreService Options
    eloqOptions
        .addOptionChaining("storage.eloq.storage.dataStoreServiceConfigFilePath",
                           "eloqDataStoreServiceConfigFilePath",
                           moe::String,
                           "Eloq DataStoreService config file path")
        .setDefault(moe::Value(""));
    // add option dssPeerNode
    eloqOptions
        .addOptionChaining("storage.eloq.storage.dssPeerNode",
                           "eloqDssPeerNode",
                           moe::String,
                           "Eloq DataStoreService peer node endpoint")
        .setDefault(moe::Value(""));
    // RocksDB Cloud
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudStoragePath",
                           "eloqRocksdbCloudStoragePath",
                           moe::String,
                           "RocksDB Cloud storage path")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.awsAccessKeyId",
                           "eloqAwsAccessKeyId",
                           moe::String,
                           "AWS SDK access key id")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.awsSecretKey",
                           "eloqAwsSecretKey",
                           moe::String,
                           "AWS SDK secret key")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudBucketName",
                           "eloqRocksdbCloudBucketName",
                           moe::String,
                           "RocksDB cloud bucket name")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudBucketPrefix",
                           "eloqRocksdbCloudBucketPrefix",
                           moe::String,
                           "RocksDB cloud bucket prefix")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudObjectPath",
                           "eloqRocksdbCloudObjectPath",
                           moe::String,
                           "RocksDB cloud object path")
        .setDefault(moe::Value("rocksdb-cloud"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudRegion",
                           "eloqRocksdbCloudRegion",
                           moe::String,
                           "RocksDB cloud region")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudEndpointUrl",
                           "eloqRocksdbCloudEndpointUrl",
                           moe::String,
                           "RocksDB Cloud endpoint URL")
        .setDefault(moe::Value(""));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudSstFileCacheSize",
                           "eloqRocksdbCloudSstFileCacheSize",
                           moe::String,
                           "RocksDB Cloud SST file cache size")
        .setDefault(moe::Value("3GB"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudSstFileCacheNumShardBits",
                           "eloqRocksdbCloudSstFileCacheNumShardBits",
                           moe::Int,
                           "RocksDB Cloud SST file cache num shard bits")
        .setDefault(moe::Value(5));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbTargetFileSizeBase",
                           "eloqRocksdbTargetFileSizeBase",
                           moe::String,
                           "RocksDB target file size")
        .setDefault(moe::Value("64MB"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbSstFilesSizeLimit",
                           "eloqRocksdbSstFilesSizeLimit",
                           moe::String,
                           "RocksDB sst files size limit")
        .setDefault(moe::Value("500MB"));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudReadyTimeout",
                           "eloqRocksdbCloudReadyTimeout",
                           moe::Int,
                           "RocksDB Cloud becomes ready timeout(seconds)")
        .validRange(1, 120)
        .setDefault(moe::Value(10));
    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbCloudFileDeletionDelay",
                           "eloqRocksdbCloudFileDeletionDelay",
                           moe::Int,
                           "RocksDB Cloud file deletion delay")
        .validRange(1, 3600)
        .setDefault(moe::Value(60));

    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbMaxBackgroundJobs",
                           "eloqrocksdbMaxBackgroundJobs",
                           moe::Int,
                           "RocksDB Cloud Max Background Jobs")
        .validRange(1, 1024)
        .setDefault(moe::Value(4));

    eloqOptions
        .addOptionChaining("storage.eloq.storage.rocksdbMaxSubCompactions",
                           "eloqrocksdbMaxSubCompactions",
                           moe::Int,
                           "RocksDB Cloud Max SubCompactions")
        .validRange(1, 1024)
        .setDefault(moe::Value(1));


    // Options for metrics
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.enableMetrics",
                           "eloqEnableMetrics",
                           moe::Bool,
                           "Enable metrics collection. When this option is set to false, all other "
                           "fine-grained control options for metrics will be disabled.")
        .setDefault(moe::Value(false));
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.metricsPort",
                           "eloqMetricsPort",
                           moe::Int,
                           "Port for metrics collection")
        .validRange(1, UINT16_MAX)
        .setDefault(moe::Value(18081));

    eloqOptions
        .addOptionChaining("storage.eloq.metrics.enableMemoryUsage",
                           "eloqEnableMemoryUsage",
                           moe::Bool,
                           "Enable memory usage metrics.")
        .setDefault(moe::Value(true));
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.collectMemoryUsageRound",
                           "eloqCollectMemoryUsageRound",
                           moe::Int,
                           "Rounds interval for memory usage collection")
        .validRange(1, std::numeric_limits<int32_t>::max())
        .setDefault(moe::Value(10000));
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.enableCacheHitRate",
                           "eloqEnableCacheHitRate",
                           moe::Bool,
                           "Enable cache hit rate metrics.")
        .setDefault(moe::Value(true));
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.enableTxMetrics",
                           "eloqEnableTxMetrics",
                           moe::Bool,
                           "Enable transaction service metrics.")
        .setDefault(moe::Value(true));
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.collectTxDurationRound",
                           "eloqCollectTxDurationRound",
                           moe::Int,
                           "Round interval for transaction duration collection")
        .validRange(1, std::numeric_limits<int32_t>::max())
        .setDefault(moe::Value(100));
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.enableBusyRoundMetrics",
                           "eloqEnableBusyRoundMetrics",
                           moe::Bool,
                           "Enable busy round metrics when reaching the busy round threshold")
        .setDefault(moe::Value(true));
    eloqOptions
        .addOptionChaining("storage.eloq.metrics.busyRoundThreshold",
                           "eloqBusyRoundThreshold",
                           moe::Int,
                           "Threshold for busy round metrics collection")
        .validRange(1, std::numeric_limits<int32_t>::max())
        .setDefault(moe::Value(10));


    eloqOptions
        .addOptionChaining("storage.eloq.metrics.enableRemoteRequestMetrics",
                           "eloqEnableRemoteRequestMetrics",
                           moe::Bool,
                           "Enable remote request metrics.")
        .setDefault(moe::Value(true));

    return options->addSection(eloqOptions);
}

Status EloqGlobalOptions::store(const moe::Environment& params,
                                const std::vector<std::string>& args) {

    // Eloq TxService Options
    if (params.count("storage.eloq.txService.localIP")) {
        auto localIP = params["storage.eloq.txService.localIP"].as<std::string>();
        if (localIP.find_first_of(':') == std::string::npos) {
            localIP.append(":8000");
        }
        auto hostAndPort = mongo::HostAndPort::parse(localIP);
        if (hostAndPort.isOK()) {
            invariant(hostAndPort.getValue().hasPort());
            eloqGlobalOptions.localAddr = std::move(hostAndPort.getValue());
        } else {
            return hostAndPort.getStatus();
        }
    }
    if (params.count("storage.eloq.txService.ipList")) {
        ipList = params["storage.eloq.txService.ipList"].as<std::string>();
    }
    if (ipList.empty()) {
        ipList = eloqGlobalOptions.localAddr.toString();
    }
    if (params.count("storage.eloq.txService.txlogServiceList")) {
        auto txlogServiceList =
            params["storage.eloq.txService.txlogServiceList"].as<std::vector<std::string>>();
        for (std::string& txlog : txlogServiceList) {
            auto hostAndPort = mongo::HostAndPort::parse(txlog);
            if (hostAndPort.isOK()) {
                if (hostAndPort.getValue().hasPort()) {
                    eloqGlobalOptions.txlogServiceAddrs.push_back(
                        std::move(hostAndPort.getValue()));
                } else {
                    return Status{ErrorCodes::InvalidOptions,
                                  str::stream() << txlog << " does not include a port"};
                }
            } else {
                return hostAndPort.getStatus();
            }
        }
    }
    if (params.count("storage.eloq.txService.forkHostManager")) {
        eloqGlobalOptions.forkHostManager =
            params["storage.eloq.txService.forkHostManager"].as<bool>();
    }

    if (params.count("storage.eloq.txService.hmIP") &&
        params.count("storage.eloq.txService.hmPort")) {
        auto hmIP = params["storage.eloq.txService.hmIP"].as<std::string>();
        auto hmPort = params["storage.eloq.txService.hmPort"].as<int>();

        if (hmIP.empty()) {
            return Status{ErrorCodes::InvalidOptions, "Host manager IP cannot be empty"};
        }

        eloqGlobalOptions.hostManagerAddr = mongo::HostAndPort(hmIP, hmPort);
    }
    if (params.count("storage.eloq.txService.hmBinPath")) {
        auto hmBinPath = params["storage.eloq.txService.hmBinPath"].as<std::string>();
        if (hmBinPath.empty()) {
            char pathBuf[PATH_MAX];
            ssize_t len = ::readlink("/proc/self/exe", pathBuf, sizeof(pathBuf));
            len -= strlen("/mongod");
            pathBuf[len] = '\0';
            hmBinPath = std::string(pathBuf, len);
            hmBinPath.append("/host_manager");
        }
        eloqGlobalOptions.hostManagerBinPath = std::move(hmBinPath);
    }
    if (params.count("storage.eloq.txService.rangeSplitWorkerNum")) {
        eloqGlobalOptions.rangeSplitWorkerNum =
            params["storage.eloq.txService.rangeSplitWorkerNum"].as<int>();
    }
    if (params.count("storage.eloq.txService.nodeMemoryLimitMB")) {
        eloqGlobalOptions.nodeMemoryLimitMB =
            params["storage.eloq.txService.nodeMemoryLimitMB"].as<int>();
    }
    if (params.count("storage.eloq.txService.checkpointerIntervalSec")) {
        eloqGlobalOptions.checkpointerIntervalSec =
            params["storage.eloq.txService.checkpointerIntervalSec"].as<int>();
    }
    if (params.count("storage.eloq.txService.checkpointerDelaySec")) {
        eloqGlobalOptions.checkpointerDelaySec =
            params["storage.eloq.txService.checkpointerDelaySec"].as<int>();
    }
    if (params.count("storage.eloq.txService.collectActiveTxTsIntervalSec")) {
        eloqGlobalOptions.collectActiveTxTsIntervalSec =
            params["storage.eloq.txService.collectActiveTxTsIntervalSec"].as<int>();
    }
    if (params.count("storage.eloq.txService.deadlockIntervalSec")) {
        eloqGlobalOptions.deadlockIntervalSec =
            params["storage.eloq.txService.deadlockIntervalSec"].as<int>();
    }
    if (params.count("storage.eloq.txService.txlogGroupReplicaNum")) {
        eloqGlobalOptions.txlogGroupReplicaNum =
            params["storage.eloq.txService.txlogGroupReplicaNum"].as<int>();
    }
    if (params.count("storage.eloq.txService.txlogRocksDBScanThreadNum")) {
        eloqGlobalOptions.txlogRocksDBScanThreads =
            params["storage.eloq.txService.txlogRocksDBScanThreadNum"].as<int>();
    }
    if (params.count("storage.eloq.txService.useKeyCache")) {
        eloqGlobalOptions.useKeyCache = params["storage.eloq.txService.useKeyCache"].as<bool>();
    }
    if (params.count("storage.eloq.txService.enableMVCC")) {
        eloqGlobalOptions.enableMVCC = params["storage.eloq.txService.enableMVCC"].as<bool>();
    }
    if (params.count("storage.eloq.txService.ccProtocol")) {
        const std::string& s = params["storage.eloq.txService.ccProtocol"].as<std::string>();
        if (s == "OCC") {
            ccProtocol = txservice::CcProtocol::OCC;
        } else if (s == "OccRead") {
            ccProtocol = txservice::CcProtocol::OccRead;
        } else if (s == "Locking") {
            ccProtocol = txservice::CcProtocol::Locking;
        } else {
            return Status{ErrorCodes::InvalidOptions,
                          str::stream() << s << " is not a valid CcProtocol"};
        }
    }
    if (params.count("storage.eloq.txService.skipRedoLog")) {
        eloqGlobalOptions.skipRedoLog = params["storage.eloq.txService.skipRedoLog"].as<bool>();
    }
    if (params.count("storage.eloq.txService.kickoutDataForTest")) {
        eloqGlobalOptions.kickoutDataForTest =
            params["storage.eloq.txService.kickoutDataForTest"].as<bool>();
    }
    if (params.count("storage.eloq.txService.realtimeSampling")) {
        eloqGlobalOptions.realtimeSampling =
            params["storage.eloq.txService.realtimeSampling"].as<bool>();
    }
    if (params.count("storage.eloq.txService.enableHeapDefragment")) {
        eloqGlobalOptions.enableHeapDefragment =
            params["storage.eloq.txService.enableHeapDefragment"].as<bool>();
    }
    if (params.count("storage.eloq.txService.nodeGroupReplicaNum")) {
        eloqGlobalOptions.nodeGroupReplicaNum =
            params["storage.eloq.txService.nodeGroupReplicaNum"].as<int>();
    }
    if (params.count("storage.eloq.txService.bthreadWorkerNum")) {
        eloqGlobalOptions.bthreadWorkerNum =
            params["storage.eloq.txService.bthreadWorkerNum"].as<int>();
    }

    // txlog
    if (params.count("storage.eloq.txService.txlogRocksDBStoragePath")) {
        eloqGlobalOptions.txlogRocksDBStoragePath =
            params["storage.eloq.txService.txlogRocksDBStoragePath"].as<std::string>();
    }
    if (params.count("storage.eloq.txService.txlogRocksDBScanThreadNum")) {
        eloqGlobalOptions.txlogRocksDBScanThreads =
            params["storage.eloq.txService.txlogRocksDBScanThreadNum"].as<int>();
    }

    // Eloq Storage Options

    if (params.count("storage.eloq.storage.keyspaceName")) {
        eloqGlobalOptions.keyspaceName =
            params["storage.eloq.storage.keyspaceName"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.cassHosts")) {
        eloqGlobalOptions.cassHosts = params["storage.eloq.storage.cassHosts"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.cassPort")) {
        eloqGlobalOptions.cassPort = params["storage.eloq.storage.cassPort"].as<int>();
    }
    if (params.count("storage.eloq.storage.cassQueueSizeIO")) {
        eloqGlobalOptions.cassQueueSizeIO =
            params["storage.eloq.storage.cassQueueSizeIO"].as<int>();
    }
    if (params.count("storage.eloq.storage.cassKeyspaceClass")) {
        eloqGlobalOptions.cassKeyspaceClass =
            params["storage.eloq.storage.cassKeyspaceClass"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.cassReplicationFactor")) {
        eloqGlobalOptions.cassReplicationFactor =
            params["storage.eloq.storage.cassReplicationFactor"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.cassHighCompressionRatio")) {
        eloqGlobalOptions.cassHighCompressionRatio =
            params["storage.eloq.storage.cassHighCompressionRatio"].as<bool>();
    }
    if (params.count("storage.eloq.storage.cassUser")) {
        eloqGlobalOptions.cassUser = params["storage.eloq.storage.cassUser"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.cassPassword")) {
        eloqGlobalOptions.cassPassword =
            params["storage.eloq.storage.cassPassword"].as<std::string>();
    }

    // Eloq DataStoreService Options

    if (params.count("storage.eloq.storage.dataStoreServiceConfigFilePath")) {
        eloqGlobalOptions.dataStoreServiceConfigFilePath =
            params["storage.eloq.storage.dataStoreServiceConfigFilePath"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.dssPeerNode")) {
        eloqGlobalOptions.dssPeerNode =
            params["storage.eloq.storage.dssPeerNode"].as<std::string>();
    }

    // RocksDB Cloud Options Parse
    if (params.count("storage.eloq.storage.rocksdbCloudStoragePath")) {
        eloqGlobalOptions.rocksdbCloudStoragePath =
            params["storage.eloq.storage.rocksdbCloudStoragePath"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.awsAccessKeyId")) {
        eloqGlobalOptions.awsAccessKeyId =
            params["storage.eloq.storage.awsAccessKeyId"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.awsSecretKey")) {
        eloqGlobalOptions.awsSecretKey =
            params["storage.eloq.storage.awsSecretKey"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudBucketName")) {
        eloqGlobalOptions.rocksdbCloudBucketName =
            params["storage.eloq.storage.rocksdbCloudBucketName"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudBucketPrefix")) {
        eloqGlobalOptions.rocksdbCloudBucketPrefix =
            params["storage.eloq.storage.rocksdbCloudBucketPrefix"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudObjectPath")) {
        eloqGlobalOptions.rocksdbCloudObjectPath =
            params["storage.eloq.storage.rocksdbCloudObjectPath"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudRegion")) {
        eloqGlobalOptions.rocksdbCloudRegion =
            params["storage.eloq.storage.rocksdbCloudRegion"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudEndpointUrl")) {
        eloqGlobalOptions.rocksdbCloudEndpointUrl =
            params["storage.eloq.storage.rocksdbCloudEndpointUrl"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudSstFileCacheSize")) {
        eloqGlobalOptions.rocksdbCloudSstFileCacheSize =
            params["storage.eloq.storage.rocksdbCloudSstFileCacheSize"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudSstFileCacheNumShardBits")) {
        eloqGlobalOptions.rocksdbCloudSstFileCacheNumShardBits =
            params["storage.eloq.storage.rocksdbCloudSstFileCacheNumShardBits"].as<int>();
    }
    if (params.count("storage.eloq.storage.rocksdbTargetFileSizeBase")) {
        eloqGlobalOptions.rocksdbTargetFileSizeBase =
            params["storage.eloq.storage.rocksdbTargetFileSizeBase"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbSstFilesSizeLimit")) {
        eloqGlobalOptions.rocksdbSstFilesSizeLimit =
            params["storage.eloq.storage.rocksdbSstFilesSizeLimit"].as<std::string>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudReadyTimeout")) {
        eloqGlobalOptions.rocksdbCloudReadyTimeout =
            params["storage.eloq.storage.rocksdbCloudReadyTimeout"].as<int>();
    }
    if (params.count("storage.eloq.storage.rocksdbCloudFileDeletionDelay")) {
        eloqGlobalOptions.rocksdbCloudFileDeletionDelay =
            params["storage.eloq.storage.rocksdbCloudFileDeletionDelay"].as<int>();
    }

    if (params.count("storage.eloq.storage.rocksdbMaxBackgroundJobs")) {
        eloqGlobalOptions.rocksdbMaxBackgroundJobs =
            params["storage.eloq.storage.rocksdbMaxBackgroundJobs"].as<int>();
    }

    if (params.count("storage.eloq.storage.rocksdbMaxSubCompactions")) {
        eloqGlobalOptions.rocksdbMaxSubCompactions =
            params["storage.eloq.storage.rocksdbMaxSubCompactions"].as<int>();
    }


    // Parse metrics options

    if (params.count("storage.eloq.metrics.enableMetrics")) {
        enableMetrics = params["storage.eloq.metrics.enableMetrics"].as<bool>();
    }

    if (params.count("storage.eloq.metrics.metricsPort")) {
        metricsPort = static_cast<uint16_t>(params["storage.eloq.metrics.metricsPort"].as<int>());
        metricsPortString = std::to_string(metricsPort);
    }

    if (params.count("storage.eloq.metrics.enableMemoryUsage")) {
        enableMemoryUsage =
            enableMetrics && params["storage.eloq.metrics.enableMemoryUsage"].as<bool>();
    }

    if (params.count("storage.eloq.metrics.collectMemoryUsageRound")) {
        collectMemoryUsageRound = params["storage.eloq.metrics.collectMemoryUsageRound"].as<int>();
    }

    if (params.count("storage.eloq.metrics.enableCacheHitRate")) {
        enableCacheHitRate =
            enableMetrics && params["storage.eloq.metrics.enableCacheHitRate"].as<bool>();
    }

    if (params.count("storage.eloq.metrics.enableTxMetrics")) {
        enableTxMetrics =
            enableMetrics && params["storage.eloq.metrics.enableTxMetrics"].as<bool>();
    }

    if (params.count("storage.eloq.metrics.collectTxDurationRound")) {
        collectTxDurationRound = params["storage.eloq.metrics.collectTxDurationRound"].as<int>();
    }

    if (params.count("storage.eloq.metrics.enableBusyRoundMetrics")) {
        enableBusyRoundMetrics =
            enableMetrics && params["storage.eloq.metrics.enableBusyRoundMetrics"].as<bool>();
    }

    if (params.count("storage.eloq.metrics.busyRoundThreshold")) {
        busyRoundThreshold = params["storage.eloq.metrics.busyRoundThreshold"].as<int>();
    }

    if (params.count("storage.eloq.metrics.enableRemoteRequestMetrics")) {
        enableRemoteRequestMetrics =
            enableMetrics && params["storage.eloq.metrics.enableRemoteRequestMetrics"].as<bool>();
    }


    return Status::OK();
}

std::vector<std::string> EloqGlobalOptions::TxlogIPs() const {
    std::vector<std::string> ips;
    ips.reserve(txlogServiceAddrs.size());
    for (const mongo::HostAndPort& addr : txlogServiceAddrs) {
        ips.push_back(addr.host());
    }
    return ips;
}

std::vector<uint16_t> EloqGlobalOptions::TxlogPorts() const {
    std::vector<uint16_t> ports;
    for (const mongo::HostAndPort& addr : txlogServiceAddrs) {
        invariant(addr.hasPort());
        ports.push_back(addr.port());
    }
    return ports;
}

}  // namespace mongo
