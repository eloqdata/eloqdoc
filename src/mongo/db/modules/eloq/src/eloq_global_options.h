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
#pragma once

#include <cstdint>

#include "mongo/util/net/hostandport.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"

#include "mongo/db/modules/eloq/tx_service/include/cc_protocol.h"

namespace mongo {

namespace moe = mongo::optionenvironment;

class EloqGlobalOptions {
public:
    EloqGlobalOptions() = default;
    Status add(moe::OptionSection* options);
    Status store(const moe::Environment& params, const std::vector<std::string>& args);

    std::vector<std::string> TxlogIPs() const;
    std::vector<uint16_t> TxlogPorts() const;

    // basic options
    // bool bootstrap{false};
    // tx_service
    mongo::HostAndPort localAddr;
    std::string ipList;
    mongo::HostAndPort hostManagerAddr;
    std::string hostManagerBinPath;
    // uint16_t coreNum{0};
    uint16_t rangeSplitWorkerNum{0};
    uint32_t nodeMemoryLimitMB{0};
    uint32_t checkpointerIntervalSec{0};
    uint32_t checkpointerDelaySec{0};
    uint32_t collectActiveTxTsIntervalSec{0};
    uint32_t deadlockIntervalSec{0};
    uint32_t txlogGroupReplicaNum{0};
    uint32_t nodeGroupReplicaNum{0};
    uint16_t bthreadWorkerNum{0};
    bool useKeyCache{false};
    bool enableMVCC{true};
    txservice::CcProtocol ccProtocol{txservice::CcProtocol::OccRead};
    bool skipRedoLog{false};
    bool kickoutDataForTest{false};
    bool realtimeSampling{true};
    bool enableHeapDefragment{false};

    // txlog
    std::string txlogRocksDBStoragePath;
    uint16_t txlogRocksDBScanThreads{1};
    std::vector<mongo::HostAndPort> txlogServiceAddrs;

    // storage
    std::string keyspaceName;

    std::string cassHosts;
    uint16_t cassPort{0};
    uint32_t cassQueueSizeIO{0};
    std::string cassKeyspaceClass;
    std::string cassReplicationFactor;
    bool cassHighCompressionRatio{false};
    std::string cassUser;
    std::string cassPassword;

    // Eloq Data Store Service
    std::string dataStoreServiceConfigFilePath;
    std::string dssPeerNode;

    // rocksdb cloud
    std::string rocksdbCloudStoragePath;
    std::string awsAccessKeyId;
    std::string awsSecretKey;
    std::string rocksdbCloudBucketName;
    std::string rocksdbCloudBucketPrefix;
    std::string rocksdbCloudRegion;
    std::string rocksdbCloudEndpointUrl;
    std::string rocksdbCloudSstFileCacheSize;
    int rocksdbCloudSstFileCacheNumShardBits{5};  // default 1 shard
    std::string rocksdbTargetFileSizeBase;
    std::string rocksdbSstFilesSizeLimit;
    uint32_t rocksdbCloudReadyTimeout{0};
    uint32_t rocksdbCloudFileDeletionDelay{0};
    uint32_t rocksdbMaxBackgroundJobs{4};
    uint32_t rocksdbMaxSubCompactions{1};  // no subcompactions

    // metrics
    bool enableMetrics{false};
    uint16_t metricsPort{18081};
    std::string metricsPortString;
    bool enableMemoryUsage{true};
    uint32_t collectMemoryUsageRound{10000};
    bool enableCacheHitRate{true};
    bool enableTxMetrics{true};
    uint32_t collectTxDurationRound{100};
    bool enableBusyRoundMetrics{true};
    uint32_t busyRoundThreshold{10};
    bool enableRemoteRequestMetrics{true};
};

extern EloqGlobalOptions eloqGlobalOptions;
}  // namespace mongo
