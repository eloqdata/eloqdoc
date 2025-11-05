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

#include "mongo/util/net/hostandport.h"
#include "mongo/util/options_parser/startup_option_init.h"
#include "mongo/util/options_parser/startup_options.h"
#include <cstdint>
#include <sys/types.h>

#include <cstdint>
#include <sys/types.h>

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
    bool forkHostManager{false};
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
    bool useKeyCache{false};
    txservice::CcProtocol ccProtocol{txservice::CcProtocol::OccRead};
    bool skipRedoLog{false};
    bool kickoutDataForTest{false};
    bool realtimeSampling{true};
    bool enableHeapDefragment{false};
    bool enableIOuring{false};
    bool raftlogAsyncFsync{false};

    // txlog
    std::string txlogRocksDBStoragePath;
    uint16_t txlogRocksDBScanThreads{1};
    std::vector<mongo::HostAndPort> txlogServiceAddrs;
    std::string txlogRocksDBCloudRegion;
    std::string txlogRocksDBCloudBucketName;
    std::string txlogRocksDBCloudBucketPrefix;
    std::string txlogRocksDBCloudObjectPath;
    std::string txlogRocksDBCloudEndpointUrl;
    std::string txlogRocksDBCloudSstFileCacheSize;
    int txlogRocksDBCloudSstFileCacheNumShardBits{5};
    uint32_t txlogRocksDBCloudReadyTimeout{0};
    uint32_t txlogRocksDBCloudFileDeletionDelay{0};

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
    std::string dssBranchName;

    // rocksdb cloud
    std::string rocksdbCloudStoragePath;
    std::string awsAccessKeyId;
    std::string awsSecretKey;
    std::string rocksdbCloudBucketName;
    std::string rocksdbCloudBucketPrefix;
    std::string rocksdbCloudObjectPath;
    std::string rocksdbCloudRegion;
    std::string rocksdbCloudEndpointUrl;
    std::string rocksdbCloudSstFileCacheSize;
    int rocksdbCloudSstFileCacheNumShardBits{5};  // default 1 shard
    std::string rocksdbTargetFileSizeBase;
    std::string rocksdbSstFilesSizeLimit;
    uint32_t rocksdbCloudReadyTimeout{0};
    uint32_t rocksdbCloudFileDeletionDelay{0};
    uint32_t rocksdbCloudPurgerPeriodicitySecs{10 * 60};  // default 10 minutes
    uint32_t rocksdbMaxBackgroundJobs{4};
    uint32_t rocksdbMaxSubCompactions{1};  // no subcompactions

    // eloqstore
    bool eloqStoreSkipVerifyChecksum{false};
    bool eloqStoreDataAppendMode{false};
    bool eloqStoreEnableCompression{false};
    uint32_t eloqStoreWorkerCount{1};
    uint32_t eloqStoreOpenFilesLimit{1024};
    uint32_t eloqStoreDataPageRestartInterval{16};
    uint32_t eloqStoreIndexPageRestartInterval{16};
    uint32_t eloqStoreInitPageCount{1 << 15};
    uint32_t eloqStoreManifestLimit{8 << 20};
    uint32_t eloqStoreIoQueueSize{4096};
    uint32_t eloqStoreMaxInflightWrite{64 << 10};
    uint32_t eloqStoreMaxWriteBatchPages{256};
    uint32_t eloqStoreBufRingSize{1 << 12};
    uint32_t eloqStoreCoroutineStackSize{32 * 1024};
    uint32_t eloqStoreNumRetainedArchives{0};
    uint32_t eloqStoreArchiveIntervalSecs{86400};
    uint32_t eloqStoreMaxArchiveTasks{256};
    uint32_t eloqStoreFileAmplifyFactor{4};
    uint32_t eloqStoreReserveSpaceRatio{100};
    uint32_t eloqStoreDataPageSize{1 << 12};
    uint32_t eloqStorePagesPerFileShift{11};
    uint32_t eloqStoreOverflowPointers{16};
    std::string eloqStoreCloudStorePath;
    std::string eloqStoreLocalSpaceLimit;
    std::string eloqStoreStoragePathList;
    std::string eloqStoreIndexBufferPoolSize;

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
