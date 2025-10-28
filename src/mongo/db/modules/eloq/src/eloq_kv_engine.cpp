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

#include <cstddef>
#include <filesystem>
#include <iomanip>  // std::setfill, std::setw
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/kv/kv_catalog_feature_tracker.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "log_utils.h"
#include "mongo/db/modules/eloq/data_substrate/eloq_metrics/include/metrics.h"
#include "mongo/db/modules/eloq/data_substrate/store_handler/kv_store.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/catalog_key_record.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/dead_lock_check.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/sequences/sequences.h"
#include "mongo/db/modules/eloq/src/base/eloq_key.h"
#include "mongo/db/modules/eloq/src/base/eloq_log_agent.h"
#include "mongo/db/modules/eloq/src/base/eloq_record.h"
#include "mongo/db/modules/eloq/src/base/eloq_util.h"
#include "mongo/db/modules/eloq/src/base/metrics_registry_impl.h"
#include "mongo/db/modules/eloq/src/eloq_global_options.h"
#include "mongo/db/modules/eloq/src/eloq_index.h"
#include "mongo/db/modules/eloq/src/eloq_kv_engine.h"
#include "mongo/db/modules/eloq/src/eloq_record_store.h"
#include "mongo/db/modules/eloq/src/eloq_recovery_unit.h"

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_key.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_record.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_service.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_service_metrics.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_util.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/type.h"

#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"

#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) ||  \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS) || \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB) || defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE))
#define ELOQDS 1
#endif

#if (defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3) || \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_GCS))
#define ELOQDS_RKDB_CLOUD 1
#endif

#if defined(DATA_STORE_TYPE_DYNAMODB)
#include "store_handler/dynamo_handler.h"
#elif defined(DATA_STORE_TYPE_BIGTABLE)
#include "store_handler/bigtable_handler.h"
#elif ELOQDS
#include "mongo/db/modules/eloq/data_substrate/store_handler/data_store_service_client.h"
#include "mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service/data_store_service.h"
#include "mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service/data_store_service_config.h"
#if ELOQDS_RKDB_CLOUD
#include "store_handler/eloq_data_store_service/rocksdb_cloud_data_store_factory.h"
#include "store_handler/eloq_data_store_service/rocksdb_config.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB)
#include "store_handler/eloq_data_store_service/rocksdb_config.h"
#include "store_handler/eloq_data_store_service/rocksdb_data_store_factory.h"
#elif defined(DATA_STORE_TYPE_ELOQDSS_ELOQSTORE)
#include "store_handler/eloq_data_store_service/eloq_store_data_store_factory.h"
#endif
#else
#endif

#if (defined(DATA_STORE_TYPE_DYNAMODB) || defined(LOG_STATE_TYPE_RKDB_S3) || \
     defined(DATA_STORE_TYPE_ELOQDSS_ROCKSDB_CLOUD_S3))
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#endif

// Log state type
#if !defined(LOG_STATE_TYPE_RKDB_CLOUD)

// Only if LOG_STATE_TYPE_RKDB_CLOUD undefined
#if ((defined(LOG_STATE_TYPE_RKDB_S3) || defined(LOG_STATE_TYPE_RKDB_GCS)) && \
     !defined(LOG_STATE_TYPE_RKDB))
#define LOG_STATE_TYPE_RKDB_CLOUD 1
#endif

#endif

#if !defined(LOG_STATE_TYPE_RKDB_ALL)

// Only if LOG_STATE_TYPE_RKDB_ALL undefined
#if (defined(LOG_STATE_TYPE_RKDB_S3) || defined(LOG_STATE_TYPE_RKDB_GCS) || \
     defined(LOG_STATE_TYPE_RKDB))
#define LOG_STATE_TYPE_RKDB_ALL 1
#endif

#endif

#if defined(LOG_STATE_TYPE_RKDB_CLOUD)
#include "mongo/db/modules/eloq/data_substrate/eloq_log_service/include/rocksdb_cloud_config.h"
#endif

// register catalog factory for data_substrate to link
Eloq::MongoCatalogFactory catalogFactory;
txservice::CatalogFactory* eloqdoc_catalog_factory = &catalogFactory;

mongo::MongoSystemHandler mongoSystemHandler;
txservice::SystemHandler* eloqdoc_system_handler = &mongoSystemHandler;

// data substrate config
DEFINE_string(data_substrate_config, "", "Data Substrate Configuration");

namespace mongo {

extern std::function<std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    getTxServiceFunctors;

std::string_view extractDbName(std::string_view nss) {
    auto pos = nss.find('.');
    if (pos == std::string_view::npos) {
        return "";
    } else {
        return nss.substr(0, pos);
    }
}

bool EloqKVEngine::InitMetricsRegistry() {

    Eloq::MetricsRegistryImpl::MetricsRegistryResult metricsRegistryResult =
        Eloq::MetricsRegistryImpl::GetRegistry();

    if (metricsRegistryResult.not_ok_ != nullptr) {
        return false;
    }

    _metricsRegistry = std::move(metricsRegistryResult.metrics_registry_);
    return true;
}

EloqKVEngine::EloqKVEngine(const std::string& path) : _dbPath(path) {
    log() << "Starting Eloq storage engine. dbPath: " << path;
    log() << "Standalone mode: Initializing data substrate...";
    DataSubstrate::InitializeGlobal(FLAGS_data_substrate_config);

    _logServer = DataSubstrate::GetGlobal()->GetLogServer();
    _txService = DataSubstrate::GetGlobal()->GetTxService();

#ifdef EXT_TX_PROC_ENABLED
    getTxServiceFunctors = _txService->GetTxProcFunctors();
#endif

    log() << "Eloq storage engine initialized.";
}

EloqKVEngine::~EloqKVEngine() {
    MONGO_LOG(1) << "EloqKVEngine::~EloqKVEngine";
    if (_txService) {
        cleanShutdown();
    }
}

bool EloqKVEngine::supportsRecoveryTimestamp() const {
    return false;
}

boost::optional<Timestamp> EloqKVEngine::getRecoveryTimestamp() const {
    assert(!supportsRecoveryTimestamp());
    return boost::none;
}

RecoveryUnit* EloqKVEngine::newRecoveryUnit() {
    MONGO_LOG(1) << "EloqKVEngine::newRecoveryUnit";
    invariant(_txService != nullptr);
    return new EloqRecoveryUnit(_txService);
}

RecoveryUnit::UPtr EloqKVEngine::newRecoveryUnitUPtr() {
    MONGO_UNREACHABLE;
    // MONGO_LOG(1) << "EloqKVEngine::newRecoveryUnitUPtr";
    // return ObjectPool<EloqRecoveryUnit>::newObject<RecoveryUnit>(_txService.get()
    //                                                                   );
}

void EloqKVEngine::listDatabases(std::vector<std::string>& out) const {
    MONGO_LOG(1) << "EloqKVEngine::listDatabases";

    std::vector<std::string> tables;

    const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
    bool success = Eloq::GetAllTables(tables, coro.yieldFuncPtr, coro.resumeFuncPtr);
    if (!success) {
        error() << "Failed to discover table names.";
        uassertStatusOK(Status{ErrorCodes::InternalError, "Failed to discover collection names."});
        return;
    }

    std::unordered_set<std::string> dbNames;
    for (const auto& tableName : tables) {
        std::string_view dbName = extractDbName(tableName);
        if (!dbName.empty()) {
            dbNames.emplace(dbName);
        }
    }

    out.reserve(dbNames.size());
    for (auto it = dbNames.begin(); it != dbNames.end();) {
        out.push_back(std::move(dbNames.extract(it++).value()));
    }

    std::string dbString;
    for (const auto& name : out) {
        dbString.append(name).append("|");
    }
    MONGO_LOG(1) << "tables: " << dbString;
}

bool EloqKVEngine::databaseExists(std::string_view dbName) const {
    MONGO_LOG(1) << "EloqKVEngine::databaseExists"
                 << ". dbName: " << dbName;

    std::vector<std::string> tables;

    const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
    bool success = Eloq::GetAllTables(tables, coro.yieldFuncPtr, coro.resumeFuncPtr);

    if (!success) {
        error() << "Failed to discover collection names.";
        uassertStatusOK(Status{ErrorCodes::InternalError, "Failed to discover collection names."});
    }
    for (const auto& tableName : tables) {
        if (dbName == extractDbName(tableName)) {
            return true;
        }
    }
    return false;
}

void EloqKVEngine::listCollections(std::string_view dbName, std::vector<std::string>& out) const {
    MONGO_LOG(1) << "EloqKVEngine::listCollections"
                 << ". db: " << dbName;
    std::vector<std::string> allCollections;

    const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
    bool success = Eloq::GetAllTables(allCollections, coro.yieldFuncPtr, coro.resumeFuncPtr);

    if (!success) {
        error() << "Failed to discover collection names.";
        uassertStatusOK(Status{ErrorCodes::InternalError, "Failed to discover collection names."});
    }
    for (auto& collection : allCollections) {
        MONGO_LOG(1) << "dbname:" << extractDbName(collection);
        if (extractDbName(collection) == dbName) {
            out.push_back(std::move(collection));
        }
    }
    std::string str;
    for (const auto& name : out) {
        str.append(name).append("|");
    }
    MONGO_LOG(1) << "tables: " << str;
}
void EloqKVEngine::listCollections(std::string_view dbName, std::set<std::string>& out) const {
    MONGO_LOG(1) << "EloqKVEngine::listCollections"
                 << ". db: " << dbName;
    std::vector<std::string> allCollections;

    const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
    bool success = Eloq::GetAllTables(allCollections, coro.yieldFuncPtr, coro.resumeFuncPtr);

    if (!success) {
        error() << "Failed to discover collection names.";
        uassertStatusOK(Status{ErrorCodes::InternalError, "Failed to discover collection names."});
    }
    for (auto& collection : allCollections) {
        MONGO_LOG(1) << "dbname:" << extractDbName(collection);
        if (extractDbName(collection) == dbName) {
            out.emplace(std::move(collection));
        }
    }
    std::string str;
    for (const auto& name : out) {
        str.append(name).append("|");
    }
    MONGO_LOG(1) << "tables: " << str;
}

Status EloqKVEngine::lockCollection(
    OperationContext* opCtx, StringData ns, bool isForWrite, bool* exists, std::string* version) {
    MONGO_LOG(1) << "EloqKVEngine::lockCollection"
                 << ". ns: " << ns << ", isForWrite: " << isForWrite;
    auto ru = EloqRecoveryUnit::get(opCtx);
    txservice::TableName tableName = Eloq::MongoTableToTxServiceTableName(ns.toStringView(), false);

    // lockCollection bypass read from discovered table map. DatabaseImpl::getCollection() will
    // rebuild Collection handler/cache if version changed.
    txservice::CatalogKey catalogKey{tableName};
    txservice::CatalogRecord catalogRecord;
    auto [found, err] = ru->readCatalog(catalogKey, catalogRecord, isForWrite);
    if (err != txservice::TxErrorCode::NO_ERROR) {
        MONGO_LOG(1) << "ReadCatalog Error. [ErrorCode]: " << err << ". " << tableName.StringView();
        return TxErrorCodeToMongoStatus(err);
    }

    if (found) {
        *exists = true;
        *version = catalogRecord.Schema()->VersionStringView();

        auto schema =
            std::static_pointer_cast<const Eloq::MongoTableSchema>(catalogRecord.CopySchema());
        auto dirtySchema =
            std::static_pointer_cast<const Eloq::MongoTableSchema>(catalogRecord.CopyDirtySchema());
        ru->tryInsertDiscoveredTable(tableName, std::move(schema), std::move(dirtySchema));
    } else {
        *exists = false;
    }
    return Status::OK();
}

void EloqKVEngine::onAuthzDataChanged(OperationContext* opCtx) {
    MONGO_LOG(1) << "EloqKVEngine::onAuthzDataChanged";
    auto ru = EloqRecoveryUnit::get(opCtx);
    ru->notifyReloadCache(opCtx);
}

std::unique_ptr<RecordStore> EloqKVEngine::getRecordStore(OperationContext* opCtx,
                                                          StringData ns,
                                                          StringData ident,
                                                          const CollectionOptions& options) {
    MONGO_LOG(1) << "EloqKVEngine::getRecordStore"
                 << ". ns: " << ns << ". ident: " << ident;
    if (isMongoCatalog(ident.toStringView())) {
        return std::make_unique<EloqCatalogRecordStore>(opCtx, ns);
    }

    EloqRecordStore::Params params{
        Eloq::MongoTableToTxServiceTableName(ns.toStringView(), true), ns, ident, options.capped};

    if (options.capped) {
        params.cappedMaxSize = options.cappedSize ? options.cappedSize : 4096;
        params.cappedMaxDocs = options.cappedMaxDocs ? options.cappedMaxDocs : -1;
    }

    auto recordStore = std::make_unique<EloqRecordStore>(opCtx, params);
    return recordStore;
}

std::unique_ptr<RecordStore> EloqKVEngine::getGroupedRecordStore(OperationContext* opCtx,
                                                                 StringData ns,
                                                                 StringData ident,
                                                                 const CollectionOptions& options,
                                                                 KVPrefix prefix) {
    MONGO_LOG(1) << "EloqKVEngine::getGroupedRecordStore";

    invariant(prefix == KVPrefix::kNotPrefixed);
    return getRecordStore(opCtx, ns, ident, options);
}

Status EloqKVEngine::createRecordStore(OperationContext* opCtx,
                                       StringData ns,
                                       StringData ident,
                                       const CollectionOptions& options) {
    MONGO_LOG(1) << "EloqKVEngine::createRecordStore"
                 << ". ns: " << ns << ". ident: " << ident;
    if (!isMongoCatalog(ns.toStringView())) {
        MONGO_LOG(1) << "Eloq do nothing here for normal table";
        return Status::OK();
    }

    MONGO_UNREACHABLE;
}

Status EloqKVEngine::createGroupedRecordStore(OperationContext* opCtx,
                                              StringData ns,
                                              StringData ident,
                                              const CollectionOptions& options,
                                              KVPrefix prefix) {
    MONGO_LOG(1) << "EloqKVEngine::createGroupedRecordStore"
                 << ". prefix: " << prefix.toString();

    invariant(prefix == KVPrefix::kNotPrefixed);
    return createRecordStore(opCtx, ns, ident, options);
}

SortedDataInterface* EloqKVEngine::getSortedDataInterface(OperationContext* opCtx,
                                                          StringData ident,
                                                          const IndexDescriptor* desc) {
    MONGO_LOG(1) << "EloqKVEngine::getSortedDataInterface"
                 << ". ident: " << ident;

    txservice::TableName tableName{Eloq::MongoTableToTxServiceTableName(desc->parentNS(), true)};
    txservice::TableName indexName{desc->isIdIndex()
                                       ? tableName
                                       : Eloq::MongoIndexToTxServiceTableName(
                                             desc->parentNS(), desc->indexName(), desc->unique())};
    MONGO_LOG(1) << "tableName: " << tableName.StringView();

    std::unique_ptr<EloqIndex> index;

    if (desc->isIdIndex()) {
        MONGO_LOG(1) << "EloqKVEngine::getSortedDataInterface"
                     << ". IdIndex";
        index =
            std::make_unique<EloqIdIndex>(opCtx, std::move(tableName), std::move(indexName), desc);
    } else {
        if (desc->unique()) {
            MONGO_LOG(1) << "EloqKVEngine::getSortedDataInterface"
                         << ". UniqueIndex";
            index = std::make_unique<EloqUniqueIndex>(
                opCtx, std::move(tableName), std::move(indexName), desc);
        } else {
            MONGO_LOG(1) << "EloqKVEngine::getSortedDataInterface"
                         << ". StandardIndex";
            index = std::make_unique<EloqStandardIndex>(
                opCtx, std::move(tableName), std::move(indexName), desc);
        }
    }

    return index.release();
}

SortedDataInterface* EloqKVEngine::getGroupedSortedDataInterface(OperationContext* opCtx,
                                                                 StringData ident,
                                                                 const IndexDescriptor* desc,
                                                                 KVPrefix prefix) {
    MONGO_LOG(1) << "EloqKVEngine::getGroupedSortedDataInterface";

    invariant(prefix == KVPrefix::kNotPrefixed);
    return getSortedDataInterface(opCtx, ident, desc);
}

Status EloqKVEngine::createSortedDataInterface(OperationContext* opCtx,
                                               StringData ident,
                                               const IndexDescriptor* desc) {
    MONGO_LOG(1) << "EloqKVEngine::createSortedDataInterface. "
                 << "ident: " << ident;

    assert(!isMongoCatalog(ident.toStringView()));
    MONGO_LOG(1) << "Eloq do nothing here";
    return Status::OK();
    MONGO_UNREACHABLE;
}

Status EloqKVEngine::createGroupedSortedDataInterface(OperationContext* opCtx,
                                                      StringData ident,
                                                      const IndexDescriptor* desc,
                                                      KVPrefix prefix) {
    MONGO_LOG(1) << "EloqKVEngine::createGroupedSortedDataInterface";
    invariant(prefix == KVPrefix::kNotPrefixed);
    return createSortedDataInterface(opCtx, ident, desc);
}

int64_t EloqKVEngine::getIdentSize(OperationContext* opCtx, StringData ident) {
    MONGO_LOG(1) << "EloqKVEngine::getIdentSize"
                 << ". ident: " << ident;
    return 0;
}

Status EloqKVEngine::repairIdent(OperationContext* opCtx, StringData ident) {
    // MONGO_UNREACHABLE;
    // No need to repair
    return Status::OK();
}

Status EloqKVEngine::dropIdent(OperationContext* opCtx, StringData ident) {
    MONGO_LOG(1) << "EloqKVEngine::dropIdent"
                 << ". ident: " << ident;
    // Attention please!
    // EloqRecordStore and EloqIndex now have been destructed
    return Status::OK();
}

bool EloqKVEngine::supportsDirectoryPerDB() const {
    return false;
}

bool EloqKVEngine::hasIdent(OperationContext* opCtx, StringData ident) const {
    MONGO_LOG(1) << "EloqKVEngine::hasIdent"
                 << ". ident: " << ident;
    if (isMongoCatalog(ident.toStringView())) {
        return true;
    }
    MONGO_UNREACHABLE;
    // This function seems to be only used for checking _mdb_catalog
    // which always exists in Eloq (catalog_cc_map)
}

std::vector<std::string> EloqKVEngine::getAllIdents(OperationContext* opCtx) const {
    MONGO_LOG(1) << "EloqKVEngine::getAllIdents";
    std::vector<std::string> all;

    std::vector<std::string> tableNameVector;

    const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
    bool success = Eloq::GetAllTables(tableNameVector, coro.yieldFuncPtr, coro.resumeFuncPtr);

    if (!success) {
        error() << "Failed to discover collection names.";
        uassertStatusOK(Status{ErrorCodes::InternalError, "Failed to discover collection names."});
    }

    auto ru = EloqRecoveryUnit::get(opCtx);

    for (auto& nameString : tableNameVector) {
        if (isMongoCatalog(nameString)) {
            continue;
        }
        txservice::TableName tableName{
            std::move(nameString), txservice::TableType::Primary, txservice::TableEngine::EloqDoc};
        txservice::CatalogKey catalogKey{tableName};
        txservice::CatalogRecord catalogRecord;
        auto [exists, errorCode] = ru->readCatalog(catalogKey, catalogRecord, false);
        if (errorCode != txservice::TxErrorCode::NO_ERROR) {
            error() << "ReadCatalog Error. [ErrorCode]: " << errorCode << ", "
                    << txservice::TxErrorMessage(errorCode) << tableName.StringView();
            uassertStatusOK(TxErrorCodeToMongoStatus(errorCode));
        }
        if (!exists) {
            MONGO_LOG(0) << "Collection not exists. " << tableName.StringView();
            continue;
        }

        std::string metadata, kvInfo, keySchemaTsString;
        EloqDS::DeserializeSchemaImage(
            catalogRecord.Schema()->SchemaImage(), metadata, kvInfo, keySchemaTsString);

        BSONObj obj{metadata.data()};
        if (!metadata.empty()) {
            MONGO_LOG(1) << "metadata: " << obj.jsonString();
        }

        if (KVCatalog::FeatureTracker::isFeatureDocument(obj)) {
            all.emplace_back(kFeatureDocumentSV);
        } else {
            all.emplace_back(obj["ident"].String());
        }

        BSONElement e = obj["idxIdent"];
        if (!e.isABSONObj()) {
            continue;
        }
        BSONObj idxIdent = e.Obj();

        for (const auto& ident : idxIdent) {
            all.emplace_back(ident.String());
        }
    }

    std::string output;
    for (const auto& name : all) {
        output.append(name).append("|");
    }
    MONGO_LOG(1) << "idents: " << output;

    return all;
}

void EloqKVEngine::cleanShutdown() {
    MONGO_LOG(0) << "EloqKVEngine::cleanShutdown";
#ifndef ELOQ_MODULE_ENABLED
    auto* data_substrate = DataSubstrate::GetGlobal();
    if (data_substrate != nullptr) {
        data_substrate->Shutdown();
    }
#else
    // 1.When merged into ConvergedDB, `_txService->Shutdown()` should be moved out.
    // 2.eloq::unregister_module is not allowed to called in a brpc-worker thread.
    bool done = false;
    coro::Mutex mux;
    coro::ConditionVariable cv;
    std::thread thd([this, &done, &mux, &cv]() {
        std::unique_lock lk(mux);
        auto* data_substrate = DataSubstrate::GetGlobal();
        if (data_substrate != nullptr) {
            data_substrate->Shutdown();
        }
        done = true;
        cv.notify_one();
    });
    thd.detach();
    std::unique_lock lk(mux);
    cv.wait(lk, [&done]() { return done; });
#endif
    _txService = nullptr;
    _logServer = nullptr;
}

void EloqKVEngine::setJournalListener(JournalListener* jl) {
    //
}

Timestamp EloqKVEngine::getAllCommittedTimestamp() const {
    return Timestamp{0};
}

void EloqKVEngine::startOplogManager(OperationContext* opCtx, EloqRecordStore* oplogRecordStore) {
    //
}

void EloqKVEngine::haltOplogManager(EloqRecordStore* oplogRecordStore, bool shuttingDown) {
    //
}

inline void CustomPrefix(std::ostream& s, const google::LogMessageInfo& l, void*) {
    s << "["                                   //
      << std::setw(4) << 1900 + l.time.year()  // YY
      << '-'                                   // -
      << std::setw(2) << 1 + l.time.month()    // MM
      << '-'                                   // -
      << std::setw(2) << l.time.day()          // DD
      << 'T'                                   // T
      << std::setw(2) << l.time.hour()         // hh
      << ':'                                   // :
      << std::setw(2) << l.time.min()          // mm
      << ':'                                   // :
      << std::setw(2) << l.time.sec()          // ss
      << '.'                                   // .
      << std::setfill('0') << std::setw(6)     //
      << l.time.usec()                         // usec
      << " " << l.severity[0] << " "
      << "" << l.thread_id << "] "
#ifndef DISABLE_CODE_LINE_IN_LOG
      << "[" << l.filename << ':' << l.line_number << "]";
#else
        ;
#endif
};

void EloqKVEngine::InitGlog() {
    std::filesystem::path systemLogPath(serverGlobalParams.logpath);
    std::string logFilePath;
    if (systemLogPath.has_parent_path()) {
        logFilePath = systemLogPath.parent_path().string();
        logFilePath.append("/txservice");
    } else {
        logFilePath = "txservice";
    }
    FLAGS_logtostdout = false;
    FLAGS_logtostderr = false;
    FLAGS_minloglevel = 0;
    FLAGS_stderrthreshold = google::GLOG_FATAL;
    FLAGS_logbuflevel = -1;
    FLAGS_log_file_header = false;
    google::SetLogDestination(google::INFO, (logFilePath + ".INFO.").c_str());
    google::SetLogDestination(google::WARNING, (logFilePath + ".WARNING.").c_str());
    google::SetLogDestination(google::ERROR, (logFilePath + ".ERROR.").c_str());
    google::SetLogSymlink(google::INFO, "txservice");
    google::SetLogSymlink(google::WARNING, "txservice");
    google::SetLogSymlink(google::ERROR, "txservice");
    google::InitGoogleLogging("txservice", &CustomPrefix);
}

MongoSystemHandler::MongoSystemHandler() {
    thd_ = std::thread([this]() {
        while (shutdown_.load(std::memory_order_acquire) == false) {
            std::unique_lock lk(mux_);
            cv_.wait(lk, [this]() {
                return !work_queue_.empty() || shutdown_.load(std::memory_order_acquire);
            });

            if (!work_queue_.empty()) {
                std::packaged_task<bool()> work = std::move(work_queue_.front());
                work_queue_.pop_front();
                lk.unlock();
                work();
            }
        }
    });
}

MongoSystemHandler::~MongoSystemHandler() {
    std::unique_lock<std::mutex> lk(mux_);
    bool status = false;
    if (shutdown_.compare_exchange_strong(status, true, std::memory_order_relaxed)) {
        cv_.notify_one();
        lk.unlock();
        thd_.join();
    }
}

void MongoSystemHandler::ReloadCache(std::function<void(bool)> done) {
    std::packaged_task<bool()> work([done = std::move(done)]() {
        mongo::Status status = mongo::Status::OK();

        auto serviceContext = mongo::getGlobalServiceContext();
        if (serviceContext == nullptr || !serviceContext->isStartupComplete()) {
            done(true);
            return true;
        }

        auto client = mongo::getGlobalServiceContext()->makeClient("eloq_table_schema");
        auto opCtx = serviceContext->makeOperationContext(client.get());
        auto const globalAuthzManager = mongo::AuthorizationManager::get(serviceContext);

        for (int i = 0; i < 5; i++) {
            status = globalAuthzManager->initialize(opCtx.get());
            if (status.isOK()) {
                break;
            }
        }

        if (!status.isOK()) {
            mongo::error() << "reload_acl_and_cache failed";
        }
        done(status.isOK());
        return status.isOK();
    });

    SubmitWork(std::move(work));
}

void MongoSystemHandler::SubmitWork(std::packaged_task<bool()> work) {
    std::unique_lock<std::mutex> lk(mux_);
    if (shutdown_.load(std::memory_order_acquire) == false) {
        work_queue_.push_back(std::move(work));
        cv_.notify_one();
    }
}

}  // namespace mongo
