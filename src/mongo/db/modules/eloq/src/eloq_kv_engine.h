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

#include <memory>
#include <unordered_map>
#include <vector>

#include "mongo/db/storage/kv/kv_engine.h"

#include "mongo/db/modules/eloq/src/base/eloq_catalog_factory.h"
#include "mongo/db/modules/eloq/src/base/eloq_log_agent.h"
#include "mongo/db/modules/eloq/src/eloq_index.h"
#include "mongo/db/modules/eloq/src/eloq_record_store.h"

#ifdef OPEN_LOG_SERVICE
#include "mongo/db/modules/eloq/log_service/include/log_server.h"
#else
#include "mongo/db/modules/eloq/eloq_log_service/include/log_server.h"
#include "mongo/db/modules/eloq/eloq_log_service/include/log_utils.h"
#endif
#include "mongo/db/modules/eloq/tx_service/include/sharder.h"
#include "mongo/db/modules/eloq/tx_service/include/tx_service.h"

namespace mongo {
class MongoSystemHandler : public txservice::SystemHandler {
public:
    MongoSystemHandler();
    ~MongoSystemHandler() override;

    void ReloadCache(std::function<void(bool)> done) override;

private:
    void SubmitWork(std::packaged_task<bool()> work);

private:
    std::thread thd_;
    std::deque<std::packaged_task<bool()>> work_queue_;
    std::condition_variable cv_;
    std::mutex mux_;
    std::atomic<bool> shutdown_{false};
};

class EloqKVEngine final : public KVEngine {
    // friend class EloqRecordStore;

public:
    explicit EloqKVEngine(const std::string& path);
    void initDataStoreService(
        bool isSingleNode,
        uint32_t nodeId,
        uint32_t nativeNgId,
        const std::unordered_map<uint32_t, std::vector<txservice::NodeConfig>>& ng_configs);

    ~EloqKVEngine() override;

    // void waitBootstrap();

    // void notifyStartupComplete() override;

    // void checkpoint() override;

    bool supportsRecoveryTimestamp() const override;

    boost::optional<Timestamp> getRecoveryTimestamp() const override;

    // void setOldestActiveTransactionTimestampCallback(
    //     StorageEngine::OldestActiveTransactionTimestampCallback callback) override;

    RecoveryUnit* newRecoveryUnit() override;
    RecoveryUnit::UPtr newRecoveryUnitUPtr() override;

    void listDatabases(std::vector<std::string>& out) const override;
    bool databaseExists(std::string_view dbName) const override;
    void listCollections(std::string_view dbName, std::vector<std::string>& out) const override;
    void listCollections(std::string_view dbName, std::set<std::string>& out) const override;

    Status lockCollection(OperationContext* opCtx,
                          StringData ns,
                          bool isForWrite,
                          bool* exists,
                          std::string* version) override;

    void onAuthzDataChanged(OperationContext* opCtx) override;

    std::unique_ptr<RecordStore> getRecordStore(OperationContext* opCtx,
                                                StringData ns,
                                                StringData ident,
                                                const CollectionOptions& options) override;
    std::unique_ptr<RecordStore> getGroupedRecordStore(OperationContext* opCtx,
                                                       StringData ns,
                                                       StringData ident,
                                                       const CollectionOptions& options,
                                                       KVPrefix prefix) override;

    SortedDataInterface* getSortedDataInterface(OperationContext* opCtx,
                                                StringData ident,
                                                const IndexDescriptor* desc) override;
    SortedDataInterface* getGroupedSortedDataInterface(OperationContext* opCtx,
                                                       StringData ident,
                                                       const IndexDescriptor* desc,
                                                       KVPrefix prefix) override;

    Status createRecordStore(OperationContext* opCtx,
                             StringData ns,
                             StringData ident,
                             const CollectionOptions& options) override;

    Status createGroupedRecordStore(OperationContext* opCtx,
                                    StringData ns,
                                    StringData ident,
                                    const CollectionOptions& options,
                                    KVPrefix prefix) override;

    Status createSortedDataInterface(OperationContext* opCtx,
                                     StringData ident,
                                     const IndexDescriptor* desc) override;
    Status createGroupedSortedDataInterface(OperationContext* opCtx,
                                            StringData ident,
                                            const IndexDescriptor* desc,
                                            KVPrefix prefix) override;

    // Status dropSortedDataInterface(OperationContext* opCtx, StringData ident) override;

    int64_t getIdentSize(OperationContext* opCtx, StringData ident) override;

    Status repairIdent(OperationContext* opCtx, StringData ident) override;

    Status dropIdent(OperationContext* opCtx, StringData ident) override;

    // void dropIdentForImport(OperationContext* opCtx, StringData ident) override;

    bool isDurable() const override {
        return true;
    }

    bool isEphemeral() const override {
        return false;
    }

    bool supportsDocLocking() const override {
        return true;
    }

    bool supportsDirectoryPerDB() const override;

    bool supportsCappedCollections() const override {
        return false;
    }
    /*
     * retrieve Eloq catalog
     */
    bool hasIdent(OperationContext* opCtx, StringData ident) const override;

    /*
     * retrieve Mongo catalog recordstore
     */
    std::vector<std::string> getAllIdents(OperationContext* opCtx) const override;

    void cleanShutdown() override;

    void setJournalListener(JournalListener* jl) override;

    Timestamp getAllCommittedTimestamp() const override;

    bool supportsReadConcernSnapshot() const override {
        return true;
    }

    bool supportsReadConcernMajority() const override {
        return true;
    }

    void startOplogManager(OperationContext* opCtx, EloqRecordStore* oplogRecordStore);

    void haltOplogManager(EloqRecordStore* oplogRecordStore, bool shuttingDown);

private:
    bool InitMetricsRegistry();

    void InitGlog();

    void shutdownTxService();

private:
    std::unique_ptr<txservice::TxService> _txService;
    std::unique_ptr<txlog::LogServer> _logServer;
    Eloq::MongoCatalogFactory _catalogFactory;
    MongoSystemHandler _mongoSystemHandler;
    std::string _dbPath;
    std::unique_ptr<Eloq::MongoLogAgent> _logAgent;
    std::unique_ptr<metrics::MetricsRegistry> _metricsRegistry{nullptr};
};
}  // namespace mongo
