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

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/storage/record_store.h"

#include "mongo/db/modules/eloq/tx_service/include/type.h"

namespace mongo {
inline BSONObj kIdKeyPattern = BSON("_id" << 1);
inline Ordering kIdOrdering = Ordering::make(kIdKeyPattern);

class EloqCatalogRecordStore final : public RecordStore {
public:
    explicit EloqCatalogRecordStore(OperationContext* opCtx, StringData ns);
    EloqCatalogRecordStore(const EloqCatalogRecordStore&) = delete;
    EloqCatalogRecordStore(EloqCatalogRecordStore&&) = delete;
    EloqCatalogRecordStore& operator=(const EloqCatalogRecordStore&) = delete;
    EloqCatalogRecordStore& operator=(EloqCatalogRecordStore&&) = delete;

    ~EloqCatalogRecordStore() override;

    const char* name() const override {
        MONGO_UNREACHABLE;
    }

    const std::string& getIdent() const override {
        MONGO_UNREACHABLE;
    }

    long long dataSize(OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    long long numRecords(OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    bool isCapped() const override {
        MONGO_UNREACHABLE;
    }

    void setCappedCallback(CappedCallback* cb) override {
        MONGO_UNREACHABLE;
    }

    int64_t storageSize(OperationContext* opCtx,
                        BSONObjBuilder* extraInfo = nullptr,
                        int infoLevel = 0) const override {
        MONGO_UNREACHABLE;
    }

    RecordData dataFor(OperationContext* opCtx, const RecordId& loc) const override;

    bool findRecord(OperationContext* opCtx, const RecordId& id, RecordData* out) const override;
    bool findRecord(OperationContext* opCtx,
                    const RecordId& id,
                    RecordData* out,
                    bool isForWrite) const override;

    void deleteRecord(OperationContext* opCtx, const RecordId& id) override;

    StatusWith<RecordId> insertRecord(OperationContext* opCtx,
                                      const char* data,
                                      int len,
                                      Timestamp timestamp,
                                      bool enforceQuota) override;

    Status insertRecords(OperationContext* opCtx,
                         std::vector<Record>* records,
                         std::vector<Timestamp>* timestamps,
                         bool enforceQuota) override {
        MONGO_UNREACHABLE;
    }


    Status insertRecordsWithDocWriter(OperationContext* opCtx,
                                      const DocWriter* const* docs,
                                      const Timestamp* timestamps,
                                      size_t nDocs,
                                      RecordId* idsOut = nullptr) override {
        MONGO_UNREACHABLE;
    }

    Status updateRecord(OperationContext* opCtx,
                        const RecordId& id,
                        const char* data,
                        int len,
                        bool enforceQuota,
                        UpdateNotifier* notifier) override;

    bool updateWithDamagesSupported() const override {
        MONGO_UNREACHABLE;
    }

    StatusWith<RecordData> updateWithDamages(OperationContext* opCtx,
                                             const RecordId& loc,
                                             const RecordData& oldRec,
                                             const char* damageSource,
                                             const mutablebson::DamageVector& damages) override {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* opCtx,
                                                    bool forward = true) const override;

    std::unique_ptr<RecordCursor> getCursorForRepair(OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    std::unique_ptr<RecordCursor> getRandomCursor(OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    std::vector<std::unique_ptr<RecordCursor>> getManyCursors(
        OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    Status truncate(OperationContext* opCtx) override {
        MONGO_UNREACHABLE;
    }

    void cappedTruncateAfter(OperationContext* opCtx, RecordId end, bool inclusive) override {
        MONGO_UNREACHABLE;
    }

    bool compactSupported() const override {
        MONGO_UNREACHABLE;
    }

    Status validate(OperationContext* opCtx,
                    ValidateCmdLevel level,
                    ValidateAdaptor* adaptor,
                    ValidateResults* results,
                    BSONObjBuilder* output) override {
        MONGO_UNREACHABLE;
    }

    void appendCustomStats(OperationContext* opCtx,
                           BSONObjBuilder* result,
                           double scale) const override {
        MONGO_UNREACHABLE;
    }

    void updateStatsAfterRepair(OperationContext* opCtx,
                                long long numRecords,
                                long long dataSize) override {
        MONGO_UNREACHABLE;
    }

    void waitForAllEarlierOplogWritesToBeVisible(OperationContext* opCtx) const override {
        MONGO_UNREACHABLE;
    }

    void getAllCollections(std::vector<std::string>& collections) const override;

private:
    static constexpr uint16_t kMaxRetryLimit{10};
};

// every RecordStore is corresponding to a table in txservice
class EloqRecordStore final : public RecordStore {
public:
    struct Params {
        txservice::TableName tableName;
        StringData ns;
        StringData ident;
        bool isCapped{false};
        bool overwrite{false};
        int64_t cappedMaxSize{-1};
        int64_t cappedMaxDocs{-1};
        CappedCallback* cappedCallback{nullptr};
        bool isReadOnly{false};
    };

    explicit EloqRecordStore(OperationContext* opCtx, Params& params);
    EloqRecordStore(const EloqRecordStore&) = delete;
    EloqRecordStore(EloqRecordStore&&) = delete;
    EloqRecordStore& operator=(const EloqRecordStore&) = delete;
    EloqRecordStore& operator=(EloqRecordStore&&) = delete;

    ~EloqRecordStore() override;

    const txservice::TableName* tableName() const {
        return &_tableName;
    }

    const char* name() const override;

    const std::string& getIdent() const override;

    long long dataSize(OperationContext* opCtx) const override;

    long long numRecords(OperationContext* opCtx) const override;

    bool isCapped() const override;

    void setCappedCallback(CappedCallback* cb) override;

    int64_t storageSize(OperationContext* opCtx,
                        BSONObjBuilder* extraInfo = nullptr,
                        int infoLevel = 0) const override;

    RecordData dataFor(OperationContext* opCtx, const RecordId& loc) const override;

    bool findRecord(OperationContext* opCtx, const RecordId& id, RecordData* out) const override;

    void deleteRecord(OperationContext* opCtx, const RecordId& id) override;

    StatusWith<RecordId> insertRecord(OperationContext* opCtx,
                                      const char* data,
                                      int len,
                                      Timestamp timestamp,
                                      bool enforceQuota) override;

    Status insertRecords(OperationContext* opCtx,
                         std::vector<Record>* records,
                         std::vector<Timestamp>* timestamps,
                         bool enforceQuota) override;


    Status insertRecordsWithDocWriter(OperationContext* opCtx,
                                      const DocWriter* const* docs,
                                      const Timestamp* timestamps,
                                      size_t nDocs,
                                      RecordId* idsOut = nullptr) override;

    Status updateRecord(OperationContext* opCtx,
                        const RecordId& id,
                        const char* data,
                        int len,
                        bool enforceQuota,
                        UpdateNotifier* notifier) override;

    bool updateWithDamagesSupported() const override;

    StatusWith<RecordData> updateWithDamages(OperationContext* opCtx,
                                             const RecordId& loc,
                                             const RecordData& oldRec,
                                             const char* damageSource,
                                             const mutablebson::DamageVector& damages) override;

    std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* opCtx,
                                                    bool forward = true) const override;

    std::unique_ptr<RecordCursor> getCursorForRepair(OperationContext* opCtx) const override;

    std::unique_ptr<RecordCursor> getRandomCursor(OperationContext* opCtx) const override;

    std::vector<std::unique_ptr<RecordCursor>> getManyCursors(
        OperationContext* opCtx) const override;

    Status truncate(OperationContext* opCtx) override;

    void cappedTruncateAfter(OperationContext* opCtx, RecordId end, bool inclusive) override;

    bool compactSupported() const override;

    Status validate(OperationContext* opCtx,
                    ValidateCmdLevel level,
                    ValidateAdaptor* adaptor,
                    ValidateResults* results,
                    BSONObjBuilder* output) override;

    void appendCustomStats(OperationContext* opCtx,
                           BSONObjBuilder* result,
                           double scale) const override;

    void updateStatsAfterRepair(OperationContext* opCtx,
                                long long numRecords,
                                long long dataSize) override;

    void waitForAllEarlierOplogWritesToBeVisible(OperationContext* opCtx) const override;

private:
    Status _insertRecords(OperationContext* opCtx,
                          Record* records,
                          const Timestamp* timestamps,
                          size_t nRecords);

    // Mongo use this as the identity for storage engine.
    // For Eloq, _ident equals to _tableName
    std::string _ident;

    // table name in txservice. string owner
    txservice::TableName _tableName;

    const bool _isCatalog;
    // std::unordered_map<RecordId, std::string, RecordId::Hasher> _metadataCache;

    // The capped settings should not be updated once operations have started
    const bool _isCapped;

    // True if the namespace of this record store starts with "local.oplog.", and false otherwise.
    // Oplog is used to replication in Mongo which is useless for Eloq
    const bool _isOplog{false};

    // Optional. Specify a maximum size in bytes for a capped collection. Once a capped collection
    // reaches its maximum size, MongoDB removes the older documents to make space for the new
    // documents. The size field is required for capped collections and ignored for other
    // collections.
    int64_t _cappedMaxSize;
    // Optional. The maximum number of documents allowed in the capped collection. The size limit
    // takes precedence over this limit. If a capped collection reaches the size limit before it
    // reaches the maximum number of documents, MongoDB removes old documents. If you prefer to use
    // the max limit, ensure that the size limit, which is required for a capped collection, is
    // sufficient to contain the maximum number of documents.
    int64_t _cappedMaxDocs;

    // useless now
    CappedCallback* _cappedCallback;
    mutable stdx::mutex _cappedCallbackMutex;

    bool _shuttingDown;
};

}  // namespace mongo
