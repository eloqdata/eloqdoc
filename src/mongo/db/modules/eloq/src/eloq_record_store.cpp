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

#include <cassert>
#include <chrono>
#include <thread>
#include <utility>

#include "boost/optional/optional.hpp"

#include "mongo/base/error_codes.h"
#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/index/multikey_paths.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/kv/kv_catalog_feature_tracker.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/src/base/eloq_key.h"
#include "mongo/db/modules/eloq/src/base/eloq_record.h"
#include "mongo/db/modules/eloq/src/base/eloq_util.h"
#include "mongo/db/modules/eloq/src/eloq_record_store.h"
#include "mongo/db/modules/eloq/src/eloq_recovery_unit.h"
#include "mongo/db/modules/eloq/store_handler/kv_store.h"

#include "mongo/db/modules/eloq/tx_service/include/catalog_key_record.h"
#include "mongo/db/modules/eloq/tx_service/include/tx_key.h"
#include "mongo/db/modules/eloq/tx_service/include/tx_request.h"
#include "mongo/db/modules/eloq/tx_service/include/type.h"

#include <butil/time.h>
#include <bvar/latency_recorder.h>

namespace recorder {
bvar::LatencyRecorder kCatalogReadLatency{"mongo_catalog_read"};
bvar::LatencyRecorder bVarUpdateRecord("update_record");
}  // namespace recorder

namespace Eloq {
extern std::unique_ptr<txservice::store::DataStoreHandler> storeHandler;
}
namespace mongo {

class EloqCatalogRecordStoreCursor : public SeekableRecordCursor {
public:
    explicit EloqCatalogRecordStoreCursor(OperationContext* opCtx)
        : _ru{EloqRecoveryUnit::get(opCtx)} {
        MONGO_LOG(1) << "EloqCatalogRecordStoreCursor::EloqCatalogRecordStoreCursor";
        // always do full table scan

        const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
        Eloq::GetAllTables(_tableNameVector, coro.yieldFuncPtr, coro.resumeFuncPtr);
        std::string output;
        for (const auto& name : _tableNameVector) {
            output.append(name).append("|");
        }
        MONGO_LOG(1) << "tables: " << output;
        _iter = _tableNameVector.begin();
    }

    EloqCatalogRecordStoreCursor(const EloqCatalogRecordStoreCursor&) = delete;
    EloqCatalogRecordStoreCursor(EloqCatalogRecordStoreCursor&&) = delete;
    EloqCatalogRecordStoreCursor& operator=(const EloqCatalogRecordStoreCursor&) = delete;
    EloqCatalogRecordStoreCursor& operator=(EloqCatalogRecordStoreCursor&&) = delete;

    ~EloqCatalogRecordStoreCursor() override {
        MONGO_LOG(1) << "EloqCatalogRecordStoreCursor::~EloqCatalogRecordStoreCursor";
    }

    boost::optional<Record> next() override {
        MONGO_LOG(1) << "EloqCatalogRecordStoreCursor::next";
        // Traverse the _tableNamevector until find a exist table and then return the metadata
        while (_iter != _tableNameVector.end()) {
            RecordId id{*_iter};
            txservice::TableName tableName{
                std::move(*_iter), txservice::TableType::Primary, txservice::TableEngine::EloqDoc};
            ++_iter;

            txservice::CatalogKey catalogKey{tableName};
            txservice::CatalogRecord catalogRecord;
            auto [exists, errorCode] = _ru->readCatalog(catalogKey, catalogRecord, false);
            uassertStatusOK(TxErrorCodeToMongoStatus(errorCode));

            if (!exists) {
                continue;
            }

            // Make sure _metadata is empty before DeserializeSchemaImage
            _metadata.clear();
            Eloq::DeserializeSchemaImage(catalogRecord.Schema()->SchemaImage(), _metadata);
            if (!_metadata.empty()) {
                MONGO_LOG(1) << "metadata: " << BSONObj{_metadata.data()}.jsonString();
            }
            return {{std::move(id), {_metadata.data(), static_cast<int>(_metadata.size())}}};
        }

        return {};
    }

    boost::optional<Record> seekExact(const RecordId& id) override {
        MONGO_UNREACHABLE;
    }

    void saveUnpositioned() override {
        MONGO_UNREACHABLE;
    }

    void save() override {
        MONGO_UNREACHABLE;
    }

    bool restore() override {
        MONGO_UNREACHABLE;
    }

    void detachFromOperationContext() override {
        MONGO_UNREACHABLE;
    }
    void reattachToOperationContext(OperationContext* opCtx) override {
        MONGO_UNREACHABLE;
    }

private:
    EloqRecoveryUnit* _ru;  // not owned

    std::vector<std::string> _tableNameVector;
    std::vector<std::string>::iterator _iter;

    // The RecordData returned after calling next() is actually store in here.
    // We should guarantee the corresponding memory is valid until the next time next() is called
    // so that thereis no need let the RecordData object getOwned and  we can avoid a memory
    // allocation.
    std::string _metadata;
    // std::string _kvInfo;              // useless now
    // std::string _keySchemasTsString;  // useless now
};


EloqCatalogRecordStore::EloqCatalogRecordStore(OperationContext* opCtx, StringData ns)
    : RecordStore{ns} {
    MONGO_LOG(1) << "EloqCatalogRecordStore::EloqCatalogRecordStore";
}

EloqCatalogRecordStore::~EloqCatalogRecordStore() {
    MONGO_LOG(1) << "EloqCatalogRecordStore::~EloqCatalogRecordStore";
}

RecordData EloqCatalogRecordStore::dataFor(OperationContext* opCtx, const RecordId& loc) const {
    MONGO_LOG(1) << "EloqCatalogRecordStore::dataFor";
    RecordData data;
    invariant(findRecord(opCtx, loc, &data));
    return data;
}

bool EloqCatalogRecordStore::findRecord(OperationContext* opCtx,
                                        const RecordId& id,
                                        RecordData* out) const {
    return findRecord(opCtx, id, out, false);
}

bool EloqCatalogRecordStore::findRecord(OperationContext* opCtx,
                                        const RecordId& id,
                                        RecordData* out,
                                        bool isForWrite) const {
    MONGO_LOG(1) << "EloqCatalogRecordStore::findRecord"
                 << ". id: " << id.toString();

    txservice::TableName tableName{
        id.getStringView(), txservice::TableType::Primary, txservice::TableEngine::EloqDoc};
    MONGO_LOG(1) << "tableName: " << tableName.StringView();

    auto ru = EloqRecoveryUnit::get(opCtx);
    const auto [table, err] = ru->discoverTable(tableName, isForWrite);
    if (err != txservice::TxErrorCode::NO_ERROR) {
        uassertStatusOK(TxErrorCodeToMongoStatus(err));
    }

    if (table == nullptr) {
        return false;
    }

    if (MONGO_unlikely(!ru->unreadyIsEmpty())) {
        auto bsonObj = ru->getUnreadyTable(tableName);
        if (!bsonObj.isEmpty()) {
            MONGO_LOG(1) << "Fetch metadata from unready table cache.";
            *out = RecordData{bsonObj.objdata(), static_cast<int>(bsonObj.objsize())};
            return true;
        }
    }

    auto tableSchema = static_cast<const Eloq::MongoTableSchema*>(table->_schema.get());

    const std::string& metadata = tableSchema->MetaDataStr();
    *out = RecordData{metadata.data(), static_cast<int>(metadata.size())};
    return true;
}

void EloqCatalogRecordStore::deleteRecord(OperationContext* opCtx, const RecordId& id) {
    MONGO_LOG(1) << "EloqCatalogRecordStore::deleteRecord"
                 << ". id: " << id;

    txservice::TableName tableName{
        id.getStringView(), txservice::TableType::Primary, txservice::TableEngine::EloqDoc};
    MONGO_LOG(1) << "tableName: " << tableName.StringView();
    txservice::CatalogKey catalogKey{tableName};
    txservice::CatalogRecord catalogRecord;

    auto ru = EloqRecoveryUnit::get(opCtx);
    for (uint16_t i = 1; i < kMaxRetryLimit; ++i) {
        auto [exist, errorCode] = ru->readCatalog(catalogKey, catalogRecord, true);
        if (errorCode != txservice::TxErrorCode::NO_ERROR) {
            MONGO_LOG(1) << "Eloq readCatalog error with write intent. Another transaction "
                            "may do DDL on the same table.";
        } else {
            if (!exist) {
                return;
            }

            auto status = ru->dropTable(tableName, catalogRecord);
            if (status.isOK()) {
                ru->deleteDiscoveredTable(tableName);
                return;
            }
        }

        MONGO_LOG(1) << "Fail to drop table in Eloq";
        opCtx->sleepForRandomMilliseconds();
        MONGO_LOG(1) << "Retry count: " << i;
        catalogRecord.Reset();
    }

    error() << "[Drop Table] opertion reaches the maximum number of retries.";
    uasserted(70001, "[Drop Table] opertion reaches the maximum number of retries.");
}

StatusWith<RecordId> EloqCatalogRecordStore::insertRecord(
    OperationContext* opCtx, const char* data, int len, Timestamp timestamp, bool enforceQuota) {
    MONGO_LOG(1) << "EloqCatalogRecordStore::insertRecord. Insert into txservice catalog_cc_map";

    auto ru = EloqRecoveryUnit::get(opCtx);
    RecordId recordId;
    BSONObj obj{data};
    if (auto nsElem = obj["ns"]; nsElem.ok() && !nsElem.isNull()) {
        recordId = RecordId{nsElem.valuestr(), static_cast<size_t>(nsElem.valuestrsize() - 1)};
    } else {
        dassert(KVCatalog::FeatureTracker::isFeatureDocument(obj));
        recordId = RecordId{kFeatureDocumentSV};
    }
    MONGO_LOG(1) << "record id: " << recordId.toString() << ". data: " << obj.jsonString();

    txservice::TableName tableName{
        recordId.getStringView(), txservice::TableType::Primary, txservice::TableEngine::EloqDoc};
    if (Eloq::ContainsUnreadyIndex(obj)) {
        ru->putUnreadyTable(tableName, obj);
        MONGO_LOG(1) << "Put unready table into cache. TableName: " << tableName.StringView();
        return {recordId};
    } else {
        if (!ru->unreadyIsEmpty()) {
            ru->eraseUnreadyTable(tableName);
            MONGO_LOG(1) << "Erase unready table for tableName: " << tableName.StringView();
        }
    }

    std::string_view metadata{data, static_cast<size_t>(len)};
    txservice::CatalogKey catalogKey{tableName};
    txservice::CatalogRecord catalogRecord;

    auto [exist, errorCode] = ru->readCatalog(catalogKey, catalogRecord, true);
    if (errorCode != txservice::TxErrorCode::NO_ERROR) {
        if (errorCode == txservice::TxErrorCode::WRITE_WRITE_CONFLICT) {
            MONGO_LOG(1) << "Eloq readCatalog error with write intent. Another transaction "
                            "may do DDL on the same table.";
            return {ErrorCodes::WriteConflict,
                    "[Create Table] Another transaction may do DDL on the same table"};
        } else {
            MONGO_LOG(0) << "Eloq readCatalog error with write intent."
                         << txservice::TxErrorMessage(errorCode);
            return {ErrorCodes::InternalError, txservice::TxErrorMessage(errorCode)};
        }
    } else {
        if (exist) {
            const char* msg = "Collection already exists in Eloq storage engine";
            warning() << msg << ", ns: " << tableName.StringView();
            return {ErrorCodes::NamespaceExists, msg};
        }

        auto status = ru->createTable(tableName, metadata);
        if (status.isOK()) {
            return {recordId};
        } else {
            MONGO_LOG(1) << "Fail to create table in Eloq. Error: " << status.toString();
            return status;
        }
    }
}

Status EloqCatalogRecordStore::updateRecord(OperationContext* opCtx,
                                            const RecordId& id,
                                            const char* data,
                                            int len,
                                            bool enforceQuota,
                                            UpdateNotifier* notifier) {
    MONGO_LOG(1) << "EloqCatalogRecordStore::updateRecord";

    BSONObj obj{data};
    MONGO_LOG(1) << ". id: " << id << ". data: " << obj.jsonString();

    txservice::TableName tableName{
        id.getStringView(), txservice::TableType::Primary, txservice::TableEngine::EloqDoc};
    MONGO_LOG(1) << "tableName: " << tableName.StringView();

    auto ru = EloqRecoveryUnit::get(opCtx);
    if (Eloq::ContainsUnreadyIndex(obj)) {
        ru->putUnreadyTable(tableName, obj);
        MONGO_LOG(1) << "Put unready table into cache. TableName: " << tableName.StringView();
        return Status::OK();
    } else {
        if (!ru->unreadyIsEmpty()) {
            ru->eraseUnreadyTable(tableName);
            MONGO_LOG(1) << "Erase unready table for tableName: " << tableName.StringView();
        }
    }

    std::string_view newMetadata{data, static_cast<size_t>(len)};

    txservice::CatalogKey catalogKey{tableName};
    txservice::CatalogRecord catalogRecord;


    for (uint16_t i = 1; i < kMaxRetryLimit; ++i) {
        auto [exist, errorCode] = ru->readCatalog(catalogKey, catalogRecord, true);
        if (errorCode != txservice::TxErrorCode::NO_ERROR) {
            MONGO_LOG(1) << "Eloq readCatalog error with write intent. Another transaction "
                            "may do DDL on the same table.";
        } else {
            if (!exist) {
                return {ErrorCodes::InternalError, "Try to Update a non-exist table"};
            }
            std::string oldMetadata, kvInfo, keySchemaTsString;
            EloqDS::DeserializeSchemaImage(
                catalogRecord.Schema()->SchemaImage(), oldMetadata, kvInfo, keySchemaTsString);

            bool insideDmlTxn = false;
            std::string newSchemaImage;
            Status status = ru->updateTable(
                tableName, catalogRecord, oldMetadata, newMetadata, &newSchemaImage, &insideDmlTxn);
            if (status.isOK()) {
                // If the transaction is a pure DDL transaction, it will commit immediatelly.
                // If the transaction is a DML transaction which triggers a DDL transaction, it will
                // commit until EloqRecoveryUnit::commitUnitOfWork().
                if (insideDmlTxn) {
                    uint64_t pendingVersion = catalogRecord.SchemaTs() + 1;
                    ru->updateDiscoveredTable(tableName, newSchemaImage, pendingVersion);
                    return Status::OK();
                } else {
                    ru->deleteDiscoveredTable(tableName);
                    return Status::OK();
                }
            } else {
                return status;
            }
        }

        MONGO_LOG(1) << "Fail to create table in Eloq.";
        opCtx->sleepForRandomMilliseconds();
        MONGO_LOG(1) << "Retry count: " << i;
        catalogRecord.Reset();
    }

    return {ErrorCodes::InternalError,
            "[Create Table] opertion reaches the maximum number of retries."};
}

std::unique_ptr<SeekableRecordCursor> EloqCatalogRecordStore::getCursor(OperationContext* opCtx,
                                                                        bool forward) const {
    MONGO_LOG(1) << "EloqCatalogRecordStore::getCursor";

    return std::make_unique<EloqCatalogRecordStoreCursor>(opCtx);
}

void EloqCatalogRecordStore::getAllCollections(std::vector<std::string>& collections) const {
    MONGO_LOG(1) << "EloqCatalogRecordStore::getAllCollections";

    const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
    Eloq::GetAllTables(collections, coro.yieldFuncPtr, coro.resumeFuncPtr);
    std::string output;
    for (const auto& name : collections) {
        output.append(name).append("|");
    }
    MONGO_LOG(1) << "tables: " << output;
}

class EloqRecordStoreCursor : public SeekableRecordCursor {
public:
    explicit EloqRecordStoreCursor(OperationContext* opCtx, const EloqRecordStore* rs, bool forward)
        : _opCtx{opCtx},
          _ru{EloqRecoveryUnit::get(opCtx)},
          _tableName{rs->tableName()},
          _keySchema(_ru->getIndexSchema(*rs->tableName())),
          _forward{forward} {
        MONGO_LOG(1) << "EloqRecordStoreCursor::EloqRecordStoreCursor";
    }

    EloqRecordStoreCursor(const EloqRecordStoreCursor&) = delete;
    EloqRecordStoreCursor(EloqRecordStoreCursor&&) = delete;
    EloqRecordStoreCursor& operator=(const EloqRecordStoreCursor&) = delete;
    EloqRecordStoreCursor& operator=(EloqRecordStoreCursor&&) = delete;

    ~EloqRecordStoreCursor() override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::~EloqRecordStoreCursor";
    }

    void reset(OperationContext* opCtx, const EloqRecordStore* rs, bool forward) {
        _opCtx = opCtx;
        _ru = EloqRecoveryUnit::get(opCtx);
        _tableName = rs->tableName();
        _keySchema = _ru->getIndexSchema(*rs->tableName());
        _forward = forward;
        _eof = false;
        _lastMongoKey.reset();
        _cursor.reset();
    }

    boost::optional<Record> next() override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::next"
                     << ". forward: " << _forward;
        if (_eof) {
            return {};
        }

        if (!_cursor) {
            _seekCursor();
        }
        assert(_cursor);

        txservice::TxErrorCode txErr = _cursor->nextBatchTuple();
        uassertStatusOK(TxErrorCodeToMongoStatus(txErr));

        const txservice::ScanBatchTuple* scanTuple = _cursor->currentBatchTuple();
        if (scanTuple == nullptr) {
            MONGO_LOG(1) << "reach the end";
            _eof = true;
            return {};
        }

        const auto* key = scanTuple->key_.GetKey<Eloq::MongoKey>();
        const auto* record = static_cast<const Eloq::MongoRecord*>(scanTuple->record_);
        if (key == nullptr) {
            MONGO_LOG(1) << "reach the end";
            _eof = true;
            return {};
        }

        RecordId id = key->ToRecordId(false);
        MONGO_LOG(1) << "id: " << id
                     << ". record:" << BSONObj{record->EncodedBlobData()}.jsonString();
        return {{std::move(id),
                 {record->EncodedBlobData(), static_cast<int>(record->EncodedBlobSize())}}};
    }

    boost::optional<Record> seekExact(const RecordId& id) override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::seekExact. table: " << _tableName->StringView()
                     << ", id: " << id;

        if (_cursor) {
            _cursor.reset();
        }

        EloqKVPair& kvPair = _ru->getKVPair();
        Eloq::MongoKey& store_pkey = kvPair.keyRef();
        const Eloq::MongoRecord* store_record = kvPair.getValuePtr();

        // _id don't need getKV if it has been stored in KVPair
        if (store_record == nullptr || store_pkey.PackedKeyStringView() != id.getStringView()) {
            store_pkey.SetPackedKey(id);
            bool isForWrite = _opCtx->isUpsert();
            auto [exists, err] =
                _ru->getKVInternal(_opCtx, *_tableName, _keySchema->SchemaTs(), isForWrite);
            uassertStatusOK(TxErrorCodeToMongoStatus(err));
            if (!exists) {
                MONGO_LOG(1) << "no found. id: " << id << ". Txservice error code: " << err;
                return {};
            }
            store_record = kvPair.getValuePtr();
            MONGO_LOG(1) << "keyStore:" << store_pkey.ToString();
        }

        if (_lastMongoKey) {
            _lastMongoKey->Copy(store_pkey);
        } else {
            _lastMongoKey.emplace(store_pkey);
        }

        return {
            {id,
             {store_record->EncodedBlobData(), static_cast<int>(store_record->EncodedBlobSize())}}};
    }

    void saveUnpositioned() override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::saveUnpositioned";
        _lastMongoKey.reset();
        _cursor.reset();
    }

    void save() override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::save";
        if (!_eof && _cursor && _cursor->currentBatchTuple() != nullptr) {
            _lastMongoKey.emplace(*_cursor->currentBatchTuple()->key_.GetKey<Eloq::MongoKey>());
        }
        _cursor.reset();
    }

    bool restore() override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::restore";
        assert(!_cursor);
        // Don't open scan here.
        // Mongo may call seekExact which don't need a scan in TxService
        return true;
    }

    void detachFromOperationContext() override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::detachFromOperationContext";
        assert(_opCtx);
        _opCtx = nullptr;
        _ru = nullptr;
    }

    void reattachToOperationContext(OperationContext* opCtx) override {
        MONGO_LOG(1) << "EloqRecordStoreCursor::reattachToOperationContext";
        assert(!_opCtx);
        _opCtx = opCtx;
        _ru = EloqRecoveryUnit::get(opCtx);
    }

private:
    void _seekCursor(bool startInclusive = false) {
        MONGO_LOG(1) << "EloqRecordStoreCursor::_seekIter";

        _cursor.emplace(_opCtx);
        if (_lastMongoKey) {
            _startKey = txservice::TxKey(&_lastMongoKey.get());
        } else {
            if (_forward) {
                _startKey = Eloq::MongoKey::GetNegInfTxKey();
            } else {
                _startKey = Eloq::MongoKey::GetPosInfTxKey();
            }
        }
        if (_forward) {
            _endKey = Eloq::MongoKey::GetPosInfTxKey();
        } else {
            _endKey = Eloq::MongoKey::GetNegInfTxKey();
        }

        bool endInclusive = false;
        bool isForWrite = _opCtx->isUpsert();
        _cursor->indexScanOpen(_tableName,
                               _keySchema->SchemaTs(),
                               txservice::ScanIndexType::Primary,
                               &_startKey,
                               startInclusive,
                               &_endKey,
                               endInclusive,
                               _forward ? txservice::ScanDirection::Forward
                                        : txservice::ScanDirection::Backward,
                               isForWrite);
    }

    OperationContext* _opCtx;                         // not owned
    EloqRecoveryUnit* _ru;                            // not owned
    const txservice::TableName* _tableName{nullptr};  // not owned
    const txservice::KeySchema* _keySchema{nullptr};  // not owned

    bool _forward;
    bool _eof{false};
    boost::optional<Eloq::MongoKey> _lastMongoKey;

    // const Eloq::MongoKey* _scanTupleKey{nullptr};
    // const Eloq::MongoRecord* _scanTupleRecord{nullptr};

    txservice::TxKey _startKey;
    txservice::TxKey _endKey;

    // Mongo use EloqRecordStoreCursor even for exact match operation
    // which actually does not need construct a Cursor in Eloq's design.
    // So use boost::optional to delay the contruction
    boost::optional<EloqCursor> _cursor{boost::none};
};


EloqRecordStore::EloqRecordStore(OperationContext* opCtx, Params& params)
    : RecordStore{params.ns},
      _tableName{std::move(params.tableName)},
      _isCatalog{isMongoCatalog(params.ident.toStringView())},
      _isCapped{params.isCapped},
      _cappedMaxSize{params.cappedMaxSize},
      _cappedMaxDocs{params.cappedMaxDocs},
      _cappedCallback{params.cappedCallback},
      _shuttingDown{false} {
    MONGO_LOG(1) << "EloqRecordStore::EloqRecordStore";

    if (_isCapped) {
        invariant(_cappedMaxSize > 0);
        invariant(_cappedMaxDocs == -1 || _cappedMaxDocs > 0);
    } else {
        invariant(_cappedMaxSize == -1);
        invariant(_cappedMaxDocs == -1);
    }
}

EloqRecordStore::~EloqRecordStore() {
    MONGO_LOG(1) << "EloqRecordStoreCursor::~EloqRecordStore";
    {
        stdx::lock_guard<stdx::mutex> lk(_cappedCallbackMutex);
        _shuttingDown = true;
    }

    MONGO_LOG(1) << "EloqRecordStore ns: " << ns();
}

const char* EloqRecordStore::name() const {
    return kEloqEngineName.rawData();
}

const std::string& EloqRecordStore::getIdent() const {
    return _ident;
}

long long EloqRecordStore::dataSize(OperationContext* opCtx) const {
    MONGO_LOG(1) << "EloqRecordStore::dataSize";
    return 0;
}

long long EloqRecordStore::numRecords(OperationContext* opCtx) const {
    MONGO_LOG(1) << "EloqRecordStore::numRecords";

    auto ru = EloqRecoveryUnit::get(opCtx);
    const auto [table, err] = ru->discoverTable(_tableName);
    if (err != txservice::TxErrorCode::NO_ERROR) {
        MONGO_LOG(1) << "EloqRecordStore::numRecords"
                     << ". Fail to discover table";
        return -1;
    }

    if (table == nullptr || table->_schema->StatisticsObject() == nullptr) {
        MONGO_LOG(1) << "EloqRecordStore::numRecords"
                     << ". table or table statistics not exists.";
        return -1;
    }

    const txservice::Distribution* distribution =
        table->_schema->StatisticsObject()->GetDistribution(_tableName);
    if (distribution != nullptr) {
        // Currently, the accuracy of the record count is guaranteed only for single-core
        // single-machine setups and for up to 1000 entries in multi-core single-machine setups.
        // You should not rely on it.
        auto size = distribution->Records();
        MONGO_LOG(1) << "EloqRecordStore::numRecords"
                     << ". size: " << size;
        return static_cast<long long>(size);
    } else {
        MONGO_LOG(1) << "EloqRecordStore::numRecords"
                     << ". distribution == nullptr";
        return 0;
    }
}

bool EloqRecordStore::isCapped() const {
    MONGO_LOG(1) << "EloqRecordStore::isCapped";
    return _isCapped;
}

void EloqRecordStore::setCappedCallback(CappedCallback* cb) {
    MONGO_LOG(1) << "EloqRecordStore::setCappedCallback";
    std::scoped_lock<stdx::mutex> lk{_cappedCallbackMutex};
    _cappedCallback = cb;
}

int64_t EloqRecordStore::storageSize(OperationContext* opCtx,
                                     BSONObjBuilder* extraInfo,
                                     int infoLevel) const {
    MONGO_LOG(1) << "EloqRecordStore::storageSize";
    int64_t size = dataSize(opCtx);
    MONGO_LOG(1) << "size: " << size;
    return size;
}

RecordData EloqRecordStore::dataFor(OperationContext* opCtx, const RecordId& loc) const {
    MONGO_LOG(1) << "EloqRecordStore::dataFor";
    RecordData data;
    invariant(findRecord(opCtx, loc, &data));
    return data;
}

bool EloqRecordStore::findRecord(OperationContext* opCtx,
                                 const RecordId& id,
                                 RecordData* out) const {
    // butil::Timer timer;
    // timer.start();
    MONGO_LOG(1) << "EloqRecordStore::findRecord"
                 << ". id: " << id.toString();

    auto ru = EloqRecoveryUnit::get(opCtx);

    Eloq::MongoKey mongoKey(id);
    Eloq::MongoRecord mongoRecord;
    uint64_t keySchemaVersion = ru->getIndexSchema(_tableName)->SchemaTs();

    bool isForWrite = opCtx->isUpsert();
    auto [exists, err] =
        ru->getKV(opCtx, _tableName, keySchemaVersion, &mongoKey, &mongoRecord, isForWrite);
    uassertStatusOK(TxErrorCodeToMongoStatus(err));
    if (!exists) {
        MONGO_LOG(1) << "not exists";
        return false;
    }

    *out =
        RecordData{mongoRecord.EncodedBlobData(), static_cast<int>(mongoRecord.EncodedBlobSize())}
            .getOwned();


    // timer.stop();
    // recorder::kCatalogReadLatency << timer.u_elapsed();
    return true;
}

void EloqRecordStore::deleteRecord(OperationContext* opCtx, const RecordId& id) {
    MONGO_LOG(1) << "EloqRecordStore::deleteRecord"
                 << ". id: " << id;
    auto ru = EloqRecoveryUnit::get(opCtx);

    const EloqRecoveryUnit::DiscoveredTable& table = ru->discoveredTable(_tableName);

    // For primary index.
    auto mongoKey = std::make_unique<Eloq::MongoKey>(id);
    Eloq::MongoRecord mongoRecord;
    uint64_t keySchemaVersion = table._schema->KeySchema()->SchemaTs();

    // read the record if the table is creating indexes.
    if (table._creatingIndexes.size() > 0) {
        auto [exists, err] =
            ru->getKV(opCtx, _tableName, keySchemaVersion, mongoKey.get(), &mongoRecord, true);
        uassertStatusOK(TxErrorCodeToMongoStatus(err));
    }

    auto err = ru->setKV(_tableName,
                         keySchemaVersion,
                         std::move(mongoKey),
                         nullptr,
                         txservice::OperationType::Delete,
                         false);
    uassertStatusOK(TxErrorCodeToMongoStatus(err));

    // remove record from creating index.
    if (table._creatingIndexes.size() > 0) {
        BSONObj recordObj(mongoRecord.EncodedBlobData());
        for (const EloqRecoveryUnit::SecondaryIndex* index : table._creatingIndexes) {
            const txservice::TableName& indexName = index->first;
            const auto* keySchema =
                static_cast<const Eloq::MongoKeySchema*>(index->second.sk_schema_.get());

            BSONObjSet keys = SimpleBSONObjComparator::kInstance.makeBSONObjSet();
            MultikeyPaths multikeyPaths;
            keySchema->GetKeys(recordObj, &keys, &multikeyPaths);
            for (const BSONObj& bsonKey : keys) {
                KeyString keyString(KeyString::kLatestVersion, bsonKey, keySchema->Ordering(), id);
                auto mongoKey =
                    std::make_unique<Eloq::MongoKey>(keyString.getBuffer(), keyString.getSize());
                err = ru->setKV(indexName,
                                keySchema->SchemaTs(),
                                std::move(mongoKey),
                                nullptr,
                                txservice::OperationType::Delete,
                                false);
                uassertStatusOK(TxErrorCodeToMongoStatus(err));
            }
        }
    }
}

StatusWith<RecordId> EloqRecordStore::insertRecord(
    OperationContext* opCtx, const char* data, int len, Timestamp timestamp, bool enforceQuota) {
    MONGO_LOG(1) << "EloqRecordStore::insertRecord";
    Record record{RecordId{}, RecordData{data, len}};
    Status status = _insertRecords(opCtx, &record, &timestamp, 1);
    if (!status.isOK()) {
        return {status};
    }
    return {record.id};
}

Status EloqRecordStore::insertRecords(OperationContext* opCtx,
                                      std::vector<Record>* records,
                                      std::vector<Timestamp>* timestamps,
                                      bool enforceQuota) {
    MONGO_LOG(1) << "EloqRecordStore::insertRecords";
    return _insertRecords(opCtx, records->data(), timestamps->data(), records->size());
}


Status EloqRecordStore::insertRecordsWithDocWriter(OperationContext* opCtx,
                                                   const DocWriter* const* docs,
                                                   const Timestamp* timestamps,
                                                   size_t nDocs,
                                                   RecordId* idsOut) {
    MONGO_LOG(1) << "EloqRecordStore::insertRecordsWithDocWriter";
    return Status::OK();
}

Status EloqRecordStore::updateRecord(OperationContext* opCtx,
                                     const RecordId& id,
                                     const char* data,
                                     int len,
                                     bool enforceQuota,
                                     UpdateNotifier* notifier) {
    butil::Timer timer;
    timer.start();
    auto recordLatency = [&timer]() {
        timer.stop();
        recorder::bVarUpdateRecord << timer.u_elapsed();
    };
    auto guard = MakeGuard(recordLatency);

    mongo::BSONObj recordObj(data);
    MONGO_LOG(1) << "EloqRecordStore::updateRecord"
                 << ". id: " << id;

    auto ru = EloqRecoveryUnit::get(opCtx);

    const EloqRecoveryUnit::DiscoveredTable& table = ru->discoveredTable(_tableName);

    // For primary index
    auto mongoKey = std::make_unique<Eloq::MongoKey>(id);
    auto mongoRecord = std::make_unique<Eloq::MongoRecord>();
    uint64_t pkeySchemaVersion = table._schema->KeySchema()->SchemaTs();

    mongoRecord->SetEncodedBlob(reinterpret_cast<const unsigned char*>(data), len);
    auto err = ru->setKV(_tableName,
                         pkeySchemaVersion,
                         std::move(mongoKey),
                         std::move(mongoRecord),
                         txservice::OperationType::Update,
                         false);
    if (err != txservice::TxErrorCode::NO_ERROR) {
        return TxErrorCodeToMongoStatus(err);
    }

    // For creating index
    try {
        for (const EloqRecoveryUnit::SecondaryIndex* index : table._creatingIndexes) {
            const txservice::TableName& indexName = index->first;
            const auto* keySchema =
                static_cast<const Eloq::MongoKeySchema*>(index->second.sk_schema_.get());
            if (keySchema->Unique()) {
                uasserted(ErrorCodes::ConflictingOperationInProgress,
                          str::stream()
                              << "A conflict create-unique-index transaction is running.");
            }

            BSONObjSet keys = SimpleBSONObjComparator::kInstance.makeBSONObjSet();
            MultikeyPaths multikeyPaths;
            keySchema->GetKeys(recordObj, &keys, &multikeyPaths);
            if (keys.size() > 1 || Eloq::MongoKeySchema::IsMultiKeyFromPaths(multikeyPaths)) {
                if (!keySchema->IsMultiKey() ||
                    !std::includes(keySchema->MongoMultiKeyPaths().cbegin(),
                                   keySchema->MongoMultiKeyPaths().cend(),
                                   multikeyPaths.cbegin(),
                                   multikeyPaths.cend())) {
                    uasserted(ErrorCodes::ConflictingOperationInProgress,
                              str::stream() << "A conflict create-index transaction is running.");
                }
            }

            const BSONObj& skObj = *keys.cbegin();
            KeyString keyString(KeyString::kLatestVersion, skObj, keySchema->Ordering(), id);
            auto mongoKey =
                std::make_unique<Eloq::MongoKey>(keyString.getBuffer(), keyString.getSize());
            auto mongoRecord = std::make_unique<Eloq::MongoRecord>();
            if (const auto& typeBits = keyString.getTypeBits(); !typeBits.isAllZeros()) {
                mongoRecord->SetUnpackInfo(typeBits.getBuffer(), typeBits.getSize());
            }
            err = ru->setKV(indexName,
                            keySchema->SchemaTs(),
                            std::move(mongoKey),
                            std::move(mongoRecord),
                            txservice::OperationType::Update,
                            false);
            uassertStatusOK(TxErrorCodeToMongoStatus(err));
        }
    } catch (const mongo::DBException& e) {
        MONGO_LOG(1)
            << "EloqRecordStore::udpateRecord update record for dirty indexes raise DBException "
            << e.what();
        return e.toStatus();
    }

    return Status::OK();
}

bool EloqRecordStore::updateWithDamagesSupported() const {
    return false;
}

StatusWith<RecordData> EloqRecordStore::updateWithDamages(
    OperationContext* opCtx,
    const RecordId& loc,
    const RecordData& oldRec,
    const char* damageSource,
    const mutablebson::DamageVector& damages) {
    MONGO_UNREACHABLE;
    return Status::OK();
}

std::unique_ptr<SeekableRecordCursor> EloqRecordStore::getCursor(OperationContext* opCtx,
                                                                 bool forward) const {
    MONGO_LOG(1) << "EloqRecordStore::getCursor";
    if (_isCatalog) {
        MONGO_UNREACHABLE;
        return std::make_unique<EloqCatalogRecordStoreCursor>(opCtx);
    } else {
        return std::make_unique<EloqRecordStoreCursor>(opCtx, this, forward);
    }
}

std::unique_ptr<RecordCursor> EloqRecordStore::getCursorForRepair(OperationContext* opCtx) const {
    MONGO_LOG(1) << "EloqRecordStore::getCursorForRepair";
    uassertStatusOK(Status(ErrorCodes::BadValue, "Not supported feature"));
    return {};
}

std::unique_ptr<RecordCursor> EloqRecordStore::getRandomCursor(OperationContext* opCtx) const {
    MONGO_LOG(1) << "EloqRecordStore::getRandomCursor";
    uassertStatusOK(Status(ErrorCodes::BadValue, "Not supported feature"));
    return {};
}

std::vector<std::unique_ptr<RecordCursor>> EloqRecordStore::getManyCursors(
    OperationContext* opCtx) const {
    MONGO_LOG(1) << "EloqRecordStore::getManyCursors";
    // MONGO_UNREACHABLE;
    return RecordStore::getManyCursors(opCtx);
}

Status EloqRecordStore::truncate(OperationContext* opCtx) {
    MONGO_LOG(1) << "EloqRecordStore::truncate";
    return Status(ErrorCodes::BadValue, "Not supported feature");
}

void EloqRecordStore::cappedTruncateAfter(OperationContext* opCtx, RecordId end, bool inclusive) {
    MONGO_LOG(1) << "EloqRecordStore::cappedTruncateAfter";
    uassertStatusOK(Status(ErrorCodes::BadValue, "Not supported feature"));
}

bool EloqRecordStore::compactSupported() const {
    MONGO_LOG(1) << "EloqRecordStore::compactSupported";
    return false;
}

Status EloqRecordStore::validate(OperationContext* opCtx,
                                 ValidateCmdLevel level,
                                 ValidateAdaptor* adaptor,
                                 ValidateResults* results,
                                 BSONObjBuilder* output) {
    MONGO_LOG(1) << "EloqRecordStore::validate";
    long long nrecords{0};
    long long dataSizeTotal{0};
    long long nInvalid{0};

    results->valid = true;

    try {
        auto cursor = getCursor(opCtx, true);
        // int interruptInterval {4096};

        while (auto record = cursor->next()) {
            // if (!(nrecords % interruptInterval))
            //     opCtx->checkForInterrupt();
            ++nrecords;
            auto dataSize = record->data.size();
            dataSizeTotal += dataSize;
            size_t validatedSize{0};
            Status status = adaptor->validate(record->id, record->data, &validatedSize);

            // The validatedSize equals dataSize below is not a general requirement, but must be
            // true for WT today because we never pad records.
            if (!status.isOK() || validatedSize != static_cast<size_t>(dataSize)) {
                if (results->valid) {
                    // Only log once.
                    results->errors.emplace_back(
                        "detected one or more invalid documents (see logs)");
                }
                nInvalid++;
                results->valid = false;
                log() << "document at location: " << record->id << " is corrupted";
            }
        }

        if (results->valid) {
            updateStatsAfterRepair(opCtx, nrecords, dataSizeTotal);
        }

        output->append("nInvalidDocuments", nInvalid);
        output->appendNumber("nrecords", nrecords);
        return Status::OK();
    } catch (const mongo::DBException& e) {
        results->valid = false;
        return e.toStatus();
    }
}

/**
 * @param scaleSize - amount by which to scale size metrics
 * appends any custom stats from the RecordStore or other unique stats
 */
void EloqRecordStore::appendCustomStats(OperationContext* opCtx,
                                        BSONObjBuilder* result,
                                        double scale) const {
    MONGO_LOG(1) << "EloqRecordStore::appendCustomStats";
    // MONGO_UNREACHABLE;
}

void EloqRecordStore::updateStatsAfterRepair(OperationContext* opCtx,
                                             long long numRecords,
                                             long long dataSize) {
    MONGO_LOG(1) << "EloqRecordStore::updateStatsAfterRepair";
}

void EloqRecordStore::waitForAllEarlierOplogWritesToBeVisible(OperationContext* opCtx) const {
    //
}

struct BatchReadEntry {
    BatchReadEntry() : keyString(KeyString::kLatestVersion) {}
    void resetToKey(const BSONObj& idObj) {
        keyString.resetToKey(idObj, kIdOrdering);
        mongoKey = std::make_unique<Eloq::MongoKey>(keyString);
    }

    KeyString keyString;
    std::unique_ptr<Eloq::MongoKey> mongoKey;
    Eloq::MongoRecord mongoRecord;
};

Status EloqRecordStore::_insertRecords(OperationContext* opCtx,
                                       Record* records,
                                       const Timestamp* timestamps,
                                       size_t nRecords) {
    MONGO_LOG(1) << "EloqRecordStore::_insertRecords"
                 << ". tableName: " << _tableName.StringView() << ". nRecords: " << nRecords;
    // Only check if a write lock is held for regular (non-temporary) record stores.
    // dassert(opCtx->lockState()->isWriteLocked());

    invariant(nRecords != 0);

    int64_t totalLength = 0;
    for (size_t i = 0; i < nRecords; i++) {
        const auto& record = records[i];
        assert(record.id.isNull());
        totalLength += record.data.size();
    }

    if (_isCapped && totalLength > _cappedMaxSize) {
        return {ErrorCodes::BadValue, "object to insert exceeds cappedMaxSize"};
    }

    auto batchEntries = std::make_unique<BatchReadEntry[]>(nRecords);
    std::vector<txservice::ScanBatchTuple> batchTuples;
    batchTuples.reserve(nRecords);
    for (size_t i = 0; i < nRecords; i++) {
        Record& record = records[i];
        BSONObj obj{record.data.data()};
        const BSONObj idObj = getIdBSONObjWithoutFieldName(obj);
        MONGO_LOG(1) << idObj.jsonString();
        Status s = checkKeySize(idObj, "RecordStore");
        if (!s.isOK()) {
            return s;
        }

        BatchReadEntry& entry = batchEntries[i];
        entry.resetToKey(idObj);
        record.id = RecordId{entry.keyString.getBuffer(), entry.keyString.getSize()};
        batchTuples.emplace_back(txservice::TxKey(entry.mongoKey.get()), &entry.mongoRecord);
    }

    auto ru = EloqRecoveryUnit::get(opCtx);
    MONGO_LOG(1) << "Insert into a Data Table.";
    const EloqRecoveryUnit::DiscoveredTable& table = ru->discoveredTable(_tableName);
    uint64_t pkeySchemaVersion = table._schema->KeySchema()->SchemaTs();
    txservice::TxErrorCode err =
        ru->batchGetKV(opCtx, _tableName, pkeySchemaVersion, batchTuples, true);
    if (err != txservice::TxErrorCode::NO_ERROR) {
        return TxErrorCodeToMongoStatus(err);
    }

    for (size_t i = 0; i < nRecords; i++) {
        const txservice::ScanBatchTuple& tuple = batchTuples[i];
        if (tuple.status_ == txservice::RecordStatus::Normal) {
            return {ErrorCodes::DuplicateKey, "DuplicateKey"};
        } else {
            invariant(tuple.status_ == txservice::RecordStatus::Deleted);
        }

        std::unique_ptr<Eloq::MongoKey>& mongoKey = batchEntries[i].mongoKey;
        std::unique_ptr<Eloq::MongoRecord> mongoRecord = std::make_unique<Eloq::MongoRecord>();
        const RecordData& data = records[i].data;
        mongoRecord->SetEncodedBlob(reinterpret_cast<const unsigned char*>(data.data()),
                                    data.size());
        const KeyString& ks = batchEntries[i].keyString;
        if (const auto& typeBits = ks.getTypeBits(); !typeBits.isAllZeros()) {
            mongoRecord->SetUnpackInfo(typeBits.getBuffer(), typeBits.getSize());
        }
        bool checkUnique = true;
        err = ru->setKV(_tableName,
                        pkeySchemaVersion,
                        std::move(mongoKey),
                        std::move(mongoRecord),
                        txservice::OperationType::Insert,
                        checkUnique);
        if (err != txservice::TxErrorCode::NO_ERROR) {
            return TxErrorCodeToMongoStatus(err);
        }

        // For creating index.
        try {
            BSONObj obj(data.data());
            for (const EloqRecoveryUnit::SecondaryIndex* index : table._creatingIndexes) {
                const txservice::TableName& indexName = index->first;
                const auto* keySchema =
                    static_cast<const Eloq::MongoKeySchema*>(index->second.sk_schema_.get());

                bool unique = keySchema->Unique();
                if (unique) {
                    uasserted(ErrorCodes::ConflictingOperationInProgress,
                              str::stream()
                                  << "A conflict create-unique-index transaction is running.");
                }

                BSONObjSet keys = SimpleBSONObjComparator::kInstance.makeBSONObjSet();
                MultikeyPaths multikeyPaths;
                keySchema->GetKeys(obj, &keys, &multikeyPaths);
                invariant(keys.size() > 0 && multikeyPaths.size() > 0);
                if (keys.size() > 1 || Eloq::MongoKeySchema::IsMultiKeyFromPaths(multikeyPaths)) {
                    if (!keySchema->IsMultiKey() ||
                        !std::includes(keySchema->MongoMultiKeyPaths().cbegin(),
                                       keySchema->MongoMultiKeyPaths().cend(),
                                       multikeyPaths.cbegin(),
                                       multikeyPaths.cend())) {
                        uasserted(ErrorCodes::ConflictingOperationInProgress,
                                  str::stream()
                                      << "A conflict create-index transaction is running.");
                    }
                }

                const BSONObj& skObj = *keys.cbegin();
                KeyString keyString(
                    KeyString::kLatestVersion, skObj, keySchema->Ordering(), records[i].id);
                auto mongoKey =
                    std::make_unique<Eloq::MongoKey>(keyString.getBuffer(), keyString.getSize());
                auto mongoRecord = std::make_unique<Eloq::MongoRecord>();
                if (const auto& typeBits = keyString.getTypeBits(); !typeBits.isAllZeros()) {
                    mongoRecord->SetUnpackInfo(typeBits.getBuffer(), typeBits.getSize());
                }
                bool checkUnique = unique;
                err = ru->setKV(indexName,
                                keySchema->SchemaTs(),
                                std::move(mongoKey),
                                std::move(mongoRecord),
                                txservice::OperationType::Insert,
                                checkUnique);
                uassertStatusOK(TxErrorCodeToMongoStatus(err));
            }
        } catch (const mongo::DBException& e) {
            MONGO_LOG(1) << "EloqRecordStore::_insertRecords insert record for dirty indexes raise "
                            "DBException "
                         << e.what();
            return e.toStatus();
        }
    }

    return Status::OK();
}


}  // namespace mongo
