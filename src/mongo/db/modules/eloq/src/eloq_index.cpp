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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage  // NO LINT

#include <cassert>
#include <memory>
#include <string_view>
#include <utility>

#include "mongo/base/object_pool.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/src/base/eloq_key.h"
#include "mongo/db/modules/eloq/src/base/eloq_util.h"
#include "mongo/db/modules/eloq/src/eloq_cursor.h"
#include "mongo/db/modules/eloq/src/eloq_index.h"
#include "mongo/db/modules/eloq/src/eloq_recovery_unit.h"

#include "mongo/db/modules/eloq/tx_service/include/tx_key.h"
#include "mongo/db/modules/eloq/tx_service/include/tx_request.h"
#include "mongo/db/modules/eloq/tx_service/include/type.h"

#include <butil/time.h>
#include <bvar/latency_recorder.h>

namespace recorder {
static bvar::LatencyRecorder kIdReadLatency("mongo_id_read");
}

namespace mongo {
enum class IndexCursorType { ID = 0, UNIQUE, STANDARD };
class EloqIndexCursor final : public SortedDataInterface::Cursor {
public:
    EloqIndexCursor(const EloqIndex* idx,
                    OperationContext* opCtx,
                    bool forward,
                    IndexCursorType cursorType)
        : _opCtx{opCtx},
          _ru{EloqRecoveryUnit::get(opCtx)},
          _idx{idx},
          _indexName{&_idx->getIndexName()},
          _indexSchema{_ru->getIndexSchema(idx->getTableName(), idx->getIndexName())},
          _indexType{cursorType},
          _scanType{_indexType == IndexCursorType::ID ? txservice::ScanIndexType::Primary
                                                      : txservice::ScanIndexType::Secondary},
          _forward{forward},
          _key{_idx->keyStringVersion()},
          _typeBits{_idx->keyStringVersion()},
          _query{_idx->keyStringVersion()},
          _kvPair{&_ru->getKVPair()} {
        // _timer.start();
        MONGO_LOG(1) << "EloqIndexCursor::EloqIndexCursor " << _indexName->StringView();
    }

    ~EloqIndexCursor() override {
        MONGO_LOG(1) << "EloqIndexCursor::~EloqIndexCursor";
        // _timer.stop();
        // recorder::kIdReadLatency << _timer.u_elapsed();
    }

    void reset(const EloqIndex* idx,
               OperationContext* opCtx,
               bool forward,
               IndexCursorType cursorType) {
        MONGO_LOG(1) << "EloqIndexCursor::reset";

        _opCtx = opCtx;
        _ru = EloqRecoveryUnit::get(opCtx);
        _idx = idx;
        _indexName = &_idx->getIndexName();
        _indexSchema = _ru->getIndexSchema(idx->getTableName(), idx->getIndexName());
        _indexType = cursorType;
        _scanType = (_indexType == IndexCursorType::ID) ? txservice::ScanIndexType::Primary
                                                        : txservice::ScanIndexType::Secondary;
        _forward = forward;
        _cursor.reset();

        _key.reset(_idx->keyStringVersion());
        _typeBits.reset(_idx->keyStringVersion());
        _query.reset(_idx->keyStringVersion());
        _id = RecordId{};
        _eof = true;
        _endPosition.reset();

        _scanTupleKey = nullptr;
        _scanTupleRecord = nullptr;

        _startKey = Eloq::MongoKey::GetNegInfTxKey();
        _endKey = Eloq::MongoKey::GetNegInfTxKey();

        _kvPair = &_ru->getKVPair();
    }

    void setEndPosition(const BSONObj& key, bool inclusive) override {
        MONGO_LOG(1) << "EloqIndexCursor::setEndPosition " << _indexName->StringView()
                     << ". endKey: " << key << ". inclusive: " << inclusive;
        if (key.isEmpty()) {
            // This means scan to end of index.
            _endPosition.reset();
            return;
        }

        // NOTE: this uses the opposite rules as a normal seek because a forward scan should
        // end after the key if inclusive and before if exclusive.
        const auto discriminator =
            _forward == inclusive ? KeyString::kExclusiveAfter : KeyString::kExclusiveBefore;
        _endPosition.emplace(_idx->keyStringVersion());
        _endPosition->resetToKey(stripFieldNames(key), _idx->ordering(), discriminator);
        MONGO_LOG(1) << "endPosition: " << _endPosition->toString();
    }

    boost::optional<IndexKeyEntry> seek(const BSONObj& key,
                                        bool inclusive,
                                        RequestedInfo parts = kKeyAndLoc) override {
        MONGO_LOG(1) << "EloqIndexCursor::seek " << _indexName->StringView() << ". key: " << key
                     << ". inclusive: " << inclusive;
        // dassert(_opCtx->lockState()->isReadLocked());

        // if ((!_endPosition) && (inclusive) && (_indexType == IndexCursorType::ID)) {
        //     return _idRead(key, parts);
        // }

        const BSONObj finalKey = stripFieldNames(key);
        const auto discriminator =
            _forward == inclusive ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;

        // By using a discriminator other than kInclusive, there is no need to distinguish
        // unique vs non-unique key formats since both start with the key.
        _query.resetToKey(finalKey, _idx->ordering(), discriminator);
        _seekCursor(_query, inclusive);
        _updatePosition();
        return _curr(parts);
    }

    boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
                                        RequestedInfo parts = kKeyAndLoc) override {
        MONGO_LOG(1) << "EloqIndexCursor::seek with seekPoint. " << _indexName->StringView();

        // dassert(_opCtx->lockState()->isReadLocked());
        // TODO(starrysky): don't go to a bson obj then to a KeyString, go straight
        BSONObj key = IndexEntryComparison::makeQueryObject(seekPoint, _forward);

        // makeQueryObject handles the discriminator in the real exclusive cases.
        const auto discriminator =
            _forward ? KeyString::kExclusiveBefore : KeyString::kExclusiveAfter;
        _query.resetToKey(key, _idx->ordering(), discriminator);
        _seekCursor(_query, true);
        _updatePosition();
        return _curr(parts);
    }

    /*
     * It seems seekExact is only performed on IdIndex
     */
    boost::optional<IndexKeyEntry> seekExact(const BSONObj& key,
                                             RequestedInfo parts = kKeyAndLoc) override {

        MONGO_LOG(1) << "EloqIndexCursor::seekExact " << _indexName->StringView()
                     << ". key: " << key;
        return _idRead(key, parts);
    }

    boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) override {
        MONGO_LOG(1) << "EloqIndexCursor::next " << _indexName->StringView();
        if (_eof) {
            return {};
        }
        _updatePosition();
        return _curr(parts);
    }

    void save() override {
        MONGO_LOG(1) << "EloqIndexCursor::save " << _indexName->StringView();
        _cursor.reset();
    }

    void saveUnpositioned() override {
        MONGO_LOG(1) << "EloqIndexCursor::saveUnpositioned " << _indexName->StringView();

        _cursor.reset();
    }

    void restore() override {
        MONGO_LOG(1) << "EloqIndexCursor::restore " << _indexName->StringView();
        if (_eof) {
            return;
        }

        assert(!_cursor);
        // Place the cursor after the last returned key when restore
        _seekCursor(_key, false);
    }

    void detachFromOperationContext() override {
        MONGO_LOG(1) << "EloqIndexCursor::detachFromOperationContext";
        assert(_opCtx);
        _opCtx = nullptr;
        _ru = nullptr;
    }

    void reattachToOperationContext(OperationContext* opCtx) override {
        MONGO_LOG(1) << "EloqIndexCursor::reattachToOperationContext";
        assert(!_opCtx);
        _opCtx = opCtx;
        _ru = EloqRecoveryUnit::get(_opCtx);
    }

private:
    butil::Timer _timer;
    boost::optional<IndexKeyEntry> _idRead(const BSONObj& key, RequestedInfo parts) {
        MONGO_LOG(1) << "EloqIndexCursor::_idRead " << _indexName->StringView()
                     << ". key: " << key.jsonString();

        _eof = false;

        const BSONObj finalKey = stripFieldNames(key);
        MONGO_LOG(1) << "finalKey" << finalKey;

        _key.resetToKey(finalKey, _idx->ordering());
        _kvPair->keyRef().SetPackedKey(_key.getBuffer(), _key.getSize());
        std::string_view sv{_key.getBuffer(), _key.getSize()};
        bool isForWrite = _opCtx->isUpsert();
        auto [exists, err] =
            _ru->getKVInternal(_opCtx, *_indexName, _indexSchema->SchemaTs(), isForWrite);
        uassertStatusOK(TxErrorCodeToMongoStatus(err));
        if (exists) {
            // valid
            _id = RecordId{_key.getBuffer(), _key.getSize()};
            _typeBits.reset();
        } else {
            _eof = true;
            _kvPair->setValuePtr(nullptr);
        }

        return _curr(parts);
    }

    // Seeks to query. Returns true on exact match.
    bool _seekCursor(const KeyString& query, bool startInclusive) {
        MONGO_LOG(1) << "EloqIndexCursor::_seekCursor " << _indexName->StringView();

        _cursor.emplace(_opCtx);

        txservice::ScanDirection direction =
            _forward ? txservice::ScanDirection::Forward : txservice::ScanDirection::Backward;

        _startKey =
            txservice::TxKey(std::make_unique<Eloq::MongoKey>(query.getBuffer(), query.getSize()));

        if (_endPosition) {
            _endKey = txservice::TxKey(std::make_unique<Eloq::MongoKey>(_endPosition->getBuffer(),
                                                                        _endPosition->getSize()));
        } else {
            if (_forward) {
                _endKey = Eloq::MongoKey::GetPosInfTxKey();
            } else {
                _endKey = Eloq::MongoKey::GetNegInfTxKey();
            }
        }

        bool endInclusive = false;
        bool isForWrite = _opCtx->isUpsert() && _indexName->IsBase();
        // end_inclusive semantics has been handled by _endPosition
        _cursor->indexScanOpen(_indexName,
                               _indexSchema->SchemaTs(),
                               _scanType,
                               &_startKey,
                               startInclusive,
                               &_endKey,
                               endInclusive,
                               direction,
                               isForWrite);

        return true;
    }

    void _updatePosition(bool inNext = true) {
        MONGO_LOG(1) << "EloqIndexCursor::_updatePosition " << _indexName->StringView()
                     << ". inNext" << inNext;
        _eof = false;

        if (inNext) {
            _scanTupleKey = nullptr;
            _scanTupleRecord = nullptr;

            txservice::TxErrorCode err = _cursor->nextBatchTuple();
            uassertStatusOK(TxErrorCodeToMongoStatus(err));

            const txservice::ScanBatchTuple* scanTuple = _cursor->currentBatchTuple();
            if (scanTuple != nullptr) {
                _scanTupleKey = scanTuple->key_.GetKey<Eloq::MongoKey>();
                _scanTupleRecord = static_cast<const Eloq::MongoRecord*>(scanTuple->record_);
            }
        }

        if (_scanTupleKey == nullptr) {
            _eof = true;
            return;
        }

        _key.resetFromBuffer(_scanTupleKey->Data(), _scanTupleKey->Size());

        if (_atOrPastEndPointAfterSeeking()) {
            _eof = true;
            return;
        }

        _updateIdAndTypeBits();
    }

    void _updateIdAndTypeBits() {
        MONGO_LOG(1) << "EloqIndexCursor::_updateIdAndTypeBits " << _indexName->StringView();

        switch (_indexType) {
            case IndexCursorType::ID: {
                _id = _scanTupleKey->ToRecordId(false);
                BufReader br{_scanTupleRecord->UnpackInfoData(),
                             static_cast<unsigned int>(_scanTupleRecord->UnpackInfoSize())};
                _typeBits.resetFromBuffer(&br);
                _kvPair->setValuePtr(_scanTupleRecord);
            } break;

            case IndexCursorType::UNIQUE: {
                _id = _scanTupleRecord->ToRecordId(false);
                BufReader br{_scanTupleRecord->UnpackInfoData(),
                             static_cast<unsigned int>(_scanTupleRecord->UnpackInfoSize())};
                _typeBits.resetFromBuffer(&br);
                _kvPair->setValuePtr(nullptr);
            } break;

            case IndexCursorType::STANDARD: {
                _id = KeyString::decodeRecordIdStrAtEnd(_key.getBuffer(), _key.getSize());
                BufReader br{_scanTupleRecord->UnpackInfoData(),
                             static_cast<unsigned int>(_scanTupleRecord->UnpackInfoSize())};
                _typeBits.resetFromBuffer(&br);
                _kvPair->setValuePtr(nullptr);
            } break;
        };

        MONGO_LOG(1) << "EloqIndexCursor::_updateIdAndTypeBits " << _indexName->StringView()
                     << ". _id: " << _id.toString();
    }

    boost::optional<IndexKeyEntry> _curr(RequestedInfo parts) const {
        MONGO_LOG(1) << "EloqIndexCursor::_curr " << _indexName->StringView();
        if (_eof) {
            return {};
        }

        // dassert(!_atOrPastEndPointAfterSeeking());

        BSONObj bson;
        if (parts & kWantKey) {
            bson = KeyString::toBson(_key.getBuffer(), _key.getSize(), _idx->ordering(), _typeBits);
            MONGO_LOG(1) << "bson: " << bson << ". _id: " << _id;
        }
        return {{std::move(bson), _id}};
    }

    bool _atOrPastEndPointAfterSeeking() const {
        MONGO_LOG(1) << "EloqIndexCursor::_atOrPastEndPointAfterSeeking "
                     << _indexName->StringView();
        if (_eof) {
            return true;
        }
        if (!_endPosition) {
            return false;
        }

        const int cmp = _key.compare(*_endPosition);

        // We set up _endPosition to be in between the last in-range value and the first
        // out-of-range value. In particular, it is constructed to never equal any legal index
        // key.
        assert(cmp != 0);

        if (_forward) {
            // We may have landed after the end point.
            return cmp > 0;
        } else {
            // We may have landed before the end point.
            return cmp < 0;
        }
    }

private:
    OperationContext* _opCtx;                  // not owned
    EloqRecoveryUnit* _ru;                     // not owned
    const EloqIndex* _idx;                     // not owned
    const txservice::TableName* _indexName;    // not owned
    const txservice::KeySchema* _indexSchema;  // not owned
    IndexCursorType _indexType;
    txservice::ScanIndexType _scanType;
    bool _forward;

    // query related
    KeyString _key;
    KeyString::TypeBits _typeBits;
    KeyString _query;
    RecordId _id;
    bool _eof{true};
    boost::optional<KeyString> _endPosition;

    const Eloq::MongoKey* _scanTupleKey{nullptr};
    const Eloq::MongoRecord* _scanTupleRecord{nullptr};

    txservice::TxKey _startKey;
    txservice::TxKey _endKey;
    EloqKVPair* _kvPair;

    Eloq::MongoKey _currentKey;
    Eloq::MongoRecord _currentRecord;

    boost::optional<EloqCursor> _cursor;
};

class EloqIndex::BulkBuilder : public SortedDataBuilderInterface {
public:
    BulkBuilder(EloqIndex* idx, OperationContext* opCtx)
        : _ordering(idx->_ordering), _opCtx(opCtx), _idx(idx), _ru(EloqRecoveryUnit::get(_opCtx)) {
        MONGO_LOG(1) << "EloqIndex::BulkBuilder::BulkBuilder";
    }

    void commit(bool mayInterrupt) override {
        MONGO_LOG(1) << "EloqIndex::BulkBuilder::commit";
        WriteUnitOfWork uow(_opCtx);
        uow.commit();
    }

    ~BulkBuilder() override {
        MONGO_LOG(1) << "EloqIndex::BulkBuilder::~BulkBuilder";
    }

protected:
    const Ordering _ordering;
    OperationContext* const _opCtx;
    EloqIndex* const _idx;
    EloqRecoveryUnit* _ru;
};

class EloqIndex::IdBulkBuilder : public EloqIndex::BulkBuilder {
public:
    IdBulkBuilder(EloqIndex* idx, OperationContext* opCtx) : BulkBuilder(idx, opCtx) {
        MONGO_LOG(1) << "EloqIndex::IdBulkBuilder";
        assert(_idx->isIdIndex());
    }

    Status addKey(const BSONObj& key, const RecordId& loc) override {
        MONGO_LOG(1) << "EloqIndex::IdBulkBuilder::addKey";

        // No action is required here, as the txservice has already built the index.
        return Status::OK();
        // return _idx->insert(_opCtx, key, loc, false);
    }
};

class EloqIndex::UniqueBulkBuilder : public EloqIndex::BulkBuilder {
public:
    UniqueBulkBuilder(EloqIndex* idx, OperationContext* opCtx, bool dupsAllowed)
        : BulkBuilder(idx, opCtx), _dupsAllowed(dupsAllowed) {
        MONGO_LOG(1) << "EloqIndex::UniqueBulkBuilder";
        assert(!_idx->isIdIndex());
    }

    Status addKey(const BSONObj& key, const RecordId& loc) override {
        MONGO_LOG(1) << "EloqIndex::UniqueBulkBuilder::addKey";
        // No action is required here, as the txservice has already built the index.
        return Status::OK();
        // return _idx->insert(_opCtx, key, loc, false);
    }

private:
    const bool _dupsAllowed;
};

class EloqIndex::StandardBulkBuilder : public EloqIndex::BulkBuilder {
public:
    using EloqIndex::BulkBuilder::BulkBuilder;
    Status addKey(const BSONObj& key, const RecordId& id) override {
        MONGO_LOG(1) << "EloqIndex::StandardBulkBuilder::addKey"
                     << ". key: " << key << ". id: " << id;
        // No action is required here, as the txservice has already built the index.
        return Status::OK();
        // return _idx->insert(_opCtx, key, id, false);
    }
};

// EloqIndex
EloqIndex::EloqIndex(OperationContext* ctx,
                     txservice::TableName&& tableName,
                     txservice::TableName&& indexName,
                     const IndexDescriptor* desc)
    : _tableName{std::move(tableName)},
      _indexName{std::move(indexName)},
      _ordering{Ordering::make(desc->keyPattern())},
      _desc{desc},
      _keyPattern{desc->keyPattern()} {
    MONGO_LOG(1) << "EloqIndex::EloqIndex"
                 << ". tableName: " << _tableName.StringView()
                 << ", indexName: " << _indexName.StringView();
}

EloqIndex::~EloqIndex() {
    MONGO_LOG(1) << "EloqIndex::~EloqIndex " << _indexName.StringView();
}

Status EloqIndex::dupKeyCheck(OperationContext* opCtx, const BSONObj& key, const RecordId& id) {
    return Status::OK();
}

void EloqIndex::fullValidate(OperationContext* opCtx,
                             long long* numKeysOut,
                             ValidateResults* fullResults) const {
    MONGO_LOG(1) << "EloqIndex::fullValidate";
    auto cursor = newCursorPtr(opCtx);
    long long count{0};

    for (auto kv = cursor->seek(BSONObj{}, true, Cursor::kKeyAndLoc); kv; kv = cursor->next()) {
        count++;
    }
    if (numKeysOut) {
        *numKeysOut = count;
    }
}

bool EloqIndex::appendCustomStats(OperationContext* opCtx,
                                  BSONObjBuilder* output,
                                  double scale) const {
    return false;
}

long long EloqIndex::getSpaceUsedBytes(OperationContext* opCtx) const {
    MONGO_LOG(1) << "EloqIndex::getSpaceUsedBytes";
    return 0;
}

bool EloqIndex::isEmpty(OperationContext* opCtx) {
    return false;
}

Status EloqIndex::initAsEmpty(OperationContext* opCtx) {
    // no-op
    return Status::OK();
}

// EloqIdIndex
std::unique_ptr<SortedDataInterface::Cursor> EloqIdIndex::newCursor(OperationContext* opCtx,
                                                                    bool isForward) const {
    MONGO_LOG(1) << "EloqIdIndex::newCursor";
    return std::make_unique<EloqIndexCursor>(this, opCtx, isForward, IndexCursorType::ID);
}

SortedDataInterface::Cursor::UPtr EloqIdIndex::newCursorPtr(OperationContext* opCtx,
                                                            bool isForward) const {
    MONGO_LOG(1) << "EloqIdIndex::newCursorPtr";
    // return std::make_unique<EloqIndexCursor>(*this, opCtx, isForward, IndexCursorType::ID);
    return ObjectPool<EloqIndexCursor>::newObject<SortedDataInterface::Cursor>(
        this, opCtx, isForward, IndexCursorType::ID);
}

SortedDataBuilderInterface* EloqIdIndex::getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) {
    MONGO_LOG(1) << "EloqIdIndex::getBulkBuilder";
    // Duplicates are not actually allowed on the _id index, however we accept the parameter
    // regardless.
    assert(!dupsAllowed);
    return new IdBulkBuilder(this, opCtx);
}

Status EloqIdIndex::insert(OperationContext* opCtx,
                           const BSONObj& key,
                           const RecordId& id,
                           bool dupsAllowed) {
    MONGO_LOG(1) << "EloqIdIndex::insert"
                 << ". key:" << key << ". id:" << id;
    assert(!dupsAllowed);
    Status s = checkKeySize(key, _indexName.StringView());
    if (!s.isOK()) {
        return s;
    }
    return Status::OK();
    MONGO_UNREACHABLE;
    // IdIndex refers to the same table in TxService as its corresponding RecordStore.
    // So we do nothing here and in _unindex.
}

void EloqIdIndex::unindex(OperationContext* opCtx,
                          const BSONObj& key,
                          const RecordId& id,
                          bool dupsAllowed) {
    MONGO_LOG(1) << "EloqIdIndex::_unindex";
    MONGO_LOG(1) << "key: " << key << ". recordid: " << id;
    assert(!dupsAllowed);

    return;
    // do delete in EloqRecordStore::deleteRecord

    MONGO_UNREACHABLE;
}

// EloqUniqueIndex
std::unique_ptr<SortedDataInterface::Cursor> EloqUniqueIndex::newCursor(OperationContext* opCtx,
                                                                        bool isForward) const {
    MONGO_LOG(1) << "EloqUniqueIndex::newCursor";
    return std::make_unique<EloqIndexCursor>(this, opCtx, isForward, IndexCursorType::UNIQUE);
}

SortedDataInterface::Cursor::UPtr EloqUniqueIndex::newCursorPtr(OperationContext* opCtx,
                                                                bool isForward) const {
    MONGO_LOG(1) << "EloqUniqueIndex::newCursorPtr";
    return ObjectPool<EloqIndexCursor>::newObject<SortedDataInterface::Cursor>(
        this, opCtx, isForward, IndexCursorType::UNIQUE);
}

SortedDataBuilderInterface* EloqUniqueIndex::getBulkBuilder(OperationContext* opCtx,
                                                            bool dupsAllowed) {
    MONGO_LOG(1) << "EloqUniqueIndex::getBulkBuilder";
    return new UniqueBulkBuilder(this, opCtx, dupsAllowed);
}

Status EloqUniqueIndex::insert(OperationContext* opCtx,
                               const BSONObj& key,
                               const RecordId& id,
                               bool dupsAllowed) {
    MONGO_LOG(1) << "EloqUniqueIndex::insert";
    assert(!dupsAllowed);
    Status s = checkKeySize(key, _indexName.StringView());
    if (!s.isOK()) {
        return s;
    }

    auto ru = EloqRecoveryUnit::get(opCtx);

    // key as MongoKey
    KeyString keyString{keyStringVersion(), key, _ordering};
    auto valueItem = id.getStringView();

    auto mongoKey = std::make_unique<Eloq::MongoKey>(keyString.getBuffer(), keyString.getSize());
    auto mongoRecord = std::make_unique<Eloq::MongoRecord>();
    uint64_t keySchemaVersion = ru->getIndexSchema(_tableName, _indexName)->SchemaTs();

    auto [exists, err] =
        ru->getKV(opCtx, _indexName, keySchemaVersion, mongoKey.get(), mongoRecord.get(), true);
    if (err != txservice::TxErrorCode::NO_ERROR) {
        return TxErrorCodeToMongoStatus(err);
    }

    if (exists) {
        return {ErrorCodes::Error::DuplicateKey, "Duplicate Key: " + _indexName.String()};
    }

    mongoRecord->SetEncodedBlob(valueItem);
    if (const auto& typeBits = keyString.getTypeBits(); !typeBits.isAllZeros()) {
        mongoRecord->SetUnpackInfo(typeBits.getBuffer(), typeBits.getSize());
    }
    err = ru->setKV(_indexName,
                    keySchemaVersion,
                    std::move(mongoKey),
                    std::move(mongoRecord),
                    txservice::OperationType::Insert,
                    true);

    return TxErrorCodeToMongoStatus(err);
}

void EloqUniqueIndex::unindex(OperationContext* opCtx,
                              const BSONObj& key,
                              const RecordId& id,
                              bool dupsAllowed) {
    MONGO_LOG(1) << "EloqUniqueIndex::unindex";
    assert(!dupsAllowed);

    auto ru = EloqRecoveryUnit::get(opCtx);

    const Eloq::MongoKeySchema* keySchema = ru->getIndexSchema(_tableName, _indexName);
    uint64_t keySchemaVersion = keySchema->SchemaTs();

    // key as MongoKey. Unlike WiredTiger, whose unique key encodes(key, id), Eloq puts id in
    // MongoRecord.
    KeyString keyString{keyStringVersion(), key, _ordering};
    auto mongoKey = std::make_unique<Eloq::MongoKey>(keyString.getBuffer(), keyString.getSize());

    if (keySchema->IndexDescriptor()->isPartial()) {
        Eloq::MongoRecord mongoRecord;
        auto [exists, err] =
            ru->getKV(opCtx, _indexName, keySchemaVersion, mongoKey.get(), &mongoRecord, true);
        if (err != txservice::TxErrorCode::NO_ERROR) {
            uassertStatusOK(TxErrorCodeToMongoStatus(err));
        }
        if (exists) {
            std::string_view encodedBlob(mongoRecord.EncodedBlobData(),
                                         mongoRecord.EncodedBlobSize());
            if (encodedBlob != id.getStringView()) {
                return;
            }
        } else {
            return;
        }
    }

    txservice::TxErrorCode err = ru->setKV(_indexName,
                                           keySchemaVersion,
                                           std::move(mongoKey),
                                           nullptr,
                                           txservice::OperationType::Delete,
                                           false);
    uassertStatusOK(TxErrorCodeToMongoStatus(err));
}

std::unique_ptr<SortedDataInterface::Cursor> EloqStandardIndex::newCursor(OperationContext* opCtx,
                                                                          bool isForward) const {
    MONGO_LOG(1) << "EloqStandardIndex::newCursor";
    return std::make_unique<EloqIndexCursor>(this, opCtx, isForward, IndexCursorType::STANDARD);
}

SortedDataInterface::Cursor::UPtr EloqStandardIndex::newCursorPtr(OperationContext* opCtx,
                                                                  bool isForward) const {
    MONGO_LOG(1) << "EloqStandardIndex::newCursorPtr";
    return ObjectPool<EloqIndexCursor>::newObject<SortedDataInterface::Cursor>(
        this, opCtx, isForward, IndexCursorType::STANDARD);
}


SortedDataBuilderInterface* EloqStandardIndex::getBulkBuilder(OperationContext* opCtx,
                                                              bool dupsAllowed) {
    MONGO_LOG(1) << "EloqStandardIndex::getBulkBuilder";
    // We aren't unique so dups better be allowed.
    assert(dupsAllowed);
    return new StandardBulkBuilder(this, opCtx);
}

Status EloqStandardIndex::insert(OperationContext* opCtx,
                                 const BSONObj& key,
                                 const RecordId& id,
                                 bool dupsAllowed) {
    MONGO_LOG(1) << "EloqStandardIndex::insert"
                 << ". key: " << key << ". RecordId: " << id;
    assert(dupsAllowed);
    Status s = checkKeySize(key, _indexName.StringView());
    if (!s.isOK()) {
        return s;
    }

    auto ru = EloqRecoveryUnit::get(opCtx);

    KeyString keyString{keyStringVersion(), key, _ordering, id};

    auto mongoKey = std::make_unique<Eloq::MongoKey>(keyString.getBuffer(), keyString.getSize());
    auto mongoRecord = std::make_unique<Eloq::MongoRecord>();
    if (const auto& typeBits = keyString.getTypeBits(); !typeBits.isAllZeros()) {
        mongoRecord->SetUnpackInfo(typeBits.getBuffer(), typeBits.getSize());
    }
    uint64_t keySchemaVersion = ru->getIndexSchema(_tableName, _indexName)->SchemaTs();

    txservice::TxErrorCode err = ru->setKV(_indexName,
                                           keySchemaVersion,
                                           std::move(mongoKey),
                                           std::move(mongoRecord),
                                           txservice::OperationType::Insert,
                                           false);
    return TxErrorCodeToMongoStatus(err);
}

void EloqStandardIndex::unindex(OperationContext* opCtx,
                                const BSONObj& key,
                                const RecordId& id,
                                bool dupsAllowed) {
    MONGO_LOG(1) << "EloqStandardIndex::_unindex";

    auto ru = EloqRecoveryUnit::get(opCtx);

    KeyString keyString{keyStringVersion(), key, _ordering, id};

    auto mongoKey = std::make_unique<Eloq::MongoKey>(keyString.getBuffer(), keyString.getSize());
    uint64_t keySchemaVersion = ru->getIndexSchema(_tableName, _indexName)->SchemaTs();

    txservice::TxErrorCode err = ru->setKV(_indexName,
                                           keySchemaVersion,
                                           std::move(mongoKey),
                                           nullptr,
                                           txservice::OperationType::Delete,
                                           false);
    uassertStatusOK(TxErrorCodeToMongoStatus(err));
}

}  // namespace mongo
