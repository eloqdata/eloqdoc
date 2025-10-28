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

#include <string>

#include "mongo/db/record_id.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/sorted_data_interface.h"

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/type.h"

namespace mongo {
// please refer class WiredTigerIdIndex and https://www.mongodb.com/docs/manual/indexes/
class EloqIndex : public SortedDataInterface {
    friend class EloqIndexCursor;

public:
    EloqIndex(OperationContext* ctx,
              txservice::TableName&& tableName,
              txservice::TableName&& indexName,
              const IndexDescriptor* desc);
    ~EloqIndex() override;

    Status dupKeyCheck(OperationContext* opCtx, const BSONObj& key, const RecordId& id) override;

    void fullValidate(OperationContext* opCtx,
                      long long* numKeysOut,
                      ValidateResults* fullResults) const override;

    bool appendCustomStats(OperationContext* opCtx,
                           BSONObjBuilder* output,
                           double scale) const override;

    long long getSpaceUsedBytes(OperationContext* opCtx) const override;

    bool isEmpty(OperationContext* opCtx) override;

    Status initAsEmpty(OperationContext* opCtx) override;

    const txservice::TableName& getTableName() const {
        return _tableName;
    }

    const txservice::TableName& getIndexName() const {
        return _indexName;
    }

    const BSONObj& keyPattern() const {
        return _keyPattern;
    }

    Ordering ordering() const {
        return _ordering;
    }

    KeyString::Version keyStringVersion() const {
        return KeyString::kLatestVersion;
    }

    virtual bool isIdIndex() const = 0;

    virtual bool unique() const = 0;

protected:
    class BulkBuilder;
    class IdBulkBuilder;
    class UniqueBulkBuilder;
    class StandardBulkBuilder;

    txservice::TableName _tableName;
    txservice::TableName _indexName;
    const Ordering _ordering;

    const IndexDescriptor* _desc;
    const BSONObj _keyPattern;
    const BSONObj _collation;
};

class EloqIdIndex final : public EloqIndex {
public:
    using EloqIndex::EloqIndex;

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                           bool isForward = true) const override;

    SortedDataInterface::Cursor::UPtr newCursorPtr(OperationContext* opCtx,
                                                   bool isForward = true) const override;

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;


    bool isIdIndex() const override {
        return true;
    }

    bool unique() const override {
        return false;
    }

    Status insert(OperationContext* opCtx,
                  const BSONObj& key,
                  const RecordId& id,
                  bool dupsAllowed) override;

    void unindex(OperationContext* opCtx,
                 const BSONObj& key,
                 const RecordId& id,
                 bool dupsAllowed) override;
};


class EloqUniqueIndex final : public EloqIndex {
public:
    using EloqIndex::EloqIndex;

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                           bool isForward = true) const override;
    SortedDataInterface::Cursor::UPtr newCursorPtr(OperationContext* opCtx,
                                                   bool isForward) const override;

    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;

    bool isIdIndex() const override {
        return false;
    }

    bool unique() const override {
        return true;
    }

    Status insert(OperationContext* opCtx,
                  const BSONObj& key,
                  const RecordId& id,
                  bool dupsAllowed) override;

    void unindex(OperationContext* opCtx,
                 const BSONObj& key,
                 const RecordId& id,
                 bool dupsAllowed) override;
};

class EloqStandardIndex final : public EloqIndex {
public:
    using EloqIndex::EloqIndex;

    std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* opCtx,
                                                           bool isForward = true) const override;
    SortedDataInterface::Cursor::UPtr newCursorPtr(OperationContext* opCtx,
                                                   bool isForward) const override;
    SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx, bool dupsAllowed) override;

    bool isIdIndex() const override {
        return false;
    }

    bool unique() const override {
        return false;
    }

    Status insert(OperationContext* opCtx,
                  const BSONObj& key,
                  const RecordId& id,
                  bool dupsAllowed) override;
    void unindex(OperationContext* opCtx,
                 const BSONObj& key,
                 const RecordId& id,
                 bool dupsAllowed) override;
};

}  // namespace mongo
