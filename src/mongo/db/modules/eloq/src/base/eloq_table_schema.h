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
#include <optional>
#include <string>
#include <utility>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/operation_context.h"

#include "mongo/db/modules/eloq/src/base/eloq_key.h"
#include "mongo/db/modules/eloq/src/base/eloq_record.h"

#ifdef verify
#undef verify
#endif

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/catalog_factory.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/schema.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/table_statistics.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_record.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/type.h"

namespace Eloq {
struct MongoMultiKeyPaths final : public txservice::MultiKeyPaths {
    explicit MongoMultiKeyPaths(const mongo::IndexDescriptor* desc);

    explicit MongoMultiKeyPaths(mongo::MultikeyPaths multikey_paths)
        : multikey_paths_(std::move(multikey_paths)) {}

    txservice::MultiKeyPaths::Uptr Clone() const override {
        return std::make_unique<MongoMultiKeyPaths>(multikey_paths_);
    }

    std::string Serialize(const txservice::KeySchema* key_schema) const override;
    bool Deserialize(const txservice::KeySchema* key_schema, const std::string& str) override;

    bool Contain(const txservice::MultiKeyPaths& rhs) const override;

    bool MergeWith(const txservice::MultiKeyPaths& rhs) override;

    bool Contain(const MongoMultiKeyPaths& rhs) const;
    bool Contain(const mongo::MultikeyPaths& rhs) const;

    bool MergeWith(const MongoMultiKeyPaths& rhs);
    bool MergeWith(const mongo::MultikeyPaths& rhs);

    mongo::MultikeyPaths multikey_paths_;

private:
    // Mongo defines decode/encode method for MultiKeyPaths as private functions.
    // Make a copy of them. See collection_catalog_entry.h.
    static void appendMultikeyPathsAsBytes(mongo::BSONObj keyPattern,
                                           const mongo::MultikeyPaths& multikeyPaths,
                                           mongo::BSONObjBuilder* bob);

    static void parseMultikeyPathsFromBytes(mongo::BSONObj multikeyPathsObj,
                                            mongo::MultikeyPaths* multikeyPaths);

    static constexpr size_t kMaxKeyPatternPathLength = 2048;
};

struct MongoMultiKeyPathsRef final : public txservice::MultiKeyPaths {
    explicit MongoMultiKeyPathsRef(const mongo::MultikeyPaths& multikey_paths)
        : multikey_paths_(multikey_paths) {}

    txservice::MultiKeyPaths::Uptr Clone() const override {
        return std::make_unique<MongoMultiKeyPaths>(multikey_paths_);
    }

private:
    std::string Serialize(const txservice::KeySchema* key_schema) const override {
        invariant(false);
        return "";
    }

    bool Deserialize(const txservice::KeySchema* key_schema, const std::string& str) override {
        invariant(false);
        return false;
    }

    bool Contain(const txservice::MultiKeyPaths& rhs) const override {
        invariant(false);
        return false;
    }

    bool MergeWith(const txservice::MultiKeyPaths& rhs) override {
        invariant(false);
        return false;
    }

private:
    const mongo::MultikeyPaths& multikey_paths_;
};

class MongoKeySchemaBase : public txservice::KeySchema {};

class MongoKeySchema final : public MongoKeySchemaBase {
    friend class MongoTableSchema;
    friend class MongoSkEncoder;
    friend class MongoUniqueSkEncoder;
    friend class MongoStandardSkEncoder;
    friend struct MongoMultiKeyPaths;
    friend struct MongoMultiKeyPathsRef;

public:
    MongoKeySchema(const txservice::TableName& index_name,
                   uint64_t key_version,
                   std::string_view ns,
                   const mongo::CollectionCatalogEntry::IndexMetaData* index_meta_data);

    bool CompareKeys(const txservice::TxKey& key1,
                     const txservice::TxKey& key2,
                     size_t* const column_index) const override {
        *column_index = 0;

        // are they different?
        auto mongoKey1 = key1.GetKey<MongoKey>();
        auto mongoKey2 = key2.GetKey<MongoKey>();
        return mongoKey1 != mongoKey2;
    }

    uint16_t ExtendKeyParts() const override {
        return 1;
    }

    uint64_t SchemaTs() const override {
        return schema_ts_;
    }

    bool IsMultiKey() const override {
        return index_meta_data_->multikey;
    }

    const txservice::MultiKeyPaths* MultiKeyPaths() const override {
        return &multikey_paths_;
    }

    const mongo::MultikeyPaths& MongoMultiKeyPaths() const {
        return index_meta_data_->multikeyPaths;
    }

    void GetKeys(const mongo::BSONObj& obj,
                 mongo::BSONObjSet* keys,
                 mongo::MultikeyPaths* multikeyPaths) const {
        // Check for partial index
        auto filter_expr = entry_->getFilterExpression();
        if (filter_expr && !filter_expr->matchesBSON(obj)) {
            return;
        }

        // Check for sparse index will be done in getKeys.
        entry_->accessMethod()->getKeys(
            obj, mongo::IndexAccessMethod::GetKeysMode::kEnforceConstraints, keys, multikeyPaths);
    }

    const mongo::IndexDescriptor* IndexDescriptor() const {
        return entry_->descriptor();
    }

    mongo::Ordering Ordering() const {
        return entry_->ordering();
    }

    bool Unique() const {
        return entry_->descriptor()->unique();
    }

    static bool IsMultiKeyFromPaths(const mongo::MultikeyPaths& multikey_paths) {
        return std::any_of(
            multikey_paths.cbegin(),
            multikey_paths.cend(),
            [](const std::set<std::size_t>& components) { return !components.empty(); });
    }

private:
    static std::unique_ptr<mongo::IndexAccessMethod> CreateIndexAccessMethod(
        mongo::OperationContext* opCtx, mongo::StringData ns, mongo::IndexCatalogEntry* index);

    static std::unique_ptr<mongo::IndexCatalogEntry> CreateEntry(
        std::string_view ns,
        const mongo::CollectionCatalogEntry::IndexMetaData* index_meta_data_,
        std::unique_ptr<mongo::IndexDescriptor> descriptor);

    txservice::TableName index_name_;
    // The timestamp when this key was created.
    uint64_t schema_ts_{1};
    const mongo::CollectionCatalogEntry::IndexMetaData* index_meta_data_;
    // Interface to access all kinds of index.
    // Work in the same way as the Mongo compute layer.
    std::unique_ptr<mongo::IndexCatalogEntry> entry_;
    MongoMultiKeyPathsRef multikey_paths_;
};

class MongoEmptyKeySchema final : public MongoKeySchemaBase {
public:
    explicit MongoEmptyKeySchema(uint64_t key_version) : schema_ts_(key_version) {}

    bool CompareKeys(const txservice::TxKey& key1,
                     const txservice::TxKey& key2,
                     size_t* const column_index) const override {
        assert(false);
        return false;
    }

    uint16_t ExtendKeyParts() const override {
        return 1;
    }

    uint64_t SchemaTs() const override {
        return schema_ts_;
    }

    bool IsMultiKey() const override {
        assert(false);
        return false;
    }

    const txservice::MultiKeyPaths* MultiKeyPaths() const override {
        assert(false);
        return nullptr;
    }

private:
    // The timestamp when this key was created.
    uint64_t schema_ts_{1};
};

/*
 * Actually there is nothing about record schema beyond BSON in MongoDB.
 */
class MongoRecordSchema final : public txservice::RecordSchema {
public:
    MongoRecordSchema() = default;
};

class MongoTableSchema final : public txservice::TableSchema {
public:
    explicit MongoTableSchema(const txservice::TableName& table_name,
                              const std::string& catalog_image,
                              uint64_t version);

    MongoTableSchema(const MongoTableSchema& rhs) = delete;

    txservice::TableSchema::uptr Clone() const override;

    const txservice::TableName& GetBaseTableName() const override {
        return base_table_name_;
    }

    const txservice::KeySchema* KeySchema() const override {
        return key_schema_.get();
    }

    const txservice::RecordSchema* RecordSchema() const override {
        return &record_schema_;
    }

    const std::string& SchemaImage() const override {
        return schema_image_;
    }

    const std::unordered_map<uint16_t,
                             std::pair<txservice::TableName, txservice::SecondaryKeySchema>>*
    GetIndexes() const override {
        return &indexes_;
    }

    txservice::KVCatalogInfo* GetKVCatalogInfo() const override {
        return kv_info_.get();
    }

    void SetKVCatalogInfo(const std::string& kv_info) override;

    uint64_t Version() const override {
        return version_;
    }

    std::string_view VersionStringView() const override {
        return md_version_;
    }

    std::vector<txservice::TableName> IndexNames() const override {
        std::vector<txservice::TableName> index_names;
        index_names.reserve(indexes_.size());
        for (const auto& index_entry : indexes_) {
            index_names.emplace_back(index_entry.second.first);
        }
        return index_names;
    }

    size_t IndexesSize() const override {
        return indexes_.size();
    }

    const txservice::SecondaryKeySchema* IndexKeySchema(
        const txservice::TableName& index_name) const override {
        for (const auto& index_entry : indexes_) {
            if (index_entry.second.first == index_name) {
                return &index_entry.second.second;
            }
        }

        return nullptr;
    }

    uint16_t IndexOffset(const txservice::TableName& index_name) const override {
        for (const auto& [offset, index] : indexes_) {
            if (index.first == index_name) {
                return offset;
            }
        }
        return UINT16_MAX;
    }

    void BindStatistics(std::shared_ptr<txservice::Statistics> statistics) override {
        std::shared_ptr<txservice::TableStatistics<MongoKey>> table_statistics =
            std::dynamic_pointer_cast<txservice::TableStatistics<MongoKey>>(statistics);
        invariant(table_statistics != nullptr);
        table_statistics_ = table_statistics;
    }

    std::shared_ptr<txservice::Statistics> StatisticsObject() const override {
        return table_statistics_;
    }

    txservice::SkEncoder::uptr CreateSkEncoder(
        const txservice::TableName& index_name) const override;

    bool HasAutoIncrement() const override {
        return false;
    }

    const txservice::TableName* GetSequenceTableName() const override {
        return nullptr;
    }

    std::pair<txservice::TxKey, txservice::TxRecord::Uptr> GetSequenceKeyAndInitRecord(
        const txservice::TableName& table_name) const override {
        return {MongoKey::GetNegInfTxKey(), nullptr};
    }

    const std::string& MetaDataStr() const {
        return meta_data_str_;
    }

    void IndexesSetMultiKeyAttr(const std::vector<txservice::MultiKeyAttr>& indexes) override;

private:
    const txservice::TableName base_table_name_;  // string owner
    std::string schema_image_;

    std::string meta_data_str_;
    mongo::BSONObj meta_data_obj_;
    mongo::CollectionCatalogEntry::MetaData md_;

    std::string md_version_;

    std::string key_schemas_ts_str_;
    const uint64_t version_;

    std::string kv_info_str_;
    txservice::KVCatalogInfo::uptr kv_info_;

    std::unique_ptr<MongoKeySchemaBase> key_schema_;  // pk schema
    MongoRecordSchema record_schema_;
    std::unordered_map<uint16_t, std::pair<txservice::TableName, txservice::SecondaryKeySchema>>
        indexes_;  // sk schema

    std::shared_ptr<txservice::TableStatistics<MongoKey>> table_statistics_{nullptr};
};

class MongoSkEncoder : public txservice::SkEncoder {
public:
    explicit MongoSkEncoder(const MongoKeySchema* key_schema)
        : key_schema_(key_schema),
          mutable_multikey_(false),
          mutable_multikey_paths_(key_schema->IndexDescriptor()) {}

    bool IsMultiKey() const override {
        return mutable_multikey_;
    }

    const txservice::MultiKeyPaths* MultiKeyPaths() const final {
        return &mutable_multikey_paths_;
    }

    std::string SerializeMultiKeyPaths() const final {
        return mutable_multikey_paths_.Serialize(key_schema_);
    }

protected:
    bool GenerateBSONKeys(const txservice::TxKey* pk,
                          const txservice::TxRecord* record,
                          mongo::BSONObjSet* skeys);

    mongo::RecordId GenerateRecordID(const txservice::TxKey* pk) const;

private:
    void MergeMultiKeyAttr(const mongo::MultikeyPaths& multikey_paths);

protected:
    const MongoKeySchema* key_schema_;

    bool mutable_multikey_;
    MongoMultiKeyPaths mutable_multikey_paths_;
};

class MongoStandardSkEncoder final : public MongoSkEncoder {
public:
    explicit MongoStandardSkEncoder(const MongoKeySchema* key_schema)
        : MongoSkEncoder(key_schema) {}

    int32_t AppendPackedSk(const txservice::TxKey* pk,
                           const txservice::TxRecord* record,
                           uint64_t version,
                           std::vector<txservice::WriteEntry>& dest_vec) override;
};

class MongoUniqueSkEncoder final : public MongoSkEncoder {
public:
    explicit MongoUniqueSkEncoder(const MongoKeySchema* key_schema) : MongoSkEncoder(key_schema) {}

    int32_t AppendPackedSk(const txservice::TxKey* pk,
                           const txservice::TxRecord* record,
                           uint64_t version_ts,
                           std::vector<txservice::WriteEntry>& dest_vec) override;
};

}  // namespace Eloq
