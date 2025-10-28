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

#include <memory>

#include "mongo/base/string_data.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/index/2d_access_method.h"
#include "mongo/db/index/btree_access_method.h"
#include "mongo/db/index/fts_access_method.h"
#include "mongo/db/index/hash_access_method.h"
#include "mongo/db/index/haystack_access_method.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index/s2_access_method.h"
#include "mongo/db/index_names.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/service_context.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/data_substrate/store_handler/kv_store.h"
#include "mongo/db/modules/eloq/src/base/eloq_record.h"
#include "mongo/db/modules/eloq/src/base/eloq_table_schema.h"
#include "mongo/db/modules/eloq/src/base/eloq_util.h"
#include "mongo/db/modules/eloq/src/eloq_record_store.h"

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/type.h"

#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"

namespace Eloq {

MongoMultiKeyPaths::MongoMultiKeyPaths(const mongo::IndexDescriptor* desc) {
    const std::string& type = desc->getAccessMethodName();
    if (type == mongo::IndexNames::BTREE || type == mongo::IndexNames::GEO_2DSPHERE) {
        multikey_paths_.resize(desc->keyPattern().nFields());
    }
}

std::string MongoMultiKeyPaths::Serialize(const txservice::KeySchema* key_schema) const {
    auto mongo_key_schema = static_cast<const MongoKeySchema*>(key_schema);
    const mongo::BSONObj& key_pattern = mongo_key_schema->IndexDescriptor()->keyPattern();

    mongo::BSONObjBuilder builder;
    appendMultikeyPathsAsBytes(key_pattern, multikey_paths_, &builder);
    mongo::BSONObj obj = builder.done();
    return std::string(obj.objdata(), obj.objsize());
}

bool MongoMultiKeyPaths::Deserialize(const txservice::KeySchema* key_schema,
                                     const std::string& str) {
    mongo::BSONObj obj(str.c_str());
    parseMultikeyPathsFromBytes(obj, &multikey_paths_);
    return true;
}

bool MongoMultiKeyPaths::Contain(const txservice::MultiKeyPaths& rhs) const {
    return Contain(static_cast<const MongoMultiKeyPaths&>(rhs));
}

bool MongoMultiKeyPaths::Contain(const MongoMultiKeyPaths& rhs) const {
    return Contain(rhs.multikey_paths_);
}

bool MongoMultiKeyPaths::Contain(const mongo::MultikeyPaths& rhs) const {
    invariant(multikey_paths_.size() == rhs.size());

    bool contains = true;
    for (size_t i = 0; i < multikey_paths_.size(); ++i) {
        if (!std::includes(
                multikey_paths_.begin(), multikey_paths_.end(), rhs.begin(), rhs.end())) {
            contains = false;
            break;
        }
    }
    return contains;
}

bool MongoMultiKeyPaths::MergeWith(const txservice::MultiKeyPaths& rhs) {
    return MergeWith(static_cast<const MongoMultiKeyPaths&>(rhs));
}

bool MongoMultiKeyPaths::MergeWith(const MongoMultiKeyPaths& rhs) {
    return MergeWith(rhs.multikey_paths_);
}

bool MongoMultiKeyPaths::MergeWith(const mongo::MultikeyPaths& rhs) {
    bool changed = false;
    if (multikey_paths_.empty() && !rhs.empty()) {
        multikey_paths_ = rhs;
        changed = true;
    } else if (!multikey_paths_.empty() && !rhs.empty()) {
        invariant(multikey_paths_.size() == rhs.size());
        for (size_t i = 0; i < multikey_paths_.size(); ++i) {
            std::set<size_t>& components = multikey_paths_[i];
            for (size_t component : rhs[i]) {
                auto [it, inserted] = components.insert(component);
                changed |= inserted;
            }
        }
    } else {
        // do nothing
    }
    return changed;
}


void MongoMultiKeyPaths::appendMultikeyPathsAsBytes(mongo::BSONObj keyPattern,
                                                    const mongo::MultikeyPaths& multikeyPaths,
                                                    mongo::BSONObjBuilder* bob) {
    char multikeyPathsEncodedAsBytes[kMaxKeyPatternPathLength] = {};

    size_t i = 0;
    for (const auto keyElem : keyPattern) {
        mongo::StringData keyName = keyElem.fieldNameStringData();
        size_t numParts = mongo::FieldRef{keyName}.numParts();
        invariant(numParts > 0);
        invariant(numParts <= kMaxKeyPatternPathLength);

        std::fill_n(multikeyPathsEncodedAsBytes, numParts, 0);
        for (const auto multikeyComponent : multikeyPaths[i]) {
            multikeyPathsEncodedAsBytes[multikeyComponent] = 1;
        }
        bob->appendBinData(
            keyName, numParts, mongo::BinDataGeneral, &multikeyPathsEncodedAsBytes[0]);

        ++i;
    }
}

void MongoMultiKeyPaths::parseMultikeyPathsFromBytes(mongo::BSONObj multikeyPathsObj,
                                                     mongo::MultikeyPaths* multikeyPaths) {
    invariant(multikeyPaths);
    for (auto elem : multikeyPathsObj) {
        std::set<size_t> multikeyComponents;
        int len;
        const char* data = elem.binData(len);
        invariant(len > 0);
        invariant(static_cast<size_t>(len) <= kMaxKeyPatternPathLength);

        for (int i = 0; i < len; ++i) {
            if (data[i]) {
                multikeyComponents.insert(i);
            }
        }
        multikeyPaths->push_back(multikeyComponents);
    }
}

MongoKeySchema::MongoKeySchema(const txservice::TableName& index_name,
                               uint64_t key_version,
                               std::string_view ns,
                               const mongo::CollectionCatalogEntry::IndexMetaData* index_meta_data)
    : index_name_(index_name.StringView(), index_name.Type(), index_name.Engine()),
      schema_ts_(key_version),
      index_meta_data_(index_meta_data),
      entry_(CreateEntry(
          ns,
          index_meta_data_,
          std::make_unique<mongo::IndexDescriptor>(
              nullptr,
              mongo::IndexNames::findPluginName(index_meta_data->spec.getObjectField("key")),
              index_meta_data->spec))),
      multikey_paths_(index_meta_data->multikeyPaths) {}


// Refer to KVDatabaseCatalogEntry::getIndex
std::unique_ptr<mongo::IndexAccessMethod> MongoKeySchema::CreateIndexAccessMethod(
    mongo::OperationContext* opCtx, mongo::StringData ns, mongo::IndexCatalogEntry* index) {
    mongo::IndexDescriptor* desc = index->descriptor();
    const std::string& type = desc->getAccessMethodName();
    // SortedDataInterface is useless for KeySchema.
    mongo::SortedDataInterface* sdi = nullptr;

    if (type == mongo::IndexNames::BTREE) {
        return std::make_unique<mongo::BtreeAccessMethod>(index, sdi);
    } else if (type == mongo::IndexNames::HASHED) {
        return std::make_unique<mongo::HashAccessMethod>(index, sdi);
    } else if (type == mongo::IndexNames::GEO_2DSPHERE) {
        return std::make_unique<mongo::S2AccessMethod>(index, sdi);
    } else if (type == mongo::IndexNames::TEXT) {
        return std::make_unique<mongo::FTSAccessMethod>(index, sdi);
    } else if (type == mongo::IndexNames::GEO_HAYSTACK) {
        return std::make_unique<mongo::HaystackAccessMethod>(index, sdi);
    } else if (type == mongo::IndexNames::GEO_2D) {
        return std::make_unique<mongo::TwoDAccessMethod>(index, sdi);
    } else {
        mongo::log() << "Unsupported index type '" << type << "' for keyPattern "
                     << desc->keyPattern();
        MONGO_UNREACHABLE;
    }
}

std::unique_ptr<mongo::IndexCatalogEntry> MongoKeySchema::CreateEntry(
    std::string_view ns,
    const mongo::CollectionCatalogEntry::IndexMetaData* index_meta_data_,
    std::unique_ptr<mongo::IndexDescriptor> descriptor) {

    auto client = mongo::getGlobalServiceContext()->makeClient("eloq_table_schema");
    auto opCtx = mongo::getGlobalServiceContext()->makeOperationContext(client.get());
    auto entry = std::make_unique<mongo::IndexCatalogEntry>(opCtx.get(),
                                                            mongo::StringData{ns.data(), ns.size()},
                                                            std::move(descriptor),
                                                            index_meta_data_->ready,
                                                            index_meta_data_->head,
                                                            index_meta_data_->multikey,
                                                            index_meta_data_->multikeyPaths,
                                                            index_meta_data_->prefix);
    entry->init(
        CreateIndexAccessMethod(opCtx.get(), mongo::StringData{ns.data(), ns.size()}, entry.get()));
    return entry;
}

MongoTableSchema::MongoTableSchema(const txservice::TableName& table_name,
                                   const std::string& catalog_image,
                                   uint64_t version)
    : base_table_name_{table_name.StringView().data(),
                       table_name.StringView().size(),
                       table_name.Type(),
                       table_name.Engine()},
      schema_image_{catalog_image},
      version_{version} {
    EloqDS::DeserializeSchemaImage(
        catalog_image, meta_data_str_, kv_info_str_, key_schemas_ts_str_);

    txservice::TableKeySchemaTs key_schemas_ts{txservice::TableEngine::EloqDoc};
    size_t ts_offset = 0;
    key_schemas_ts.Deserialize(key_schemas_ts_str_.data(), ts_offset);

    size_t offset = 0;
    auto* ds = DataSubstrate::GetGlobal();
    invariant(ds != nullptr);
    auto* storeHandler = ds->GetStoreHandler();
    invariant(storeHandler != nullptr);
    kv_info_ = storeHandler->DeserializeKVCatalogInfo(kv_info_str_, offset);
    invariant(kv_info_.get() != nullptr);

    invariant(!meta_data_str_.empty());
    meta_data_obj_ = mongo::BSONObj(meta_data_str_.c_str()).getOwned();

    if (mongo::KVCatalog::FeatureTracker::isFeatureDocument(meta_data_obj_)) {
        uint64_t key_schema_ts = key_schemas_ts.GetKeySchemaTs(table_name);
        key_schema_ts = key_schema_ts == 1 ? version_ : key_schema_ts;
        key_schema_ = std::make_unique<MongoEmptyKeySchema>(key_schema_ts);
        return;
    }

    md_.parse(meta_data_obj_.getObjectField("md"));
    md_version_ = md_.catalogVersion();
    std::string_view ns{md_.ns};

    // pk
    uint64_t key_schema_ts = key_schemas_ts.GetKeySchemaTs(table_name);
    key_schema_ts = key_schema_ts == 1 ? version_ : key_schema_ts;
    const mongo::CollectionCatalogEntry::IndexMetaData& index_meta_data =
        md_.indexes[md_.findIndexOffset("_id_")];
    invariant(index_meta_data.ready);
    key_schema_ =
        std::make_unique<MongoKeySchema>(base_table_name_, key_schema_ts, ns, &index_meta_data);

    // sk
    for (uint16_t kid = 0; kid < md_.indexes.size(); ++kid) {
        const mongo::CollectionCatalogEntry::IndexMetaData& index_meta_data = md_.indexes[kid];
        invariant(index_meta_data.ready);
        std::string name = index_meta_data.name();
        if (name != "_id_") {
            bool is_unique = index_meta_data.spec["unique"].booleanSafe();
            txservice::TableName index_name =
                MongoIndexToTxServiceTableName(md_.ns, name, is_unique);
            uint64_t key_schema_ts = key_schemas_ts.GetKeySchemaTs(index_name);
            key_schema_ts = key_schema_ts == 1 ? version_ : key_schema_ts;

            auto sk_schema =
                std::make_unique<MongoKeySchema>(index_name, key_schema_ts, ns, &index_meta_data);
            indexes_.try_emplace(
                kid,
                std::move(index_name),
                txservice::SecondaryKeySchema(std::move(sk_schema), key_schema_.get()));
        }
    }
}

txservice::TableSchema::uptr MongoTableSchema::Clone() const {
    auto table_schema =
        std::make_unique<MongoTableSchema>(base_table_name_, schema_image_, version_);
    table_schema->BindStatistics(table_statistics_);
    return table_schema;
}

void MongoTableSchema::SetKVCatalogInfo(const std::string& kv_info) {
    size_t offset = 0;
    auto* ds = DataSubstrate::GetGlobal();
    invariant(ds != nullptr);
    auto* storeHandler = ds->GetStoreHandler();
    invariant(storeHandler != nullptr);
    kv_info_ = storeHandler->DeserializeKVCatalogInfo(kv_info, offset);
}

txservice::SkEncoder::uptr MongoTableSchema::CreateSkEncoder(
    const txservice::TableName& index_name) const {

    txservice::SkEncoder::uptr sk_encoder;
    auto key_schema =
        static_cast<const MongoKeySchema*>(IndexKeySchema(index_name)->sk_schema_.get());
    const mongo::BSONObj& spec = key_schema->index_meta_data_->spec;
    invariant(index_name.IsUniqueSecondary() == spec["unique"].booleanSafe());
    if (index_name.IsUniqueSecondary()) {
        return std::make_unique<MongoUniqueSkEncoder>(key_schema);
    } else {
        return std::make_unique<MongoStandardSkEncoder>(key_schema);
    }
}

// See KVCatalog:putMetaData about how rebuild metadata.
void MongoTableSchema::IndexesSetMultiKeyAttr(const std::vector<txservice::MultiKeyAttr>& indexes) {
    assert(!indexes.empty());

    for (const txservice::MultiKeyAttr& index : indexes) {
        uint16_t kid = IndexOffset(*index.index_name_);

        const auto& multikey_paths = static_cast<const MongoMultiKeyPaths&>(*index.multikey_paths_);

        md_.indexes[kid].multikey = index.multikey_;
        md_.indexes[kid].multikeyPaths = multikey_paths.multikey_paths_;
        txservice::SecondaryKeySchema& index_schema = indexes_.at(kid).second;
        std::string_view ns{md_.ns};

        index_schema.sk_schema_ = std::make_unique<MongoKeySchema>(
            *index.index_name_, index_schema.SchemaTs(), ns, &md_.indexes[kid]);
    }

    // rebuilt doc
    mongo::BSONObjBuilder b;
    md_.updateCatalogVersion();
    b.append("md", md_.toBSON());

    mongo::BSONObjBuilder newIdentMap;
    mongo::BSONObj oldIdentMap;
    if (meta_data_obj_["idxIdent"].isABSONObj()) {
        oldIdentMap = meta_data_obj_["idxIdent"].Obj();
    }

    // fix ident map
    for (const auto& index : md_.indexes) {
        std::string name = index.name();
        mongo::BSONElement e = oldIdentMap[name];
        if (e.type() == mongo::String) {
            newIdentMap.append(e);
            continue;
        }
        // missing, create new
        std::string uniqueIdent = md_.ns;
        uniqueIdent.append(".").append(name);
        newIdentMap.append(name, uniqueIdent);
    }
    b.append("idxIdent", newIdentMap.obj());

    // add whatever is left
    b.appendElementsUnique(meta_data_obj_);
    meta_data_obj_ = b.obj().getOwned();
    MONGO_LOG(1) << "IndexesSetMultiKeyAttr rebuild meta_data_obj_: "
                 << meta_data_obj_.jsonString();

    meta_data_str_.assign(meta_data_obj_.objdata(), meta_data_obj_.objsize());
    schema_image_ = EloqDS::SerializeSchemaImage(meta_data_str_, kv_info_str_, key_schemas_ts_str_);
}

bool MongoSkEncoder::GenerateBSONKeys(const txservice::TxKey* pk,
                                      const txservice::TxRecord* record,
                                      mongo::BSONObjSet* skeys) {
    bool succeed = false;

    const auto* mongo_rec = static_cast<const MongoRecord*>(record);

    mongo::BSONObj record_obj(mongo_rec->EncodedBlobData());

    invariant(([pk, &record_obj]() {
                  const MongoKey* mongo_pk = pk->GetKey<MongoKey>();
                  mongo::RecordId record_id(mongo_pk->Data(), mongo_pk->Size());
                  mongo::BSONObj id_obj = mongo::getIdBSONObjWithoutFieldName(record_obj);
                  mongo::KeyString keystring_pk(
                      mongo::KeyString::kLatestVersion, id_obj, mongo::kIdOrdering);
                  return record_id ==
                      mongo::RecordId(keystring_pk.getBuffer(), keystring_pk.getSize());
              }()) == true);

    try {
        mongo::MultikeyPaths multikey_paths;
        key_schema_->GetKeys(record_obj, skeys, &multikey_paths);

        if (mongo::failIndexKeyTooLong.load()) {
            for (const mongo::BSONObj& bson_sk : *skeys) {
                uassertStatusOK(
                    mongo::checkKeySize(bson_sk, key_schema_->index_name_.StringView()));
            }
        }

        // In sparse index, some records may miss the index field
        // invariant(skeys->size() > 0);

        // For multi-key index
        if (skeys->size() > 1 || MongoKeySchema::IsMultiKeyFromPaths(multikey_paths)) {
            MergeMultiKeyAttr(multikey_paths);
        }
        txservice::SkEncoder::ClearError();
        succeed = true;
    } catch (const mongo::DBException& e) {
        txservice::SkEncoder::SetError(e.code(), e.what());
        MONGO_LOG(1) << "MongoSkEncoder::GenerateBSONKeys raise DBException" << e.what();
        succeed = false;
    }

    return succeed;
}

mongo::RecordId MongoSkEncoder::GenerateRecordID(const txservice::TxKey* pk) const {
    const MongoKey* mongo_pk = pk->GetKey<MongoKey>();
    return mongo::RecordId(mongo_pk->Data(), mongo_pk->Size());
}

void MongoSkEncoder::MergeMultiKeyAttr(const mongo::MultikeyPaths& multikey_paths) {
    mutable_multikey_ = true;
    mutable_multikey_paths_.MergeWith(multikey_paths);
}

int32_t MongoStandardSkEncoder::AppendPackedSk(const txservice::TxKey* pk,
                                               const txservice::TxRecord* record,
                                               uint64_t version_ts,
                                               std::vector<txservice::WriteEntry>& dest_vec) {
    mongo::BSONObjSet skeys = mongo::SimpleBSONObjComparator::kInstance.makeBSONObjSet();
    bool succeed = GenerateBSONKeys(pk, record, &skeys);
    if (!succeed) {
        return -1;
    }
    if (!skeys.empty()) {
        mongo::RecordId record_id = GenerateRecordID(pk);
        for (const mongo::BSONObj& bson_sk : skeys) {
            mongo::KeyString keystring_sk(
                mongo::KeyString::kLatestVersion, bson_sk, key_schema_->Ordering(), record_id);
            auto mongo_sk =
                std::make_unique<MongoKey>(keystring_sk.getBuffer(), keystring_sk.getSize());
            auto mongo_sk_rec = std::make_unique<MongoRecord>();
            if (const auto& type_bits = keystring_sk.getTypeBits(); !type_bits.isAllZeros()) {
                mongo_sk_rec->SetUnpackInfo(type_bits.getBuffer(), type_bits.getSize());
            }
            dest_vec.emplace_back(
                txservice::TxKey(std::move(mongo_sk)), std::move(mongo_sk_rec), version_ts);
        }
    }
    return static_cast<int32_t>(skeys.size());
}

int32_t MongoUniqueSkEncoder::AppendPackedSk(const txservice::TxKey* pk,
                                             const txservice::TxRecord* record,
                                             uint64_t version_ts,
                                             std::vector<txservice::WriteEntry>& dest_vec) {
    mongo::BSONObjSet skeys = mongo::SimpleBSONObjComparator::kInstance.makeBSONObjSet();
    bool succeed = GenerateBSONKeys(pk, record, &skeys);
    if (!succeed) {
        return -1;
    }
    if (!skeys.empty()) {
        mongo::RecordId record_id = GenerateRecordID(pk);
        for (const mongo::BSONObj& bson_sk : skeys) {
            mongo::KeyString keystring_sk(
                mongo::KeyString::kLatestVersion, bson_sk, key_schema_->Ordering());
            auto mongo_sk =
                std::make_unique<MongoKey>(keystring_sk.getBuffer(), keystring_sk.getSize());
            auto mongo_sk_rec = std::make_unique<MongoRecord>();
            mongo_sk_rec->SetEncodedBlob(record_id.getStringView());
            if (const auto& type_bits = keystring_sk.getTypeBits(); !type_bits.isAllZeros()) {
                mongo_sk_rec->SetUnpackInfo(type_bits.getBuffer(), type_bits.getSize());
            }
            dest_vec.emplace_back(
                txservice::TxKey(std::move(mongo_sk)), std::move(mongo_sk_rec), version_ts);
        }
    }
    return static_cast<int32_t>(skeys.size());
}

}  // namespace Eloq
