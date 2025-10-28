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

// must declare this macro in every file
// because braft and mongodb both use the third-party abseil,
// that will declaration some same symbols

// #ifndef DYNAMIC_ANNOTATIONS_PROVIDE_RUNNING_ON_VALGRIND
// #define DYNAMIC_ANNOTATIONS_PROVIDE_RUNNING_ON_VALGRIND 0
// #endif

#include <string>
#include <string_view>
#include <utility>

#include "mongo/db/storage/bson_collection_catalog_entry.h"
#include "mongo/db/storage/kv/kv_catalog_feature_tracker.h"
#include "mongo/util/assert_util.h"

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/constants.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/error_messages.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/store/data_store_handler.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/type.h"

#include "mongo/db/modules/eloq/data_substrate/core/include/data_substrate.h"

namespace txservice {}  // namespace txservice
namespace Eloq {

inline bool GetAllTables(std::vector<std::string>& tables,
                         const std::function<void()>* yieldFuncPtr,
                         const std::function<void()>* resumeFuncPtr) {
    auto* storeHandler = DataSubstrate::GetGlobal()->GetStoreHandler();
    bool success = storeHandler->DiscoverAllTableNames(tables, yieldFuncPtr, resumeFuncPtr);
    if (!success) {
        return false;
    }

    tables.erase(std::remove(tables.begin(), tables.end(), txservice::sequence_table_name_sv),
                 tables.end());
    return true;
}

/*
 * If you only need metadata
 */
inline void DeserializeSchemaImage(const std::string& image, std::string& metadata) {
    size_t offset = 0;
    const char* buf = image.data();

    size_t len_val = *reinterpret_cast<const size_t*>(buf + offset);
    offset += sizeof(size_t);
    metadata.append(buf + offset, len_val);
    offset += len_val;

    len_val = *reinterpret_cast<const size_t*>(buf + offset);
    offset += sizeof(size_t) + len_val;

    len_val = *reinterpret_cast<const size_t*>(buf + offset);
    offset += sizeof(size_t) + len_val;

    assert(offset == image.length());
}

inline txservice::TableName MongoTableToTxServiceTableName(std::string_view ns, bool own_string) {
    return own_string
        ? txservice::TableName{ns.data(),
                               ns.size(),
                               txservice::TableType::Primary,
                               txservice::TableEngine::EloqDoc}
        : txservice::TableName{ns, txservice::TableType::Primary, txservice::TableEngine::EloqDoc};
}

inline txservice::TableName MongoIndexToTxServiceTableName(std::string_view ns,
                                                           std::string_view index_name,
                                                           bool is_unique) {
    std::string table_name;
    table_name.reserve(ns.size() + txservice::UNIQUE_INDEX_NAME_PREFIX.size() + index_name.size());

    table_name.append(ns);
    if (is_unique) {
        table_name.append(txservice::UNIQUE_INDEX_NAME_PREFIX);
    } else {
        table_name.append(txservice::INDEX_NAME_PREFIX);
    }
    table_name.append(index_name);

    return txservice::TableName{std::move(table_name),
                                is_unique ? txservice::TableType::UniqueSecondary
                                          : txservice::TableType::Secondary,
                                txservice::TableEngine::EloqDoc};
}

inline bool ContainsUnreadyIndex(const mongo::BSONObj& obj) {
    // a special bson
    if (mongo::KVCatalog::FeatureTracker::isFeatureDocument(obj)) {
        return false;
    }

    mongo::BSONCollectionCatalogEntry::MetaData md;
    md.parse(obj.getObjectField("md"));

    for (const auto& index : md.indexes) {
        if (!index.ready) {
            return true;
        }
    }
    return false;
}

inline std::pair<std::string, std::set<txservice::TableName>> ExtractReadyIndexesSet(
    const mongo::BSONObj& obj) {
    std::set<txservice::TableName> ready_indexes;
    // a special bson
    if (mongo::KVCatalog::FeatureTracker::isFeatureDocument(obj)) {
        return {"", std::move(ready_indexes)};
    }

    mongo::BSONCollectionCatalogEntry::MetaData md;
    md.parse(obj.getObjectField("md"));

    for (const auto& index : md.indexes) {
        if (auto name = index.name(); name != "_id_" && index.ready) {
            bool is_unique = index.spec["unique"].booleanSafe();
            auto table_name = MongoIndexToTxServiceTableName(md.ns, name, is_unique);
            ready_indexes.insert(std::move(table_name));
        }
    }

    return {std::move(md.ns), std::move(ready_indexes)};
}
}  // namespace Eloq


namespace mongo {
Status TxErrorCodeToMongoStatus(txservice::TxErrorCode txErr);

inline constexpr std::string_view kMongoCatalogTableNameSV{"_mdb_catalog"};
inline bool isMongoCatalog(std::string_view sv) {
    return sv == kMongoCatalogTableNameSV;
}

inline constexpr std::string_view kFeatureDocumentSV{"featureDocument"};
inline constexpr StringData kEloqEngineName = "eloq"_sd;

inline bool hasFieldNames(const BSONObj& obj) {
    for (const auto& e : obj) {
        if (e.fieldName()[0]) {
            return true;
        }
    }
    return false;
}

inline BSONObj stripFieldNames(const BSONObj& query) {
    if (!hasFieldNames(query)) {
        return query;
    }

    BSONObjBuilder bb;
    for (const auto& e : query) {
        bb.appendAs(e, StringData());
    }
    return bb.obj();
}

inline BSONObj getIdBSONObjWithoutFieldName(const BSONObj& obj) {
    auto e = obj["_id"];
    if (e.eoo()) {
        MONGO_UNREACHABLE;
        return {};
    }
    auto size = e.size() + 5 /* bson over head */ - 3 /* remove _id string */;
    BSONObjBuilder builder{size};
    builder.appendAs(e, "");
    return builder.obj();
}

inline constexpr int MaxKeySize = 1024;
inline Status checkKeySize(const BSONObj& key, std::string_view indexName) {
    if (key.objsize() >= MaxKeySize) {
        std::stringstream ss;
        ss << "Insert " << indexName
           << " fail: key too large to index, key size: " << key.objsize();
        return {ErrorCodes::KeyTooLong, ss.str()};
    }
    return Status::OK();
}

}  // namespace mongo
