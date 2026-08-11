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
    auto* storeHandler = DataSubstrate::Instance().GetStoreHandler();
    bool success = storeHandler->DiscoverAllTableNames(
        txservice::TableEngine::EloqDoc, tables, yieldFuncPtr, resumeFuncPtr);
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
/**
 * Throws mongo::WriteConflictException if txErr belongs to the group of tx
 * service errors that mean "another transaction got there first"; returns
 * normally for every other code, including NO_ERROR.
 *
 * The group is wider than its name suggests: besides WRITE_WRITE_CONFLICT it
 * covers OCC_BREAK_REPEATABLE_READ, DEAD_LOCK_ABORT, GET_RANGE_ID_ERROR,
 * SI_R4W_ERR_KEY_WAS_UPDATED and UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL. All of
 * them are resolved the same way -- abort the transaction and let the command
 * layer re-run it -- which is what WriteConflictException means to mongo.
 *
 * This exists for call sites that must surface a conflict immediately while
 * keeping their own retry loop for transient errors, so they cannot simply run
 * TxErrorCodeToMongoStatus through uassertStatusOK. TxErrorCodeToMongoStatus
 * calls this first, so the two can never drift apart.
 *
 * The thrown type matters: writeConflictRetry catches only
 * WriteConflictException, which is `final : public DBException` and has no
 * inheritance relationship with ExceptionFor<ErrorCodes::WriteConflict>.
 * Returning a Status carrying ErrorCodes::WriteConflict would sail straight
 * through every writeConflictRetry boundary.
 */
void ThrowIfWriteConflict(txservice::TxErrorCode txErr);

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
