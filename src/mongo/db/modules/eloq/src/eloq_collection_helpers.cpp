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

#include "mongo/db/modules/eloq/src/eloq_collection_helpers.h"

#include "mongo/base/error_codes.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/storage/key_string.h"

#include "mongo/db/modules/eloq/src/base/eloq_util.h"
#include "mongo/db/modules/eloq/src/eloq_recovery_unit.h"

namespace mongo {
namespace eloq_collection_helpers {

Status validateUniqueIndexConstraints(
    OperationContext* opCtx,
    const NamespaceString& ns,
    IndexCatalog* indexCatalog,
    std::vector<InsertStatement>::const_iterator begin,
    std::vector<InsertStatement>::const_iterator end) {
    // Only validate for Eloq storage engine
    auto* eloqRU = dynamic_cast<EloqRecoveryUnit*>(opCtx->recoveryUnit());
    if (!eloqRU) {
        // Not Eloq storage engine, skip validation
        return Status::OK();
    }

    // Get all unique indexes
    IndexCatalog::IndexIterator indexIter = indexCatalog->getIndexIterator(opCtx, false);
    std::vector<IndexCatalogEntry*> uniqueIndexes;
    while (indexIter.more()) {
        IndexCatalogEntry* entry = indexIter.next();
        const IndexDescriptor* desc = entry->descriptor();
        if (desc->unique() && entry->isReady(opCtx)) {
            uniqueIndexes.push_back(entry);
        }
    }

    // If no unique indexes, nothing to validate
    if (uniqueIndexes.empty()) {
        return Status::OK();
    }

    // Construct table name for the collection
    txservice::TableName tableName{Eloq::MongoTableToTxServiceTableName(ns.ns(), true)};

    // For each unique index, track keys seen in the current batch to detect duplicates within batch
    std::map<const IndexDescriptor*, BSONObjSet> batchKeys;

    // For each document to be inserted
    for (auto docIt = begin; docIt != end; ++docIt) {
        const BSONObj& doc = docIt->doc;

        // For each unique index
        for (IndexCatalogEntry* entry : uniqueIndexes) {
            const IndexDescriptor* desc = entry->descriptor();
            const IndexAccessMethod* accessMethod = entry->accessMethod();

            // Extract keys for this index
            BSONObjSet keys = SimpleBSONObjComparator::kInstance.makeBSONObjSet();
            MultikeyPaths multikeyPaths;
            accessMethod->getKeys(
                doc, IndexAccessMethod::GetKeysMode::kEnforceConstraints, &keys, &multikeyPaths);

            // Construct index table name
            txservice::TableName indexName{desc->isIdIndex()
                                               ? tableName
                                               : Eloq::MongoIndexToTxServiceTableName(
                                                     desc->parentNS(), desc->indexName(), true)};

            // Get index schema version
            uint64_t keySchemaVersion = eloqRU->getIndexSchema(tableName, indexName)->SchemaTs();

            // Check each key for duplicates
            for (const BSONObj& key : keys) {
                // First, check if this key was already seen in the current batch
                auto& seenKeys = batchKeys[desc];
                if (seenKeys.count(key) > 0) {
                    // Duplicate key within the batch
                    return {ErrorCodes::DuplicateKey,
                            "Duplicate Key: " + indexName.String() + " key: " + key.toString()};
                }

                // Convert BSON key to MongoKey
                KeyString keyString{KeyString::kLatestVersion, key, desc->ordering()};
                auto mongoKey = std::make_unique<Eloq::MongoKey>(keyString.getBuffer(),
                                                                  keyString.getSize());

                // Check if key exists in database or writeset (getKV checks both)
                Eloq::MongoRecord mongoRecord;
                auto [exists, err] = eloqRU->getKV(
                    opCtx, indexName, keySchemaVersion, mongoKey.get(), &mongoRecord, true);

                if (err != txservice::TxErrorCode::NO_ERROR) {
                    return Eloq::TxErrorCodeToMongoStatus(err);
                }

                if (exists) {
                    // Duplicate key found in database or writeset
                    return {ErrorCodes::DuplicateKey,
                            "Duplicate Key: " + indexName.String() + " key: " + key.toString()};
                }

                // Add key to batch keys set for duplicate detection within batch
                seenKeys.insert(key.getOwned());
            }
        }
    }

    return Status::OK();
}

}  // namespace eloq_collection_helpers
}  // namespace mongo

