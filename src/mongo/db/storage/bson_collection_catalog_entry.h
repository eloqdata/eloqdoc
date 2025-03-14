// bson_collection_catalog_entry.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/index/multikey_paths.h"
#include "mongo/db/storage/kv/kv_prefix.h"
#include "mongo/db/storage/record_store.h"

namespace mongo {

/**
 * This is a helper class for any storage engine that wants to store catalog information
 * as BSON. It is totally optional to use this.
 */
class BSONCollectionCatalogEntry : public CollectionCatalogEntry {
public:
    BSONCollectionCatalogEntry(StringData ns);

    virtual ~BSONCollectionCatalogEntry() {}

    // virtual CollectionOptions getCollectionOptions(OperationContext* opCtx) const = 0;
    MetaData getMetaData(OperationContext* opCtx) const override;

    virtual int getTotalIndexCount(OperationContext* opCtx) const;

    virtual int getCompletedIndexCount(OperationContext* opCtx) const;

    virtual BSONObj getIndexSpec(OperationContext* opCtx, StringData idxName) const;

    virtual void getAllIndexes(OperationContext* opCtx, std::vector<std::string>* names) const;

    virtual void getReadyIndexes(OperationContext* opCtx, std::vector<std::string>* names) const;

    virtual void getAllUniqueIndexes(OperationContext* opCtx,
                                     std::vector<std::string>* names) const;

    virtual bool isIndexMultikey(OperationContext* opCtx,
                                 StringData indexName,
                                 MultikeyPaths* multikeyPaths) const;

    virtual RecordId getIndexHead(OperationContext* opCtx, StringData indexName) const;

    virtual bool isIndexReady(OperationContext* opCtx, StringData indexName) const;

    virtual bool isIndexPresent(OperationContext* opCtx, StringData indexName) const;

    virtual KVPrefix getIndexPrefix(OperationContext* opCtx, StringData indexName) const;

    // RecordStore* getRecordStore() override {
    //     return nullptr;
    // }

    // virtual const RecordStore* getRecordStore() const {
    //     return nullptr;
    // }

    // ------ for implementors

protected:
    virtual MetaData _getMetaData(OperationContext* opCtx) const = 0;
};
}  // namespace mongo
