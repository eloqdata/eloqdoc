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

#include "mongo/platform/basic.h"

#include "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_record_store.h"

#include "mongo/base/init.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index_names.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_engine.h"
#include "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_recovery_unit.h"
#include "mongo/db/storage/record_store_test_harness.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

class NamespaceCatalogTest : public unittest::Test {
public:
    NamespaceCatalogTest() : opCtx(engine.newRecoveryUnit()) {
        ASSERT_OK(engine.createRecordStore(&opCtx, "_mdb_catalog", "_mdb_catalog", {}));
        catalog = engine.getRecordStore(&opCtx, "_mdb_catalog", "_mdb_catalog", {});
    }

    StatusWith<RecordId> insert(const BSONObj& doc) {
        return catalog->insertRecord(&opCtx, doc.objdata(), doc.objsize(), Timestamp(), false);
    }

    BSONObj metadata() {
        return BSON("ns" << "test.collection" << "md"
                         << BSON("options" << BSON("catalogVersion" << "version-1")));
    }

    EphemeralForTestEngine engine;
    OperationContextNoop opCtx;
    std::unique_ptr<RecordStore> catalog;
};

TEST_F(NamespaceCatalogTest, NamespaceKeysAndEnumerationFollowTransactionRollback) {
    {
        WriteUnitOfWork wuow(&opCtx);
        auto id = insert(metadata());
        ASSERT_OK(id.getStatus());
        ASSERT_EQ(RecordId("test.collection", 15), id.getValue());
        ASSERT_TRUE(engine.databaseExists("test"));
        // Roll back this insertion.
    }
    ASSERT_FALSE(engine.databaseExists("test"));
    {
        WriteUnitOfWork wuow(&opCtx);
        ASSERT_OK(insert(metadata()).getStatus());
        wuow.commit();
    }
    std::vector<std::string> databases;
    engine.listDatabases(databases);
    ASSERT_EQ(1U, databases.size());
    ASSERT_EQ("test", databases.front());
    std::vector<std::string> collections;
    engine.listCollections("test", collections);
    ASSERT_EQ(1U, collections.size());
    ASSERT_EQ("test.collection", collections.front());
    std::set<std::string> names;
    engine.listCollections("test", names);
    ASSERT_EQ(1U, names.count("test.collection"));
    ASSERT_FALSE(engine.databaseExists("other"));

    bool exists = false;
    std::string version;
    ASSERT_OK(engine.lockCollection(&opCtx, "test.collection", false, &exists, &version));
    ASSERT_TRUE(exists);
    ASSERT_EQ("version-1", version);
    ASSERT_OK(engine.lockCollection(&opCtx, "test.missing", true, &exists, &version));
    ASSERT_FALSE(exists);
    ASSERT_TRUE(version.empty());
}

TEST_F(NamespaceCatalogTest, DuplicateInsertionAndAbortedDeletionPreserveMetadata) {
    {
        WriteUnitOfWork wuow(&opCtx);
        ASSERT_OK(insert(metadata()).getStatus());
        wuow.commit();
    }
    {
        WriteUnitOfWork wuow(&opCtx);
        ASSERT_EQ(ErrorCodes::DuplicateKey, insert(metadata()).getStatus().code());
    }
    {
        WriteUnitOfWork wuow(&opCtx);
        catalog->deleteRecord(&opCtx, RecordId("test.collection", 15));
        ASSERT_FALSE(engine.databaseExists("test"));
    }
    ASSERT_TRUE(engine.databaseExists("test"));
    {
        WriteUnitOfWork wuow(&opCtx);
        catalog->deleteRecord(&opCtx, RecordId("test.collection", 15));
        wuow.commit();
    }
    ASSERT_FALSE(engine.databaseExists("test"));
}

TEST_F(NamespaceCatalogTest, FeatureDocumentDoesNotAppearAsACollection) {
    WriteUnitOfWork wuow(&opCtx);
    auto id = insert(BSON("isFeatureDoc" << true << "ns" << BSONNULL));
    ASSERT_OK(id.getStatus());
    ASSERT_EQ(RecordId("featureDocument", 15), id.getValue());
    RecordData data;
    ASSERT_TRUE(catalog->findRecord(&opCtx, id.getValue(), &data, true));
    std::vector<std::string> collections;
    catalog->getAllCollections(collections);
    ASSERT_TRUE(collections.empty());
    wuow.commit();
}

TEST_F(NamespaceCatalogTest, PooledIndexCursorReturnsDocumentForIDHack) {
    auto records = engine.getRecordStore(&opCtx, "test.collection", "test.collection", {});
    IndexDescriptor descriptor(nullptr, IndexNames::BTREE,
                               BSON("v" << 2 << "key" << BSON("_id" << 1) << "name" << "_id_"
                                        << "ns" << "test.collection" << "unique" << true));
    ASSERT_OK(engine.createSortedDataInterface(&opCtx, "test.collection._id_", &descriptor));
    std::unique_ptr<SortedDataInterface> index(
        engine.getSortedDataInterface(&opCtx, "test.collection._id_", &descriptor));
    BSONObj document = BSON("_id" << 42 << "payload" << "record from index lookup");
    {
        WriteUnitOfWork wuow(&opCtx);
        auto id = records->insertRecord(&opCtx, document.objdata(), document.objsize(),
                                        Timestamp(), false);
        ASSERT_OK(id.getStatus());
        ASSERT_OK(index->insert(&opCtx, BSON("" << 42), id.getValue(), false));
        wuow.commit();
    }
    auto cursor = index->newCursorPtr(&opCtx);
    auto entry = cursor->seekExact(BSON("" << 42));
    ASSERT_TRUE(entry);
    ASSERT_TRUE(entry->record);
    ASSERT_EQ(42, entry->record->toBson()["_id"].Int());
    ASSERT_EQ("record from index lookup", entry->record->toBson()["payload"].String());
}

class EphemeralForTestHarnessHelper final : public RecordStoreHarnessHelper {
public:
    EphemeralForTestHarnessHelper() {}

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore() {
        return newNonCappedRecordStore("a.b");
    }

    virtual std::unique_ptr<RecordStore> newNonCappedRecordStore(const std::string& ns) {
        return stdx::make_unique<EphemeralForTestRecordStore>(ns, &data);
    }

    virtual std::unique_ptr<RecordStore> newCappedRecordStore(int64_t cappedSizeBytes,
                                                              int64_t cappedMaxDocs) {
        return newCappedRecordStore("a.b", cappedSizeBytes, cappedMaxDocs);
    }

    virtual std::unique_ptr<RecordStore> newCappedRecordStore(const std::string& ns,
                                                              int64_t cappedSizeBytes,
                                                              int64_t cappedMaxDocs) final {
        return stdx::make_unique<EphemeralForTestRecordStore>(
            ns, &data, true, cappedSizeBytes, cappedMaxDocs);
    }

    std::unique_ptr<RecoveryUnit> newRecoveryUnit() final {
        return stdx::make_unique<EphemeralForTestRecoveryUnit>();
    }

    bool supportsDocLocking() final {
        return false;
    }

    std::shared_ptr<void> data;
};

std::unique_ptr<HarnessHelper> makeHarnessHelper() {
    return stdx::make_unique<EphemeralForTestHarnessHelper>();
}

MONGO_INITIALIZER(RegisterHarnessFactory)(InitializerContext* const) {
    mongo::registerHarnessHelperFactory(makeHarnessHelper);
    return Status::OK();
}
}  // namespace
}  // namespace mongo
