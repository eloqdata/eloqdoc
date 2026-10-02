/**
 *    Copyright (C) 2026-present EloqData Inc.
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
 */

/**
 * @file
 * DDL decoder tests (docs/cdc-change-stream.md §5.4 "Per DDL record", §5.4a; plan C7): the
 * change each recorded schema op reports, the ops no event reports, and the schema images the
 * stream refuses.
 */

#include "mongo/platform/basic.h"

#include <cstring>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_ddl_decoder.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_test_schema_op.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using cdc_test::idIndexSpec;
using cdc_test::indexSpec;
using cdc_test::schemaImage;
using cdc_test::schemaOp;

const StringData kNs = "test.c"_sd;

TEST(CdcDdlDecoder, CreateReportsTheOptionsListCollectionsReports) {
    // The server's own options, the UUID and EloqDoc's catalog version, are left out.
    BSONObjBuilder stored;
    stored.appendBinData("uuid", 16, newUUID, "0123456789abcdef");
    stored.append("catalogVersion", "12345");
    stored.append("validator", BSON("a" << BSON("$gt" << 0)));
    stored.append("collation", BSON("locale" << "fr"));
    const boost::optional<CdcDdlChange> change = decodeCdcDdl(schemaOp(
        kNs, CdcDdlOpType::kCreateTable, "", schemaImage(kNs, stored.obj(), {idIndexSpec(kNs)})));
    ASSERT(change);
    ASSERT_EQ(change->operationType, "create"_sd);
    ASSERT_EQ(change->ns, kNs);
    ASSERT_BSONOBJ_EQ(change->operationDescription,
                      BSON("validator" << BSON("a" << BSON("$gt" << 0)) << "collation"
                                       << BSON("locale" << "fr")));
    ASSERT_FALSE(change->dropsCollection);

    // A collection with no options of the user's has an empty description.
    const boost::optional<CdcDdlChange> plain = decodeCdcDdl(cdc_test::createOp(kNs));
    ASSERT(plain);
    ASSERT_BSONOBJ_EQ(plain->operationDescription, BSONObj());
}

TEST(CdcDdlDecoder, DropReportsTheCollectionAndNothingElse) {
    const boost::optional<CdcDdlChange> change = decodeCdcDdl(cdc_test::dropOp(kNs));
    ASSERT(change);
    ASSERT_EQ(change->operationType, "drop"_sd);
    ASSERT_EQ(change->ns, kNs);
    ASSERT(change->operationDescription.isEmpty());
    ASSERT(change->dropsCollection);
}

TEST(CdcDdlDecoder, AddIndexReportsEveryIndexItAdded) {
    const BSONObj options = BSON("catalogVersion" << "1");
    const BSONObj existing = indexSpec(kNs, "x_1", BSON("x" << 1));
    const BSONObj a = indexSpec(kNs, "a_1", BSON("a" << 1));
    const BSONObj b = indexSpec(
        kNs, "b_-1", BSON("b" << -1), BSON("unique" << true << "expireAfterSeconds" << 60));
    const txlog::SchemaOpMessage op =
        schemaOp(kNs,
                 CdcDdlOpType::kAddIndex,
                 schemaImage(kNs, options, {idIndexSpec(kNs), existing}),
                 schemaImage(kNs, options, {idIndexSpec(kNs), existing, a, b}));

    const boost::optional<CdcDdlChange> change = decodeCdcDdl(op);
    ASSERT(change);
    ASSERT_EQ(change->operationType, "createIndexes"_sd);
    ASSERT_EQ(change->ns, kNs);
    // Each spec as listIndexes reports it, unique and TTL included, without the ns.
    ASSERT_BSONOBJ_EQ(change->operationDescription,
                      BSON("indexes" << BSON_ARRAY(a.removeField("ns") << b.removeField("ns"))));
    ASSERT_FALSE(change->dropsCollection);
}

TEST(CdcDdlDecoder, RolledBackAddIndexReportsItsIndexesDropped) {
    const BSONObj options = BSON("catalogVersion" << "1");
    const BSONObj a = indexSpec(kNs, "a_1", BSON("a" << 1), BSON("unique" << true));
    const txlog::SchemaOpMessage op = schemaOp(kNs,
                                               CdcDdlOpType::kAddIndex,
                                               schemaImage(kNs, options, {idIndexSpec(kNs)}),
                                               schemaImage(kNs, options, {idIndexSpec(kNs), a}),
                                               true);
    const boost::optional<CdcDdlChange> change = decodeCdcDdl(op);
    ASSERT(change);
    ASSERT_EQ(change->operationType, "dropIndexes"_sd);
    ASSERT_BSONOBJ_EQ(
        change->operationDescription,
        BSON("indexes" << BSON_ARRAY(BSON("v" << 2 << "key" << BSON("a" << 1) << "name"
                                              << "a_1"))));
}

TEST(CdcDdlDecoder, DropIndexReportsEveryIndexItDropped) {
    const BSONObj options = BSON("catalogVersion" << "1");
    const BSONObj a = indexSpec(kNs, "a_1", BSON("a" << 1));
    const BSONObj b = indexSpec(kNs, "b_1", BSON("b" << 1), BSON("sparse" << true));
    const txlog::SchemaOpMessage op = schemaOp(kNs,
                                               CdcDdlOpType::kDropIndex,
                                               schemaImage(kNs, options, {idIndexSpec(kNs), a, b}),
                                               schemaImage(kNs, options, {idIndexSpec(kNs), a}));
    const boost::optional<CdcDdlChange> change = decodeCdcDdl(op);
    ASSERT(change);
    ASSERT_EQ(change->operationType, "dropIndexes"_sd);
    // The version, key pattern and name only.
    ASSERT_BSONOBJ_EQ(
        change->operationDescription,
        BSON("indexes" << BSON_ARRAY(BSON("v" << 2 << "key" << BSON("b" << 1) << "name"
                                              << "b_1"))));
}

TEST(CdcDdlDecoder, OpsNoEventReportsAreSkipped) {
    txlog::SchemaOpMessage otherEngine = cdc_test::createOp(kNs);
    otherEngine.set_table_engine(txlog::TableEngine::EloqKv);
    ASSERT_FALSE(decodeCdcDdl(otherEngine));

    txlog::SchemaOpMessage columnOp = cdc_test::createOp(kNs);
    columnOp.mutable_column_op();
    ASSERT_FALSE(decodeCdcDdl(columnOp));

    // A logical alter (Update) and a truncate.
    for (uint32_t type : {1u, 7u}) {
        txlog::SchemaOpMessage op = cdc_test::addIndexOp(kNs);
        op.mutable_table_op()->set_op_type(type);
        ASSERT_FALSE(decodeCdcDdl(op)) << type;
    }

    txlog::SchemaOpMessage rolledBackCreate = cdc_test::createOp(kNs);
    rolledBackCreate.set_rolled_back(true);
    ASSERT_FALSE(decodeCdcDdl(rolledBackCreate));
    txlog::SchemaOpMessage rolledBackDrop = cdc_test::dropOp(kNs);
    rolledBackDrop.set_rolled_back(true);
    ASSERT_FALSE(decodeCdcDdl(rolledBackDrop));

    // The server's feature document, an engine table that is no collection.
    ASSERT_FALSE(decodeCdcDdl(cdc_test::createOp("featureDocument")));
}

TEST(CdcDdlDecoder, UndecodableImagesFailTheStream) {
    const std::string good = schemaImage(kNs, BSONObj(), {idIndexSpec(kNs)});
    size_t metadataLength = 0;
    std::memcpy(&metadataLength, good.data(), sizeof(metadataLength));

    std::string pastTheEnd = good.substr(0, sizeof(size_t) + metadataLength - 1);
    std::string notBson = good;
    notBson[sizeof(size_t) + 3] = 0x7f;  // a BSON length far past the part's
    std::string noMd = good;
    {
        const BSONObj metadata = BSON("ns" << kNs);
        size_t length = static_cast<size_t>(metadata.objsize());
        noMd.assign(reinterpret_cast<const char*>(&length), sizeof(length));
        noMd.append(metadata.objdata(), length);
    }
    for (const std::string& image : {std::string("abc"), pastTheEnd, notBson, noMd}) {
        ASSERT_THROWS_CODE(decodeCdcDdl(schemaOp(kNs, CdcDdlOpType::kCreateTable, "", image)),
                           AssertionException,
                           ErrorCodes::ChangeStreamFatalError);
        ASSERT_THROWS_CODE(decodeCdcDdl(schemaOp(kNs, CdcDdlOpType::kDropIndex, image, good)),
                           AssertionException,
                           ErrorCodes::ChangeStreamFatalError);
    }
    // A drop reads no image.
    ASSERT(decodeCdcDdl(schemaOp(kNs, CdcDdlOpType::kDropTable, "abc", "")));
}

}  // namespace
}  // namespace mongo
