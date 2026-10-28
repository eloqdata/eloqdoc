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
 * Tests for the redo-log blob decoder (docs/cdc-change-stream.md, F1-F3 and §5.4 steps 1-2).
 *
 * The blobs here are built by BlobBuilder below, which writes the format exactly as
 * TransactionExecution::FillDataLogRequest does: the same field order, the same native-endian
 * integers, and the same rule that a record follows every op but Delete. Building them rather
 * than capturing them from a running tx service is what keeps this a unit test; the format was
 * read off the writer and off the reader recovery uses (ReplayLogCc, in cc_request.h and
 * template_cc_map.h), and any drift in either would show up as these tests failing to parse.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/base/cdc_blob_decoder.h"

#include <string>
#include <vector>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

// txservice wire values, as the writer emits them.
const uint8_t kEngineNone = 0;
const uint8_t kEngineEloqSql = 1;
const uint8_t kEngineEloqKv = 2;
const uint8_t kEngineEloqDoc = 3;

const uint8_t kTypePrimary = 0;
const uint8_t kTypeSecondary = 1;
const uint8_t kTypeUniqueSecondary = 2;

/** Appends a host-order integer, as the writer's reinterpret_cast+append does. */
template <typename T>
void appendNative(std::string* out, T value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

/** `size_t unpack_len | unpack_info | size_t blob_len | BSON`, per EloqRecord::Serialize. */
std::string serializeRecord(const BSONObj& doc, StringData unpackInfo = ""_sd) {
    std::string out;
    appendNative<size_t>(&out, unpackInfo.size());
    out.append(unpackInfo.rawData(), unpackInfo.size());
    appendNative<size_t>(&out, static_cast<size_t>(doc.objsize()));
    out.append(doc.objdata(), doc.objsize());
    return out;
}

/** Builds a node group's log blob section by section, in the writer's layout. */
class BlobBuilder {
public:
    BlobBuilder& section(StringData tableName, uint8_t engine, uint8_t type) {
        finishSection();
        appendNative<uint8_t>(&_blob, static_cast<uint8_t>(tableName.size()));
        _blob.append(tableName.rawData(), tableName.size());
        appendNative<uint8_t>(&_blob, engine);
        appendNative<uint8_t>(&_blob, type);
        // Reserved, refilled by finishSection once the entries are known.
        _kvLenAt = _blob.size();
        appendNative<uint32_t>(&_blob, 0);
        _inSection = true;
        return *this;
    }

    /** An EloqDoc document collection: the table name is the namespace (F2). */
    BlobBuilder& collection(StringData ns) {
        return section(ns, kEngineEloqDoc, kTypePrimary);
    }

    BlobBuilder& entry(StringData keyString, CdcOperation op, const BSONObj& doc) {
        invariant(_inSection);
        appendNative<uint16_t>(&_blob, static_cast<uint16_t>(keyString.size()));
        _blob.append(keyString.rawData(), keyString.size());
        appendNative<uint8_t>(&_blob, static_cast<uint8_t>(op));
        if (op != CdcOperation::kDelete) {
            _blob.append(serializeRecord(doc));
        }
        return *this;
    }

    BlobBuilder& insert(StringData key, const BSONObj& doc) {
        return entry(key, CdcOperation::kInsert, doc);
    }
    BlobBuilder& update(StringData key, const BSONObj& doc) {
        return entry(key, CdcOperation::kUpdate, doc);
    }
    BlobBuilder& remove(StringData key) {
        return entry(key, CdcOperation::kDelete, BSONObj());
    }

    /** Raw bytes inside a section, for an engine whose entries this decoder never interprets. */
    BlobBuilder& opaqueBytes(StringData bytes) {
        invariant(_inSection);
        _blob.append(bytes.rawData(), bytes.size());
        return *this;
    }

    std::string done() {
        finishSection();
        return _blob;
    }

private:
    void finishSection() {
        if (!_inSection) {
            return;
        }
        const uint32_t kvLen = static_cast<uint32_t>(_blob.size() - _kvLenAt - sizeof(uint32_t));
        std::string encoded;
        appendNative<uint32_t>(&encoded, kvLen);
        _blob.replace(_kvLenAt, sizeof(uint32_t), encoded);
        _inSection = false;
    }

    std::string _blob;
    size_t _kvLenAt = 0;
    bool _inSection = false;
};

std::vector<CdcBlobDecoder::Entry> decodeAll(StringData blob, CdcStreamScope scope) {
    std::vector<CdcBlobDecoder::Entry> out;
    CdcBlobDecoder decoder(blob, std::move(scope));
    CdcBlobDecoder::Entry entry;
    while (decoder.next(&entry)) {
        out.push_back(entry);
    }
    return out;
}

//
// Entries and their contents.
//

TEST(CdcBlobDecoder, DecodesOneInsert) {
    const BSONObj doc = BSON("_id" << 1 << "x"
                                   << "hello");
    const std::string blob = BlobBuilder().collection("test.c").insert("KEY1", doc).done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_EQ(entries[0].ns, "test.c");
    ASSERT(entries[0].op == CdcOperation::kInsert);
    ASSERT_EQ(entries[0].keyString, "KEY1");
    ASSERT_BSONOBJ_EQ(BSONObj(entries[0].documentBson().rawData()), doc);
}

TEST(CdcBlobDecoder, DeleteCarriesNoRecord) {
    const std::string blob = BlobBuilder().collection("test.c").remove("KEY1").done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT(entries[0].op == CdcOperation::kDelete);
    ASSERT_EQ(entries[0].keyString, "KEY1");
    ASSERT_TRUE(entries[0].record.empty());
    ASSERT_TRUE(entries[0].documentBson().empty());
}

TEST(CdcBlobDecoder, MixedOpsInOneSectionStayInOrder) {
    // The delete in the middle is the case that desynchronises a decoder which assumes every
    // entry carries a record.
    const std::string blob = BlobBuilder()
                                 .collection("test.c")
                                 .insert("K1", BSON("_id" << 1))
                                 .remove("K2")
                                 .update("K3", BSON("_id" << 3 << "v" << 9))
                                 .remove("K4")
                                 .insert("K5", BSON("_id" << 5))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 5U);
    ASSERT_EQ(entries[0].keyString, "K1");
    ASSERT_EQ(entries[1].keyString, "K2");
    ASSERT_EQ(entries[2].keyString, "K3");
    ASSERT_EQ(entries[3].keyString, "K4");
    ASSERT_EQ(entries[4].keyString, "K5");
    ASSERT(entries[2].op == CdcOperation::kUpdate);
    ASSERT_BSONOBJ_EQ(BSONObj(entries[2].documentBson().rawData()), BSON("_id" << 3 << "v" << 9));
    for (uint32_t i = 0; i < entries.size(); ++i) {
        ASSERT_EQ(entries[i].entry, i);
        ASSERT_EQ(entries[i].section, 0U);
    }
}

TEST(CdcBlobDecoder, KeysAndDocumentsMayContainArbitraryBytes) {
    // A KeyString is full of NULs and 0xFF; nothing may treat it as a C string.
    const std::string key("\x00\x2a\xFF\x00\xFF", 5);
    const BSONObj doc = BSON("_id" << 1 << "bin" << BSONBinData("\x00\xFF\x00", 3, BinDataGeneral));
    const std::string blob = BlobBuilder().collection("test.c").insert(key, doc).done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_EQ(entries[0].keyString, StringData(key));
    ASSERT_BSONOBJ_EQ(BSONObj(entries[0].documentBson().rawData()), doc);
}

TEST(CdcBlobDecoder, RecordWithUnpackInfoIsSkippedToReachTheDocument) {
    // The document sits after a length-prefixed unpack-info block, which may be non-empty.
    const BSONObj doc = BSON("_id" << 7);
    std::string blob;
    {
        BlobBuilder b;
        b.collection("test.c");
        std::string entry;
        appendNative<uint16_t>(&entry, 2);
        entry.append("K7", 2);
        appendNative<uint8_t>(&entry, static_cast<uint8_t>(CdcOperation::kInsert));
        entry.append(serializeRecord(doc, "unpack-info-bytes"_sd));
        b.opaqueBytes(entry);
        blob = b.done();
    }

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_BSONOBJ_EQ(BSONObj(entries[0].documentBson().rawData()), doc);
}

//
// Section selection. Everything unwanted is stepped over by its length.
//

TEST(CdcBlobDecoder, SkipsOtherEnginesIncludingEloqKvObjectCommands) {
    // An EloqKV object-command section shares the header layout, so it is skipped whole; its
    // body is deliberately bytes that would not parse as entries.
    const std::string blob = BlobBuilder()
                                 .section("kvtable", kEngineEloqKv, kTypePrimary)
                                 .opaqueBytes(StringData("\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 7))
                                 .collection("test.c")
                                 .insert("K1", BSON("_id" << 1))
                                 .section("sqltable", kEngineEloqSql, kTypePrimary)
                                 .opaqueBytes(StringData("\xFE\xFE", 2))
                                 .section("bucket", kEngineNone, kTypePrimary)
                                 .opaqueBytes(StringData("\x01", 1))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_EQ(entries[0].ns, "test.c");
    // Its position still counts the sections that were skipped.
    ASSERT_EQ(entries[0].section, 1U);
}

TEST(CdcBlobDecoder, SkipsSecondaryIndexTables) {
    // A secondary index is a separate table in the same blob (F2); documents come only from the
    // primary table.
    const std::string blob = BlobBuilder()
                                 .collection("test.c")
                                 .insert("K1", BSON("_id" << 1))
                                 .section("test.c$idx", kEngineEloqDoc, kTypeSecondary)
                                 .opaqueBytes(StringData("\xAA\xBB", 2))
                                 .section("test.c$uniq", kEngineEloqDoc, kTypeUniqueSecondary)
                                 .opaqueBytes(StringData("\xCC", 1))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_EQ(entries[0].ns, "test.c");
}

TEST(CdcBlobDecoder, SkipsExcludedDatabasesInEveryScope) {
    const std::string blob = BlobBuilder()
                                 .collection("local.oplog")
                                 .insert("K1", BSON("_id" << 1))
                                 .collection("admin.__eloq_cdc_progress")
                                 .insert("K2", BSON("_id" << 2))
                                 .collection("config.settings")
                                 .insert("K3", BSON("_id" << 3))
                                 .collection("test.c")
                                 .insert("K4", BSON("_id" << 4))
                                 .done();

    for (auto scope : {CdcStreamScope::deployment(), CdcStreamScope::database("test")}) {
        const auto entries = decodeAll(blob, scope);
        ASSERT_EQ(entries.size(), 1U);
        ASSERT_EQ(entries[0].ns, "test.c");
    }
    // Even asked for by name: the consumer's own progress document must never be mirrored.
    ASSERT_TRUE(decodeAll(blob, CdcStreamScope::collection("admin.__eloq_cdc_progress")).empty());
}

TEST(CdcBlobDecoder, DatabaseScopeTakesOnlyItsOwnDatabase) {
    const std::string blob = BlobBuilder()
                                 .collection("alpha.one")
                                 .insert("K1", BSON("_id" << 1))
                                 .collection("beta.two")
                                 .insert("K2", BSON("_id" << 2))
                                 .collection("alpha.three")
                                 .insert("K3", BSON("_id" << 3))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::database("alpha"));
    ASSERT_EQ(entries.size(), 2U);
    ASSERT_EQ(entries[0].ns, "alpha.one");
    ASSERT_EQ(entries[1].ns, "alpha.three");
    // A database whose name merely prefixes another is not included.
    ASSERT_TRUE(decodeAll(blob, CdcStreamScope::database("alph")).empty());
}

TEST(CdcBlobDecoder, CollectionScopeTakesOnlyItsOwnNamespace) {
    const std::string blob = BlobBuilder()
                                 .collection("test.c")
                                 .insert("K1", BSON("_id" << 1))
                                 .collection("test.cc")
                                 .insert("K2", BSON("_id" << 2))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::collection("test.c"));
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_EQ(entries[0].keyString, "K1");
}

//
// Positions. These are what a resume token records, so they must not depend on the scope.
//

TEST(CdcBlobDecoder, PositionsAreIndependentOfTheScope) {
    // The same blob read by three different streams must number the shared entry identically,
    // because a token has to stay valid whatever scope produced it (§5.6).
    const std::string blob = BlobBuilder()
                                 .collection("other.x")
                                 .insert("A1", BSON("_id" << 1))
                                 .section("kv", kEngineEloqKv, kTypePrimary)
                                 .opaqueBytes(StringData("\xFF", 1))
                                 .collection("test.c")
                                 .insert("B1", BSON("_id" << 1))
                                 .insert("B2", BSON("_id" << 2))
                                 .done();

    const auto deployment = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(deployment.size(), 3U);
    ASSERT_EQ(deployment[0].section, 0U);
    ASSERT_EQ(deployment[2].section, 2U);

    const auto narrow = decodeAll(blob, CdcStreamScope::collection("test.c"));
    ASSERT_EQ(narrow.size(), 2U);
    for (size_t i = 0; i < narrow.size(); ++i) {
        const auto& wide = deployment[i + 1];
        ASSERT_EQ(narrow[i].section, wide.section);
        ASSERT_EQ(narrow[i].entry, wide.entry);
        ASSERT_EQ(narrow[i].keyString, wide.keyString);
    }
}

TEST(CdcBlobDecoder, EntryOrdinalsRestartPerSection) {
    const std::string blob = BlobBuilder()
                                 .collection("test.a")
                                 .insert("A1", BSON("_id" << 1))
                                 .insert("A2", BSON("_id" << 2))
                                 .collection("test.b")
                                 .insert("B1", BSON("_id" << 1))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 3U);
    ASSERT_EQ(entries[0].section, 0U);
    ASSERT_EQ(entries[0].entry, 0U);
    ASSERT_EQ(entries[1].section, 0U);
    ASSERT_EQ(entries[1].entry, 1U);
    ASSERT_EQ(entries[2].section, 1U);
    ASSERT_EQ(entries[2].entry, 0U);
}

//
// Shapes that must not upset the walk.
//

TEST(CdcBlobDecoder, EmptyBlobYieldsNothing) {
    ASSERT_TRUE(decodeAll(""_sd, CdcStreamScope::deployment()).empty());
}

TEST(CdcBlobDecoder, EmptySectionIsSteppedOver) {
    const std::string blob = BlobBuilder()
                                 .collection("test.empty")
                                 .collection("test.c")
                                 .insert("K1", BSON("_id" << 1))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_EQ(entries[0].ns, "test.c");
    ASSERT_EQ(entries[0].section, 1U);
}

TEST(CdcBlobDecoder, TrailingSectionsOutOfScopeEndTheWalk) {
    const std::string blob = BlobBuilder()
                                 .collection("test.c")
                                 .insert("K1", BSON("_id" << 1))
                                 .collection("local.x")
                                 .insert("K2", BSON("_id" << 2))
                                 .done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
}

TEST(CdcBlobDecoder, EmptyKeyAndEmptyDocument) {
    const std::string blob = BlobBuilder().collection("test.c").insert(""_sd, BSONObj()).done();

    const auto entries = decodeAll(blob, CdcStreamScope::deployment());
    ASSERT_EQ(entries.size(), 1U);
    ASSERT_TRUE(entries[0].keyString.empty());
    ASSERT_BSONOBJ_EQ(BSONObj(entries[0].documentBson().rawData()), BSONObj());
}

//
// Corruption is an error, never a read past the end.
//

TEST(CdcBlobDecoder, RejectsTruncatedBlobAtEveryPrefix) {
    const std::string blob =
        BlobBuilder().collection("test.c").insert("K1", BSON("_id" << 1)).remove("K2").done();

    for (size_t len = 1; len < blob.size(); ++len) {
        // Any prefix of a well-formed blob is malformed: a header or an entry is cut in two.
        ASSERT_THROWS_CODE(decodeAll(StringData(blob.data(), len), CdcStreamScope::deployment()),
                           AssertionException,
                           ErrorCodes::BadValue);
    }
    ASSERT_EQ(decodeAll(blob, CdcStreamScope::deployment()).size(), 2U);
}

//
// Corruption *inside* a section, with the section's own length left consistent. The truncation
// test above never reaches these paths: cutting the blob makes kv_len overrun first, so the walk
// throws while reading the section body and no entry is ever parsed. These are the checks that
// stop a corrupt length from swallowing the following entries and reading them as one record.
//

/**
 * Asserts the blob is rejected by the *entry* parser rather than by the section walk. The
 * distinction is the whole point of these cases, so it is asserted rather than assumed: the
 * section walk reports a truncated "entries" block, and any of these tests would silently stop
 * covering its intended check if it started failing there instead.
 */
void assertRejectedInsideAnEntry(StringData blob, StringData expectedField) {
    ASSERT_THROWS_WITH_CHECK(decodeAll(blob, CdcStreamScope::deployment()),
                             AssertionException,
                             [&](const AssertionException& ex) {
                                 ASSERT_EQ(ex.code(), ErrorCodes::BadValue);
                                 ASSERT_STRING_CONTAINS(ex.what(), expectedField.toString());
                                 const std::string what(ex.what());
                                 ASSERT_TRUE(what.find("its entries") == std::string::npos)
                                     << "failed in the section walk, not the entry parser: "
                                     << what;
                             });
}

TEST(CdcBlobDecoder, RejectsAKeyLengthPastTheEndOfItsSection) {
    std::string blob;
    {
        BlobBuilder b;
        b.collection("test.c");
        std::string entry;
        appendNative<uint16_t>(&entry, 4096);  // far more key than the section holds
        entry.append("K1", 2);
        b.opaqueBytes(entry);
        blob = b.done();
    }
    assertRejectedInsideAnEntry(blob, "inside its key"_sd);
}

TEST(CdcBlobDecoder, RejectsARecordDocumentLengthPastTheEndOfItsSection) {
    // The entry's key and op are well formed, so this is caught only by bounds-checking the
    // record itself; unchecked, it would run into whatever follows in the blob.
    std::string blob;
    {
        BlobBuilder b;
        b.collection("test.c");
        std::string entry;
        appendNative<uint16_t>(&entry, 2);
        entry.append("K1", 2);
        appendNative<uint8_t>(&entry, static_cast<uint8_t>(CdcOperation::kInsert));
        appendNative<size_t>(&entry, 0);     // empty unpack info
        appendNative<size_t>(&entry, 4096);  // document longer than the section
        entry.append("{}", 2);
        b.opaqueBytes(entry);
        blob = b.done();
    }
    assertRejectedInsideAnEntry(blob, "inside its record document"_sd);
}

TEST(CdcBlobDecoder, RejectsAnUnpackInfoLengthPastTheEndOfItsSection) {
    std::string blob;
    {
        BlobBuilder b;
        b.collection("test.c");
        std::string entry;
        appendNative<uint16_t>(&entry, 2);
        entry.append("K1", 2);
        appendNative<uint8_t>(&entry, static_cast<uint8_t>(CdcOperation::kInsert));
        appendNative<size_t>(&entry, 4096);  // unpack info longer than the section
        b.opaqueBytes(entry);
        blob = b.done();
    }
    assertRejectedInsideAnEntry(blob, "inside its record unpack info"_sd);
}

TEST(CdcBlobDecoder, RejectsAnEntryHeaderCutByItsSectionLength) {
    // A section that ends part-way through an entry's fixed-size fields: the length is
    // consistent with the bytes present, so only the per-read bounds checks catch it.
    // One byte is too few for the key length itself; two or three give a length of 0x0201, far
    // past the end. Both are entry-parser failures, on different reads.
    {
        BlobBuilder b;
        b.collection("test.c");
        b.opaqueBytes(StringData("\x01", 1));
        assertRejectedInsideAnEntry(b.done(), "before its key length"_sd);
    }
    for (size_t entryBytes = 2; entryBytes <= 3; ++entryBytes) {
        BlobBuilder b;
        b.collection("test.c");
        b.opaqueBytes(StringData("\x01\x02\x03", entryBytes));
        assertRejectedInsideAnEntry(b.done(), "inside its key"_sd);
    }
}

TEST(CdcBlobDecoder, RejectsAnUnknownOperationByte) {
    std::string blob;
    {
        BlobBuilder b;
        b.collection("test.c");
        std::string entry;
        appendNative<uint16_t>(&entry, 2);
        entry.append("K1", 2);
        appendNative<uint8_t>(&entry, 42);  // not an OperationType
        b.opaqueBytes(entry);
        blob = b.done();
    }
    ASSERT_THROWS_CODE(
        decodeAll(blob, CdcStreamScope::deployment()), AssertionException, ErrorCodes::BadValue);
}

TEST(CdcBlobDecoder, RejectsASectionLengthPastTheEndOfTheBlob) {
    std::string blob = BlobBuilder().collection("test.c").insert("K1", BSON("_id" << 1)).done();
    // Overwrite the section's kv_len with something far too large. The header is
    // 1 + strlen("test.c") + 1 + 1 bytes, so the length sits at offset 9.
    const size_t kvLenAt = 1 + StringData("test.c").size() + 1 + 1;
    std::string huge;
    appendNative<uint32_t>(&huge, 0xFFFFFF);
    blob.replace(kvLenAt, sizeof(uint32_t), huge);

    ASSERT_THROWS_CODE(
        decodeAll(blob, CdcStreamScope::deployment()), AssertionException, ErrorCodes::BadValue);
}

}  // namespace
}  // namespace mongo
