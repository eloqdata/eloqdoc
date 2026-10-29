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
 * Resume-token codec tests (docs/cdc-change-stream.md, §5.6). A token is never compared as bytes
 * and never interpreted by a client, so what matters is that every field survives a round trip,
 * that a malformed token is rejected rather than half-read, and that a group missing from a
 * token gets the right restart timestamp.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/base/cdc_resume_token.h"

#include <string>
#include <vector>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/hex.h"

namespace mongo {
namespace {

using Form = CdcResumeTokenEntry::Form;
using Kind = CdcResumeToken::Kind;

CdcResumeTokenEntry makeEntry(uint32_t group,
                              Form form,
                              std::string dataKey,
                              std::string ddlKey,
                              uint32_t section = 0,
                              uint32_t entry = 0) {
    CdcResumeTokenEntry e;
    e.logGroupId = group;
    e.form = form;
    e.dataKey = std::move(dataKey);
    e.ddlKey = std::move(ddlKey);
    e.section = section;
    e.entry = entry;
    return e;
}

void assertRoundTrips(const CdcResumeToken& token, StringData caseName) {
    const auto decoded = CdcResumeToken::decode(token.encode());
    ASSERT_OK(decoded.getStatus()) << caseName;
    const CdcResumeToken& out = decoded.getValue();

    ASSERT_EQ(out.timestamp(), token.timestamp()) << caseName;
    ASSERT_EQ(static_cast<int>(out.kind()), static_cast<int>(token.kind())) << caseName;
    ASSERT_EQ(out.entries().size(), token.entries().size()) << caseName;
    for (size_t i = 0; i < token.entries().size(); ++i) {
        const auto& a = token.entries()[i];
        const auto& b = out.entries()[i];
        ASSERT_EQ(b.logGroupId, a.logGroupId) << caseName << " entry " << i;
        ASSERT_EQ(static_cast<int>(b.form), static_cast<int>(a.form)) << caseName << " entry " << i;
        ASSERT_EQ(b.dataKey, a.dataKey) << caseName << " entry " << i;
        ASSERT_EQ(b.ddlKey, a.ddlKey) << caseName << " entry " << i;
        ASSERT_EQ(b.section, a.section) << caseName << " entry " << i;
        ASSERT_EQ(b.entry, a.entry) << caseName << " entry " << i;
    }

    // And through the driver-facing {_data: hex} shape.
    const auto reparsed = CdcResumeToken::parse(token.toBSON());
    ASSERT_OK(reparsed.getStatus()) << caseName << " via BSON";
    ASSERT_EQ(reparsed.getValue().encode(), token.encode()) << caseName << " via BSON";
}

TEST(CdcResumeToken, WatermarkWithNoEntries) {
    // What a fresh stream issues before it has produced anything (§5.1).
    assertRoundTrips(CdcResumeToken::makeWatermark(1758400000000000ULL), "empty watermark");
}

TEST(CdcResumeToken, WatermarkWithEntries) {
    CdcResumeToken token(1758400000000123ULL,
                         Kind::kWatermark,
                         {makeEntry(0, Form::kAfterKey, "data-key-0", "ddl-key-0"),
                          makeEntry(3, Form::kAfterKey, "data-key-3", "")});
    assertRoundTrips(token, "watermark with entries");
}

TEST(CdcResumeToken, EventTokenWithFormOneEntry) {
    // The group the event came from carries the record's key plus the position inside it; every
    // other group carries only "after this key".
    CdcResumeToken token(1758400000000456ULL,
                         Kind::kEvent,
                         {makeEntry(0, Form::kAtKeyAfterPosition, "data-key-0", "ddl-key-0", 2, 17),
                          makeEntry(1, Form::kAfterKey, "data-key-1", "ddl-key-1")});
    assertRoundTrips(token, "event token");

    const auto decoded = CdcResumeToken::decode(token.encode());
    ASSERT_OK(decoded.getStatus());
    const auto* entry = decoded.getValue().entryFor(0);
    ASSERT(entry);
    ASSERT_EQ(entry->section, 2u);
    ASSERT_EQ(entry->entry, 17u);
    ASSERT(!decoded.getValue().entryFor(42));
}

TEST(CdcResumeToken, InvalidateToken) {
    CdcResumeToken token(1758400000000789ULL,
                         Kind::kInvalidate,
                         {makeEntry(0, Form::kAfterKey, "drop-data", "drop-ddl")});
    assertRoundTrips(token, "invalidate token");
}

TEST(CdcResumeToken, BinaryKeysWithNulsAndHighBytes) {
    // Storage keys are raw bytes: big-endian timestamps are full of NULs and 0xFF bytes.
    const std::string dataKey("\x00\x06\x39\xFF\x00\x00\xFF\xFF", 8);
    const std::string ddlKey("\xFF\x00\xFF\x00", 4);
    CdcResumeToken token(
        42, Kind::kEvent, {makeEntry(7, Form::kAtKeyAfterPosition, dataKey, ddlKey, 1, 2)});
    assertRoundTrips(token, "binary keys");
}

TEST(CdcResumeToken, EmptyKeys) {
    // A group that has produced from only one of the two streams has an empty key for the other.
    CdcResumeToken token(9, Kind::kEvent, {makeEntry(0, Form::kAfterKey, "", "")});
    assertRoundTrips(token, "empty keys");
}

TEST(CdcResumeToken, ManyGroups) {
    std::vector<CdcResumeTokenEntry> entries;
    for (uint32_t i = 0; i < 64; ++i) {
        // A form-0 entry carries no position: "resume after this key" needs none, so the encoder
        // omits section/entry and they come back as 0.
        const bool atKey = (i % 2 == 0);
        entries.push_back(makeEntry(i * 3,
                                    atKey ? Form::kAtKeyAfterPosition : Form::kAfterKey,
                                    str::stream() << "data-" << i,
                                    str::stream() << "ddl-" << i,
                                    atKey ? i : 0,
                                    atKey ? i * 7 : 0));
    }
    assertRoundTrips(CdcResumeToken(1234, Kind::kEvent, std::move(entries)), "many groups");
}

TEST(CdcResumeToken, ExtremeTimestamps) {
    assertRoundTrips(CdcResumeToken::makeWatermark(0), "watermark at 0");
    assertRoundTrips(CdcResumeToken::makeWatermark(std::numeric_limits<uint64_t>::max() - 1),
                     "watermark one below max");
    // An event token computes no successor, so the maximum is representable for it.
    assertRoundTrips(CdcResumeToken(std::numeric_limits<uint64_t>::max(), Kind::kEvent, {}),
                     "event token at max");
}

TEST(CdcResumeToken, MaximumWatermarkIsRejectedRatherThanWrappingToZero) {
    // Its successor is unrepresentable; accepting it and wrapping would restart the stream at the
    // beginning of history, which is the opposite of what the watermark asserts.
    const CdcResumeToken token =
        CdcResumeToken::makeWatermark(std::numeric_limits<uint64_t>::max());
    const auto decoded = CdcResumeToken::decode(token.encode());
    ASSERT_NOT_OK(decoded.getStatus());
    ASSERT_EQ(decoded.getStatus().code(), ErrorCodes::FailedToParse);
    ASSERT_EQ(CdcResumeToken::parse(token.toBSON()).getStatus().code(), ErrorCodes::FailedToParse);

    // And an in-process token that somehow held it fails loudly instead of returning 0.
    ASSERT_THROWS_CODE(
        token.startTimestampForAbsentGroup(), AssertionException, ErrorCodes::FailedToParse);
}

TEST(CdcResumeToken, EventOrInvalidateAtTimestampZeroIsRejected) {
    // Every commit timestamp is positive, so neither kind can name timestamp 0, and accepting one
    // would restart the groups it does not name from 0, which the scan source refuses outright.
    // The bytes come from a client, so this must be a parse error, never a server abort.
    for (Kind kind : {Kind::kEvent, Kind::kInvalidate}) {
        const CdcResumeToken token(0, kind, {});
        ASSERT_EQ(CdcResumeToken::decode(token.encode()).getStatus().code(),
                  ErrorCodes::FailedToParse);
        ASSERT_EQ(CdcResumeToken::parse(token.toBSON()).getStatus().code(),
                  ErrorCodes::FailedToParse);
        ASSERT_THROWS_CODE(
            token.startTimestampForAbsentGroup(), AssertionException, ErrorCodes::FailedToParse);
    }
    // The token from the report: version 2, timestamp 0, event kind, no entries.
    ASSERT_EQ(CdcResumeToken::parse(BSON("_data" << "020000000000000000000000")).getStatus().code(),
              ErrorCodes::FailedToParse);
    // A watermark at 0 stays valid: it restarts at 1.
    ASSERT_OK(CdcResumeToken::decode(CdcResumeToken::makeWatermark(0).encode()).getStatus());
}

TEST(CdcResumeToken, RestartTimestampAtTheBoundaryDoesNotWrap) {
    const uint64_t max = std::numeric_limits<uint64_t>::max();

    // The largest watermark that can exist restarts at the largest timestamp, not at 0.
    ASSERT_EQ(CdcResumeToken::makeWatermark(max - 1).startTimestampForAbsentGroup(), max);
    ASSERT_EQ(CdcResumeToken::makeWatermark(0).startTimestampForAbsentGroup(), 1u);

    // Event and invalidate tokens take their own timestamp, so the maximum is safe for them.
    ASSERT_EQ(CdcResumeToken(max, Kind::kEvent, {}).startTimestampForAbsentGroup(), max);
    ASSERT_EQ(CdcResumeToken(max, Kind::kInvalidate, {}).startTimestampForAbsentGroup(), max);
}

TEST(CdcResumeToken, MaximumEntryCountParsesAndRejectsADuplicateWithinIt) {
    // The entry count is client-supplied and tops out at 65535. Duplicate detection must not be
    // pairwise: at this size that is ~2.1 billion comparisons for a token of about 1 MB, which
    // would let a client tie up a worker with a single well-formed request.
    const uint16_t count = std::numeric_limits<uint16_t>::max();
    std::vector<CdcResumeTokenEntry> entries;
    entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        entries.push_back(makeEntry(i, Form::kAfterKey, "d", "l"));
    }
    const CdcResumeToken token(7, Kind::kEvent, std::move(entries));

    const auto decoded = CdcResumeToken::decode(token.encode());
    ASSERT_OK(decoded.getStatus());
    ASSERT_EQ(decoded.getValue().entries().size(), count);
    ASSERT(decoded.getValue().entryFor(count - 1));

    // A duplicate buried at the very end is still caught.
    std::vector<CdcResumeTokenEntry> withDuplicate = token.entries();
    withDuplicate.back().logGroupId = 0;
    ASSERT_EQ(
        CdcResumeToken::decode(CdcResumeToken(7, Kind::kEvent, std::move(withDuplicate)).encode())
            .getStatus()
            .code(),
        ErrorCodes::FailedToParse);
}

//
// Restart positions for a group the token does not name.
//

TEST(CdcResumeToken, AbsentGroupStartsAboveAWatermark) {
    // A watermark certifies its own timestamp consumed.
    ASSERT_EQ(CdcResumeToken::makeWatermark(100).startTimestampForAbsentGroup(), 101u);
}

TEST(CdcResumeToken, AbsentGroupStartsAtAnEventTimestamp) {
    // Records at the event's timestamp from other groups may not have been produced yet.
    ASSERT_EQ(CdcResumeToken(100, Kind::kEvent, {}).startTimestampForAbsentGroup(), 100u);
    ASSERT_EQ(CdcResumeToken(100, Kind::kInvalidate, {}).startTimestampForAbsentGroup(), 100u);
}

//
// A token comes from a client, so every malformed shape must be a parse error.
//

TEST(CdcResumeToken, RejectsWrongVersion) {
    std::string bytes = CdcResumeToken::makeWatermark(5).encode();
    bytes[0] = 0x01;
    const auto result = CdcResumeToken::decode(bytes);
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQ(result.getStatus().code(), ErrorCodes::FailedToParse);
}

TEST(CdcResumeToken, RejectsUnknownKind) {
    std::string bytes = CdcResumeToken::makeWatermark(5).encode();
    bytes[1 + sizeof(uint64_t)] = 0x09;
    ASSERT_EQ(CdcResumeToken::decode(bytes).getStatus().code(), ErrorCodes::FailedToParse);
}

TEST(CdcResumeToken, RejectsTruncatedToken) {
    const std::string full =
        CdcResumeToken(
            1, Kind::kEvent, {makeEntry(0, Form::kAtKeyAfterPosition, "abc", "de", 1, 2)})
            .encode();
    for (size_t len = 0; len < full.size(); ++len) {
        const auto result = CdcResumeToken::decode(StringData(full.data(), len));
        ASSERT_NOT_OK(result.getStatus()) << "truncation to " << len << " bytes was accepted";
    }
    ASSERT_OK(CdcResumeToken::decode(full).getStatus());
}

TEST(CdcResumeToken, RejectsTrailingBytes) {
    std::string bytes = CdcResumeToken::makeWatermark(5).encode();
    bytes.push_back('x');
    ASSERT_EQ(CdcResumeToken::decode(bytes).getStatus().code(), ErrorCodes::FailedToParse);
}

TEST(CdcResumeToken, RejectsDuplicateLogGroup) {
    // Entries are keyed by log group; two entries for one group would make the resume ambiguous.
    const CdcResumeToken token(
        1,
        Kind::kEvent,
        {makeEntry(4, Form::kAfterKey, "a", "b"), makeEntry(4, Form::kAfterKey, "c", "d")});
    ASSERT_EQ(CdcResumeToken::decode(token.encode()).getStatus().code(), ErrorCodes::FailedToParse);
}

TEST(CdcResumeToken, RejectsUnknownEntryForm) {
    std::string bytes =
        CdcResumeToken(1, Kind::kEvent, {makeEntry(0, Form::kAfterKey, "a", "b")}).encode();
    // version(1) + ts(8) + kind(1) + count(2) + groupId(4) = 16
    bytes[16] = 0x07;
    ASSERT_EQ(CdcResumeToken::decode(bytes).getStatus().code(), ErrorCodes::FailedToParse);
}

//
// The BSON wrapper drivers see.
//

TEST(CdcResumeTokenBson, ShapeIsDataHexOnly) {
    const BSONObj bson = CdcResumeToken::makeWatermark(0x0102030405060708ULL).toBSON();
    ASSERT_EQ(bson.nFields(), 1);
    ASSERT_EQ(bson.firstElementFieldName(), StringData("_data"));
    ASSERT_EQ(bson.firstElement().type(), BSONType::String);
    // version 02, then the big-endian timestamp, then kind 01 (watermark) and a zero count.
    ASSERT_EQ(bson.firstElement().String(), "020102030405060708010000");
}

TEST(CdcResumeTokenBson, RejectsMissingOrWrongTypedData) {
    ASSERT_EQ(CdcResumeToken::parse(BSONObj()).getStatus().code(), ErrorCodes::FailedToParse);
    ASSERT_EQ(CdcResumeToken::parse(BSON("_data" << 1)).getStatus().code(),
              ErrorCodes::FailedToParse);
    ASSERT_EQ(CdcResumeToken::parse(BSON("other" << "02")).getStatus().code(),
              ErrorCodes::FailedToParse);
}

TEST(CdcResumeTokenBson, RejectsExtraFields) {
    BSONObj token = CdcResumeToken::makeWatermark(1).toBSON();
    BSONObjBuilder b;
    b.appendElements(token);
    b.append("extra", 1);
    ASSERT_EQ(CdcResumeToken::parse(b.obj()).getStatus().code(), ErrorCodes::FailedToParse);
}

TEST(CdcResumeTokenBson, RejectsBadHex) {
    ASSERT_EQ(CdcResumeToken::parse(BSON("_data" << "0201020")).getStatus().code(),
              ErrorCodes::FailedToParse);
    ASSERT_EQ(CdcResumeToken::parse(BSON("_data" << "02zz")).getStatus().code(),
              ErrorCodes::FailedToParse);
}

}  // namespace
}  // namespace mongo
