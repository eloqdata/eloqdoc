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
 * Round-trip tests for the canonical delete-key decoder (docs/cdc-change-stream.md, §5.4 step 3;
 * §10.7 lists the cases). Every value is encoded with the real KeyString encoder and decoded
 * without its type bits, which is all a delete event has.
 *
 * Each case checks three things:
 *   - the decoded value compares equal to the original under MongoDB's comparison, which is
 *     exactly what makes deleteOne({_id}) match on the sink;
 *   - a sentinel encoded *after* the value survives, which proves the decoder consumed exactly
 *     the bytes the encoder wrote -- a decoder that read one byte too few or too many would
 *     produce a different sentinel or throw;
 *   - the canonical value compares equal to what the stock decoder produces from the same bytes
 *     *with* its type bits, so the two agree on the value and differ only in representation.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/base/cdc_key_decoder.h"

#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/platform/decimal128.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

const char kSentinel[] = "cdc-sentinel-2f9c";

Ordering allAscending() {
    return Ordering::make(BSONObj());
}

// There is no known value where the stock decoder and this one disagree, so both equalities
// below are asserted for every case with no exceptions. Should one ever be found, name it here
// explicitly rather than weakening the assertions: "the stock decoder is wrong too" is how a bug
// in this decoder would hide.

/**
 * Encodes {"": value, "": <sentinel>} with the real encoder, decodes it without type bits, and
 * makes the three assertions described at the top of the file.
 */
void assertDecodesCanonically(const BSONElement& value, StringData caseName) {
    BSONObjBuilder keyBuilder;
    keyBuilder.appendAs(value, "");
    keyBuilder.append("", kSentinel);
    const BSONObj key = keyBuilder.obj();

    const KeyString ks(KeyString::kLatestVersion, key, allAscending());
    const BSONObj canonical =
        CdcKeyDecoder::toBsonCanonical(ks.getBuffer(), ks.getSize(), allAscending());
    const BSONObj stock =
        KeyString::toBson(ks.getBuffer(), ks.getSize(), allAscending(), ks.getTypeBits());

    ASSERT_EQ(canonical.nFields(), 2) << caseName << ": decoded " << canonical;

    BSONObjIterator canonicalIt(canonical);
    BSONObjIterator stockIt(stock);
    const BSONElement decodedValue = canonicalIt.next();
    const BSONElement stockValue = stockIt.next();

    // NaN is the one value MongoDB's element comparison does not report as equal to itself
    // across every path, so it is checked directly; every NaN compares equal to every other.
    if (value.isNumber() && std::isnan(value.numberDouble())) {
        ASSERT_TRUE(decodedValue.isNumber() && std::isnan(decodedValue.numberDouble()))
            << caseName << ": expected NaN, got " << decodedValue;
    } else {
        // The contract, asserted for every value with no escape hatch: what comes back is the
        // value that went in. A delete event's documentKey has to match the document on the
        // sink, so anything less is silent divergence.
        ASSERT_EQ(decodedValue.woCompare(value, /*considerFieldName*/ false), 0)
            << caseName << ": decoded " << decodedValue << " != original " << value
            << " (stock decoded " << stockValue << ")";

        // And it agrees with what MongoDB itself recovers with the type bits in hand.
        ASSERT_EQ(decodedValue.woCompare(stockValue, /*considerFieldName*/ false), 0)
            << caseName << ": decoded " << decodedValue << " != stock " << stockValue;
    }

    const BSONElement decodedSentinel = canonicalIt.next();
    ASSERT_EQ(decodedSentinel.type(), BSONType::String)
        << caseName << ": sentinel was not a string; byte consumption is off";
    ASSERT_EQ(decodedSentinel.String(), kSentinel)
        << caseName << ": sentinel corrupted; byte consumption is off";
}

void assertDecodesCanonically(const BSONObj& wrapped, StringData caseName) {
    assertDecodesCanonically(wrapped.firstElement(), caseName);
}

//
// Integers.
//

TEST(CdcKeyDecoder, Int32Values) {
    for (int v : {0,
                  1,
                  -1,
                  2,
                  255,
                  256,
                  65535,
                  65536,
                  std::numeric_limits<int>::max(),
                  std::numeric_limits<int>::min()}) {
        assertDecodesCanonically(BSON("" << v), str::stream() << "int " << v);
    }
}

TEST(CdcKeyDecoder, Int64Values) {
    // The values above 2**32 are the ones the stock decoder would narrow to int with empty type
    // bits, silently corrupting the key.
    for (long long v : {1LL << 31,
                        (1LL << 31) + 1,
                        1LL << 40,
                        (1LL << 53) - 1,
                        1LL << 53,
                        (1LL << 53) + 1,
                        (1LL << 62),
                        std::numeric_limits<long long>::max(),
                        -(1LL << 53) - 1,
                        std::numeric_limits<long long>::min() + 1}) {
        assertDecodesCanonically(BSON("" << v), str::stream() << "long " << v);
    }
}

TEST(CdcKeyDecoder, LongLongMin) {
    // Encoded as a large-magnitude double, whose value is exact for this one number.
    assertDecodesCanonically(BSON("" << std::numeric_limits<long long>::min()), "LLONG_MIN");
}

TEST(CdcKeyDecoder, IntegerValuedDoublesDecodeAsIntegers) {
    // The canonical decoder narrows these to int/long, which compares equal by value.
    for (double v : {0.0, 1.0, -1.0, 4294967296.0, -4294967296.0}) {
        assertDecodesCanonically(BSON("" << v), str::stream() << "integral double " << v);
    }
}

//
// Doubles with fractions, across the integer-part widths that set fracBytes.
//

TEST(CdcKeyDecoder, DoublesWithFractionsInEveryWidth) {
    const std::vector<double> values = {0.5,
                                        1.5,
                                        -1.5,
                                        200.5,
                                        50000.25,
                                        12000000.125,
                                        3000000000.5,
                                        800000000000.5,
                                        200000000000000.5,
                                        double((1LL << 54)) + 0.5,
                                        -double((1LL << 54)) - 0.5,
                                        3.14159265358979,
                                        -2.718281828459045};
    for (double v : values) {
        assertDecodesCanonically(BSON("" << v), str::stream() << "double " << v);
    }
}

TEST(CdcKeyDecoder, DoublesAtAndBeyondTwoToThe63) {
    const double kMinLarge = 1ULL << 63;
    for (double v : {kMinLarge,
                     -kMinLarge,
                     kMinLarge * 2,
                     -kMinLarge * 2,
                     std::ldexp(1.0, 200),
                     std::ldexp(-1.0, 200),
                     std::numeric_limits<double>::max(),
                     std::numeric_limits<double>::lowest()}) {
        assertDecodesCanonically(BSON("" << v), str::stream() << "large double " << v);
    }
}

TEST(CdcKeyDecoder, InfinitiesAndNaN) {
    assertDecodesCanonically(BSON("" << std::numeric_limits<double>::infinity()), "+Inf");
    assertDecodesCanonically(BSON("" << -std::numeric_limits<double>::infinity()), "-Inf");
    assertDecodesCanonically(BSON("" << std::numeric_limits<double>::quiet_NaN()), "NaN double");
    assertDecodesCanonically(BSON("" << Decimal128::kPositiveNaN), "NaN decimal");
    assertDecodesCanonically(BSON("" << Decimal128::kPositiveInfinity), "+Inf decimal");
    assertDecodesCanonically(BSON("" << Decimal128::kNegativeInfinity), "-Inf decimal");
}

//
// Zeros. Every one of these encodes to the same single ctype byte and carries no key bytes at
// all, so only the type bits distinguish them; all of them compare equal.
//

TEST(CdcKeyDecoder, EveryZero) {
    assertDecodesCanonically(BSON("" << 0), "int zero");
    assertDecodesCanonically(BSON("" << 0LL), "long zero");
    assertDecodesCanonically(BSON("" << 0.0), "double zero");
    assertDecodesCanonically(BSON("" << -0.0), "negative double zero");
    assertDecodesCanonically(BSON("" << Decimal128("0")), "decimal zero");
    assertDecodesCanonically(BSON("" << Decimal128("0E+3")), "decimal zero, exponent +3");
    assertDecodesCanonically(BSON("" << Decimal128("0E-3")), "decimal zero, exponent -3");
    assertDecodesCanonically(BSON("" << Decimal128("-0")), "negative decimal zero");
}

//
// Decimals. These span the decimal-continuation markers: a decimal that is exactly a double
// (DCM 0), one equal to its double rounded to 15 digits (DCM 2), and ones needing a 34-digit
// continuation (DCM 1 and 3, chosen by whether the decimal is below or above that rounding).
//

TEST(CdcKeyDecoder, DecimalsAcrossContinuationMarkers) {
    const std::vector<std::string> values = {"0.5",
                                             "-0.5",
                                             "0.25",
                                             "1.25",
                                             "0.1",
                                             "-0.1",
                                             "3.14159265358979",
                                             "0.1000000000000000000000000000000001",
                                             "0.9999999999999999999999999999999999",
                                             "1.000000000000000000000000000000001",
                                             "123.4567890123456789012345678901234",
                                             "-123.4567890123456789012345678901234"};
    for (const auto& v : values) {
        assertDecodesCanonically(BSON("" << Decimal128(v)), str::stream() << "decimal " << v);
    }
}

TEST(CdcKeyDecoder, DecimalsAboveTwoToThe53) {
    const std::vector<std::string> values = {"9007199254740993",          // 2**53 + 1, integral
                                             "18446744073709551617",      // > 2**64, integral
                                             "18446744073709551617.25"};  // and with a fraction
    for (const auto& v : values) {
        assertDecodesCanonically(BSON("" << Decimal128(v)), str::stream() << "big decimal " << v);
    }

    assertDecodesCanonically(BSON("" << Decimal128("9007199254740993.5")),
                             "big decimal 9007199254740993.5");
}

TEST(CdcKeyDecoder, EightByteIntegerPartWithAFraction) {
    // fracBytes == 0: the integer part fills all eight bytes, so no fraction bytes remain and
    // the decimal-continuation marker is implied rather than read from the key. This row of the
    // §5.4 table had no coverage.
    const std::vector<std::string> values = {"36028797018963969.5",     // 2**55 + 1, fractional
                                             "-36028797018963969.5",
                                             "1000000000000000000.5",
                                             "9223372036854775807.5"};  // LLONG_MAX + a fraction
    for (const auto& v : values) {
        assertDecodesCanonically(BSON("" << Decimal128(v)),
                                 str::stream() << "8-byte int part " << v);
    }
}

TEST(CdcKeyDecoder, TwoToThe53Plus1WithAFractionSurvivesBothDecoders) {
    // Regression guard for a fix in the shared decoder. key_string.cpp selected the exact-integer
    // branch with `integerPart > kMaxIntForDouble`, a double, so the int64 integer part was
    // converted first and 2**53 + 1 rounded to 2**53: the branch that exists for precisely this
    // value was skipped and the result came back one unit low. Every caller of
    // KeyString::toBson inherited it, EloqIndexCursor::_curr among them. Both decoders now
    // compare the bound as an integer.
    const BSONObj key = BSON("" << Decimal128("9007199254740993.5"));
    const KeyString ks(KeyString::kLatestVersion, key, allAscending());

    const BSONObj canonical =
        CdcKeyDecoder::toBsonCanonical(ks.getBuffer(), ks.getSize(), allAscending());
    const BSONObj stock =
        KeyString::toBson(ks.getBuffer(), ks.getSize(), allAscending(), ks.getTypeBits());

    ASSERT_EQ(canonical.firstElement().woCompare(key.firstElement(), false), 0)
        << "canonical decoder lost the value: " << canonical;
    ASSERT_EQ(stock.firstElement().woCompare(key.firstElement(), false), 0)
        << "stock decoder lost the value: " << stock;
}

TEST(CdcKeyDecoder, NeighboursOfTwoToThe53PlusOne) {
    // 2**53 and 2**53 + 2 are both exactly representable as doubles, so they were decoded
    // correctly even by the buggy comparison; 2**53 + 1 was the only integer part that rounded
    // into the wrong branch. Kept so the boundary stays covered from both sides.
    assertDecodesCanonically(BSON("" << Decimal128("9007199254740992.5")), "2**53 + 0.5");
    assertDecodesCanonically(BSON("" << Decimal128("9007199254740994.5")), "2**53 + 2.5");
}

TEST(CdcKeyDecoder, HugeDecimalsBeyondDoubleRange) {
    for (const auto& v : {"1E+400", "-1E+400", "1.234567890123456789E+6000"}) {
        assertDecodesCanonically(BSON("" << Decimal128(v)), str::stream() << "huge decimal " << v);
    }
}

TEST(CdcKeyDecoder, TinyDoublesAndDecimals) {
    // Below 2**-255: the exponent-shifted small-magnitude encoding.
    assertDecodesCanonically(BSON("" << std::ldexp(1.0, -300)), "tiny double 2**-300");
    assertDecodesCanonically(BSON("" << -std::ldexp(1.0, -300)), "tiny double -2**-300");
    // Below 2**-1074: subnormal, where the encoder rounds down and the decoder unscales in the
    // decimal domain.
    assertDecodesCanonically(BSON("" << Decimal128("1E-320")), "tiny decimal 1E-320");
    assertDecodesCanonically(BSON("" << Decimal128("1E-400")), "tiny decimal 1E-400");
    assertDecodesCanonically(BSON("" << Decimal128("-1E-400")), "tiny decimal -1E-400");
    // Between 2**-255 and 1: the common small-double case with a 2-bit DCM.
    assertDecodesCanonically(BSON("" << std::ldexp(1.0, -100)), "small double 2**-100");
    assertDecodesCanonically(BSON("" << Decimal128("1E-30")), "small decimal 1E-30");
}

//
// Non-numeric types.
//

TEST(CdcKeyDecoder, Strings) {
    for (const auto& v : {"", "a", "hello world", "unicode \xc3\xa9\xc3\xa8"}) {
        assertDecodesCanonically(BSON("" << v), str::stream() << "string '" << v << "'");
    }
}

TEST(CdcKeyDecoder, StringWithEmbeddedNuls) {
    // Encoded as "\x00\xFF" pairs, which the reader must stitch back together.
    const std::string withNuls("a\0b\0\0c", 6);
    assertDecodesCanonically(BSON("" << withNuls), "string with NULs");
}

TEST(CdcKeyDecoder, SymbolDecodesAsEqualString) {
    // Only the type bits say Symbol; a Symbol compares equal to the String of the same
    // characters, so the canonical decoder emits a String.
    BSONObjBuilder b;
    b.appendSymbol("", "sym");
    assertDecodesCanonically(b.obj(), "symbol");
}

TEST(CdcKeyDecoder, NegativeSmallMagnitudeDecimals) {
    // The negative small-magnitude branch inverts every byte before reading, including the
    // selector and the continuation flag.
    for (const auto& v : {"-1E-30", "-1E-320", "-1E-400", "-0.000000000000000000000000001"}) {
        assertDecodesCanonically(BSON("" << Decimal128(v)),
                                 str::stream() << "negative tiny decimal " << v);
    }
    assertDecodesCanonically(BSON("" << -std::ldexp(1.0, -100)), "negative small double");
}

TEST(CdcKeyDecoder, CodeWithScope) {
    // Its scope is a nested object, decoded through the same object walker.
    assertDecodesCanonically(
        BSON("" << BSONCodeWScope("return x;", BSON("x" << 1 << "y" << Decimal128("2.5")))),
        "code with scope");
    assertDecodesCanonically(BSON("" << BSONCodeWScope("return 1;", BSONObj())),
                             "code with empty scope");
}

TEST(CdcKeyDecoder, LargeBinDataUsesTheFourByteLengthPath) {
    // A size of 0xff switches the encoder to a u32 length; anything shorter uses one byte.
    for (size_t size : {size_t(254), size_t(255), size_t(256), size_t(70000)}) {
        std::vector<char> bytes(size);
        for (size_t i = 0; i < size; ++i) {
            bytes[i] = static_cast<char>(i % 251);
        }
        assertDecodesCanonically(
            BSON("" << BSONBinData(bytes.data(), static_cast<int>(size), BinDataGeneral)),
            str::stream() << "bindata of " << size << " bytes");
    }
}

TEST(CdcKeyDecoder, OtherScalarTypes) {
    assertDecodesCanonically(BSON("" << BSONNULL), "null");
    assertDecodesCanonically(BSON("" << BSONUndefined), "undefined");
    assertDecodesCanonically(BSON("" << true), "true");
    assertDecodesCanonically(BSON("" << false), "false");
    assertDecodesCanonically(BSON("" << OID::gen()), "oid");
    assertDecodesCanonically(BSON("" << Date_t::fromMillisSinceEpoch(1758400000000LL)), "date");
    assertDecodesCanonically(BSON("" << Date_t::fromMillisSinceEpoch(-1000)), "date before epoch");
    assertDecodesCanonically(BSON("" << Timestamp(1758400000, 7)), "timestamp");
    assertDecodesCanonically(BSON("" << MINKEY), "minkey");
    assertDecodesCanonically(BSON("" << MAXKEY), "maxkey");
    assertDecodesCanonically(BSON("" << BSONRegEx("^ab", "i")), "regex");
    assertDecodesCanonically(BSON("" << BSONCode("return 1;")), "code");
    assertDecodesCanonically(BSON("" << BSONDBRef("db.coll", OID::gen())), "dbref");

    const char binData[] = {0x01, 0x02, 0x00, 0x03};
    assertDecodesCanonically(BSON("" << BSONBinData(binData, 4, BinDataGeneral)), "bindata");
}

//
// Nested documents and arrays apply the same table element by element.
//

TEST(CdcKeyDecoder, NestedDocument) {
    assertDecodesCanonically(BSON("" << BSON("a" << 1 << "b"
                                                 << "two"
                                                 << "c"
                                                 << 3.5)),
                             "nested document");
}

TEST(CdcKeyDecoder, NestedDocumentWithWideNumerics) {
    assertDecodesCanonically(
        BSON("" << BSON("big" << (1LL << 53) + 1 << "dec" << Decimal128("0.1") << "neg"
                              << std::numeric_limits<long long>::min())),
        "nested document with wide numerics");
}

TEST(CdcKeyDecoder, NestedArray) {
    assertDecodesCanonically(BSON("" << BSON_ARRAY(1 << "two" << 3.5 << BSONNULL)),
                             "nested array");
}

TEST(CdcKeyDecoder, DeeplyNested) {
    assertDecodesCanonically(
        BSON("" << BSON("a" << BSON_ARRAY(BSON("b" << BSON_ARRAY(1 << Decimal128("2.5")))))),
        "deeply nested");
}

//
// The _id-shaped entry points.
//

TEST(CdcKeyDecoder, DecodeIdKeyNamesTheField) {
    const BSONObj key = BSON("" << 42);
    const KeyString ks(KeyString::kLatestVersion, key, allAscending());
    const BSONObj id =
        CdcKeyDecoder::decodeIdKey(StringData(ks.getBuffer(), ks.getSize()));
    ASSERT_BSONOBJ_EQ(id, BSON("_id" << 42));
}

TEST(CdcKeyDecoder, DecodeIdKeyPreservesWideLongs) {
    // The case the stock decoder corrupts: with empty type bits it would emit int(integerPart).
    const long long wide = (1LL << 53) + 1;
    const KeyString ks(KeyString::kLatestVersion, BSON("" << wide), allAscending());
    const BSONObj id = CdcKeyDecoder::decodeIdKey(StringData(ks.getBuffer(), ks.getSize()));
    ASSERT_EQ(id["_id"].numberLong(), wide);
}

TEST(CdcKeyDecoder, DecodeLoggedIdKeyStripsLengthPrefix) {
    const KeyString ks(KeyString::kLatestVersion, BSON("" << 7), allAscending());

    // MongoKey::Serialize writes a native-endian uint16 length, then the KeyString bytes.
    std::string logged;
    const uint16_t len = static_cast<uint16_t>(ks.getSize());
    logged.append(reinterpret_cast<const char*>(&len), sizeof(len));
    logged.append(ks.getBuffer(), ks.getSize());

    StringData keyBytes;
    const BSONObj id = CdcKeyDecoder::decodeLoggedIdKey(logged, "_id"_sd, &keyBytes);
    ASSERT_BSONOBJ_EQ(id, BSON("_id" << 7));
    // eloq.keyBytes is the logged KeyString itself, verbatim.
    ASSERT_EQ(keyBytes, StringData(ks.getBuffer(), ks.getSize()));
}

TEST(CdcKeyDecoder, DecodeLoggedIdKeyRejectsTruncatedInput) {
    ASSERT_THROWS_CODE(
        CdcKeyDecoder::decodeLoggedIdKey(StringData("\x05", 1)), AssertionException,
        ErrorCodes::BadValue);

    // A length prefix that promises more bytes than follow.
    std::string logged;
    const uint16_t len = 64;
    logged.append(reinterpret_cast<const char*>(&len), sizeof(len));
    logged.append("short");
    ASSERT_THROWS_CODE(
        CdcKeyDecoder::decodeLoggedIdKey(logged), AssertionException, ErrorCodes::BadValue);
}

TEST(CdcKeyDecoder, DecodeIdKeyRejectsMultiValuedKey) {
    const KeyString ks(
        KeyString::kLatestVersion, BSON("" << 1 << "" << 2), allAscending());
    ASSERT_THROWS_CODE(CdcKeyDecoder::decodeIdKey(StringData(ks.getBuffer(), ks.getSize())),
                       AssertionException,
                       ErrorCodes::BadValue);
}

//
// Descending keys invert every byte; the decoder must undo that the same way.
//

TEST(CdcKeyDecoder, DescendingOrdering) {
    const Ordering descending = Ordering::make(BSON("a" << -1));
    const BSONObj key = BSON("" << (1LL << 53) + 1);
    const KeyString ks(KeyString::kLatestVersion, key, descending);
    const BSONObj decoded =
        CdcKeyDecoder::toBsonCanonical(ks.getBuffer(), ks.getSize(), descending);
    ASSERT_EQ(decoded.firstElement().woCompare(key.firstElement(), false), 0);
}

}  // namespace
}  // namespace mongo
