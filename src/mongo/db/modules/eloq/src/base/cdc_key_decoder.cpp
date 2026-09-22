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

#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/base/cdc_key_decoder.h"

#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <string>

#include "mongo/base/data_view.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/platform/decimal128.h"
#include "mongo/platform/endian.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/bufreader.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

// KeyString's canonical type bytes. These live in key_string.cpp's anonymous namespace, so they
// are restated here; a mismatch would show up immediately in the round-trip test, which encodes
// with the real KeyString.
namespace CType {
const uint8_t kMinKey = 10;
const uint8_t kUndefined = 15;
const uint8_t kNullish = 20;
const uint8_t kNumeric = 30;
const uint8_t kStringLike = 60;
const uint8_t kObject = 70;
const uint8_t kArray = 80;
const uint8_t kBinData = 90;
const uint8_t kOID = 100;
const uint8_t kBool = 110;
const uint8_t kDate = 120;
const uint8_t kTimestamp = 130;
const uint8_t kRegEx = 140;
const uint8_t kDBRef = 150;
const uint8_t kCode = 160;
const uint8_t kCodeWithScope = 170;
const uint8_t kMaxKey = 240;

const uint8_t kNumericNaN = kNumeric + 0;
const uint8_t kNumericNegativeLargeMagnitude = kNumeric + 1;
const uint8_t kNumericNegative8ByteInt = kNumeric + 2;
const uint8_t kNumericNegative1ByteInt = kNumeric + 9;
const uint8_t kNumericNegativeSmallMagnitude = kNumeric + 10;
const uint8_t kNumericZero = kNumeric + 11;
const uint8_t kNumericPositiveSmallMagnitude = kNumeric + 12;
const uint8_t kNumericPositive1ByteInt = kNumeric + 13;
const uint8_t kNumericPositive8ByteInt = kNumeric + 20;
const uint8_t kNumericPositiveLargeMagnitude = kNumeric + 21;

const uint8_t kBoolFalse = kBool + 0;
const uint8_t kBoolTrue = kBool + 1;

size_t numBytesForInt(uint8_t ctype) {
    if (ctype >= kNumericPositive1ByteInt) {
        return ctype - kNumericPositive1ByteInt + 1;
    }
    return kNumericNegative1ByteInt - ctype + 1;
}
}  // namespace CType

const uint8_t kEnd = 0x4;
const uint8_t kLess = 1;
const uint8_t kGreater = 254;

// Decimal continuation markers, as stored in the two low bits of a binary fraction.
const uint8_t kDCMEqualToDouble = 0x0;
const uint8_t kDCMHasContinuationLessThanDoubleRoundedUpTo15Digits = 0x1;
const uint8_t kDCMEqualToDoubleRoundedUpTo15Digits = 0x2;
const uint8_t kDCMHasContinuationLargerThanDoubleRoundedUpTo15Digits = 0x3;

const double kMinLargeDouble = 1ULL << 63;

// The largest integer a double represents exactly. Kept as an int64, not a double as
// key_string.cpp does: the comparison below is against an int64 integer part, and converting
// that to double first rounds 2**53 + 1 down to 2**53, which makes the test false for exactly
// the values it exists to catch. See the comment at its use.
const int64_t kMaxIntForDouble = 1LL << 53;

const int kTinyDoubleExponentShift = 256;
const double kTinyDoubleExponentUpshiftFactor = std::ldexp(1, kTinyDoubleExponentShift);
const double kTinyDoubleExponentDownshiftFactor = std::ldexp(1, -kTinyDoubleExponentShift);

const double kInvPow256[] = {1.0,
                             1.0 / 256,
                             1.0 / 256 / 256,
                             1.0 / 256 / 256 / 256,
                             1.0 / 256 / 256 / 256 / 256,
                             1.0 / 256 / 256 / 256 / 256 / 256,
                             1.0 / 256 / 256 / 256 / 256 / 256 / 256,
                             1.0 / 256 / 256 / 256 / 256 / 256 / 256 / 256};

Decimal128 tinyDoubleUpshiftAsDecimal() {
    static const Decimal128 factor(std::ldexp(1, kTinyDoubleExponentShift),
                                   Decimal128::kRoundTo34Digits,
                                   Decimal128::kRoundTowardZero);
    return factor;
}

Decimal128 tinyDoubleDownshiftAsDecimal() {
    static const Decimal128 factor(std::ldexp(1, -kTinyDoubleExponentShift),
                                   Decimal128::kRoundTo34Digits,
                                   Decimal128::kRoundTowardZero);
    return factor;
}

void memcpyFlipBits(void* dst, const void* src, size_t bytes) {
    const char* input = static_cast<const char*>(src);
    char* output = static_cast<char*>(dst);
    for (size_t i = 0; i < bytes; ++i) {
        output[i] = ~input[i];
    }
}

template <typename T>
T readType(BufReader* reader, bool inverted) {
    MONGO_STATIC_ASSERT(std::is_integral<T>::value);
    T t = ConstDataView(static_cast<const char*>(reader->skip(sizeof(T)))).read<T>();
    if (inverted)
        return ~t;
    return t;
}

// True when the next byte can be inspected. KeyString terminates a key with kEnd, so a
// well-formed key always has one; guarding keeps a truncated key an exception rather than a read
// past the end.
bool canPeek(BufReader* reader) {
    return reader->remaining() > 0;
}

StringData readCString(BufReader* reader) {
    const char* start = static_cast<const char*>(reader->pos());
    const char* end = static_cast<const char*>(memchr(start, 0x0, reader->remaining()));
    uassert(ErrorCodes::BadValue, "CDC key: no null terminator in string.", end);
    size_t actualBytes = end - start;
    reader->skip(1 + actualBytes);
    return StringData(start, actualBytes);
}

StringData readCStringWithNuls(BufReader* reader, std::string* scratch) {
    const StringData initial = readCString(reader);
    if (!canPeek(reader) || reader->peek<unsigned char>() != 0xFF)
        return initial;

    scratch->append(initial.rawData(), initial.size());
    while (canPeek(reader) && reader->peek<unsigned char>() == 0xFF) {
        *scratch += '\0';
        reader->skip(1);
        const StringData nextPart = readCString(reader);
        scratch->append(nextPart.rawData(), nextPart.size());
    }
    return *scratch;
}

std::string readInvertedCString(BufReader* reader) {
    const char* start = static_cast<const char*>(reader->pos());
    const char* end = static_cast<const char*>(memchr(start, 0xFF, reader->remaining()));
    uassert(ErrorCodes::BadValue, "CDC key: no 0xFF in inverted string.", end);
    size_t actualBytes = end - start;
    std::string s(start, actualBytes);
    for (size_t i = 0; i < s.size(); i++) {
        s[i] = ~s[i];
    }
    reader->skip(1 + actualBytes);
    return s;
}

std::string readInvertedCStringWithNuls(BufReader* reader) {
    std::string out;
    bool firstPass = true;
    do {
        if (firstPass) {
            firstPass = false;
        } else {
            reader->skip(1);
            out += '\xFF';
        }
        const char* start = static_cast<const char*>(reader->pos());
        const char* end = static_cast<const char*>(memchr(start, 0xFF, reader->remaining()));
        uassert(ErrorCodes::BadValue, "CDC key: no 0xFF in inverted string.", end);
        size_t actualBytes = end - start;
        out.append(start, actualBytes);
        reader->skip(1 + actualBytes);
    } while (canPeek(reader) && reader->peek<unsigned char>() == 0x00);

    for (size_t i = 0; i < out.size(); i++) {
        out[i] = ~out[i];
    }
    return out;
}

/**
 * Reads the 8-byte continuation that follows a decimal whose 15-digit prefix is already in 'num'.
 * Byte-for-byte the stock reader; the continuation's presence is decided by markers in the key,
 * never by type bits, which is what lets this decoder run without them.
 */
Decimal128 readDecimalContinuation(BufReader* reader, bool inverted, Decimal128 num) {
    uint32_t flags = Decimal128::kNoFlag;
    uint64_t continuation = endian::bigToNative(readType<uint64_t>(reader, inverted));
    num = num.normalize();
    uassert(ErrorCodes::BadValue,
            "CDC key: invalid decimal continuation.",
            Decimal128::isValid(num.isNegative(), num.getBiasedExponent(), 0, continuation));
    num = num.add(Decimal128(num.isNegative(), num.getBiasedExponent(), 0, continuation), &flags);
    uassert(ErrorCodes::BadValue,
            "CDC key: inexact decimal continuation.",
            !(Decimal128::hasFlag(flags, Decimal128::kInexact)));
    return num;
}

/**
 * Appends an exact integer as the narrowest BSON integer type that holds it. int and long long
 * compare equal by value in MongoDB, so the choice is invisible to a query; it only keeps the
 * common case of a small _id looking like the int it almost certainly was.
 */
void appendCanonicalInt(BSONObjBuilderValueStream* stream, int64_t value) {
    if (value >= std::numeric_limits<int32_t>::min() &&
        value <= std::numeric_limits<int32_t>::max()) {
        *stream << static_cast<int>(value);
    } else {
        *stream << static_cast<long long>(value);
    }
}

void toBsonValueCanonical(uint8_t ctype,
                          BufReader* reader,
                          bool inverted,
                          BSONObjBuilderValueStream* stream);

void toBsonCanonical(BufReader* reader, bool inverted, BSONObjBuilder* builder) {
    while (readType<uint8_t>(reader, inverted) != 0) {
        if (inverted) {
            std::string name = readInvertedCString(reader);
            BSONObjBuilderValueStream& stream = *builder << name;
            toBsonValueCanonical(readType<uint8_t>(reader, inverted), reader, inverted, &stream);
        } else {
            StringData name = readCString(reader);
            BSONObjBuilderValueStream& stream = *builder << name;
            toBsonValueCanonical(readType<uint8_t>(reader, inverted), reader, inverted, &stream);
        }
    }
}

void toBsonValueCanonical(uint8_t ctype,
                          BufReader* reader,
                          bool inverted,
                          BSONObjBuilderValueStream* stream) {
    bool isNegative = false;

    switch (ctype) {
        case CType::kMinKey:
            *stream << MINKEY;
            break;
        case CType::kMaxKey:
            *stream << MAXKEY;
            break;
        case CType::kNullish:
            *stream << BSONNULL;
            break;
        case CType::kUndefined:
            *stream << BSONUndefined;
            break;
        case CType::kBoolTrue:
            *stream << true;
            break;
        case CType::kBoolFalse:
            *stream << false;
            break;

        case CType::kDate:
            *stream << Date_t::fromMillisSinceEpoch(
                endian::bigToNative(readType<uint64_t>(reader, inverted)) ^ (1LL << 63));
            break;

        case CType::kTimestamp:
            *stream << Timestamp(endian::bigToNative(readType<uint64_t>(reader, inverted)));
            break;

        case CType::kOID:
            if (inverted) {
                char buf[OID::kOIDSize];
                memcpyFlipBits(buf, reader->skip(OID::kOIDSize), OID::kOIDSize);
                *stream << OID::from(buf);
            } else {
                *stream << OID::from(reader->skip(OID::kOIDSize));
            }
            break;

        case CType::kStringLike: {
            // String or Symbol; only the type bits distinguish them and a Symbol compares equal
            // to the String with the same characters, so a String is always emitted.
            if (inverted) {
                *stream << readInvertedCStringWithNuls(reader);
            } else {
                std::string scratch;
                *stream << readCStringWithNuls(reader, &scratch);
            }
            break;
        }

        case CType::kCode: {
            if (inverted) {
                *stream << BSONCode(readInvertedCStringWithNuls(reader));
            } else {
                std::string scratch;
                *stream << BSONCode(readCStringWithNuls(reader, &scratch));
            }
            break;
        }

        case CType::kCodeWithScope: {
            std::string scratch;
            std::string code;
            if (inverted) {
                code = readInvertedCStringWithNuls(reader);
            } else {
                code = readCStringWithNuls(reader, &scratch).toString();
            }
            BSONObjBuilder scope;
            toBsonCanonical(reader, inverted, &scope);
            *stream << BSONCodeWScope(code, scope.done());
            break;
        }

        case CType::kBinData: {
            size_t size = readType<uint8_t>(reader, inverted);
            if (size == 0xff) {
                size = endian::bigToNative(readType<uint32_t>(reader, inverted));
            }
            BinDataType subType = BinDataType(readType<uint8_t>(reader, inverted));
            const void* ptr = reader->skip(size);
            if (!inverted) {
                *stream << BSONBinData(ptr, size, subType);
            } else {
                std::unique_ptr<char[]> flipped(new char[size]);
                memcpyFlipBits(flipped.get(), ptr, size);
                *stream << BSONBinData(flipped.get(), size, subType);
            }
            break;
        }

        case CType::kRegEx: {
            if (inverted) {
                std::string pattern = readInvertedCString(reader);
                std::string flags = readInvertedCString(reader);
                *stream << BSONRegEx(pattern, flags);
            } else {
                StringData pattern = readCString(reader);
                StringData flags = readCString(reader);
                *stream << BSONRegEx(pattern, flags);
            }
            break;
        }

        case CType::kDBRef: {
            size_t size = endian::bigToNative(readType<uint32_t>(reader, inverted));
            if (inverted) {
                std::unique_ptr<char[]> ns(new char[size]);
                memcpyFlipBits(ns.get(), reader->skip(size), size);
                char oidBytes[OID::kOIDSize];
                memcpyFlipBits(oidBytes, reader->skip(OID::kOIDSize), OID::kOIDSize);
                *stream << BSONDBRef(StringData(ns.get(), size), OID::from(oidBytes));
            } else {
                const char* ns = static_cast<const char*>(reader->skip(size));
                OID oid = OID::from(reader->skip(OID::kOIDSize));
                *stream << BSONDBRef(StringData(ns, size), oid);
            }
            break;
        }

        case CType::kObject: {
            BSONObjBuilder subObj(stream->subobjStart());
            toBsonCanonical(reader, inverted, &subObj);
            break;
        }

        case CType::kArray: {
            BSONObjBuilder subArr(stream->subarrayStart());
            int index = 0;
            uint8_t elemType;
            while ((elemType = readType<uint8_t>(reader, inverted)) != 0) {
                toBsonValueCanonical(
                    elemType, reader, inverted, &(subArr << BSONObjBuilder::numStr(index++)));
            }
            break;
        }

        //
        // Numerics. No bytes of any of these branches depend on the type bits.
        //

        case CType::kNumericNaN:
            // Consumes nothing. Double and decimal NaN compare equal.
            *stream << std::numeric_limits<double>::quiet_NaN();
            break;

        case CType::kNumericZero:
            // Consumes nothing: which zero it was lives entirely in the type bits, and 0, -0.0,
            // 0L and every decimal zero compare equal.
            *stream << 0;
            break;

        case CType::kNumericNegativeLargeMagnitude:
            inverted = !inverted;
            isNegative = true;
        // fallthrough (same format as the positive case, but inverted)
        case CType::kNumericPositiveLargeMagnitude: {
            uint64_t encoded = endian::bigToNative(readType<uint64_t>(reader, inverted));
            bool hasDecimalContinuation = false;
            double bin;

            if (!(encoded & (1ULL << 63))) {
                // A double at or above 2**63 in magnitude.
                hasDecimalContinuation = encoded & 1;
                encoded >>= 1;
                encoded |= 1ULL << 62;  // implied leading exponent bit
                memcpy(&bin, &encoded, sizeof(bin));
                if (isNegative)
                    bin = -bin;
            } else if (encoded == ~0ULL) {
                *stream << (isNegative ? -std::numeric_limits<double>::infinity()
                                       : std::numeric_limits<double>::infinity());
                break;
            } else {
                // A decimal too large to be a double, stored whole.
                uint64_t highbits = encoded & ~(1ULL << 63);
                uint64_t lowbits = endian::bigToNative(readType<uint64_t>(reader, inverted));
                Decimal128 dec(Decimal128::Value{lowbits, highbits});
                *stream << (isNegative ? dec.negate() : dec);
                break;
            }

            if (!hasDecimalContinuation) {
                // Exact for a double original, and for LLONG_MIN, whose double is exact.
                *stream << bin;
                break;
            }
            const auto roundAwayFromZero =
                isNegative ? Decimal128::kRoundTowardNegative : Decimal128::kRoundTowardPositive;
            Decimal128 dec(bin, Decimal128::kRoundTo34Digits, roundAwayFromZero);
            *stream << readDecimalContinuation(reader, inverted, dec);
            break;
        }

        case CType::kNumericNegativeSmallMagnitude:
            inverted = !inverted;
            isNegative = true;
        // fallthrough (same format as the positive case, but inverted)
        case CType::kNumericPositiveSmallMagnitude: {
            uint64_t encoded = endian::bigToNative(readType<uint64_t>(reader, inverted));

            switch (encoded >> 62) {
                case 0x0: {
                    // A decimal smaller in magnitude than 2**(-1074), stored whole.
                    uint64_t lowbits = endian::bigToNative(readType<uint64_t>(reader, inverted));
                    Decimal128 dec(Decimal128::Value{lowbits, encoded});
                    if (ctype == CType::kNumericNegativeSmallMagnitude)
                        dec = dec.negate();
                    *stream << dec;
                    break;
                }
                case 0x1:
                case 0x2: {
                    // A double or decimal between 2**(-1074) and 2**(-255), exponent shifted up
                    // by 256 to avoid subnormals.
                    bool hasDecimalContinuation = encoded & 1;
                    encoded -= 1ULL << 62;
                    encoded >>= 1;
                    double scaledBin;
                    memcpy(&scaledBin, &encoded, sizeof(scaledBin));
                    if (!hasDecimalContinuation) {
                        double bin = scaledBin * kTinyDoubleExponentDownshiftFactor;
                        *stream << (isNegative ? -bin : bin);
                        break;
                    }
                    Decimal128 dec;
                    if (scaledBin <
                        std::numeric_limits<double>::min() * kTinyDoubleExponentUpshiftFactor) {
                        // Subnormal once unscaled: the encoder rounded down throughout so the
                        // continuation stays positive, so unscale in the decimal domain.
                        Decimal128 scaledDec(
                            scaledBin, Decimal128::kRoundTo34Digits, Decimal128::kRoundTowardZero);
                        dec = scaledDec.multiply(tinyDoubleDownshiftAsDecimal(),
                                                 Decimal128::kRoundTowardZero);
                    } else {
                        double bin = scaledBin * kTinyDoubleExponentDownshiftFactor;
                        dec = Decimal128(
                            bin, Decimal128::kRoundTo34Digits, Decimal128::kRoundTowardPositive);
                    }
                    dec = readDecimalContinuation(reader, inverted, dec);
                    *stream << (isNegative ? dec.negate() : dec);
                    break;
                }
                case 0x3: {
                    // A double of 2**(-255) or more in magnitude; the common fractional case.
                    const uint8_t dcm = encoded & 3;
                    encoded >>= 2;
                    double bin;
                    memcpy(&bin, &encoded, sizeof(bin));
                    if (dcm == kDCMEqualToDouble) {
                        *stream << (isNegative ? -bin : bin);
                        break;
                    }
                    Decimal128 dec;
                    if (dcm == kDCMEqualToDoubleRoundedUpTo15Digits) {
                        dec = Decimal128(
                            bin, Decimal128::kRoundTo15Digits, Decimal128::kRoundTowardPositive);
                    } else {
                        dec = Decimal128(
                            bin, Decimal128::kRoundTo34Digits, Decimal128::kRoundTowardPositive);
                        dec = readDecimalContinuation(reader, inverted, dec);
                    }
                    *stream << (isNegative ? dec.negate() : dec);
                    break;
                }
                default:
                    MONGO_UNREACHABLE;
            }
            break;
        }

        case CType::kNumericNegative8ByteInt:
        case CType::kNumericNegative8ByteInt + 1:
        case CType::kNumericNegative8ByteInt + 2:
        case CType::kNumericNegative8ByteInt + 3:
        case CType::kNumericNegative8ByteInt + 4:
        case CType::kNumericNegative8ByteInt + 5:
        case CType::kNumericNegative8ByteInt + 6:
        case CType::kNumericNegative1ByteInt:
            inverted = !inverted;
            isNegative = true;
        // fallthrough (same format as the positive case, but inverted)
        case CType::kNumericPositive1ByteInt:
        case CType::kNumericPositive1ByteInt + 1:
        case CType::kNumericPositive1ByteInt + 2:
        case CType::kNumericPositive1ByteInt + 3:
        case CType::kNumericPositive1ByteInt + 4:
        case CType::kNumericPositive1ByteInt + 5:
        case CType::kNumericPositive1ByteInt + 6:
        case CType::kNumericPositive8ByteInt: {
            uint64_t encodedIntegerPart = 0;
            {
                size_t intBytesRemaining = CType::numBytesForInt(ctype);
                while (intBytesRemaining--) {
                    encodedIntegerPart =
                        (encodedIntegerPart << 8) | readType<uint8_t>(reader, inverted);
                }
            }

            const bool haveFractionalPart = (encodedIntegerPart & 1);
            int64_t integerPart = encodedIntegerPart >> 1;

            if (!haveFractionalPart) {
                if (isNegative)
                    integerPart = -integerPart;
                // Exact for int, long, and any integer-valued double or decimal in this range.
                appendCanonicalInt(stream, integerPart);
                break;
            }

            // Every V1 value with a fraction occupies 8 bytes in total.
            const size_t fracBytes = 8 - CType::numBytesForInt(ctype);
            uint64_t encodedFraction = integerPart;
            for (size_t fracBytesRemaining = fracBytes; fracBytesRemaining; fracBytesRemaining--) {
                encodedFraction = (encodedFraction << 8) | readType<uint8_t>(reader, inverted);
            }

            const double bin =
                static_cast<double>(encodedFraction & ~3ULL) * kInvPow256[fracBytes];

            // With no fraction bytes of its own the value is an 8-byte integer part plus a
            // fraction, which the encoder only produces for a decimal with a continuation.
            const uint8_t dcm = fracBytes
                ? static_cast<uint8_t>(encodedFraction & 3)
                : kDCMHasContinuationLargerThanDoubleRoundedUpTo15Digits;

            if (dcm == kDCMEqualToDouble) {
                // Exact for a double original; equal in value for a decimal that is a double.
                *stream << (isNegative ? -bin : bin);
                break;
            }

            Decimal128 dec;
            if (dcm == kDCMEqualToDoubleRoundedUpTo15Digits) {
                dec = Decimal128(
                    bin, Decimal128::kRoundTo15Digits, Decimal128::kRoundTowardPositive);
            } else {
                // An integer comparison, deliberately. key_string.cpp:1710 writes the same test
                // against a double kMaxIntForDouble, which promotes integerPart to double and so
                // answers false for 2**53 + 1 -- the first value that needs the exact branch.
                // 'bin' is then rebuilt from a binary fraction that cannot hold the integer part,
                // and the value comes back one unit low. The encoding is not lossy; only that
                // expression is.
                dec = integerPart > kMaxIntForDouble
                    ? Decimal128(integerPart)
                    : Decimal128(
                          bin, Decimal128::kRoundTo34Digits, Decimal128::kRoundTowardPositive);
                dec = readDecimalContinuation(reader, inverted, dec);
            }
            *stream << (isNegative ? dec.negate() : dec);
            break;
        }

        default:
            uasserted(ErrorCodes::BadValue,
                      str::stream() << "CDC key: unknown KeyString type byte: " << ctype);
    }
}

}  // namespace

BSONObj CdcKeyDecoder::toBsonCanonical(const char* buffer, size_t len, Ordering ord) {
    BSONObjBuilder builder;
    BufReader reader(buffer, len);
    for (int i = 0; reader.remaining(); i++) {
        const bool invert = (ord.get(i) == -1);
        uint8_t ctype = readType<uint8_t>(&reader, invert);
        if (ctype == kLess || ctype == kGreater) {
            // A query discriminator, logically part of the preceding field.
            ctype = readType<uint8_t>(&reader, invert);
        }
        if (ctype == kEnd)
            break;
        toBsonValueCanonical(ctype, &reader, invert, &(builder << ""));
    }
    return builder.obj();
}

BSONObj CdcKeyDecoder::decodeIdKey(StringData keyString, StringData fieldName) {
    BSONObj decoded = toBsonCanonical(keyString, Ordering::make(BSONObj()));
    BSONObjIterator it(decoded);
    uassert(ErrorCodes::BadValue,
            "CDC key: primary key decoded to no value.",
            it.more());
    BSONElement value = it.next();
    uassert(ErrorCodes::BadValue,
            str::stream() << "CDC key: primary key decoded to more than one value: " << decoded,
            !it.more());

    BSONObjBuilder builder;
    builder.appendAs(value, fieldName);
    return builder.obj();
}

BSONObj CdcKeyDecoder::decodeLoggedIdKey(StringData loggedKey,
                                         StringData fieldName,
                                         StringData* keyStringOut) {
    uassert(ErrorCodes::BadValue,
            "CDC key: logged key is shorter than its length prefix.",
            loggedKey.size() >= sizeof(uint16_t));
    // MongoKey::Serialize writes the length in native byte order (eloq_key.h).
    uint16_t len;
    memcpy(&len, loggedKey.rawData(), sizeof(len));
    uassert(ErrorCodes::BadValue,
            str::stream() << "CDC key: logged key length " << len << " exceeds the "
                          << (loggedKey.size() - sizeof(uint16_t))
                          << " bytes that follow it.",
            loggedKey.size() - sizeof(uint16_t) >= len);

    StringData keyString(loggedKey.rawData() + sizeof(uint16_t), len);
    if (keyStringOut) {
        *keyStringOut = keyString;
    }
    return decodeIdKey(keyString, fieldName);
}

}  // namespace mongo
