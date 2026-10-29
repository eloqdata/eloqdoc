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

#pragma once

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/ordering.h"

namespace mongo {

/**
 * Decodes KeyString V1 bytes that were logged without their type bits.
 *
 * A CDC delete event has only the logged key to recover documentKey._id from: no record follows
 * a Delete op in the redo log (docs/cdc-change-stream.md, F1/F3). The stock KeyString::toBson
 * cannot be used with empty type bits -- it would read every numeric as kInt, narrowing an int64
 * _id above 32 bits with int(integerPart), and would fail assertion 50810 on a key with a
 * fractional part or a decimal.
 *
 * This decoder never consults type bits. That is exact on byte consumption, because in KeyString
 * V1 the layout of every branch is fixed by the CType byte and by marker bits inside the key
 * itself (the fraction bit, the decimal-continuation markers, the small-magnitude selector);
 * type bits only choose the BSON type and the decimal cohort of the output. So it consumes
 * exactly the bytes the stock decoder would, and differs only where the stock decoder would have
 * used type bits to pick a representation:
 *
 *   - numerics come back as int32 when they fit, else int64, else double, else Decimal128,
 *     whichever represents the encoded value exactly;
 *   - every zero comes back as int32 0 and every NaN as double NaN (all zeros compare equal to
 *     each other, as do all NaNs);
 *   - a Symbol comes back as a String.
 *
 * MongoDB compares numbers across int, long, double and Decimal128 by value, treats all zeros
 * and all NaNs as equal within their kind, and ignores a decimal's exponent for equality, so the
 * result always compares equal to the original value and deleteOne({_id}) matches. The only
 * information it cannot reproduce is the original BSON representation, never the value.
 *
 * Consumers needing a byte-stable per-document key use the event's eloq.keyBytes field (the
 * logged KeyString itself), not documentKey.
 */
class CdcKeyDecoder {
public:
    /**
     * Decodes a whole KeyString into a BSONObj whose fields are named "" in encoded order, the
     * same shape KeyString::toBson produces. 'ord' says which fields were encoded descending.
     * Throws AssertionException on malformed input.
     */
    static BSONObj toBsonCanonical(const char* buffer, size_t len, Ordering ord);

    static BSONObj toBsonCanonical(StringData data, Ordering ord) {
        return toBsonCanonical(data.rawData(), data.size(), ord);
    }

    /**
     * Decodes the KeyString of a primary key into {<fieldName>: <value>}, for use as a change
     * event's documentKey. The key must hold exactly one value, as an _id key does.
     */
    static BSONObj decodeIdKey(StringData keyString, StringData fieldName = "_id"_sd);

    /**
     * As decodeIdKey, for an entry key as it appears in a redo-log blob: a native-endian uint16
     * length followed by that many KeyString bytes (docs/cdc-change-stream.md, F3). Also returns
     * the raw KeyString bytes through 'keyStringOut' when non-null, which is what the event's
     * eloq.keyBytes field carries.
     */
    static BSONObj decodeLoggedIdKey(StringData loggedKey,
                                     StringData fieldName = "_id"_sd,
                                     StringData* keyStringOut = nullptr);
};

}  // namespace mongo
