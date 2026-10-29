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

#include "mongo/db/modules/eloq/src/base/cdc_blob_decoder.h"

#include <cstring>

#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

// The wire values of txservice::TableEngine and txservice::TableType. Restated rather than
// included, to keep this codec free of the tx service; the round-trip tests encode with the same
// values the writer uses, so a drift would show there.
const uint8_t kTableEngineEloqDoc = 3;
const uint8_t kTableTypePrimary = 0;

// Databases no stream reports, whatever its scope (docs/cdc-change-stream.md, §5.1 and §5.4
// step 1). admin matters twice over: the reference consumer keeps its progress document there
// and relies on no source event ever naming it.
bool isExcludedDatabase(StringData db) {
    return db == "local"_sd || db == "admin"_sd || db == "config"_sd;
}

StringData databaseOf(StringData ns) {
    const size_t dot = ns.find('.');
    return dot == std::string::npos ? ns : ns.substr(0, dot);
}

/**
 * Reads a native-endian integer, refusing to run off the end. Blob integers are written with a
 * reinterpret_cast of the host value (FillDataLogRequest), so they are not byte-swapped.
 */
template <typename T>
T readNative(StringData buf, size_t* offset, StringData what) {
    uassert(ErrorCodes::BadValue,
            str::stream() << "CDC log blob truncated before its " << what,
            buf.size() - *offset >= sizeof(T));
    T value;
    memcpy(&value, buf.rawData() + *offset, sizeof(T));
    *offset += sizeof(T);
    return value;
}

StringData readBytes(StringData buf, size_t* offset, size_t len, StringData what) {
    uassert(ErrorCodes::BadValue,
            str::stream() << "CDC log blob truncated inside its " << what,
            buf.size() - *offset >= len);
    StringData out(buf.rawData() + *offset, len);
    *offset += len;
    return out;
}

}  // namespace

bool CdcStreamScope::includes(StringData ns) const {
    const StringData db = databaseOf(ns);
    if (isExcludedDatabase(db)) {
        return false;
    }
    switch (_kind) {
        case Kind::kDeployment:
            return true;
        case Kind::kDatabase:
            return db == StringData(_target);
        case Kind::kCollection:
            return ns == StringData(_target);
    }
    MONGO_UNREACHABLE;
}

StringData CdcBlobDecoder::Entry::documentBson() const {
    if (record.empty()) {
        return StringData();
    }
    // `size_t unpack_len | unpack_info | size_t blob_len | BSON` (EloqRecord::Serialize).
    size_t offset = 0;
    const size_t unpackLen = readNative<size_t>(record, &offset, "record unpack-info length"_sd);
    readBytes(record, &offset, unpackLen, "record unpack info"_sd);
    const size_t blobLen = readNative<size_t>(record, &offset, "record document length"_sd);
    return readBytes(record, &offset, blobLen, "record document"_sd);
}

CdcBlobDecoder::CdcBlobDecoder(StringData blob, CdcStreamScope scope)
    : _blob(blob), _scope(std::move(scope)) {}

void CdcBlobDecoder::advanceToNextInScopeSection() {
    while (_blobOffset < _blob.size()) {
        // Section header, in the order FillDataLogRequest writes it.
        const uint8_t nameLen = readNative<uint8_t>(_blob, &_blobOffset, "table name length"_sd);
        const StringData name = readBytes(_blob, &_blobOffset, nameLen, "table name"_sd);
        const uint8_t engine = readNative<uint8_t>(_blob, &_blobOffset, "table engine"_sd);
        const uint8_t type = readNative<uint8_t>(_blob, &_blobOffset, "table type"_sd);
        const uint32_t kvLen = readNative<uint32_t>(_blob, &_blobOffset, "entry length"_sd);
        const StringData entries = readBytes(_blob, &_blobOffset, kvLen, "entries"_sd);

        const uint32_t ordinal = _sectionsSeen++;

        // Every other engine's tables, secondary indexes and out-of-scope namespaces are stepped
        // over by the length alone. An EloqKV object-command section has this same header, so it
        // is skipped here without its body ever being interpreted.
        if (engine != kTableEngineEloqDoc || type != kTableTypePrimary) {
            continue;
        }
        if (!_scope.includes(name)) {
            continue;
        }

        _sectionNs = name;
        _sectionEntries = entries;
        _entryOffset = 0;
        _entryOrdinal = 0;
        // The ordinal among all sections, so that a position means the same thing to every
        // stream scope.
        _currentSection = ordinal;
        return;
    }
    _sectionEntries = StringData();
    _entryOffset = 0;
}

bool CdcBlobDecoder::next(Entry* out) {
    invariant(out);

    // Move on until an in-scope section with an entry left is found, or the blob runs out. An
    // in-scope section can legitimately be empty, so this is a loop rather than one step.
    while (_entryOffset >= _sectionEntries.size()) {
        if (_blobOffset >= _blob.size()) {
            return false;
        }
        advanceToNextInScopeSection();
    }

    // Entry: `u16 key_len | KeyString | u8 op | record unless Delete` (the write loop of
    // FillDataLogRequest; MongoKey::Serialize supplies the length prefix).
    const uint16_t keyLen = readNative<uint16_t>(_sectionEntries, &_entryOffset, "key length"_sd);
    const StringData key = readBytes(_sectionEntries, &_entryOffset, keyLen, "key"_sd);
    const uint8_t opByte = readNative<uint8_t>(_sectionEntries, &_entryOffset, "op"_sd);

    uassert(ErrorCodes::BadValue,
            str::stream() << "CDC log blob: unknown operation byte " << opByte,
            opByte >= static_cast<uint8_t>(CdcOperation::kUpdate) &&
                opByte <= static_cast<uint8_t>(CdcOperation::kUpsert));
    const auto op = static_cast<CdcOperation>(opByte);

    StringData record;
    if (op != CdcOperation::kDelete) {
        // Replay reads a record for every non-Delete op, so one is always there; nothing
        // delimits an entry, and assuming otherwise would desynchronise the rest of the section.
        const size_t start = _entryOffset;
        const size_t unpackLen =
            readNative<size_t>(_sectionEntries, &_entryOffset, "record unpack-info length"_sd);
        readBytes(_sectionEntries, &_entryOffset, unpackLen, "record unpack info"_sd);
        const size_t blobLen =
            readNative<size_t>(_sectionEntries, &_entryOffset, "record document length"_sd);
        readBytes(_sectionEntries, &_entryOffset, blobLen, "record document"_sd);
        record = StringData(_sectionEntries.rawData() + start, _entryOffset - start);
    }

    out->section = _currentSection;
    out->entry = _entryOrdinal++;
    out->ns = _sectionNs;
    out->op = op;
    out->keyString = key;
    out->record = record;
    return true;
}

}  // namespace mongo
