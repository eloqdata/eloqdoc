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

#include "mongo/db/modules/eloq/src/base/cdc_resume_token.h"

#include <cstring>
#include <limits>
#include <unordered_set>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/platform/endian.h"
#include "mongo/util/hex.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

using Form = CdcResumeTokenEntry::Form;
using Kind = CdcResumeToken::Kind;

template <typename T>
void appendBigEndian(std::string* out, T value) {
    T encoded = endian::nativeToBig(value);
    out->append(reinterpret_cast<const char*>(&encoded), sizeof(encoded));
}

void appendSizedString(std::string* out, const std::string& value) {
    invariant(value.size() <= std::numeric_limits<uint16_t>::max());
    appendBigEndian<uint16_t>(out, static_cast<uint16_t>(value.size()));
    out->append(value);
}

/** Reads the token's fields in order, reporting a short or malformed buffer rather than reading
 * past it: a token comes from a client and is never trusted. */
class TokenReader {
public:
    TokenReader(StringData bytes) : _pos(bytes.rawData()), _end(bytes.rawData() + bytes.size()) {}

    template <typename T>
    Status readBigEndian(T* out) {
        if (static_cast<size_t>(_end - _pos) < sizeof(T)) {
            return short_(sizeof(T));
        }
        T encoded;
        memcpy(&encoded, _pos, sizeof(encoded));
        _pos += sizeof(encoded);
        *out = endian::bigToNative(encoded);
        return Status::OK();
    }

    Status readSizedString(std::string* out) {
        uint16_t len;
        Status status = readBigEndian(&len);
        if (!status.isOK()) {
            return status;
        }
        if (static_cast<size_t>(_end - _pos) < len) {
            return short_(len);
        }
        out->assign(_pos, len);
        _pos += len;
        return Status::OK();
    }

    bool atEnd() const {
        return _pos == _end;
    }

private:
    static Status short_(size_t wanted) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "resume token ended before its next " << wanted
                                    << " bytes");
    }

    const char* _pos;
    const char* _end;
};

}  // namespace

uint64_t CdcResumeToken::startTimestampForAbsentGroup() const {
    if (_kind != Kind::kWatermark) {
        return _timestamp;
    }
    uassert(ErrorCodes::FailedToParse,
            "resume token is a watermark at the maximum timestamp, which has no successor to "
            "resume from",
            _timestamp != std::numeric_limits<uint64_t>::max());
    return _timestamp + 1;
}

const CdcResumeTokenEntry* CdcResumeToken::entryFor(uint32_t logGroupId) const {
    for (const auto& entry : _entries) {
        if (entry.logGroupId == logGroupId) {
            return &entry;
        }
    }
    return nullptr;
}

std::string CdcResumeToken::encode() const {
    std::string out;
    out.push_back(static_cast<char>(kFormatVersion));
    appendBigEndian<uint64_t>(&out, _timestamp);
    out.push_back(static_cast<char>(_kind));

    invariant(_entries.size() <= std::numeric_limits<uint16_t>::max());
    appendBigEndian<uint16_t>(&out, static_cast<uint16_t>(_entries.size()));

    for (const auto& entry : _entries) {
        appendBigEndian<uint32_t>(&out, entry.logGroupId);
        out.push_back(static_cast<char>(entry.form));
        appendSizedString(&out, entry.dataKey);
        appendSizedString(&out, entry.ddlKey);
        if (entry.form == Form::kAtKeyAfterPosition) {
            appendBigEndian<uint32_t>(&out, entry.section);
            appendBigEndian<uint32_t>(&out, entry.entry);
        }
    }
    return out;
}

BSONObj CdcResumeToken::toBSON() const {
    const std::string bytes = encode();
    BSONObjBuilder builder;
    builder.append(kDataFieldName, toHexLower(bytes.data(), static_cast<int>(bytes.size())));
    return builder.obj();
}

StatusWith<CdcResumeToken> CdcResumeToken::decode(StringData bytes) {
    TokenReader reader(bytes);

    uint8_t version;
    Status status = reader.readBigEndian(&version);
    if (!status.isOK()) {
        return status;
    }
    if (version != kFormatVersion) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "unsupported resume token version " << version
                                    << "; this server writes version "
                                    << kFormatVersion);
    }

    uint64_t timestamp;
    if (!(status = reader.readBigEndian(&timestamp)).isOK()) {
        return status;
    }

    uint8_t kindByte;
    if (!(status = reader.readBigEndian(&kindByte)).isOK()) {
        return status;
    }
    if (kindByte > static_cast<uint8_t>(Kind::kInvalidate)) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "unknown resume token kind " << kindByte);
    }
    const Kind kind = static_cast<Kind>(kindByte);

    // A watermark means "everything through this timestamp is consumed", so resuming from one
    // starts at its successor. At the maximum that successor is unrepresentable and would wrap to
    // 0, restarting the stream at the beginning of history instead of past its end. No such
    // watermark can have been issued -- timestamps are microseconds, so the maximum is some
    // 584,000 years out -- so it is rejected rather than saturated.
    if (kind == Kind::kWatermark && timestamp == std::numeric_limits<uint64_t>::max()) {
        return Status(ErrorCodes::FailedToParse,
                      "resume token is a watermark at the maximum timestamp, which has no "
                      "successor to resume from");
    }

    uint16_t entryCount;
    if (!(status = reader.readBigEndian(&entryCount)).isOK()) {
        return status;
    }

    std::vector<CdcResumeTokenEntry> entries;
    entries.reserve(entryCount);
    // Tracked in a set rather than by rescanning 'entries': the count is client-supplied and can
    // reach 65535, which a pairwise scan turns into ~2.1 billion comparisons for a token of only
    // about 1 MB.
    std::unordered_set<uint32_t> seenGroups;
    seenGroups.reserve(entryCount);
    for (uint16_t i = 0; i < entryCount; ++i) {
        CdcResumeTokenEntry entry;
        if (!(status = reader.readBigEndian(&entry.logGroupId)).isOK()) {
            return status;
        }

        uint8_t formByte;
        if (!(status = reader.readBigEndian(&formByte)).isOK()) {
            return status;
        }
        if (formByte > static_cast<uint8_t>(Form::kAtKeyAfterPosition)) {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "unknown resume token entry form " << formByte);
        }
        entry.form = static_cast<Form>(formByte);

        if (!(status = reader.readSizedString(&entry.dataKey)).isOK()) {
            return status;
        }
        if (!(status = reader.readSizedString(&entry.ddlKey)).isOK()) {
            return status;
        }
        if (entry.form == Form::kAtKeyAfterPosition) {
            if (!(status = reader.readBigEndian(&entry.section)).isOK()) {
                return status;
            }
            if (!(status = reader.readBigEndian(&entry.entry)).isOK()) {
                return status;
            }
        }

        if (!seenGroups.insert(entry.logGroupId).second) {
            return Status(ErrorCodes::FailedToParse,
                          str::stream() << "resume token names log group " << entry.logGroupId
                                        << " more than once");
        }
        entries.push_back(std::move(entry));
    }

    if (!reader.atEnd()) {
        return Status(ErrorCodes::FailedToParse,
                      "resume token has trailing bytes after its last entry");
    }

    return CdcResumeToken(timestamp, kind, std::move(entries));
}

StatusWith<CdcResumeToken> CdcResumeToken::parse(const BSONObj& token) {
    BSONElement dataElem = token[kDataFieldName];
    if (dataElem.eoo()) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "resume token is missing the '" << kDataFieldName
                                    << "' field: "
                                    << token);
    }
    if (dataElem.type() != BSONType::String) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "resume token field '" << kDataFieldName
                                    << "' must be a string, found "
                                    << typeName(dataElem.type()));
    }
    if (token.nFields() != 1) {
        return Status(ErrorCodes::FailedToParse,
                      str::stream() << "resume token must hold only '" << kDataFieldName
                                    << "': "
                                    << token);
    }

    const StringData hex = dataElem.valueStringData();
    if (hex.size() % 2 != 0) {
        return Status(ErrorCodes::FailedToParse,
                      "resume token hex string has an odd number of characters");
    }
    for (size_t i = 0; i < hex.size(); ++i) {
        if (!isxdigit(static_cast<unsigned char>(hex[i]))) {
            return Status(ErrorCodes::FailedToParse,
                          "resume token hex string has a non-hexadecimal character");
        }
    }

    std::string bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2) {
        bytes.push_back(fromHex(StringData(hex.rawData() + i, 2)));
    }
    return decode(bytes);
}

}  // namespace mongo
