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

#include <cstdint>
#include <string>
#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"

namespace mongo {

/**
 * One log group's position inside a resume token: where that group's two scans resume
 * (docs/cdc-change-stream.md, §5.6). The two keys are exactly the `continuation` the logger
 * returns, so an entry is a continuation plus a form and, for form 1, a position inside a record.
 */
struct CdcResumeTokenEntry {
    enum class Form : uint8_t {
        // Resume after this key: every entry of the record it names has been produced.
        kAfterKey = 0,
        // Resume at this key and skip through (section, entry): the record it names is the one
        // the event came from and only part of it has been produced.
        kAtKeyAfterPosition = 1,
    };

    uint32_t logGroupId = 0;
    Form form = Form::kAfterKey;
    // Storage keys, opaque here: (commit_ts, ng, txn) for the data stream, (ddl_ts, namespace,
    // class, key) for the DDL stream. Either may be empty when that stream produced nothing.
    std::string dataKey;
    std::string ddlKey;
    // Form 1 only: the last entry produced from the record at 'dataKey', as the ordinal of its
    // section in the blob and of the entry within that section.
    uint32_t section = 0;
    uint32_t entry = 0;
};

/**
 * A change-stream resume token. MongoDB's 4.0 ResumeToken is keyed on the collection UUID, which
 * this design rules out (§5.4), so only the wire shape drivers expect is kept: the token travels
 * as {_data: <hex string>} and is opaque to clients. The bytes are built only from immutable log
 * information, and are not order-preserving: tokens are decoded and interpreted per group, never
 * compared as bytes.
 *
 *   _data = 0x02                    format version
 *         | u64 timestamp           the event's timestamp; for a watermark, emit_bound
 *         | u8  kind
 *         | u16 entry count
 *         | entries: u32 log_group_id
 *                    u8  form
 *                    u16 len | data-stream key
 *                    u16 len | ddl-stream key
 *                    u32 section | u32 entry     (form 1 only)
 *
 * Multi-byte integers are big-endian. Because entries are keyed by log_group_id and counted, the
 * format survives a later change in the number of log groups: a group absent from a token holds
 * nothing at or below its timestamp, and a group named in a token but gone from the cluster is
 * ignored.
 */
class CdcResumeToken {
public:
    enum class Kind : uint8_t {
        // After a specific event. Valid for resumeAfter and startAfter.
        kEvent = 0,
        // Everything through 'timestamp' has been consumed. Issued when the source drains.
        kWatermark = 1,
        // After the watched collection's drop and the invalidate that followed it. Accepted by
        // startAfter only, as in MongoDB.
        kInvalidate = 2,
    };

    // constexpr, not const: both are passed by reference (to str::stream and the BSON builder),
    // which odr-uses them, and only a constexpr static member is implicitly inline.
    static constexpr uint8_t kFormatVersion = 0x02;
    static constexpr StringData kDataFieldName = "_data"_sd;

    CdcResumeToken() = default;
    CdcResumeToken(uint64_t timestamp, Kind kind, std::vector<CdcResumeTokenEntry> entries)
        : _timestamp(timestamp), _kind(kind), _entries(std::move(entries)) {}

    /**
     * A watermark with no entries, which is what a stream issues before it has produced anything
     * (§5.1): resuming from it starts every group at 'timestamp' + 1.
     */
    static CdcResumeToken makeWatermark(uint64_t timestamp,
                                        std::vector<CdcResumeTokenEntry> entries = {}) {
        return CdcResumeToken(timestamp, Kind::kWatermark, std::move(entries));
    }

    uint64_t timestamp() const {
        return _timestamp;
    }
    Kind kind() const {
        return _kind;
    }
    const std::vector<CdcResumeTokenEntry>& entries() const {
        return _entries;
    }

    /**
     * The entry for 'logGroupId', or nullptr when the token has none, which means that group had
     * produced nothing since the stream started or last resumed. Such a group is positioned from
     * the token's timestamp; see startTimestampForAbsentGroup(). A group named in a token but no
     * longer in the cluster is ignored instead.
     */
    const CdcResumeTokenEntry* entryFor(uint32_t logGroupId) const;

    /**
     * Where a group with no entry restarts its scan. A watermark certifies its own timestamp
     * consumed, so that group starts above it; an event or invalidate token does not, because
     * records at the event's timestamp from other groups may not have been produced yet.
     *
     * Throws for a watermark at the maximum timestamp, whose successor would wrap to 0 and
     * silently restart the stream at the beginning of history. decode() rejects such a token, so
     * this only fires on one constructed in process, which a real timestamp cannot reach.
     */
    uint64_t startTimestampForAbsentGroup() const;

    /** The raw token bytes, as laid out above. */
    std::string encode() const;

    /** The driver-facing form, {_data: <hex>}, which is what an event's _id carries. */
    BSONObj toBSON() const;

    static StatusWith<CdcResumeToken> decode(StringData bytes);

    /** Parses {_data: <hex>}; rejects anything else, as MongoDB does for its own tokens. */
    static StatusWith<CdcResumeToken> parse(const BSONObj& token);

private:
    uint64_t _timestamp = 0;
    Kind _kind = Kind::kWatermark;
    std::vector<CdcResumeTokenEntry> _entries;
};

}  // namespace mongo
