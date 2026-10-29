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
 * The change stream's resume token and its per-log-group entries (docs/cdc-change-stream.md §5.6).
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
 * @brief One log group's position inside a resume token: where that group's two scans resume
 * (docs/cdc-change-stream.md, §5.6).
 *
 * The two keys are exactly the `continuation` the logger returns, so an entry is a continuation
 * plus a form and, for form 1, a position inside a record.
 */
struct CdcResumeTokenEntry {
    /**
     * @brief How the group resumes relative to its keys.
     */
    enum class Form : uint8_t {
        /** Resume after this key: every entry of the record it names has been produced. */
        kAfterKey = 0,
        /**
         * Resume at this key and skip through (section, entry): the record it names is the one
         * the event came from and only part of it has been produced.
         */
        kAtKeyAfterPosition = 1,
    };

    uint32_t logGroupId = 0;      ///< The log group.
    Form form = Form::kAfterKey;  ///< How it resumes.
    /**
     * The data stream's position, opaque here: the storage key (commit_ts, ng, txn), big-endian.
     * Empty when that stream produced nothing.
     */
    std::string dataKey;
    /**
     * The DDL stream's position, opaque here: the record's (ddl_ts, txn), big-endian (§4.3).
     * Empty when that stream produced nothing.
     */
    std::string ddlKey;
    /**
     * Form 1 only: the ordinal, in the blob, of the section of the last entry produced from the
     * record at 'dataKey'.
     */
    uint32_t section = 0;
    /** Form 1 only: the ordinal of that entry within its section. */
    uint32_t entry = 0;
};

/**
 * @brief A change-stream resume token.
 *
 * MongoDB's 4.0 ResumeToken is keyed on the collection UUID, which this design rules out (§5.4),
 * so only the wire shape drivers expect is kept: the token travels as {_data: <hex string>} and
 * is opaque to clients. The bytes are built only from immutable log
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
    /**
     * @brief What a token records a position after.
     */
    enum class Kind : uint8_t {
        /** After a specific event. Valid for resumeAfter and startAfter. */
        kEvent = 0,
        /** Everything through 'timestamp' has been consumed. Issued when the source drains. */
        kWatermark = 1,
        /**
         * After the watched collection's drop and the invalidate that followed it. Accepted by
         * startAfter only, as in MongoDB.
         */
        kInvalidate = 2,
    };

    /**
     * @brief The format version the token's first byte carries.
     *
     * constexpr, not const, as kDataFieldName: both are passed by reference (to str::stream and
     * the BSON builder), which odr-uses them, and only a constexpr static member is implicitly
     * inline.
     */
    static constexpr uint8_t kFormatVersion = 0x02;
    static constexpr StringData kDataFieldName = "_data"_sd;  ///< The wire field holding the hex.

    CdcResumeToken() = default;

    /**
     * @brief A token of the given kind.
     *
     * @param timestamp The event's timestamp, or emit_bound for a watermark.
     * @param kind What the token records a position after.
     * @param entries The groups' positions.
     */
    CdcResumeToken(uint64_t timestamp, Kind kind, std::vector<CdcResumeTokenEntry> entries)
        : _timestamp(timestamp), _kind(kind), _entries(std::move(entries)) {}

    /**
     * @brief A watermark; with no entries, what a stream issues before it has produced anything
     * (§5.1), from which resuming starts every group at 'timestamp' + 1.
     *
     * @param timestamp emit_bound.
     * @param entries The groups' emit cursors.
     * @return The token.
     */
    static CdcResumeToken makeWatermark(uint64_t timestamp,
                                        std::vector<CdcResumeTokenEntry> entries = {}) {
        return CdcResumeToken(timestamp, Kind::kWatermark, std::move(entries));
    }

    /** The token's timestamp, in microseconds. */
    uint64_t timestamp() const {
        return _timestamp;
    }
    /** What it records a position after. */
    Kind kind() const {
        return _kind;
    }
    /** The groups' positions, in log-group order. */
    const std::vector<CdcResumeTokenEntry>& entries() const {
        return _entries;
    }

    /**
     * @brief The entry for a log group.
     *
     * No entry means that group had produced nothing since the stream started or last resumed.
     * Such a group is positioned from the token's timestamp; see startTimestampForAbsentGroup().
     * A group named in a token but no longer in the cluster is ignored instead.
     *
     * @param logGroupId The group.
     * @return The entry, or nullptr when the token has none.
     */
    const CdcResumeTokenEntry* entryFor(uint32_t logGroupId) const;

    /**
     * @brief Where a group with no entry restarts its scan.
     *
     * A watermark certifies its own timestamp consumed, so that group starts above it; an event
     * or invalidate token does not, because records at the event's timestamp from other groups
     * may not have been produced yet.
     *
     * @return The first timestamp to scan from, always at least 1.
     * @throws for a watermark at the maximum timestamp, whose successor would wrap to 0 and
     * silently restart the stream at the beginning of history, and for an event or invalidate
     * token at timestamp 0, which no change can have. decode() rejects both, so this only fires
     * on a token constructed in process.
     */
    uint64_t startTimestampForAbsentGroup() const;

    /**
     * @brief The raw token bytes, as laid out above.
     *
     * @return The bytes.
     */
    std::string encode() const;

    /**
     * @brief The driver-facing form, which is what an event's _id carries.
     *
     * @return {_data: <hex>}.
     */
    BSONObj toBSON() const;

    /**
     * @brief Decodes raw token bytes.
     *
     * @param bytes The bytes.
     * @return The token, or an error for bytes that are not a well-formed token.
     */
    static StatusWith<CdcResumeToken> decode(StringData bytes);

    /**
     * @brief Parses the driver-facing form; rejects anything else, as MongoDB does for its own
     * tokens.
     *
     * @param token {_data: <hex>}.
     * @return The token, or an error.
     */
    static StatusWith<CdcResumeToken> parse(const BSONObj& token);

private:
    uint64_t _timestamp = 0;
    Kind _kind = Kind::kWatermark;
    std::vector<CdcResumeTokenEntry> _entries;
};

}  // namespace mongo
