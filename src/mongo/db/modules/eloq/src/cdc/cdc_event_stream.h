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
 * The change stream's event producer: decodes the records the scan source releases and turns
 * each logged change into an event document whose _id is its resume token
 * (docs/cdc-change-stream.md §5.3 step 3, §5.4, §5.4a, §5.5, §5.6; plan C6, C7, C8a, C8b).
 */

#pragma once

#include <boost/optional.hpp>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/modules/eloq/src/base/cdc_blob_decoder.h"
#include "mongo/db/modules/eloq/src/base/cdc_resume_token.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_scan_source.h"
#include "mongo/db/pipeline/document.h"

namespace mongo {

/**
 * @brief Limits of a CdcEventStream; the defaults are the design's.
 */
struct CdcEventStreamOptions {
    /** Limits and timing of the underlying scan source. */
    CdcScanSourceOptions source;
    /**
     * Bytes of (txn, namespace, key) entries a group's dedup set holds at one timestamp before
     * further entries go unrecorded (cdc_dedup_set_bytes, §5.8).
     */
    int64_t dedupSetBytes = 4 * 1024 * 1024;
};

/**
 * @brief Produces a change stream's events one per pull, in decode order, each with the resume
 * token valid once it is delivered as its _id.
 *
 * Records come from a CdcScanSource, merged across log groups by timestamp. Each is decoded on
 * its own, in storage order (§5.4): a data record's in-scope document changes become insert,
 * replace and delete events (§5.4a). DDL records produce no event yet (plan C7's DDL part, with
 * A3); their position is still tracked, so tokens stay exact once they do.
 *
 * Positions (§5.6). Per log group the stream keeps an emit cursor: the key of the last record it
 * finished (a form-0 entry: resume after it), or, while a record is being produced, that record's
 * key and the (section, entry) of the last change produced from it (a form-1 entry). A group has
 * a cursor once it has produced something since the stream opened, or when the resume token
 * named it. An event's token has the event's timestamp, kind kEvent, and one entry per group with
 * a cursor; the watermark has emit_bound's timestamp, kind kWatermark and the same entries.
 * Because only one record is decoded at a time, only the group it belongs to can be in form 1.
 *
 * Dedup (§5.5). During bucket migration one transaction's entry can be logged in two records of
 * the same transaction, at the same timestamp in the same group; per group the stream remembers
 * the (txn, namespace, key) entries produced at the group's current timestamp and suppresses a
 * repeat. Past the byte limit entries go unrecorded, and a repeat among them is produced twice,
 * which the consumer absorbs as an idempotent re-application.
 *
 * Resume (§5.6). A group named in the token restarts at T_K, the later of its two keys'
 * timestamps, and records at T_K up to the entry are consumed without producing: records at or
 * below a form-0 key, or, for a form-1 entry, the named record's changes through
 * (section, entry). At one timestamp every data record precedes every DDL record, so when the DDL
 * key is at T_K every data record at T_K is consumed, and when only the data key is, no DDL record
 * at T_K is. Consumed changes go into the dedup set. A group not named in the token restarts where
 * CdcResumeToken::startTimestampForAbsentGroup() says.
 *
 * Not thread-safe: one cursor drives it, one getMore at a time.
 */
class CdcEventStream {
public:
    /**
     * @brief Creates a stream over every log group the transport reports, before it is opened.
     *
     * @param transport How to reach the log groups.
     * @param scope Which namespaces the stream covers.
     * @param options Limits.
     */
    CdcEventStream(std::shared_ptr<CdcScanTransport> transport,
                   CdcStreamScope scope,
                   CdcEventStreamOptions options = {});

    CdcEventStream(const CdcEventStream&) = delete;
    CdcEventStream& operator=(const CdcEventStream&) = delete;

    /**
     * @brief Opens a stream without a token (§5.1): fixes the start S and starts every group
     * above it, so that only later changes are reported.
     *
     * @param deadline When to give up if some group still reports no bound.
     * @param coro The calling coroutine's functors.
     * @return true if the stream is open; false at the deadline, when no start could be fixed.
     */
    bool openFresh(Date_t deadline, const CoroutineFunctors& coro);

    /**
     * @brief Opens a stream from a resume token (§5.6); never waits.
     *
     * @param token An event or watermark token.
     * @throws ChangeStreamFatalError for an invalidate token, which needs collection-level
     * invalidation (plan C10).
     */
    void openAt(const CdcResumeToken& token);

    /**
     * @brief The next event document in pull order.
     *
     * Follows CdcScanSource::next()'s deadline contract: the deadline bounds the whole batch.
     *
     * @param deadline When to stop waiting.
     * @param coro The calling coroutine's functors.
     * @return The event, or none at the deadline, when the stream is drained.
     */
    boost::optional<Document> next(Date_t deadline, const CoroutineFunctors& coro);

    /**
     * @brief The token the opening reply carries: the watermark for S, or the token the stream was
     * opened from, as the client sent it.
     *
     * @return {_data: <hex>}; empty before the stream is open.
     */
    const BSONObj& openingToken() const {
        return _openingToken;
    }

    /**
     * @brief Whether the last next() returned none, so that watermark() is valid.
     *
     * @return true when drained.
     */
    bool drained() const {
        return _source.drained();
    }

    /**
     * @brief The watermark: emit_bound, with the emit cursor of every group that is beyond it.
     *
     * @return The token; valid once drained().
     */
    CdcResumeToken watermark() const;

    /**
     * @brief How many repeated entries the dedup sets suppressed; for tests and metrics.
     *
     * @return The count.
     */
    uint64_t duplicatesSuppressed() const {
        return _duplicatesSuppressed;
    }

    /**
     * @brief How many entries went unrecorded because a dedup set was full
     * (cdc_dedup_set_overflow, §8).
     *
     * @return The count.
     */
    uint64_t dedupOverflows() const {
        return _dedupOverflows;
    }

    /**
     * @brief The underlying source; for tests and metrics.
     *
     * @return The source.
     */
    const CdcScanSource& source() const {
        return _source;
    }

private:
    /**
     * @brief A group's resume point from the token, until the group moves past T_K.
     */
    struct ResumeSkip {
        uint64_t ts = 0;          ///< T_K.
        std::string dataKey;      ///< The entry's data-stream key.
        std::string ddlKey;       ///< The entry's DDL-stream key.
        bool dataAtTs = false;    ///< The data key is at T_K (else every data record at T_K is
                                  ///< consumed).
        bool ddlAtTs = false;     ///< The DDL key is at T_K (else no DDL record at T_K is).
        bool atPosition = false;  ///< Form 1: the record at dataKey is consumed through
                                  ///< (section, entry) only.
        uint32_t section = 0;
        uint32_t entry = 0;
    };

    /**
     * @brief One log group's emit cursor, dedup set and pending resume skip.
     */
    struct Group {
        bool hasCursor = false;  ///< The group has produced something or was named in the token.
        std::string dataKey;     ///< Last data record finished, or the one being produced.
        std::string ddlKey;      ///< Last DDL record finished.
        bool partial = false;    ///< The record at dataKey is being produced (form 1).
        uint32_t section = 0;    ///< Form 1: the last change produced from it.
        uint32_t entry = 0;
        uint64_t dedupTs = 0;                   ///< The timestamp the dedup set belongs to.
        std::unordered_set<std::string> dedup;  ///< (txn, namespace, key) entries produced at it.
        int64_t dedupBytes = 0;
        boost::optional<ResumeSkip> skip;
    };

    /**
     * @brief The data record being decoded.
     */
    struct Current {
        /**
         * @brief Starts decoding a data record.
         *
         * @param r The record.
         */
        explicit Current(CdcSourceRecord r) : record(std::move(r)) {}

        CdcSourceRecord record;  ///< Keeps the page, and so the blob, alive.
        std::string key;         ///< Its data key.
        uint64_t ts = 0;
        uint64_t txn = 0;
        std::unique_ptr<CdcBlobDecoder> decoder;
        /** Form-1 resume: changes up to and including this position are consumed. */
        boost::optional<std::pair<uint32_t, uint32_t>> consumeThrough;
    };

    /**
     * @brief Starts decoding a record the source released, or accounts for it when it produces
     * nothing.
     *
     * @param record The record.
     */
    void _begin(CdcSourceRecord record);

    /**
     * @brief Records a change in its group's dedup set.
     *
     * @param group The group.
     * @param ts The change's timestamp.
     * @param dedupKey Its (txn, namespace, key) bytes.
     * @return true if the change was already produced at that timestamp.
     */
    bool _seen(Group& group, uint64_t ts, const std::string& dedupKey);

    /**
     * @brief The event document for a change, with its token as _id.
     *
     * @param current The record the change came from.
     * @param entry The change.
     * @return The document.
     */
    Document _event(const Current& current, const CdcBlobDecoder::Entry& entry);

    /**
     * @brief The token entries for every group whose cursor lies beyond what the token's own
     * timestamp already covers.
     *
     * A group left out resumes by the absent-group rule (CdcResumeToken::
     * startTimestampForAbsentGroup), which is exact for it; its key would only send the resume
     * below the token, into history the log may have purged although the token covers it.
     *
     * @param coveredThrough The last timestamp the token certifies every group consumed through:
     * a watermark's own timestamp, or one below an event's.
     * @return The entries, in log-group order.
     */
    std::vector<CdcResumeTokenEntry> _entries(uint64_t coveredThrough) const;

    CdcScanSource _source;
    const CdcStreamScope _scope;
    const CdcEventStreamOptions _options;
    std::map<uint32_t, Group> _groups;
    boost::optional<Current> _current;
    BSONObj _openingToken;
    uint64_t _duplicatesSuppressed = 0;
    uint64_t _dedupOverflows = 0;
};

}  // namespace mongo
