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
 * The change stream's record source: fetches every log group's records and releases them one at
 * a time, merged by timestamp (docs/cdc-change-stream.md §4.2, §5.1, §5.3 steps 1-2, §5.8;
 * plan C5).
 */

#pragma once

#include <boost/optional.hpp>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/modules/eloq/src/cdc/cdc_scan_client.h"

namespace mongo {

/**
 * @brief Limits and timing of a CdcScanSource; the defaults are the design's.
 */
struct CdcScanSourceOptions {
    /// Received-and-unconsumed bytes plus max_bytes per scan in flight, above which only the
    /// lagging group is fetched (cdc_cursor_buffer_bytes, §5.8).
    int64_t bufferBytes = 64 * 1024 * 1024;
    /// A scan's soft page size (cdc_scan_max_bytes).
    uint32_t maxBytesPerScan = 4 * 1024 * 1024;
    /// Longest long poll requested of the log service (cdc_scan_max_wait_ms).
    Milliseconds maxWait{30000};
    /// Added to a scan's wait_ms for the RPC timeout.
    Milliseconds rpcSlack{5000};
    /// First pause before re-scanning a group that answered an empty page early.
    Milliseconds idleBackoffInitial{100};
    /// Cap on that pause, which doubles while the group stays idle.
    Milliseconds idleBackoffMax{1000};
    /// Retry timing for a leader that cannot serve.
    CdcScanClient::Options client;
};

/**
 * @brief A record the source released: a view into the page it came in, which it keeps alive.
 */
class CdcSourceRecord {
public:
    /// Which of the group's two streams the record belongs to.
    enum class Stream { kData, kDdl };

    /**
     * @brief Views one record of a page.
     *
     * @param logGroupId The group the page came from.
     * @param page The page.
     * @param stream The stream the record belongs to.
     * @param index The record's index in that stream's repeated field.
     */
    CdcSourceRecord(uint32_t logGroupId,
                    std::shared_ptr<const txlog::CdcScanResponse> page,
                    Stream stream,
                    int index)
        : _logGroupId(logGroupId), _page(std::move(page)), _stream(stream), _index(index) {}

    /// The log group the record came from.
    uint32_t logGroupId() const {
        return _logGroupId;
    }

    /// Which stream it belongs to.
    Stream stream() const {
        return _stream;
    }

    /// Its stream timestamp: commit_ts for data, ddl_ts for DDL.
    uint64_t timestamp() const {
        return _stream == Stream::kData ? data().commit_ts() : ddl().ddl_ts();
    }

    /**
     * @brief The data record.
     *
     * @return The record; stream() must be kData.
     */
    const txlog::CdcDataRecord& data() const;

    /**
     * @brief The DDL record.
     *
     * @return The record; stream() must be kDdl.
     */
    const txlog::CdcDdlRecord& ddl() const;

    /**
     * @brief The data record's key in the log's data store: big-endian commit_ts, cc_ng_id and
     * txn_number, which is what a resume token entry records for the data stream (§5.6).
     *
     * @return The key; stream() must be kData.
     */
    std::string dataKey() const;

private:
    uint32_t _logGroupId;
    std::shared_ptr<const txlog::CdcScanResponse> _page;
    Stream _stream;
    int _index;
};

/**
 * @brief Fetches every log group's records and releases them one at a time, merged by timestamp.
 *
 * Per group it keeps the scan position (the logger's continuation after the first page), a buffer
 * of fetched and unreleased records, and the completeness boundary scanned_ts: every record of the
 * group at or below it has been fetched. From each page, scanned_ts is the page's last timestamp
 * minus one if the page was cut (has_more), else the group's arrival_complete_ts (§4.2), so an
 * empty exhausted page advances it across idle time.
 *
 * next() releases, among the groups with a buffered record, the lowest-timestamped one (ties by
 * lowest group id), and only once every other group's scanned_ts is at least its timestamp minus
 * one (the emission rule, §4.2): no other group can still produce a record below it. emitBound()
 * is the minimum scanned_ts over the groups, the watermark's timestamp once next() has reported
 * the source drained.
 *
 * Fetching (§5.3 step 1): a group is scanned when it has no call in flight, is not held back, and
 * either has nothing buffered or lags, meaning its scanned_ts is below the lowest buffered
 * timestamp minus one, so that it blocks every other group's release. Scans stop while buffered
 * bytes plus max_bytes per call in flight reach the buffer cap, except for a lagging group, which
 * is always fetched. A scan asks the logger to hold it until the caller's deadline (at most
 * Options::maxWait). An empty exhausted page that leaves the group's bound where it was is
 * followed by a pause before that group is scanned again, unless the logger held the scan for at
 * least half its wait: the logger may answer early (a full list of parked requests, or no long
 * poll at all), and re-scanning at once would turn an idle stream into a tight loop against it. A
 * wait under 20 ms is too short to tell held from early, so it never counts as held, and after a
 * call's first pass no scan goes out with less than that left.
 *
 * Every group must be started, by startAllAt(), startAt() or fixStart(), before next(). Not
 * thread-safe: one cursor drives it, one getMore at a time; its RPC callbacks only go through the
 * client and the wait object.
 */
class CdcScanSource {
public:
    /**
     * @brief Creates a source over every log group the transport reports.
     *
     * @param transport How to reach the log groups.
     * @param options Limits and timing.
     */
    explicit CdcScanSource(std::shared_ptr<CdcScanTransport> transport,
                           CdcScanSourceOptions options = {});

    CdcScanSource(const CdcScanSource&) = delete;
    CdcScanSource& operator=(const CdcScanSource&) = delete;

    /**
     * @brief The log groups, captured at construction and fixed for the source's life (§5.1).
     *
     * @return Their ids, ascending.
     */
    const std::vector<uint32_t>& logGroupIds() const {
        return _logGroupIds;
    }

    /**
     * @brief Starts every group's scan at 'fromTs'.
     *
     * @param fromTs The first timestamp to deliver; must be at least 1.
     */
    void startAllAt(uint64_t fromTs);

    /**
     * @brief Starts one group's scan at 'fromTs', for a resume that positions groups apart.
     *
     * @param logGroupId The group.
     * @param fromTs The first timestamp to deliver for it; must be at least 1.
     */
    void startAt(uint32_t logGroupId, uint64_t fromTs);

    /**
     * @brief Fixes the start of a stream opened without a token (§5.1).
     *
     * Probes every group until each reports a non-zero arrival_complete_ts, re-probing a group that
     * reports zero (the cluster has not completed a checkpoint round) with a long poll, then
     * starts every group at S + 1, where S is the lowest bound. Must be called before any group
     * is started.
     *
     * @param deadline When to give up.
     * @param coro The calling coroutine's functors.
     * @return S, or none if some group still reported zero at the deadline.
     */
    boost::optional<uint64_t> fixStart(Date_t deadline, const CoroutineFunctors& coro);

    /**
     * @brief The next record in pull order.
     *
     * Waits, yielding through the wait object, until a record is releasable or 'deadline'. At the
     * deadline it returns none rather than throwing, and the source is drained: emitBound() is
     * then the watermark covering everything released so far. Even with the deadline already
     * past, a call takes what has arrived, releases what it can, and issues the scans that a
     * later call collects.
     *
     * The deadline bounds the whole batch, not one pull, and the stage (C1, C9) must pass the
     * same absolute time to every pull of a batch:
     * - for the aggregate's first batch, now: that batch returns at once with whatever is
     *   already releasable, possibly nothing but the watermark (§5.1; the opening wait is
     *   fixStart(), before the cursor is registered);
     * - for a getMore, the deadline of its maxTimeMS, the awaitData wait.
     * A per-pull timeout would make the aggregate wait, and could make a getMore wait once per
     * pull, up to batchSize (101 by default) times.
     *
     * @param deadline When to stop waiting: the batch's deadline, as above.
     * @param coro The calling coroutine's functors.
     * @return The record, or none at the deadline.
     * @throws ChangeStreamHistoryLost if a group's position is below its retention floor.
     */
    boost::optional<CdcSourceRecord> next(Date_t deadline, const CoroutineFunctors& coro);

    /**
     * @brief The minimum scanned_ts over the groups.
     *
     * @return The bound; everything at or below it has been released once drained() holds.
     */
    uint64_t emitBound() const;

    /**
     * @brief Whether the last next() returned none, so that emitBound() is a valid watermark.
     *
     * @return true when drained.
     */
    bool drained() const {
        return _drained;
    }

    /**
     * @brief The group's completeness boundary; for tests and metrics.
     *
     * @param logGroupId The group.
     * @return Its scanned_ts.
     */
    uint64_t scannedTs(uint32_t logGroupId) const;

    /**
     * @brief How many scans of the group have been issued, probes included; for tests and metrics.
     *
     * @param logGroupId The group.
     * @return The count.
     */
    uint64_t scansIssued(uint32_t logGroupId) const;

    /**
     * @brief Received and unreleased bytes, across groups; for tests and metrics.
     *
     * @return The byte count.
     */
    int64_t bufferedBytes() const {
        return _bufferedBytes;
    }

private:
    /**
     * @brief A fetched page and how far the group has released from it.
     */
    struct Page {
        std::shared_ptr<const txlog::CdcScanResponse> response;
        int dataIndex = 0;  ///< Next unreleased data record.
        int ddlIndex = 0;   ///< Next unreleased DDL record.
        int64_t bytes = 0;  ///< Counted against the buffer cap until the page is released.
    };

    /**
     * @brief One group's fetch state.
     */
    struct Group {
        uint32_t id = 0;
        bool started = false;
        bool haveContinuation = false;  ///< After the first page, scan from 'continuation'.
        std::string continuation;
        uint64_t fromTs = 0;             ///< Before the first page, scan from here.
        uint64_t arrivalCompleteTs = 0;  ///< The latest bound the group reported.
        uint64_t scannedTs = 0;          ///< Every record at or below this has been fetched.
        std::deque<Page> pages;          ///< Fetched, unreleased records.
        Milliseconds idleBackoff{0};     ///< The next pause after an early empty page.
    };

    /// A group's next unreleased record, if it has one.
    struct Head {
        uint64_t ts = 0;
        CdcSourceRecord::Stream stream = CdcSourceRecord::Stream::kData;
    };

    /**
     * @brief The group's next unreleased record; drops fully released pages on the way.
     *
     * @param group The group.
     * @return Its head, or none if nothing is buffered.
     */
    boost::optional<Head> _head(Group& group);

    /**
     * @brief Releases the lowest buffered record if the emission rule allows it.
     *
     * @return The record, or none.
     */
    boost::optional<CdcSourceRecord> _pickReleasable();

    /**
     * @brief Takes every completed scan and folds it into its group.
     */
    void _collectCompletions();

    /**
     * @brief Folds a page into its group: bound, scanned_ts, continuation, buffer, idle pause.
     *
     * @param group The group.
     * @param completion The completed scan.
     */
    void _admit(Group& group, const CdcScanClient::Completion& completion);

    /**
     * @brief Pauses the group's next scan if the page came back empty, exhausted and early, and
     * resets the pause once the group delivers or its bound moves.
     *
     * @param group The group.
     * @param completion The completed scan.
     * @param previousBound The group's bound before this page.
     * @return true if the group was paused.
     */
    bool _pauseIfIdleEarly(Group& group,
                           const CdcScanClient::Completion& completion,
                           uint64_t previousBound);

    /**
     * @brief Issues the scans step 1 calls for.
     *
     * @param deadline The caller's deadline, which bounds each scan's wait_ms.
     */
    void _issueScans(Date_t deadline);

    /**
     * @brief The request that scans the group from its position.
     *
     * @param group The group.
     * @param deadline Bounds the request's wait_ms.
     * @return The request.
     */
    txlog::CdcScanRequest _scanRequest(const Group& group, Date_t deadline) const;

    /**
     * @brief How long the logger may hold a scan issued now.
     *
     * @param deadline The caller's deadline.
     * @return wait_ms, clamped to [0, Options::maxWait].
     */
    uint32_t _waitMs(Date_t deadline) const;

    /**
     * @brief The earliest time a group that is held back becomes ready, if any is.
     *
     * @return The time, or Date_t::max().
     */
    Date_t _earliestDeferredReady() const;

    /**
     * @brief The group's state.
     *
     * @param logGroupId The group; must be one of the source's.
     * @return Its state.
     */
    Group& _group(uint32_t logGroupId);
    const Group& _group(uint32_t logGroupId) const;

    const CdcScanSourceOptions _options;
    const std::vector<uint32_t> _logGroupIds;
    const std::shared_ptr<CdcWaitObject> _waitObject;
    CdcScanClient _client;
    std::vector<Group> _groups;  ///< In _logGroupIds order.
    int64_t _bufferedBytes = 0;
    bool _drained = false;
};

}  // namespace mongo
