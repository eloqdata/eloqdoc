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

#include "mongo/db/modules/eloq/src/cdc/cdc_scan_source.h"

#include <algorithm>
#include <limits>

#include "mongo/base/error_codes.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

/**
 * The shortest long poll worth asking for. Below it a held scan cannot be told from an early
 * answer (timer and clock granularity on both sides), and reissuing such scans back to back would
 * be a tight loop whether the logger holds them or not; so, after a call's first pass, no scan
 * goes out with less time than this left, and a scan that asked for less never counts as held.
 */
const Milliseconds kShortestLongPoll{20};

/**
 * @brief Appends 'value' big-endian.
 *
 * @param[in,out] out The string to append to.
 * @param value The value.
 * @param bytes How many low-order bytes of 'value' to write.
 */
void appendBigEndian(std::string& out, uint64_t value, int bytes) {
    for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

}  // namespace

const txlog::CdcDataRecord& CdcSourceRecord::data() const {
    invariant(_stream == Stream::kData);
    return _page->data(_index);
}

const txlog::CdcDdlRecord& CdcSourceRecord::ddl() const {
    invariant(_stream == Stream::kDdl);
    return _page->ddl(_index);
}

std::string CdcSourceRecord::dataKey() const {
    const txlog::CdcDataRecord& record = data();
    std::string key;
    key.reserve(20);
    appendBigEndian(key, record.commit_ts(), 8);
    appendBigEndian(key, record.cc_ng_id(), 4);
    appendBigEndian(key, record.txn_number(), 8);
    return key;
}

std::string CdcSourceRecord::ddlKey() const {
    const txlog::CdcDdlRecord& record = ddl();
    std::string key;
    key.reserve(16);
    appendBigEndian(key, record.ddl_ts(), 8);
    appendBigEndian(key, record.txn_number(), 8);
    return key;
}

CdcScanSource::CdcScanSource(std::shared_ptr<CdcScanTransport> transport,
                             CdcScanSourceOptions options)
    : _options(options),
      _logGroupIds(transport->logGroupIds()),
      _waitObject(CdcWaitObject::make()),
      _client(transport, _waitObject, _logGroupIds, options.client) {
    invariant(!_logGroupIds.empty());
    _groups.resize(_logGroupIds.size());
    for (size_t i = 0; i < _groups.size(); ++i) {
        _groups[i].id = _logGroupIds[i];
        _groups[i].idleBackoff = _options.idleBackoffInitial;
    }
}

void CdcScanSource::startAllAt(uint64_t fromTs) {
    for (uint32_t id : _logGroupIds) {
        startAt(id, fromTs);
    }
}

void CdcScanSource::startAt(uint32_t logGroupId, uint64_t fromTs) {
    invariant(fromTs >= 1);
    Group& group = _group(logGroupId);
    invariant(group.pages.empty());
    group.started = true;
    group.haveContinuation = false;
    group.continuation.clear();
    group.fromTs = fromTs;
    // Nothing below the start belongs to this stream, so it counts as fetched.
    group.scannedTs = fromTs - 1;
}

boost::optional<uint64_t> CdcScanSource::fixStart(Date_t deadline, const CoroutineFunctors& coro) {
    for (const Group& group : _groups) {
        invariant(!group.started);
    }

    // Groups that have answered a probe with a zero bound. A probe starts at the maximum
    // timestamp, so it can never return a record, and a long poll would hold it until the bound
    // advances or the deadline; only a group known to be at zero is probed that way.
    std::vector<bool> answeredZero(_groups.size(), false);
    for (bool first = true;; first = false) {
        const Date_t now = Date_t::now();
        for (size_t i = 0; i < _groups.size(); ++i) {
            Group& group = _groups[i];
            // As in next(), no probe goes out with less than kShortestLongPoll left, except on
            // the first pass.
            if ((!first && deadline - now <= kShortestLongPoll) || group.arrivalCompleteTs != 0 ||
                _client.inFlight(group.id) || _client.readyAt(group.id) > now) {
                continue;
            }
            txlog::CdcScanRequest probe;
            probe.set_log_group_id(group.id);
            probe.set_from_ts(std::numeric_limits<uint64_t>::max());
            probe.set_max_bytes(1);
            probe.set_wait_ms(answeredZero[i] ? _waitMs(deadline) : 0);
            _client.issue(group.id, probe, Milliseconds(probe.wait_ms()) + _options.rpcSlack);
        }

        for (size_t i = 0; i < _groups.size(); ++i) {
            Group& group = _groups[i];
            boost::optional<CdcScanClient::Completion> completion = _client.takeCompleted(group.id);
            if (completion &&
                completion->status == CdcScanClient::Completion::Status::kUnsupported) {
                _failUnsupported(group.id);
            }
            if (!completion || completion->status != CdcScanClient::Completion::Status::kOk) {
                // A failed probe was scheduled for a retry by the client; a probe from the maximum
                // timestamp cannot be below a retention floor.
                continue;
            }
            const uint64_t previousBound = group.arrivalCompleteTs;
            group.arrivalCompleteTs =
                std::max(previousBound, completion->response->arrival_complete_ts());
            answeredZero[i] = group.arrivalCompleteTs == 0;
            _pauseIfIdleEarly(group, *completion, previousBound);
        }

        uint64_t start = std::numeric_limits<uint64_t>::max();
        for (const Group& group : _groups) {
            start = std::min(start, group.arrivalCompleteTs);
        }
        if (start != 0) {
            startAllAt(start + 1);
            return start;
        }

        if (Date_t::now() >= deadline) {
            return boost::none;
        }
        Date_t wakeAt = deadline;
        for (const Group& group : _groups) {
            if (group.arrivalCompleteTs == 0 && !_client.inFlight(group.id)) {
                wakeAt = std::min(wakeAt, _client.readyAt(group.id));
            }
        }
        _waitObject->waitUntil(wakeAt, coro);
    }
}

boost::optional<CdcSourceRecord> CdcScanSource::next(Date_t deadline,
                                                     const CoroutineFunctors& coro) {
    for (const Group& group : _groups) {
        invariant(group.started);
    }

    for (bool first = true;; first = false) {
        _collectCompletions();
        if (boost::optional<CdcSourceRecord> record = _pickReleasable()) {
            _drained = false;
            return record;
        }
        // Scans go out on every pass with more than kShortestLongPoll left, and on the first pass
        // even after the deadline, so that a call made with no time left still starts the fetch
        // the next call collects.
        if (first || deadline - Date_t::now() > kShortestLongPoll) {
            _issueScans(deadline);
            // A transport may complete a call before issue() returns.
            _collectCompletions();
            if (boost::optional<CdcSourceRecord> record = _pickReleasable()) {
                _drained = false;
                return record;
            }
        }
        if (Date_t::now() >= deadline) {
            _drained = true;
            return boost::none;
        }
        _waitObject->waitUntil(std::min(deadline, _earliestDeferredReady()), coro);
    }
}

void CdcScanSource::_failUnsupported(uint32_t logGroupId) {
    uasserted(ErrorCodes::CommandNotSupported,
              str::stream() << "$changeStream is not supported by this deployment: log group "
                            << logGroupId
                            << " keeps no history a change stream can read (the MEMORY log "
                               "state); use the RocksDB or RocksDB-Cloud log state");
}

uint64_t CdcScanSource::emitBound() const {
    uint64_t bound = std::numeric_limits<uint64_t>::max();
    for (const Group& group : _groups) {
        bound = std::min(bound, group.scannedTs);
    }
    return bound;
}

uint64_t CdcScanSource::scannedTs(uint32_t logGroupId) const {
    return _group(logGroupId).scannedTs;
}

uint64_t CdcScanSource::scansIssued(uint32_t logGroupId) const {
    return _client.scansIssued(logGroupId);
}

boost::optional<CdcScanSource::Head> CdcScanSource::_head(Group& group) {
    while (!group.pages.empty()) {
        const Page& page = group.pages.front();
        const txlog::CdcScanResponse& response = *page.response;
        const bool haveData = page.dataIndex < response.data_size();
        const bool haveDdl = page.ddlIndex < response.ddl_size();
        if (!haveData && !haveDdl) {
            _bufferedBytes -= page.bytes;
            group.pages.pop_front();
            continue;
        }
        const uint64_t dataTs = haveData ? response.data(page.dataIndex).commit_ts()
                                         : std::numeric_limits<uint64_t>::max();
        const uint64_t ddlTs =
            haveDdl ? response.ddl(page.ddlIndex).ddl_ts() : std::numeric_limits<uint64_t>::max();
        // Within a group the logger's order is data before DDL at one timestamp (§4.2).
        Head head;
        if (haveData && dataTs <= ddlTs) {
            head.ts = dataTs;
            head.stream = CdcSourceRecord::Stream::kData;
        } else {
            head.ts = ddlTs;
            head.stream = CdcSourceRecord::Stream::kDdl;
        }
        return head;
    }
    return boost::none;
}

boost::optional<CdcSourceRecord> CdcScanSource::_pickReleasable() {
    Group* chosen = nullptr;
    Head chosenHead;
    for (Group& group : _groups) {
        boost::optional<Head> head = _head(group);
        // Strictly lower wins, so a tie goes to the lower group id, the earlier in _groups.
        if (head && (!chosen || head->ts < chosenHead.ts)) {
            chosen = &group;
            chosenHead = *head;
        }
    }
    if (!chosen) {
        return boost::none;
    }
    // Emission rule: every other group must have fetched everything below this timestamp.
    for (const Group& group : _groups) {
        if (&group != chosen && group.scannedTs + 1 < chosenHead.ts) {
            return boost::none;
        }
    }
    Page& page = chosen->pages.front();
    if (chosenHead.stream == CdcSourceRecord::Stream::kData) {
        return CdcSourceRecord(
            chosen->id, page.response, CdcSourceRecord::Stream::kData, page.dataIndex++);
    }
    return CdcSourceRecord(
        chosen->id, page.response, CdcSourceRecord::Stream::kDdl, page.ddlIndex++);
}

void CdcScanSource::_collectCompletions() {
    for (Group& group : _groups) {
        boost::optional<CdcScanClient::Completion> completion = _client.takeCompleted(group.id);
        if (!completion) {
            continue;
        }
        switch (completion->status) {
            case CdcScanClient::Completion::Status::kOk:
                _admit(group, *completion);
                break;
            case CdcScanClient::Completion::Status::kRetry:
                // The client has scheduled the retry; the position has not moved.
                break;
            case CdcScanClient::Completion::Status::kPositionTruncated:
                uasserted(ErrorCodes::ChangeStreamHistoryLost,
                          str::stream() << "log group " << group.id
                                        << " no longer retains the change stream's position");
            case CdcScanClient::Completion::Status::kUnsupported:
                _failUnsupported(group.id);
        }
    }
}

void CdcScanSource::_admit(Group& group, const CdcScanClient::Completion& completion) {
    const txlog::CdcScanResponse& response = *completion.response;
    const uint64_t previousBound = group.arrivalCompleteTs;
    group.arrivalCompleteTs = std::max(previousBound, response.arrival_complete_ts());
    group.continuation = response.continuation();
    group.haveContinuation = true;

    if (response.has_more()) {
        // The page may have been cut inside its last timestamp, so only what lies below it is
        // known to be fetched.
        uint64_t lastTs = 0;
        if (response.data_size() > 0) {
            lastTs = response.data(response.data_size() - 1).commit_ts();
        }
        if (response.ddl_size() > 0) {
            lastTs = std::max(lastTs, response.ddl(response.ddl_size() - 1).ddl_ts());
        }
        if (lastTs > 0) {
            group.scannedTs = std::max(group.scannedTs, lastTs - 1);
        }
    } else {
        // Exhausted through the bound: an empty page proves the interval empty.
        group.scannedTs = std::max(group.scannedTs, response.arrival_complete_ts());
    }

    if (response.data_size() > 0 || response.ddl_size() > 0) {
        Page page;
        page.response = completion.response;
        page.bytes = static_cast<int64_t>(response.ByteSizeLong());
        _bufferedBytes += page.bytes;
        group.pages.push_back(std::move(page));
    }

    _pauseIfIdleEarly(group, completion, previousBound);
}

bool CdcScanSource::_pauseIfIdleEarly(Group& group,
                                      const CdcScanClient::Completion& completion,
                                      uint64_t previousBound) {
    const txlog::CdcScanResponse& response = *completion.response;
    const bool empty = response.data_size() == 0 && response.ddl_size() == 0;
    if (!empty || response.has_more() || group.arrivalCompleteTs > previousBound) {
        group.idleBackoff = _options.idleBackoffInitial;
        return false;
    }
    // Held: the logger kept the scan for at least half its wait, so reissuing at once is another
    // long poll, not a loop. A wait under kShortestLongPoll, zero included, never counts as held.
    const Milliseconds asked(completion.request.wait_ms());
    const Milliseconds waited = completion.completedAt - completion.issuedAt;
    if (asked >= kShortestLongPoll && waited * 2 >= asked) {
        return false;
    }
    _client.deferUntil(group.id, Date_t::now() + group.idleBackoff);
    group.idleBackoff = std::min(group.idleBackoff * 2, _options.idleBackoffMax);
    return true;
}

void CdcScanSource::_issueScans(Date_t deadline) {
    const Date_t now = Date_t::now();
    boost::optional<uint64_t> lowestBuffered;
    std::vector<bool> haveBuffered(_groups.size(), false);
    for (size_t i = 0; i < _groups.size(); ++i) {
        if (boost::optional<Head> head = _head(_groups[i])) {
            haveBuffered[i] = true;
            lowestBuffered = lowestBuffered ? std::min(*lowestBuffered, head->ts) : head->ts;
        }
    }

    for (size_t i = 0; i < _groups.size(); ++i) {
        Group& group = _groups[i];
        if (_client.inFlight(group.id) || _client.readyAt(group.id) > now) {
            continue;
        }
        // A group that lags blocks the release of every other group's records above its
        // boundary, so a non-empty buffer does not stop it from being fetched.
        const bool lagging = lowestBuffered && group.scannedTs + 1 < *lowestBuffered;
        if (haveBuffered[i] && !lagging) {
            continue;
        }
        const int64_t committed = _bufferedBytes +
            static_cast<int64_t>(_client.inFlightCount()) * _options.maxBytesPerScan;
        if (!lagging && committed >= _options.bufferBytes) {
            continue;
        }
        const txlog::CdcScanRequest request = _scanRequest(group, deadline);
        _client.issue(group.id, request, Milliseconds(request.wait_ms()) + _options.rpcSlack);
    }
}

txlog::CdcScanRequest CdcScanSource::_scanRequest(const Group& group, Date_t deadline) const {
    txlog::CdcScanRequest request;
    request.set_log_group_id(group.id);
    if (group.haveContinuation) {
        request.set_continuation(group.continuation);
    } else {
        request.set_from_ts(group.fromTs);
    }
    request.set_max_bytes(_options.maxBytesPerScan);
    request.set_wait_ms(_waitMs(deadline));
    return request;
}

uint32_t CdcScanSource::_waitMs(Date_t deadline) const {
    const Milliseconds remaining = deadline - Date_t::now();
    if (remaining <= Milliseconds(0)) {
        return 0;
    }
    return static_cast<uint32_t>(
        durationCount<Milliseconds>(std::min(remaining, _options.maxWait)));
}

Date_t CdcScanSource::_earliestDeferredReady() const {
    const Date_t now = Date_t::now();
    Date_t earliest = Date_t::max();
    for (const Group& group : _groups) {
        const Date_t ready = _client.readyAt(group.id);
        if (!_client.inFlight(group.id) && ready > now) {
            earliest = std::min(earliest, ready);
        }
    }
    return earliest;
}

CdcScanSource::Group& CdcScanSource::_group(uint32_t logGroupId) {
    for (Group& group : _groups) {
        if (group.id == logGroupId) {
            return group;
        }
    }
    MONGO_UNREACHABLE;
}

const CdcScanSource::Group& CdcScanSource::_group(uint32_t logGroupId) const {
    for (const Group& group : _groups) {
        if (group.id == logGroupId) {
            return group;
        }
    }
    MONGO_UNREACHABLE;
}

}  // namespace mongo
