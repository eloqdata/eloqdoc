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
 * One change-stream cursor's CdcScan calls (docs/cdc-change-stream.md §5.3 step 1, §5.8;
 * plan C4).
 */

#pragma once

#include <boost/optional.hpp>
#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "mongo/db/modules/eloq/src/cdc/cdc_scan_transport.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_wait_object.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"

namespace mongo {

/**
 * @brief One cursor's CdcScan calls: at most one in flight per log group, each call's state
 * shared with its RPC callback, and the retry policy for a leader that cannot serve.
 *
 * Calls belong to the cursor, not to the getMore that issued them (§5.8). A call's state lives in
 * a shared_ptr that its callback also holds; the callback never touches a coroutine or this
 * object, it only marks the call complete and notifies the cursor's wait object, so a response
 * that lands after the getMore returned is simply there for the next one. The destructor cancels
 * every call still in flight and does not wait; a callback that runs afterwards finds the call
 * cancelled and drops it.
 *
 * A leader that cannot serve (NotLeader, or a transport error) is retried from the same position,
 * which only advances on a successful page:
 * - a NotLeader whose redirect names a different node ID is followed at once, unless
 *   the previous redirect was followed within one backoff period: then it is followed after the
 *   backoff, so that two nodes naming each other during an election are not bounced between at
 *   the speed of the RPC;
 * - one whose redirect is empty or names the node just asked (a leader not yet in sync, §4.6) is
 *   retried only after a backoff, never at once, so that the cursor does not loop against the log
 *   service;
 * - a transport error, or a redirect to an address the group does not know, asks for a leader
 *   refresh and backs off.
 * A redirect is compared with the call's target by node ID, looked up through the transport,
 * never as a string: the log service may name a node by hostname or by IP address.
 * The backoff doubles from Options::retryBackoffInitial to Options::retryBackoffMax and resets on a
 * successful page.
 *
 * Not thread-safe: one cursor drives it, one getMore at a time.
 */
class CdcScanClient {
public:
    /**
     * @brief Retry timing.
     */
    struct Options {
        Milliseconds retryBackoffInitial{50};  ///< First backoff after a failed scan.
        Milliseconds retryBackoffMax{1000};    ///< Cap on the doubling backoff.
    };

    /**
     * @brief What a completed scan yielded.
     */
    struct Completion {
        enum class Status {
            kOk,                 ///< 'response' holds a page.
            kRetry,              ///< Nothing usable; scan again from the same position once
                                 ///< readyAt() is reached.
            kPositionTruncated,  ///< The position is at or below the group's retention floor.
        };
        Status status = Status::kRetry;
        std::shared_ptr<const txlog::CdcScanResponse> response;  ///< Set for kOk.
        txlog::CdcScanRequest request;                           ///< The request the call carried.
        Date_t issuedAt;                                         ///< When the call was issued.
        Date_t completedAt;                                      ///< When its callback ran.
    };

    /**
     * @brief Creates a client for the given log groups.
     *
     * @param transport How to reach the log groups.
     * @param waitObject The cursor's wait object, notified by every completion.
     * @param logGroupIds The groups this client scans.
     * @param options Retry timing.
     */
    CdcScanClient(std::shared_ptr<CdcScanTransport> transport,
                  std::shared_ptr<CdcWaitObject> waitObject,
                  const std::vector<uint32_t>& logGroupIds,
                  Options options);

    CdcScanClient(const CdcScanClient&) = delete;
    CdcScanClient& operator=(const CdcScanClient&) = delete;

    /**
     * @brief Cancels every call still in flight; never waits for one.
     */
    ~CdcScanClient();

    /**
     * @brief Whether the group has a call that has been issued and not yet taken.
     *
     * @param logGroupId The group.
     * @return true if a call is in flight or completed but not taken.
     */
    bool inFlight(uint32_t logGroupId) const;

    /**
     * @brief The earliest time the group may be scanned again; in the past when it may be
     * scanned now.
     *
     * @param logGroupId The group.
     * @return The time.
     */
    Date_t readyAt(uint32_t logGroupId) const;

    /**
     * @brief Holds the group's next scan back until at least 'when'.
     *
     * @param logGroupId The group.
     * @param when The earliest time of the next scan.
     */
    void deferUntil(uint32_t logGroupId, Date_t when);

    /**
     * @brief Issues a scan of the group.
     *
     * @param logGroupId The group; must have no call in flight.
     * @param request The scan request.
     * @param rpcTimeout The call's timeout; above the request's wait_ms, which the log service
     * may hold the call for.
     */
    void issue(uint32_t logGroupId, const txlog::CdcScanRequest& request, Milliseconds rpcTimeout);

    /**
     * @brief Takes the group's completed call, if its callback has run.
     *
     * Applies the retry policy to a failed call before returning it as kRetry.
     *
     * @param logGroupId The group.
     * @return The completion, or none if no call is in flight or it has not completed.
     */
    boost::optional<Completion> takeCompleted(uint32_t logGroupId);

    /**
     * @brief How many scans of the group have been issued; for tests and metrics.
     *
     * @param logGroupId The group.
     * @return The count.
     */
    uint64_t scansIssued(uint32_t logGroupId) const;

    /**
     * @brief How many calls are in flight, across groups.
     *
     * @return The count.
     */
    size_t inFlightCount() const;

private:
    struct Call;

    /**
     * @brief A group's call slot and retry state.
     */
    struct GroupState {
        std::shared_ptr<Call> call;  ///< The call in flight, or null.
        Date_t readyAt;              ///< No scan before this.
        Milliseconds backoff{0};     ///< The next retry backoff.
        Date_t lastFollowAt;         ///< When a redirect was last followed, until a success.
        uint64_t scansIssued = 0;    ///< Scans issued so far.
    };

    /**
     * @brief Defers the group by its current backoff and doubles the backoff.
     *
     * @param group The group's state.
     */
    void _backOff(GroupState& group);

    /**
     * @brief The group's state; the group must be one this client was created for.
     *
     * @param logGroupId The group.
     * @return Its state.
     */
    GroupState& _group(uint32_t logGroupId);
    const GroupState& _group(uint32_t logGroupId) const;

    const std::shared_ptr<CdcScanTransport> _transport;
    const std::shared_ptr<CdcWaitObject> _waitObject;
    const Options _options;
    std::map<uint32_t, GroupState> _groups;
};

}  // namespace mongo
