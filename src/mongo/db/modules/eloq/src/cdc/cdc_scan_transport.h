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
 * The CDC scan path's view of the log service: the interface the scan client calls, and its
 * implementation over the tx service's log agent.
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/txlog.h"

namespace mongo {

/**
 * @brief How the scan client reaches the log groups.
 *
 * The TxLog-backed implementation sends CdcScan RPCs to the log groups' leaders; the scan path's
 * tests use a scripted log service in its place (plan F1). Implementations are thread-safe: scans
 * of different groups, and callbacks, run concurrently.
 */
class CdcScanTransport {
public:
    virtual ~CdcScanTransport() = default;

    /**
     * @brief The cluster's log groups, fixed for the cluster's lifetime (§5.1).
     *
     * @return Their ids, ascending.
     */
    virtual std::vector<uint32_t> logGroupIds() const = 0;

    /**
     * @brief Sends a CdcScan to the group's current leader, asynchronously.
     *
     * @param logGroupId The group to scan.
     * @param cntl The call's controller; its timeout bounds the call.
     * @param request The scan request.
     * @param[out] response The scan response.
     * @param[out] targetNode Set, before 'done' runs, to the node ID the call went to, or left
     * empty when no leader was known; the client compares it with the node ID of a NotLeader
     * redirect.
     * @param done Run exactly once when the call completes, possibly before this returns.
     */
    virtual void scan(uint32_t logGroupId,
                      brpc::Controller* cntl,
                      const txlog::CdcScanRequest& request,
                      txlog::CdcScanResponse* response,
                      std::optional<uint32_t>* targetNode,
                      google::protobuf::Closure* done) = 0;

    /**
     * @brief The node ID configured with a NotLeader redirect's address in the group.
     *
     * Redirects are compared by node ID, not by address: the log service may format one as a
     * hostname or as an IP address, whichever its raft configuration uses.
     *
     * @param logGroupId The group.
     * @param address The redirect's "host:port".
     * @return The node ID, or empty if no node ID of the group is configured with that address.
     */
    virtual std::optional<uint32_t> nodeByAddress(uint32_t logGroupId,
                                                  const std::string& address) = 0;

    /**
     * @brief Makes the node ID the group's leader for later scans.
     *
     * @param logGroupId The group.
     * @param nodeId A node ID, as nodeByAddress() returned it.
     */
    virtual void followNode(uint32_t logGroupId, uint32_t nodeId) = 0;

    /**
     * @brief Asks for the group's leader to be refreshed in the background; returns at once.
     *
     * @param logGroupId The group.
     */
    virtual void requestLeaderRefresh(uint32_t logGroupId) = 0;

    /**
     * @brief Starts cancelling a call issued through scan(); returns at once, never waits.
     *
     * The call's closure still runs exactly once, now or later, reporting a failure unless the
     * call had already completed. A no-op for a completed call.
     *
     * @param cntl The controller the call was issued with.
     */
    virtual void cancel(brpc::Controller* cntl) = 0;
};

/**
 * @brief The transport over the tx service's log agent (Sharder::GetLogAgent()), which keeps the
 * leader cache shared with the tx service's own log calls.
 */
class TxLogCdcScanTransport : public CdcScanTransport {
public:
    /**
     * @brief Wraps a log agent.
     *
     * @param log The tx service's log agent; must outlive this transport.
     */
    explicit TxLogCdcScanTransport(txservice::TxLog* log);

    std::vector<uint32_t> logGroupIds() const override;

    void scan(uint32_t logGroupId,
              brpc::Controller* cntl,
              const txlog::CdcScanRequest& request,
              txlog::CdcScanResponse* response,
              std::optional<uint32_t>* targetNode,
              google::protobuf::Closure* done) override;

    std::optional<uint32_t> nodeByAddress(uint32_t logGroupId, const std::string& address) override;

    void followNode(uint32_t logGroupId, uint32_t nodeId) override;

    void requestLeaderRefresh(uint32_t logGroupId) override;

    void cancel(brpc::Controller* cntl) override;

private:
    txservice::TxLog* const _log;
};

}  // namespace mongo
