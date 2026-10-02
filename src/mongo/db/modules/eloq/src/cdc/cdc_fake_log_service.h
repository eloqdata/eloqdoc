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
 * A scripted log service for the CDC scan path's unit tests (plan F1).
 */

#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "mongo/db/modules/eloq/src/cdc/cdc_scan_transport.h"

namespace mongo {

/**
 * @brief A CdcScanTransport that serves CdcScan from records a test scripts, with the logger's
 * paging contract (log.proto, docs/cdc-change-stream.md §4.2).
 *
 * Each group holds a data stream and a DDL stream, keyed as the logger keys them (a DDL record by
 * its position, big-endian ddl_ts and txn_number, §4.3), and a bound, its arrival_complete_ts. A
 * scan returns the records above the request's position and at or below the bound, as a prefix of
 * the two streams merged by timestamp with data first at one timestamp, cut at max_bytes but never
 * empty while records remain; the continuation has the logger's two-key layout.
 *
 * A test can script failures ahead of the page (a transport error, or NotLeader with a redirect),
 * give each node ID of the group one or more addresses (a hostname and an IP address, say) and
 * pick its leader, and choose per group how a call completes:
 * inline, before scan() returns; on another thread; or held until releaseHeld(), which answers
 * from the script as it is then, the way a parked long poll answers once the bound moves.
 * Nothing honours wait_ms otherwise: an unheld scan answers at once, like a logger without the
 * long poll.
 *
 * Thread-safe.
 */
class CdcFakeLogService : public CdcScanTransport {
public:
    /** How a group's calls complete. */
    enum class Completion {
        kInline,  ///< Answered before scan() returns.
        kAsync,   ///< Answered on another thread shortly after.
        kHeld,    ///< Answered by releaseHeld(), or failed by cancel().
    };

    /**
     * @brief Creates a service with the given groups, each with no records and bound 0.
     *
     * @param logGroupIds The groups, ascending.
     */
    explicit CdcFakeLogService(std::vector<uint32_t> logGroupIds);

    /**
     * @brief Joins the answering threads, and fails every call still held so that each closure
     * runs exactly once.
     */
    ~CdcFakeLogService() override;

    /**
     * @brief The logger's data-stream key: big-endian commit_ts, cc_ng_id and txn_number.
     *
     * @param commitTs The commit timestamp.
     * @param ngId The node group.
     * @param txn The transaction number.
     * @return The key.
     */
    static std::string dataKey(uint64_t commitTs, uint32_t ngId, uint64_t txn);

    /**
     * @brief Adds a data record to the group.
     *
     * @param logGroupId The group.
     * @param commitTs Its commit timestamp.
     * @param ngId Its node group.
     * @param txn Its transaction number.
     * @param blobBytes The size of its blob, which is filled with a pattern.
     */
    void addData(
        uint32_t logGroupId, uint64_t commitTs, uint32_t ngId, uint64_t txn, size_t blobBytes = 16);

    /**
     * @brief Adds a data record carrying the given blob, a node group's redo-log record as the
     * tx service writes it.
     *
     * @param logGroupId The group.
     * @param commitTs Its commit timestamp.
     * @param ngId Its node group.
     * @param txn Its transaction number.
     * @param blob The blob.
     */
    void addDataBlob(
        uint32_t logGroupId, uint64_t commitTs, uint32_t ngId, uint64_t txn, std::string blob);

    /**
     * @brief Adds a DDL record with no schema op, which no stream reports, to the group.
     *
     * @param logGroupId The group.
     * @param ddlTs Its stream timestamp.
     * @param txn Its transaction number, which orders DDL records within one timestamp.
     */
    void addDdl(uint32_t logGroupId, uint64_t ddlTs, uint64_t txn);

    /**
     * @brief Adds a DDL record carrying a schema op to the group.
     *
     * @param logGroupId The group.
     * @param ddlTs Its stream timestamp.
     * @param txn Its transaction number, which orders DDL records within one timestamp.
     * @param schemaOp The op, as the log state keeps it in the record.
     */
    void addDdl(uint32_t logGroupId, uint64_t ddlTs, uint64_t txn, txlog::SchemaOpMessage schemaOp);

    /**
     * @brief A DDL record's position, as CdcSourceRecord::ddlKey() rebuilds it.
     *
     * @param ddlTs Its stream timestamp.
     * @param txn Its transaction number.
     * @return be64 ddl_ts | be64 txn.
     */
    static std::string ddlKey(uint64_t ddlTs, uint64_t txn);

    /**
     * @brief Sets the group's arrival_complete_ts.
     *
     * @param logGroupId The group.
     * @param bound The new bound.
     */
    void setBound(uint32_t logGroupId, uint64_t bound);

    /**
     * @brief Makes a scan whose position is at or below 'floor' answer PositionTruncated.
     *
     * @param logGroupId The group.
     * @param floor The retention floor; 0 turns the check off.
     */
    void setRetentionFloor(uint32_t logGroupId, uint64_t floor);

    /**
     * @brief Makes the group's next answer a transport error.
     *
     * @param logGroupId The group.
     */
    void failNext(uint32_t logGroupId);

    /**
     * @brief Makes every later answer of the group fail with the given brpc error code, as the
     * MEMORY log state answers ENOMETHOD.
     *
     * @param logGroupId The group.
     * @param errorCode The code; 0 answers normally again.
     */
    void setFailureCode(uint32_t logGroupId, int errorCode);

    /**
     * @brief Makes the group's next answer NotLeader with the given redirect.
     *
     * @param logGroupId The group.
     * @param redirect The redirect, possibly empty.
     */
    void notLeaderNext(uint32_t logGroupId, std::string redirect);

    /**
     * @brief Adds a node ID to the group, known by each of the given addresses.
     *
     * Every group starts with node ID 0, known as "log-<group>-a:9000", as its leader.
     *
     * @param logGroupId The group.
     * @param nodeId The node ID.
     * @param addresses Its "host:port" forms, for instance a hostname and an IP address.
     */
    void addNode(uint32_t logGroupId, uint32_t nodeId, std::vector<std::string> addresses);

    /**
     * @brief Makes the node ID the group's leader, which later scans go to.
     *
     * @param logGroupId The group.
     * @param nodeId The node ID; added before.
     */
    void setLeader(uint32_t logGroupId, uint32_t nodeId);

    /**
     * @brief Sets how the group's later calls complete.
     *
     * @param logGroupId The group.
     * @param mode The completion mode.
     */
    void setCompletion(uint32_t logGroupId, Completion mode);

    /**
     * @brief Sets whether cancel() fails a held call at once, as brpc does, or leaves it held, to
     * test a callback that runs after its client is gone.
     *
     * @param runsDone true to fail the call at once (the default).
     */
    void setCancelRunsDone(bool runsDone);

    /**
     * @brief How many of the group's calls are held.
     *
     * @param logGroupId The group.
     * @return The count.
     */
    size_t heldCount(uint32_t logGroupId) const;

    /**
     * @brief Answers every held call of the group from the script as it is now.
     *
     * @param logGroupId The group.
     */
    void releaseHeld(uint32_t logGroupId);

    /**
     * @brief The group's scan requests so far, in order.
     *
     * @param logGroupId The group.
     * @return Copies of the requests.
     */
    std::vector<txlog::CdcScanRequest> requests(uint32_t logGroupId) const;

    /**
     * @brief The node ID each of the group's scans went to, in order.
     *
     * @param logGroupId The group.
     * @return The node IDs.
     */
    std::vector<uint32_t> targets(uint32_t logGroupId) const;

    /**
     * @brief The node IDs followNode() made the group's leader, in order.
     *
     * @param logGroupId The group.
     * @return The node IDs.
     */
    std::vector<uint32_t> nodesFollowed(uint32_t logGroupId) const;

    /**
     * @brief How many leader refreshes were requested for the group.
     *
     * @param logGroupId The group.
     * @return The count.
     */
    int refreshCount(uint32_t logGroupId) const;

    /**
     * @brief How many calls cancel() was asked about.
     *
     * @return The count.
     */
    int cancelCount() const;

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
    /** A scripted answer that precedes the page. */
    struct Action {
        bool fail = false;     ///< A transport error; otherwise NotLeader.
        std::string redirect;  ///< The NotLeader redirect.
    };

    /** A call that has not been answered. */
    struct PendingCall {
        brpc::Controller* cntl = nullptr;
        const txlog::CdcScanRequest* request = nullptr;
        txlog::CdcScanResponse* response = nullptr;
        google::protobuf::Closure* done = nullptr;
    };

    /** One group's script and observations. */
    struct Group {
        std::map<std::string, txlog::CdcDataRecord> data;  ///< By storage key.
        std::map<std::string, txlog::CdcDdlRecord> ddl;    ///< By storage key.
        uint64_t bound = 0;
        uint64_t floor = 0;
        std::deque<Action> actions;
        std::map<uint32_t, std::set<std::string>> nodes;  ///< Addresses by node ID.
        uint32_t leader = 0;
        int failureCode = 0;  ///< Every answer fails with this brpc code while it is set.
        Completion mode = Completion::kInline;
        std::vector<PendingCall> held;
        std::vector<txlog::CdcScanRequest> requests;
        std::vector<uint32_t> targets;
        std::vector<uint32_t> nodesFollowed;
        int refreshes = 0;
    };

    /**
     * @brief Fills in the call's answer from the group's script; does not run its closure.
     *
     * @param lk The held lock on _mutex.
     * @param group The group.
     * @param call The call.
     */
    void _answer(const std::lock_guard<std::mutex>& lk, Group& group, const PendingCall& call);

    /**
     * @brief The group; must be one of the service's.
     *
     * @param logGroupId The group's id.
     * @return Its state.
     */
    Group& _group(uint32_t logGroupId);
    const Group& _group(uint32_t logGroupId) const;

    const std::vector<uint32_t> _logGroupIds;
    mutable std::mutex _mutex;
    std::map<uint32_t, Group> _groups;
    bool _cancelRunsDone = true;
    int _cancels = 0;
    std::vector<std::thread> _threads;  ///< Answering threads of kAsync calls.
};

}  // namespace mongo
