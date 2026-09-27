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

#include "mongo/db/modules/eloq/src/cdc/cdc_scan_client.h"

#include <algorithm>
#include <mutex>
#include <optional>

#include "mongo/util/assert_util.h"

namespace mongo {

/**
 * @brief One call's state, shared by the client and the call's callback.
 *
 * The callback only sets 'completed' and notifies 'waitObject'; everything else is read by the
 * client once it sees 'completed' under the mutex, which orders the callback's writes (the
 * response, the controller's status, the target node ID) before the client's reads.
 */
struct CdcScanClient::Call {
    std::mutex mutex;
    bool completed = false;  ///< The callback ran.
    bool cancelled = false;  ///< The client is gone; the callback notifies nobody.
    brpc::Controller cntl;
    txlog::CdcScanRequest request;
    std::shared_ptr<txlog::CdcScanResponse> response = std::make_shared<txlog::CdcScanResponse>();
    std::optional<uint32_t> targetNode;  ///< The node ID the call went to, set before the callback.
    Date_t issuedAt;
    Date_t completedAt;
    std::shared_ptr<CdcWaitObject> waitObject;
};

namespace {

/**
 * @brief The closure a scan call runs once: marks the call complete and wakes the cursor, unless
 * the cursor has cancelled it. Holds the call alive until then, and deletes itself.
 */
template <typename CallT>
class ScanDone : public google::protobuf::Closure {
public:
    /**
     * @brief Binds the closure to its call.
     *
     * @param call The call's shared state.
     */
    explicit ScanDone(std::shared_ptr<CallT> call) : _call(std::move(call)) {}

    void Run() override {
        std::unique_ptr<ScanDone> self(this);
        std::shared_ptr<CdcWaitObject> wake;
        {
            std::lock_guard<std::mutex> lk(_call->mutex);
            _call->completed = true;
            _call->completedAt = Date_t::now();
            if (!_call->cancelled) {
                wake = _call->waitObject;
            }
        }
        if (wake) {
            wake->notify();
        }
    }

private:
    const std::shared_ptr<CallT> _call;
};

}  // namespace

CdcScanClient::CdcScanClient(std::shared_ptr<CdcScanTransport> transport,
                             std::shared_ptr<CdcWaitObject> waitObject,
                             const std::vector<uint32_t>& logGroupIds,
                             Options options)
    : _transport(std::move(transport)), _waitObject(std::move(waitObject)), _options(options) {
    invariant(_transport && _waitObject);
    for (uint32_t id : logGroupIds) {
        _groups[id].backoff = _options.retryBackoffInitial;
    }
}

CdcScanClient::~CdcScanClient() {
    for (auto& [id, group] : _groups) {
        if (!group.call) {
            continue;
        }
        bool cancel = false;
        {
            std::lock_guard<std::mutex> lk(group.call->mutex);
            group.call->cancelled = true;
            cancel = !group.call->completed;
        }
        if (cancel) {
            // The call's closure still runs, now or later; it holds the call and finds it
            // cancelled.
            _transport->cancel(&group.call->cntl);
        }
    }
}

bool CdcScanClient::inFlight(uint32_t logGroupId) const {
    return _group(logGroupId).call != nullptr;
}

Date_t CdcScanClient::readyAt(uint32_t logGroupId) const {
    return _group(logGroupId).readyAt;
}

void CdcScanClient::deferUntil(uint32_t logGroupId, Date_t when) {
    GroupState& group = _group(logGroupId);
    group.readyAt = std::max(group.readyAt, when);
}

void CdcScanClient::issue(uint32_t logGroupId,
                          const txlog::CdcScanRequest& request,
                          Milliseconds rpcTimeout) {
    GroupState& group = _group(logGroupId);
    invariant(!group.call);
    auto call = std::make_shared<Call>();
    call->request = request;
    call->waitObject = _waitObject;
    call->issuedAt = Date_t::now();
    call->cntl.set_timeout_ms(durationCount<Milliseconds>(rpcTimeout));
    // Every retry is the cursor's decision, made from the same position; brpc must not re-send.
    call->cntl.set_max_retry(0);
    group.call = call;
    ++group.scansIssued;
    // The closure may run before scan() returns.
    _transport->scan(logGroupId,
                     &call->cntl,
                     call->request,
                     call->response.get(),
                     &call->targetNode,
                     new ScanDone<Call>(call));
}

boost::optional<CdcScanClient::Completion> CdcScanClient::takeCompleted(uint32_t logGroupId) {
    GroupState& group = _group(logGroupId);
    if (!group.call) {
        return boost::none;
    }
    {
        std::lock_guard<std::mutex> lk(group.call->mutex);
        if (!group.call->completed) {
            return boost::none;
        }
    }
    std::shared_ptr<Call> call = std::move(group.call);

    Completion completion;
    completion.request = call->request;
    completion.issuedAt = call->issuedAt;
    completion.completedAt = call->completedAt;

    if (call->cntl.Failed()) {
        if (call->cntl.ErrorCode() == brpc::ENOMETHOD) {
            completion.status = Completion::Status::kUnsupported;
            return completion;
        }
        _transport->requestLeaderRefresh(logGroupId);
        _backOff(group);
        completion.status = Completion::Status::kRetry;
        return completion;
    }

    const txlog::CdcScanResponse& response = *call->response;
    switch (response.status()) {
        case txlog::CdcScanResponse::Success:
            group.backoff = _options.retryBackoffInitial;
            group.lastFollowAt = Date_t();
            completion.status = Completion::Status::kOk;
            completion.response = call->response;
            return completion;

        case txlog::CdcScanResponse::NotLeader: {
            // Compared by node ID: the redirect may name the node just asked in another address
            // format than the one the call went to.
            const std::string& redirect = response.redirect();
            const std::optional<uint32_t> node =
                redirect.empty() ? std::nullopt : _transport->nodeByAddress(logGroupId, redirect);
            if (node && node != call->targetNode) {
                // A different node claims the lead: ask it at once, unless the previous redirect
                // was followed within one backoff period. Two nodes that name each other during
                // an election would otherwise be followed back and forth at once for as long as
                // it lasts.
                _transport->followNode(logGroupId, *node);
                const Date_t now = Date_t::now();
                if (now - group.lastFollowAt < group.backoff) {
                    _backOff(group);
                } else {
                    group.readyAt = now;
                }
                group.lastFollowAt = now;
            } else {
                // No leader known, the node just asked (a leader not yet in sync, §4.6), or an
                // address the group does not know, perhaps a peer added since startup.
                if (!redirect.empty() && !node) {
                    _transport->requestLeaderRefresh(logGroupId);
                }
                _backOff(group);
            }
            completion.status = Completion::Status::kRetry;
            return completion;
        }

        case txlog::CdcScanResponse::PositionTruncated:
            completion.status = Completion::Status::kPositionTruncated;
            return completion;

        default:
            // An answer this client does not understand is treated like a failed call.
            _transport->requestLeaderRefresh(logGroupId);
            _backOff(group);
            completion.status = Completion::Status::kRetry;
            return completion;
    }
}

uint64_t CdcScanClient::scansIssued(uint32_t logGroupId) const {
    return _group(logGroupId).scansIssued;
}

size_t CdcScanClient::inFlightCount() const {
    return std::count_if(
        _groups.begin(), _groups.end(), [](const auto& entry) { return entry.second.call; });
}

void CdcScanClient::_backOff(GroupState& group) {
    group.readyAt = std::max(group.readyAt, Date_t::now() + group.backoff);
    group.backoff = std::min(group.backoff * 2, _options.retryBackoffMax);
}

CdcScanClient::GroupState& CdcScanClient::_group(uint32_t logGroupId) {
    auto it = _groups.find(logGroupId);
    invariant(it != _groups.end());
    return it->second;
}

const CdcScanClient::GroupState& CdcScanClient::_group(uint32_t logGroupId) const {
    auto it = _groups.find(logGroupId);
    invariant(it != _groups.end());
    return it->second;
}

}  // namespace mongo
