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

#include "mongo/db/modules/eloq/src/cdc/cdc_fake_log_service.h"

#include <cerrno>
#include <chrono>

#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

/**
 * @brief Appends the low 'bytes' bytes of 'value', big-endian.
 *
 * @param[in,out] out The string to append to.
 * @param value The value.
 * @param bytes How many bytes to write.
 */
void appendBigEndian(std::string& out, uint64_t value, int bytes) {
    for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

/**
 * @brief Reads a big-endian integer.
 *
 * @param data The first byte.
 * @param bytes How many bytes to read.
 * @return The value.
 */
uint64_t readBigEndian(const char* data, int bytes) {
    uint64_t value = 0;
    for (int i = 0; i < bytes; ++i) {
        value = (value << 8) | static_cast<unsigned char>(data[i]);
    }
    return value;
}

/** Where each stream resumes, strictly after its key (log.proto, CdcScanRequest). */
struct Position {
    std::string dataKey;
    std::string ddlKey;
};

/**
 * @brief Decodes a continuation: u16 length, data key, u16 length, DDL key, lengths big-endian.
 *
 * @param continuation The bytes.
 * @return The position.
 */
Position decodeContinuation(const std::string& continuation) {
    size_t offset = 0;
    auto readKey = [&]() {
        invariant(offset + 2 <= continuation.size());
        const size_t length = readBigEndian(continuation.data() + offset, 2);
        offset += 2;
        invariant(offset + length <= continuation.size());
        std::string key = continuation.substr(offset, length);
        offset += length;
        return key;
    };
    Position position;
    position.dataKey = readKey();
    position.ddlKey = readKey();
    invariant(offset == continuation.size());
    return position;
}

/**
 * @brief Encodes a position as a continuation.
 *
 * @param position The position.
 * @return The bytes.
 */
std::string encodeContinuation(const Position& position) {
    std::string continuation;
    appendBigEndian(continuation, position.dataKey.size(), 2);
    continuation += position.dataKey;
    appendBigEndian(continuation, position.ddlKey.size(), 2);
    continuation += position.ddlKey;
    return continuation;
}

}  // namespace

CdcFakeLogService::CdcFakeLogService(std::vector<uint32_t> logGroupIds)
    : _logGroupIds(std::move(logGroupIds)) {
    for (uint32_t id : _logGroupIds) {
        _groups[id].nodes[0].insert("log-" + std::to_string(id) + "-a:9000");
    }
}

CdcFakeLogService::~CdcFakeLogService() {
    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        threads.swap(_threads);
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    std::vector<PendingCall> held;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        for (auto& [id, group] : _groups) {
            held.insert(held.end(), group.held.begin(), group.held.end());
            group.held.clear();
        }
    }
    for (const PendingCall& call : held) {
        call.cntl->SetFailed(ECANCELED, "fake log service shut down");
        call.done->Run();
    }
}

std::string CdcFakeLogService::dataKey(uint64_t commitTs, uint32_t ngId, uint64_t txn) {
    std::string key;
    appendBigEndian(key, commitTs, 8);
    appendBigEndian(key, ngId, 4);
    appendBigEndian(key, txn, 8);
    return key;
}

void CdcFakeLogService::addData(
    uint32_t logGroupId, uint64_t commitTs, uint32_t ngId, uint64_t txn, size_t blobBytes) {
    std::string blob(blobBytes, '\0');
    for (size_t i = 0; i < blobBytes; ++i) {
        blob[i] = static_cast<char>((txn + i) & 0xff);
    }
    addDataBlob(logGroupId, commitTs, ngId, txn, std::move(blob));
}

void CdcFakeLogService::addDataBlob(
    uint32_t logGroupId, uint64_t commitTs, uint32_t ngId, uint64_t txn, std::string blob) {
    txlog::CdcDataRecord record;
    record.set_commit_ts(commitTs);
    record.set_cc_ng_id(ngId);
    record.set_txn_number(txn);
    record.set_blob(std::move(blob));
    std::lock_guard<std::mutex> lk(_mutex);
    _group(logGroupId).data[dataKey(commitTs, ngId, txn)] = std::move(record);
}

void CdcFakeLogService::addDdl(uint32_t logGroupId, uint64_t ddlTs, uint64_t txn) {
    addDdl(logGroupId, ddlTs, txn, txlog::SchemaOpMessage());
}

void CdcFakeLogService::addDdl(uint32_t logGroupId,
                               uint64_t ddlTs,
                               uint64_t txn,
                               txlog::SchemaOpMessage schemaOp) {
    txlog::CdcDdlRecord record;
    record.set_ddl_ts(ddlTs);
    record.set_catalog_ts(ddlTs);
    record.set_txn_number(txn);
    *record.mutable_schema_op() = std::move(schemaOp);
    std::lock_guard<std::mutex> lk(_mutex);
    _group(logGroupId).ddl[ddlKey(ddlTs, txn)] = std::move(record);
}

std::string CdcFakeLogService::ddlKey(uint64_t ddlTs, uint64_t txn) {
    std::string key;
    appendBigEndian(key, ddlTs, 8);
    appendBigEndian(key, txn, 8);
    return key;
}

void CdcFakeLogService::setBound(uint32_t logGroupId, uint64_t bound) {
    std::lock_guard<std::mutex> lk(_mutex);
    _group(logGroupId).bound = bound;
}

void CdcFakeLogService::setRetentionFloor(uint32_t logGroupId, uint64_t floor) {
    std::lock_guard<std::mutex> lk(_mutex);
    _group(logGroupId).floor = floor;
}

void CdcFakeLogService::failNext(uint32_t logGroupId) {
    std::lock_guard<std::mutex> lk(_mutex);
    Action action;
    action.fail = true;
    _group(logGroupId).actions.push_back(std::move(action));
}

void CdcFakeLogService::setFailureCode(uint32_t logGroupId, int errorCode) {
    std::lock_guard<std::mutex> lk(_mutex);
    _group(logGroupId).failureCode = errorCode;
}

void CdcFakeLogService::notLeaderNext(uint32_t logGroupId, std::string redirect) {
    std::lock_guard<std::mutex> lk(_mutex);
    Action action;
    action.redirect = std::move(redirect);
    _group(logGroupId).actions.push_back(std::move(action));
}

void CdcFakeLogService::addNode(uint32_t logGroupId,
                                uint32_t nodeId,
                                std::vector<std::string> addresses) {
    std::lock_guard<std::mutex> lk(_mutex);
    std::set<std::string>& known = _group(logGroupId).nodes[nodeId];
    for (std::string& address : addresses) {
        known.insert(std::move(address));
    }
}

void CdcFakeLogService::setLeader(uint32_t logGroupId, uint32_t nodeId) {
    std::lock_guard<std::mutex> lk(_mutex);
    Group& group = _group(logGroupId);
    invariant(group.nodes.count(nodeId) == 1);
    group.leader = nodeId;
}

void CdcFakeLogService::setCompletion(uint32_t logGroupId, Completion mode) {
    std::lock_guard<std::mutex> lk(_mutex);
    _group(logGroupId).mode = mode;
}

void CdcFakeLogService::setCancelRunsDone(bool runsDone) {
    std::lock_guard<std::mutex> lk(_mutex);
    _cancelRunsDone = runsDone;
}

size_t CdcFakeLogService::heldCount(uint32_t logGroupId) const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _group(logGroupId).held.size();
}

void CdcFakeLogService::releaseHeld(uint32_t logGroupId) {
    std::vector<PendingCall> held;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        Group& group = _group(logGroupId);
        held.swap(group.held);
        for (const PendingCall& call : held) {
            _answer(lk, group, call);
        }
    }
    for (const PendingCall& call : held) {
        call.done->Run();
    }
}

std::vector<txlog::CdcScanRequest> CdcFakeLogService::requests(uint32_t logGroupId) const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _group(logGroupId).requests;
}

std::vector<uint32_t> CdcFakeLogService::targets(uint32_t logGroupId) const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _group(logGroupId).targets;
}

std::vector<uint32_t> CdcFakeLogService::nodesFollowed(uint32_t logGroupId) const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _group(logGroupId).nodesFollowed;
}

int CdcFakeLogService::refreshCount(uint32_t logGroupId) const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _group(logGroupId).refreshes;
}

int CdcFakeLogService::cancelCount() const {
    std::lock_guard<std::mutex> lk(_mutex);
    return _cancels;
}

std::vector<uint32_t> CdcFakeLogService::logGroupIds() const {
    return _logGroupIds;
}

void CdcFakeLogService::scan(uint32_t logGroupId,
                             brpc::Controller* cntl,
                             const txlog::CdcScanRequest& request,
                             txlog::CdcScanResponse* response,
                             std::optional<uint32_t>* targetNode,
                             google::protobuf::Closure* done) {
    PendingCall call;
    call.cntl = cntl;
    call.request = &request;
    call.response = response;
    call.done = done;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        Group& group = _group(logGroupId);
        group.requests.push_back(request);
        group.targets.push_back(group.leader);
        *targetNode = group.leader;
        switch (group.mode) {
            case Completion::kInline:
                _answer(lk, group, call);
                break;
            case Completion::kAsync:
                _threads.emplace_back([this, logGroupId, call] {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    {
                        std::lock_guard<std::mutex> lk(_mutex);
                        _answer(lk, _group(logGroupId), call);
                    }
                    call.done->Run();
                });
                return;
            case Completion::kHeld:
                group.held.push_back(call);
                return;
        }
    }
    done->Run();
}

std::optional<uint32_t> CdcFakeLogService::nodeByAddress(uint32_t logGroupId,
                                                         const std::string& address) {
    std::lock_guard<std::mutex> lk(_mutex);
    for (const auto& [nodeId, addresses] : _group(logGroupId).nodes) {
        if (addresses.count(address) == 1) {
            return nodeId;
        }
    }
    return std::nullopt;
}

void CdcFakeLogService::followNode(uint32_t logGroupId, uint32_t nodeId) {
    std::lock_guard<std::mutex> lk(_mutex);
    Group& group = _group(logGroupId);
    invariant(group.nodes.count(nodeId) == 1);
    group.leader = nodeId;
    group.nodesFollowed.push_back(nodeId);
}

void CdcFakeLogService::requestLeaderRefresh(uint32_t logGroupId) {
    std::lock_guard<std::mutex> lk(_mutex);
    ++_group(logGroupId).refreshes;
}

void CdcFakeLogService::cancel(brpc::Controller* cntl) {
    PendingCall cancelled;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        ++_cancels;
        if (!_cancelRunsDone) {
            return;
        }
        for (auto& [id, group] : _groups) {
            for (auto it = group.held.begin(); it != group.held.end(); ++it) {
                if (it->cntl == cntl) {
                    cancelled = *it;
                    group.held.erase(it);
                    break;
                }
            }
            if (cancelled.cntl) {
                break;
            }
        }
    }
    if (cancelled.cntl) {
        cancelled.cntl->SetFailed(ECANCELED, "cancelled");
        cancelled.done->Run();
    }
}

void CdcFakeLogService::_answer(const std::lock_guard<std::mutex>& lk,
                                Group& group,
                                const PendingCall& call) {
    (void)lk;
    if (group.failureCode != 0) {
        call.cntl->SetFailed(group.failureCode, "scripted failure");
        return;
    }
    if (!group.actions.empty()) {
        Action action = std::move(group.actions.front());
        group.actions.pop_front();
        if (action.fail) {
            call.cntl->SetFailed(EHOSTDOWN, "scripted transport error");
        } else {
            call.response->set_status(txlog::CdcScanResponse::NotLeader);
            call.response->set_redirect(action.redirect);
        }
        return;
    }

    const txlog::CdcScanRequest& request = *call.request;
    txlog::CdcScanResponse& response = *call.response;
    Position position;
    uint64_t positionTs = 0;
    switch (request.start_case()) {
        case txlog::CdcScanRequest::kContinuation:
            position = decodeContinuation(request.continuation());
            if (position.dataKey.size() >= 8) {
                positionTs = readBigEndian(position.dataKey.data(), 8);
            } else if (position.ddlKey.size() >= 8) {
                positionTs = readBigEndian(position.ddlKey.data(), 8);
            }
            break;
        case txlog::CdcScanRequest::kFromTs:
            appendBigEndian(position.dataKey, request.from_ts(), 8);
            position.ddlKey = position.dataKey;
            positionTs = request.from_ts();
            break;
        default:
            // The beginning of complete history, which the logger places at the floor.
            break;
    }
    if (group.floor != 0 && positionTs != 0 && positionTs <= group.floor) {
        response.set_status(txlog::CdcScanResponse::PositionTruncated);
        return;
    }

    response.set_status(txlog::CdcScanResponse::Success);
    response.set_arrival_complete_ts(group.bound);
    auto dataIt = group.data.upper_bound(position.dataKey);
    auto ddlIt = group.ddl.upper_bound(position.ddlKey);
    size_t pageBytes = 0;
    bool hasMore = false;
    for (;;) {
        const bool haveData =
            dataIt != group.data.end() && dataIt->second.commit_ts() <= group.bound;
        const bool haveDdl = ddlIt != group.ddl.end() && ddlIt->second.ddl_ts() <= group.bound;
        if (!haveData && !haveDdl) {
            break;
        }
        const bool takeData =
            haveData && (!haveDdl || dataIt->second.commit_ts() <= ddlIt->second.ddl_ts());
        const size_t recordBytes =
            takeData ? dataIt->second.ByteSizeLong() : ddlIt->second.ByteSizeLong();
        if (pageBytes > 0 && pageBytes + recordBytes > request.max_bytes()) {
            hasMore = true;
            break;
        }
        pageBytes += recordBytes;
        if (takeData) {
            *response.add_data() = dataIt->second;
            position.dataKey = dataIt->first;
            ++dataIt;
        } else {
            *response.add_ddl() = ddlIt->second;
            position.ddlKey = ddlIt->first;
            ++ddlIt;
        }
    }
    response.set_has_more(hasMore);
    response.set_continuation(encodeContinuation(position));
}

CdcFakeLogService::Group& CdcFakeLogService::_group(uint32_t logGroupId) {
    auto it = _groups.find(logGroupId);
    invariant(it != _groups.end());
    return it->second;
}

const CdcFakeLogService::Group& CdcFakeLogService::_group(uint32_t logGroupId) const {
    auto it = _groups.find(logGroupId);
    invariant(it != _groups.end());
    return it->second;
}

}  // namespace mongo
