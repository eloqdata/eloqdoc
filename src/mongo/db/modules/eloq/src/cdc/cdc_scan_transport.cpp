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

#include "mongo/db/modules/eloq/src/cdc/cdc_scan_transport.h"

#include "mongo/util/assert_util.h"

namespace mongo {

TxLogCdcScanTransport::TxLogCdcScanTransport(txservice::TxLog* log) : _log(log) {
    invariant(_log);
}

std::vector<uint32_t> TxLogCdcScanTransport::logGroupIds() const {
    // A tx's log group is tx_number % LogGroupCount() (eloq_log_wrapper.h), so the groups are
    // numbered from 0.
    std::vector<uint32_t> ids(_log->LogGroupCount());
    for (uint32_t i = 0; i < ids.size(); ++i) {
        ids[i] = i;
    }
    return ids;
}

void TxLogCdcScanTransport::scan(uint32_t logGroupId,
                                 brpc::Controller* cntl,
                                 const txlog::CdcScanRequest& request,
                                 txlog::CdcScanResponse* response,
                                 std::optional<uint32_t>* targetNode,
                                 google::protobuf::Closure* done) {
    targetNode->reset();
    _log->CdcScan(logGroupId, cntl, request, *response, targetNode, *done);
}

std::optional<uint32_t> TxLogCdcScanTransport::nodeByAddress(uint32_t logGroupId,
                                                             const std::string& address) {
    return _log->LogNodeByAddress(logGroupId, address);
}

void TxLogCdcScanTransport::followNode(uint32_t logGroupId, uint32_t nodeId) {
    _log->UpdateLeaderCache(logGroupId, nodeId);
}

void TxLogCdcScanTransport::requestLeaderRefresh(uint32_t logGroupId) {
    _log->RequestRefreshLeader(logGroupId);
}

void TxLogCdcScanTransport::cancel(brpc::Controller* cntl) {
    brpc::StartCancel(cntl->call_id());
}

}  // namespace mongo
