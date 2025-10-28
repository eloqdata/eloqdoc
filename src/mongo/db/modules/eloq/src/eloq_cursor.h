/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the license:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include "mongo/db/operation_context.h"

#include "mongo/db/modules/eloq/src/base/eloq_key.h"

#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_execution.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/tx_request.h"

namespace mongo {
class EloqRecoveryUnit;

class EloqCursor {
public:
    explicit EloqCursor(OperationContext* opCtx);
    ~EloqCursor();

    bool indexScanIsOpen() const;
    void indexScanOpen(const txservice::TableName* tableName,
                       uint64_t keySchemaVersion,
                       txservice::ScanIndexType index_type,
                       const txservice::TxKey* start_key,
                       bool start_inclusive,
                       const txservice::TxKey* end_key,
                       bool end_inclusive,
                       txservice::ScanDirection direction,
                       bool is_for_write);
    void indexScanClose();

    txservice::TxErrorCode nextBatchTuple();
    const txservice::ScanBatchTuple* currentBatchTuple() const;


    uint32_t PrefetchSize() {
        std::array<uint32_t, 5> boundaries = {1, 4, 16, 64, 256};

        size_t idx = 0;
        for (; idx < boundaries.size(); ++idx) {
            if (_scanBatchCnt < boundaries[idx]) {
                break;
            }
        }

        return idx < boundaries.size() ? boundaries[idx] - 1 : boundaries.back() - 1;
    }

private:
    txservice::TxErrorCode _fetchBatchTuples();

    // state information used for txm
    OperationContext* _opCtx;                        // not owned
    EloqRecoveryUnit* _ru;                           // not owned
    txservice::TransactionExecution* _txm{nullptr};  // not owned

    txservice::ScanOpenTxRequest _scanOpenTxReq;
    const txservice::ScanBatchTuple* _currentBatchTuple{nullptr};
    bool _isLastScanBatch{false};
    size_t _scanAlias{UINT64_MAX};
    std::vector<txservice::ScanBatchTuple> _scanBatchVector;
    size_t _scanBatchIdx{UINT64_MAX};
    size_t _scanBatchCnt{0};
};

}  // namespace mongo
