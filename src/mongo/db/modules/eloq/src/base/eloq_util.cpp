#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/src/base/eloq_util.h"

#include "mongo/db/concurrency/write_conflict_exception.h"


namespace mongo {
Status TxErrorCodeToMongoStatus(txservice::TxErrorCode txErr) {
    if (MONGO_likely(txErr == txservice::TxErrorCode::NO_ERROR))
        return Status::OK();

    log() << "Eloq engine error report: " << txservice::TxErrorMessage(txErr);

    ErrorCodes::Error err;
    switch (txErr) {
        case txservice::TxErrorCode::TX_INIT_FAIL:
            err = ErrorCodes::NotYetInitialized;
            break;
        case txservice::TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR:
            err = ErrorCodes::TransactionTooLarge;
            break;
        case txservice::TxErrorCode::DUPLICATE_KEY:
            err = ErrorCodes::DuplicateKey;
            break;
        case txservice::TxErrorCode::UNIQUE_CONSTRAINT:
        case txservice::TxErrorCode::CAL_ENGINE_DEFINED_CONSTRAINT:
            err = ErrorCodes::CannotCreateIndex;
            break;
        case txservice::TxErrorCode::READ_WRITE_CONFLICT:
            err = ErrorCodes::SnapshotUnavailable;
            break;
        case txservice::TxErrorCode::WRITE_WRITE_CONFLICT:
        case txservice::TxErrorCode::OCC_BREAK_REPEATABLE_READ:
        case txservice::TxErrorCode::DEAD_LOCK_ABORT:
        case txservice::TxErrorCode::GET_RANGE_ID_ERROR:
        case txservice::TxErrorCode::SI_R4W_ERR_KEY_WAS_UPDATED:
        case txservice::TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL:
            // Like wtRCToStatus_slow.
            throw WriteConflictException();
            break;
        case txservice::TxErrorCode::OUT_OF_MEMORY:
            err = ErrorCodes::ExceededMemoryLimit;
            break;
        case txservice::TxErrorCode::TX_REQUEST_TO_COMMITTED_ABORTED_TX:
            err = ErrorCodes::TransactionCommitted;
            break;
        case txservice::TxErrorCode::TRANSACTION_NODE_NOT_LEADER:
            err = ErrorCodes::NotMaster;
            break;
        case txservice::TxErrorCode::INTERNAL_ERR_TIMEOUT:
        case txservice::TxErrorCode::LOG_SERVICE_UNREACHABLE:
        case txservice::TxErrorCode::WRITE_LOG_FAIL:
        case txservice::TxErrorCode::DATA_STORE_ERROR:
        case txservice::TxErrorCode::NG_TERM_CHANGED:
        case txservice::TxErrorCode::REQUEST_LOST:
            err = ErrorCodes::InternalError;
            break;
        case txservice::TxErrorCode::UPSERT_TABLE_PREPARE_FAIL:
            err = ErrorCodes::OperationFailed;
            break;
        case txservice::TxErrorCode::REQUESTD_TABLE_NOT_EXISTS:
            err = ErrorCodes::NamespaceNotFound;
            break;
        default:
            err = ErrorCodes::UnknownError;
            break;
    }

    mongoutils::str::stream s;
    s << "TxError[" << static_cast<int>(txErr) << "]: " << txservice::TxErrorMessage(txErr);
    return Status(err, s);
}
}  // namespace mongo
