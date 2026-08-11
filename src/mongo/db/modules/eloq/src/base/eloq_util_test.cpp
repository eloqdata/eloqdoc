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

#include "mongo/platform/basic.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

#include "mongo/db/modules/eloq/src/base/eloq_util.h"

namespace mongo {
namespace {

using txservice::TxErrorCode;

// The tx service errors that mean "another transaction got there first". Every
// one of them must leave a catalog record store loop through an exception, or
// the loop retries a conflict inside the losing transaction while it still
// holds the catalog read lock the winner is waiting on.
const TxErrorCode kConflictCodes[] = {
    TxErrorCode::WRITE_WRITE_CONFLICT,
    TxErrorCode::OCC_BREAK_REPEATABLE_READ,
    TxErrorCode::DEAD_LOCK_ABORT,
    TxErrorCode::GET_RANGE_ID_ERROR,
    TxErrorCode::SI_R4W_ERR_KEY_WAS_UPDATED,
    TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL,
};

// Errors the catalog record store's bounded retry loop must keep. Chief among
// them READ_CATALOG_FAIL, which means "the catalog entry is not resident, a
// fetch has been issued, try again" -- ejecting it would replace a working
// retry with a client-visible error.
const TxErrorCode kRetriedInLoopCodes[] = {
    TxErrorCode::READ_CATALOG_FAIL,
    TxErrorCode::NG_TERM_CHANGED,
    TxErrorCode::REQUEST_LOST,
    TxErrorCode::INTERNAL_ERR_TIMEOUT,
};

/**
 * Mirrors the catch clause inside writeConflictRetry (write_conflict_exception.h).
 * Returns true if the thrown object is caught by `catch (WriteConflictException const&)`,
 * which is the only handler that loop has.
 */
bool caughtByWriteConflictRetry(TxErrorCode code) {
    try {
        ThrowIfWriteConflict(code);
        return false;
    } catch (const WriteConflictException&) {
        return true;
    } catch (const DBException&) {
        return false;
    }
}

TEST(EloqUtil, ConflictsAreCaughtByWriteConflictRetry) {
    for (auto code : kConflictCodes) {
        ASSERT_TRUE(caughtByWriteConflictRetry(code))
            << "TxErrorCode " << static_cast<int>(code)
            << " does not reach a writeConflictRetry boundary";
    }
}

TEST(EloqUtil, ConflictIsNotExceptionForWriteConflict) {
    // WriteConflictException is `final : public DBException`, while
    // ExceptionFor<ErrorCodes::WriteConflict> resolves to
    // ExceptionForImpl<code, AssertionException>. They are siblings with no
    // inheritance relationship, so returning a Status carrying
    // ErrorCodes::WriteConflict and uassertStatusOK-ing it would sail straight
    // through writeConflictRetry. A test that only checked the error code
    // would pass under that defective form; this one does not.
    for (auto code : kConflictCodes) {
        bool wrongType = false;
        bool rightType = false;
        try {
            ThrowIfWriteConflict(code);
        } catch (const ExceptionFor<ErrorCodes::WriteConflict>&) {
            wrongType = true;
        } catch (const WriteConflictException&) {
            rightType = true;
        }
        ASSERT_FALSE(wrongType) << "TxErrorCode " << static_cast<int>(code)
                                << " threw ExceptionFor<WriteConflict>";
        ASSERT_TRUE(rightType);
    }
}

TEST(EloqUtil, DeadlockAbortIsInTheConflictGroup) {
    // The live reproduction logged exactly this code being swallowed for 90
    // seconds by the in-transaction retry loop. Retrying it inside the same
    // transaction is unsound: the transaction has already been chosen as the
    // deadlock victim.
    ASSERT_TRUE(caughtByWriteConflictRetry(TxErrorCode::DEAD_LOCK_ABORT));
}

TEST(EloqUtil, TransientErrorsDoNotThrow) {
    // These stay inside the catalog record store's bounded retry loop.
    for (auto code : kRetriedInLoopCodes) {
        ThrowIfWriteConflict(code);  // must return normally
    }
    ThrowIfWriteConflict(TxErrorCode::NO_ERROR);
}

TEST(EloqUtil, ConverterAndHelperCannotDrift) {
    // TxErrorCodeToMongoStatus calls ThrowIfWriteConflict first, so the group
    // has exactly one definition. Anything the helper throws for, the
    // converter must throw for too.
    for (auto code : kConflictCodes) {
        bool threw = false;
        try {
            TxErrorCodeToMongoStatus(code);
        } catch (const WriteConflictException&) {
            threw = true;
        }
        ASSERT_TRUE(threw) << "TxErrorCodeToMongoStatus returned a Status for "
                              "conflict code "
                           << static_cast<int>(code);
    }
}

TEST(EloqUtil, ConverterStillReturnsStatusForOtherErrors) {
    ASSERT_OK(TxErrorCodeToMongoStatus(TxErrorCode::NO_ERROR));

    // READ_CATALOG_FAIL has no dedicated mapping, so it lands in the default
    // arm. What matters is that it comes back as a Status rather than an
    // exception: the record store loop tests the code and retries.
    Status readCatalogFail = TxErrorCodeToMongoStatus(TxErrorCode::READ_CATALOG_FAIL);
    ASSERT_NOT_OK(readCatalogFail);

    ASSERT_EQUALS(TxErrorCodeToMongoStatus(TxErrorCode::DUPLICATE_KEY).code(),
                  ErrorCodes::DuplicateKey);
    ASSERT_EQUALS(TxErrorCodeToMongoStatus(TxErrorCode::OUT_OF_MEMORY).code(),
                  ErrorCodes::ExceededMemoryLimit);
    ASSERT_EQUALS(TxErrorCodeToMongoStatus(TxErrorCode::READ_WRITE_CONFLICT).code(),
                  ErrorCodes::SnapshotUnavailable);
}

}  // namespace
}  // namespace mongo
