// write_conflict_exception.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
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
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <exception>
#include <utility>

#include "mongo/base/string_data.h"
#include "mongo/db/curop.h"
#include "mongo/util/assert_util.h"

namespace mongo {

class OperationContext;

/**
 * This is thrown if during a write, two or more operations conflict with each other.
 * For example if two operations get the same version of a document, and then both try to
 * modify that document, this exception will get thrown by one of them.
 */
class WriteConflictException final : public DBException {
public:
    WriteConflictException();

    /**
     * Logs a WriteConflictException and performs an uninterruptible backoff.
     *
     * Use this overload for retry loops that require uninterruptible backoff. It blocks the
     * current worker thread with sleepmillis(), so operation interruption and deadlines are not
     * observed while waiting.
     *
     * @param attempt - current retry attempt count
     * @param operation - e.g. "update"
     */
    static void logAndBackoff(int attempt, StringData operation, StringData ns);

    /**
     * Logs a WriteConflictException and performs an interruptible backoff.
     *
     * Use this overload when the retry loop should remain responsive to operation interruption
     * and deadlines. Coroutine-backed executors can yield their worker while sleeping.
     *
     * @param opCtx - must not be null
     * @param attempt - current retry attempt count
     * @param operation - e.g. "update"
     */
    static void logAndBackoff(OperationContext* opCtx,
                              int attempt,
                              StringData operation,
                              StringData ns);

    /**
     * If true, will call printStackTrace on every WriteConflictException created.
     * Can be set via setParameter named traceWriteConflictExceptions.
     */
    static AtomicBool trace;

private:
    void defineOnlyInFinalSubclassToPreventSlicing() final {}
};

namespace write_conflict_retry_detail {

template <typename F, typename BackoffFn>
auto writeConflictRetryImpl(OperationContext* opCtx,
                            StringData opStr,
                            StringData ns,
                            F&& f,
                            BackoffFn backoffFn) {
    invariant(opCtx);
    invariant(opCtx->lockState());
    invariant(opCtx->recoveryUnit());

    if (opCtx->lockState()->inAWriteUnitOfWork()) {
        return f();
    }

    int attempts = 0;
    while (true) {
        try {
            return f();
        } catch (WriteConflictException const&) {
            CurOp::get(opCtx)->debug().additiveMetrics.incrementWriteConflicts(1);
            // Abandon the snapshot before backing off, not after: under Eloq the snapshot is a
            // transaction whose accumulated catalog/key intents are exactly what the conflict
            // winner is waiting to drain. Sleeping first parks this loser on top of its locks
            // for the whole backoff window, starving the winner it is about to retry against;
            // abandoning first hands the winner the backoff window to finish.
            opCtx->recoveryUnit()->abandonSnapshot();
            backoffFn(opCtx, attempts, opStr, ns);
            ++attempts;
        }
    }
}

}  // namespace write_conflict_retry_detail

/**
 * Runs the argument function f as many times as needed for f to complete or throw an exception
 * other than WriteConflictException. For each WriteConflictException, abandons the current
 * snapshot and applies uninterruptible backoff before retrying.
 *
 * The retry count is unbounded, so required timeout behavior must be enforced within f. If called
 * inside a WriteUnitOfWork, invokes f directly without adding another retry loop.
 */
template <typename F>
auto writeConflictRetry(OperationContext* opCtx, StringData opStr, StringData ns, F&& f) {
    return write_conflict_retry_detail::writeConflictRetryImpl(
        opCtx,
        opStr,
        ns,
        std::forward<F>(f),
        [](OperationContext*, int attempt, StringData operation, StringData ns) {
            WriteConflictException::logAndBackoff(attempt, operation, ns);
        });
}

/**
 * Equivalent to writeConflictRetry(), but uses OperationContext-aware backoff so the wait can
 * observe operation interruption and deadlines. Coroutine-backed executors can yield their worker
 * while sleeping.
 */
template <typename F>
auto writeConflictRetryInterruptibly(OperationContext* opCtx,
                                     StringData opStr,
                                     StringData ns,
                                     F&& f) {
    return write_conflict_retry_detail::writeConflictRetryImpl(
        opCtx,
        opStr,
        ns,
        std::forward<F>(f),
        [](OperationContext* backoffOpCtx,
           int attempt,
           StringData operation,
           StringData ns) {
            WriteConflictException::logAndBackoff(backoffOpCtx, attempt, operation, ns);
        });
}
}  // namespace mongo
