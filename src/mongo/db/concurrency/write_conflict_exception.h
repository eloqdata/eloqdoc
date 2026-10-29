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
#include "mongo/db/coro_sync.h"
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
     * @brief Logs the conflict and returns how long to back off before the retry.
     *
     * The pause is random within its range, so that operations conflicting with each other do
     * not retry in lock step.
     *
     * @param attempt What attempt this is, 1 based.
     * @param operation The operation, e.g. "update".
     * @param ns Its namespace.
     * @return The pause: none below attempt 4, then 1-2 ms, 5-10 ms from attempt 10, and from
     * attempt 100 between 10 ms and a bound that grows by 10 ms every 100 attempts, to 100 ms.
     */
    static Milliseconds logAndGetBackoff(int attempt, StringData operation, StringData ns);

    /**
     * @brief Logs the conflict and backs off before the retry, exponentially, so as not to hammer
     * the same document over and over, without blocking the thread on a service executor
     * coroutine (coro::sleepUntil).
     *
     * EloqDoc runs commands on coroutines of brpc workers, each of which also drives its core's
     * transaction processor. MongoDB's backoff slept the thread, which stalled that core for the
     * whole pause; a retry loop sleeping back to back starved a concurrent index build, the very
     * operation it was waiting for, about 200 times over, so it was replaced by this one, which
     * yields instead. Like it, it is not interruptible.
     *
     * @param attempt What attempt this is, 1 based.
     * @param operation The operation, e.g. "update".
     * @param ns Its namespace.
     * @warning It may yield the coroutine, so call it outside a catch block: coroutines that
     * share the thread would otherwise interleave their exception handling.
     */
    static void logAndBackoffYielding(int attempt, StringData operation, StringData ns) {
        const Milliseconds pause = logAndGetBackoff(attempt, operation, ns);
        if (pause > Milliseconds(0)) {
            coro::sleepUntil(Date_t::now() + pause);
        }
    }

    /**
     * Logs a WriteConflictException and performs an interruptible backoff.
     *
     * Use this overload when the retry loop should remain responsive to operation interruption
     * and deadlines. It sleeps through the operation context, which yields the coroutine on a
     * coroutine-backed executor.
     *
     * @param opCtx - must not be null
     * @param attempt - current retry attempt count
     * @param operation - e.g. "update"
     * @param ns - the operation's namespace
     * @warning It may yield the coroutine, so call it outside a catch block, as for
     * logAndBackoffYielding().
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
        }
        // Outside the handler, since both steps may yield the coroutine.
        //
        // Abandon the snapshot before backing off, not after: under Eloq the snapshot is a
        // transaction whose accumulated catalog/key intents are exactly what the conflict
        // winner is waiting to drain. Sleeping first parks this loser on top of its locks
        // for the whole backoff window, starving the winner it is about to retry against;
        // abandoning first hands the winner the backoff window to finish. Neither backoff
        // blocks the thread: both yield, so the winner's work on this thread's core goes on
        // meanwhile.
        opCtx->recoveryUnit()->abandonSnapshot();
        backoffFn(opCtx, attempts, opStr, ns);
        ++attempts;
    }
}

}  // namespace write_conflict_retry_detail

/**
 * Runs the argument function f as many times as needed for f to complete or throw an exception
 * other than WriteConflictException. For each WriteConflictException, abandons the current
 * snapshot and applies uninterruptible backoff before retrying (logAndBackoffYielding()).
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
            WriteConflictException::logAndBackoffYielding(attempt, operation, ns);
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
