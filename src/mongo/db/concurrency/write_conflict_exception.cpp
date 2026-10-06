// write_conflict_exception.cpp

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

#include <algorithm>

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kWrite

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_parameters.h"
#include "mongo/platform/random.h"
#include "mongo/util/log.h"
#include "mongo/util/stacktrace.h"

namespace mongo {

namespace {

/**
 * Chooses the randomized backoff duration for an attempt.
 * Returns 0 when the retry should proceed immediately.
 */
int64_t getBackoffMillis(int attempt) {
    if (attempt < 4) {
        return 0;
    }

    int64_t minMillis;
    int64_t maxMillis;
    if (attempt < 10) {
        minMillis = 1;
        maxMillis = 2;
    } else if (attempt < 100) {
        minMillis = 5;
        maxMillis = 10;
    } else {
        minMillis = 10;
        // Grow the upper bound by 10ms every 100 attempts, capped at 100ms.
        maxMillis =
            std::min<int64_t>((static_cast<int64_t>(attempt) / 100 + 1) * 10, 100);
    }

    static thread_local PseudoRandom backoffPrng{SecureRandom::create()->nextInt64()};
    return minMillis + backoffPrng.nextInt64(maxMillis - minMillis + 1);
}

/**
 * Implements the shared backoff calculation and logging flow.
 * SleepFn supplies either uninterruptible or OperationContext-aware sleeping.
 */
template <typename SleepFn>
void logAndBackoffImpl(int attempt,
                       StringData operation,
                       StringData ns,
                       SleepFn sleepFn) {
    const int64_t backoffMillis = getBackoffMillis(attempt);
    if (backoffMillis == 0) {
        LOG(1) << "Caught WriteConflictException doing " << operation << " on " << ns
               << ", attempt: " << attempt << " retrying immediately";
        return;
    }

    LOG(1) << "Caught WriteConflictException doing " << operation << " on " << ns
           << ", attempt: " << attempt << ", sleeping " << backoffMillis
           << "ms before retrying";
    sleepFn(Milliseconds{backoffMillis});
}

}  // namespace

AtomicBool WriteConflictException::trace(false);

WriteConflictException::WriteConflictException()
    : DBException(Status(ErrorCodes::WriteConflict, "WriteConflict")) {
    if (trace.load()) {
        printStackTrace();
    }
}

void WriteConflictException::logAndBackoff(int attempt, StringData operation, StringData ns) {
    logAndBackoffImpl(attempt, operation, ns, [](Milliseconds duration) {
        sleepmillis(duration.count());
    });
}

void WriteConflictException::logAndBackoff(OperationContext* opCtx,
                                           int attempt,
                                           StringData operation,
                                           StringData ns) {
    invariant(opCtx);
    logAndBackoffImpl(attempt, operation, ns, [opCtx](Milliseconds duration) {
        opCtx->sleepFor(duration);
    });
}

namespace {
// for WriteConflictException
ExportedServerParameter<bool, ServerParameterType::kStartupAndRuntime> TraceWCExceptionsSetting(
    ServerParameterSet::getGlobal(),
    "traceWriteConflictExceptions",
    &WriteConflictException::trace);
}
}
