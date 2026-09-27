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
 * Scan path tests (docs/cdc-change-stream.md §4.2, §5.1, §5.3 steps 1-2, §5.8; plan C4, C5): the
 * wait object on both its paths, a blocked thread and a coroutine (through fake coroutine functors
 * whose yield blocks until a resume from another thread), and the source and client against the
 * scripted log service. They check that the coroutine path resumes a waiter exactly once whoever
 * notifies first, releases the deadline timer's reference exactly once, and absorbs a stale
 * timer's wake-up in the next wait; that records come out merged by timestamp, each exactly once,
 * only once the emission rule allows; that the completeness boundary and emit_bound follow §4.2
 * across cut and empty pages; that the buffer cap never stops the lagging group; that the start
 * position waits for every bound; that a leader that cannot serve, or two nodes that name each
 * other, are retried at the right pace; and that a call outliving its waiter or its cursor is
 * harmless.
 */

#include "mongo/platform/basic.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_fake_log_service.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_scan_source.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_wait_object.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using Completion = CdcFakeLogService::Completion;

/** Tests block the calling thread instead of yielding a coroutine. */
const CoroutineFunctors& kNoCoro = CoroutineFunctors::Unavailable;

/**
 * @brief Source options with short timings, so that backoffs are visible but cheap.
 *
 * @return The options.
 */
CdcScanSourceOptions fastOptions() {
    CdcScanSourceOptions options;
    options.maxWait = Milliseconds(1000);
    options.rpcSlack = Milliseconds(1000);
    options.idleBackoffInitial = Milliseconds(10);
    options.idleBackoffMax = Milliseconds(40);
    options.client.retryBackoffInitial = Milliseconds(10);
    options.client.retryBackoffMax = Milliseconds(40);
    return options;
}

/**
 * @brief What a test compares of a released record.
 */
struct Released {
    uint32_t group = 0;
    uint64_t ts = 0;
    bool ddl = false;  ///< From the DDL stream.
    uint64_t txn = 0;

    bool operator==(const Released& other) const {
        return std::tie(group, ts, ddl, txn) ==
            std::tie(other.group, other.ts, other.ddl, other.txn);
    }
    bool operator<(const Released& other) const {
        return std::tie(group, ts, ddl, txn) <
            std::tie(other.group, other.ts, other.ddl, other.txn);
    }
};

/**
 * @brief Prints a released record in assertion messages.
 *
 * @param[in,out] os The stream.
 * @param r The record.
 * @return The stream.
 */
std::ostream& operator<<(std::ostream& os, const Released& r) {
    return os << "{g" << r.group << " ts " << r.ts << (r.ddl ? " ddl" : " data") << " txn " << r.txn
              << "}";
}

/**
 * @brief Reduces a released record to what the tests compare.
 *
 * @param record The record.
 * @return Its description.
 */
Released describe(const CdcSourceRecord& record) {
    Released r;
    r.group = record.logGroupId();
    r.ts = record.timestamp();
    r.ddl = record.stream() == CdcSourceRecord::Stream::kDdl;
    r.txn = r.ddl ? record.ddl().txn_number() : record.data().txn_number();
    return r;
}

/**
 * @brief Coroutine functors without a coroutine runtime: yield blocks the calling thread until a
 * resume, which may come from any thread.
 *
 * A resume that lands before its yield leaves a permit, so the yield returns at once; that is
 * what EloqDoc's resume does, since it only queues the coroutine on its thread group. Hooks let a
 * test run code where a real coroutine could be preempted: between registering and yielding, and
 * after being resumed but before running on.
 */
class FakeCoroutine {
public:
    FakeCoroutine() {
        _yield = [this] { _doYield(); };
        _resume = [this] { _doResume(); };
        _functors.yieldFuncPtr = &_yield;
        _functors.resumeFuncPtr = &_resume;
        _functors.longResumeFuncPtr = &_resume;
    }

    FakeCoroutine(const FakeCoroutine&) = delete;
    FakeCoroutine& operator=(const FakeCoroutine&) = delete;

    /** The functors to pass as a coroutine's. */
    const CoroutineFunctors& functors() const {
        return _functors;
    }

    /** How many times the coroutine yielded. */
    int yields() const {
        std::lock_guard<std::mutex> lk(_mutex);
        return _yields;
    }

    /** How many times it was resumed. */
    int resumes() const {
        std::lock_guard<std::mutex> lk(_mutex);
        return _resumes;
    }

    /**
     * @brief Runs 'hook' on the yielding thread just before the next yield blocks, once.
     *
     * @param hook The code to run.
     */
    void setBeforeNextYield(std::function<void()> hook) {
        std::lock_guard<std::mutex> lk(_mutex);
        _beforeNextYield = std::move(hook);
    }

    /**
     * @brief Keeps a resumed yield from returning for 'delay', as a coroutine that is resumed but
     * not yet scheduled.
     *
     * @param delay The delay; zero turns it off.
     */
    void setDelayAfterResume(Milliseconds delay) {
        std::lock_guard<std::mutex> lk(_mutex);
        _delayAfterResume = delay;
    }

private:
    void _doYield() {
        std::function<void()> hook;
        {
            std::lock_guard<std::mutex> lk(_mutex);
            ++_yields;
            hook.swap(_beforeNextYield);
        }
        if (hook) {
            hook();
        }
        Milliseconds delay{0};
        {
            std::unique_lock<std::mutex> lk(_mutex);
            _cv.wait(lk, [this] { return _permits > 0; });
            --_permits;
            delay = _delayAfterResume;
        }
        if (delay > Milliseconds(0)) {
            std::this_thread::sleep_for(delay.toSystemDuration());
        }
    }

    void _doResume() {
        {
            std::lock_guard<std::mutex> lk(_mutex);
            ++_permits;
            ++_resumes;
        }
        _cv.notify_all();
    }

    mutable std::mutex _mutex;
    std::condition_variable _cv;
    int _permits = 0;  ///< Resumes not yet consumed by a yield.
    int _yields = 0;
    int _resumes = 0;
    std::function<void()> _beforeNextYield;
    Milliseconds _delayAfterResume{0};
    std::function<void()> _yield;
    std::function<void()> _resume;
    CoroutineFunctors _functors;
};

/**
 * @brief Waits until the test holds the only reference to the wait object, which is when every
 * deadline timer armed on it has either been deleted or has run and released its reference.
 *
 * @param waitObject The wait object.
 * @param within How long to wait.
 * @return true if that happened in time.
 */
bool timerReferencesReleased(const std::shared_ptr<CdcWaitObject>& waitObject,
                             Milliseconds within) {
    const Date_t end = Date_t::now() + within;
    while (waitObject.use_count() > 1) {
        if (Date_t::now() >= end) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

/**
 * @brief Pulls records until next() reports the source drained.
 *
 * @param source The source.
 * @param idle How long each next() may wait; the last one waits all of it.
 * @param coro The functors every pull passes.
 * @return The records, in release order.
 */
std::vector<Released> drain(CdcScanSource& source,
                            Milliseconds idle = Milliseconds(100),
                            const CoroutineFunctors& coro = kNoCoro) {
    std::vector<Released> out;
    while (boost::optional<CdcSourceRecord> record = source.next(Date_t::now() + idle, coro)) {
        out.push_back(describe(*record));
    }
    ASSERT(source.drained());
    return out;
}

/**
 * @brief Asserts that timestamps never go down across the released records.
 *
 * @param out The records, in release order.
 */
void assertMergedByTimestamp(const std::vector<Released>& out) {
    for (size_t i = 1; i < out.size(); ++i) {
        ASSERT_LTE(out[i - 1].ts, out[i].ts)
            << "at " << i << ": " << out[i - 1] << " then " << out[i];
    }
}

// ------------------------------------------------------------------------------------------------
// CdcWaitObject
// ------------------------------------------------------------------------------------------------

TEST(CdcWaitObject, NotifyBeforeWaitIsKeptForTheNextWait) {
    auto waitObject = CdcWaitObject::make();
    waitObject->notify();
    Date_t start = Date_t::now();
    waitObject->waitUntil(start + Seconds(10), kNoCoro);
    ASSERT_LT(Date_t::now() - start, Seconds(5));

    // The wake-up was consumed, so the next wait runs to its deadline.
    start = Date_t::now();
    waitObject->waitUntil(start + Milliseconds(50), kNoCoro);
    ASSERT_GTE(Date_t::now() - start, Milliseconds(45));
}

TEST(CdcWaitObject, NotifyFromAnotherThreadWakesTheWaiter) {
    auto waitObject = CdcWaitObject::make();
    std::thread notifier([waitObject] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        waitObject->notify();
    });
    const Date_t start = Date_t::now();
    waitObject->waitUntil(start + Seconds(10), kNoCoro);
    notifier.join();
    ASSERT_LT(Date_t::now() - start, Seconds(5));
}

// The coroutine path (_yieldUntil): register, arm the deadline timer, yield; whoever notifies first
// resumes the coroutine exactly once, and the timer's reference to the wait object is released
// exactly once, by the waiter if it deletes the timer before it runs, else by the timer.

TEST(CdcWaitObject, CoroutineResumedByANotifyDeletesItsTimer) {
    auto waitObject = CdcWaitObject::make();
    CdcWaitObject* raw = waitObject.get();
    FakeCoroutine coro;
    std::thread notifier([raw] {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        raw->notify();
    });
    const Date_t start = Date_t::now();
    waitObject->waitUntil(start + Seconds(10), coro.functors());
    notifier.join();
    ASSERT_LT(Date_t::now() - start, Seconds(5));
    ASSERT_EQ(coro.yields(), 1);
    ASSERT_EQ(coro.resumes(), 1);
    // The timer had not run, so the waiter deleted it and released its reference before
    // returning.
    ASSERT_EQ(waitObject.use_count(), 1);
}

TEST(CdcWaitObject, CoroutineResumedByTheDeadlineTimer) {
    auto waitObject = CdcWaitObject::make();
    FakeCoroutine coro;
    const Date_t start = Date_t::now();
    waitObject->waitUntil(start + Milliseconds(50), coro.functors());
    ASSERT_GTE(Date_t::now() - start, Milliseconds(45));
    ASSERT_LT(Date_t::now() - start, Seconds(5));
    ASSERT_EQ(coro.yields(), 1);
    ASSERT_EQ(coro.resumes(), 1);
    // The timer ran, so it releases its own reference once its notify returns.
    ASSERT(timerReferencesReleased(waitObject, Seconds(1)));
}

TEST(CdcWaitObject, NotifyBetweenRegistrationAndYieldIsNotLost) {
    auto waitObject = CdcWaitObject::make();
    CdcWaitObject* raw = waitObject.get();
    FakeCoroutine coro;
    // The response lands after the waiter registered and released the lock, before it yields.
    coro.setBeforeNextYield([raw] { raw->notify(); });
    const Date_t start = Date_t::now();
    waitObject->waitUntil(start + Seconds(10), coro.functors());
    ASSERT_LT(Date_t::now() - start, Seconds(5));
    ASSERT_EQ(coro.yields(), 1);
    ASSERT_EQ(coro.resumes(), 1);
    ASSERT_EQ(waitObject.use_count(), 1);
}

TEST(CdcWaitObject, StaleTimerWakesOnlyTheNextWait) {
    auto waitObject = CdcWaitObject::make();
    CdcWaitObject* raw = waitObject.get();
    FakeCoroutine coro;
    // A response resumes the coroutine, which is not scheduled until after its deadline, so the
    // timer runs after the waiter was unregistered and before the waiter could delete it.
    coro.setDelayAfterResume(Milliseconds(150));
    std::thread notifier([raw] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        raw->notify();
    });
    waitObject->waitUntil(Date_t::now() + Milliseconds(50), coro.functors());
    notifier.join();
    coro.setDelayAfterResume(Milliseconds(0));
    // Whichever of the two came second found nobody registered and resumed nothing, and the
    // timer's reference was released once, by the timer.
    ASSERT_EQ(coro.resumes(), 1);
    ASSERT(timerReferencesReleased(waitObject, Seconds(1)));

    // The late notify is kept for the next wait, which returns at once without yielding...
    Date_t start = Date_t::now();
    waitObject->waitUntil(start + Seconds(10), coro.functors());
    ASSERT_LT(Date_t::now() - start, Seconds(1));
    ASSERT_EQ(coro.yields(), 1);

    // ...and only that one: the wait after it yields until its own deadline.
    start = Date_t::now();
    waitObject->waitUntil(start + Milliseconds(50), coro.functors());
    ASSERT_GTE(Date_t::now() - start, Milliseconds(45));
    ASSERT_EQ(coro.yields(), 2);
    ASSERT_EQ(coro.resumes(), 2);
    ASSERT(timerReferencesReleased(waitObject, Seconds(1)));
}

// ------------------------------------------------------------------------------------------------
// Order and completeness
// ------------------------------------------------------------------------------------------------

TEST(CdcScanSource, OneGroupReleasesInStorageOrderFromTheStart) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->addData(0, 4, 1, 99);  // below the start
    log->addData(0, 5, 1, 100);
    log->addDdl(0, 6, 102);
    log->addData(0, 6, 1, 101);
    log->addData(0, 7, 2, 103);
    log->addData(0, 12, 1, 104);  // above the bound
    log->setBound(0, 10);

    CdcScanSource source(log, fastOptions());
    source.startAllAt(5);
    boost::optional<CdcSourceRecord> first = source.next(Date_t::now() + Seconds(5), kNoCoro);
    ASSERT(first);
    ASSERT_EQ(first->dataKey(), CdcFakeLogService::dataKey(5, 1, 100));

    std::vector<Released> out{describe(*first)};
    for (const Released& r : drain(source)) {
        out.push_back(r);
    }
    const std::vector<Released> expected{
        {0, 5, false, 100}, {0, 6, false, 101}, {0, 6, true, 102}, {0, 7, false, 103}};
    ASSERT_EQ(out.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(out[i], expected[i]);
    }
    ASSERT_EQ(source.emitBound(), 10u);
    ASSERT_EQ(source.bufferedBytes(), 0);

    // The first scan starts at from_ts; every later one resumes from the continuation.
    const std::vector<txlog::CdcScanRequest> requests = log->requests(0);
    ASSERT_GTE(requests.size(), 2u);
    ASSERT_EQ(requests[0].from_ts(), 5u);
    for (size_t i = 1; i < requests.size(); ++i) {
        ASSERT(requests[i].start_case() == txlog::CdcScanRequest::kContinuation);
    }
}

/**
 * @brief Scripts three groups with overlapping timestamps, drains them through small pages, and
 * checks that every record comes out once, merged by timestamp and in each group's storage order.
 *
 * @param mode How the service completes calls.
 * @param coro The functors every pull passes.
 */
void runThreeGroupMerge(Completion mode, const CoroutineFunctors& coro = kNoCoro) {
    const std::vector<uint32_t> groups{0, 1, 2};
    auto log = std::make_shared<CdcFakeLogService>(groups);
    std::map<uint32_t, std::vector<Released>> script;
    uint64_t txn = 1;
    for (uint64_t i = 0; i < 90; ++i) {
        const uint32_t group = static_cast<uint32_t>((i * 7) % 3);
        // Timestamps repeat within and across groups, so pages are cut inside timestamps.
        const uint64_t ts = 10 + (i * 13) % 40;
        if (i % 11 == 0) {
            log->addDdl(group, ts, txn);
            script[group].push_back({group, ts, true, txn});
        } else {
            log->addData(group, ts, 1 + static_cast<uint32_t>(i % 2), txn, 40);
            script[group].push_back({group, ts, false, txn});
        }
        ++txn;
    }
    for (uint32_t group : groups) {
        log->setBound(group, 100);
        log->setCompletion(group, mode);
    }

    CdcScanSourceOptions options = fastOptions();
    options.maxBytesPerScan = 200;
    CdcScanSource source(log, options);
    source.startAllAt(1);
    const std::vector<Released> out = drain(source, Milliseconds(100), coro);

    assertMergedByTimestamp(out);
    std::map<uint32_t, std::vector<Released>> perGroup;
    for (const Released& r : out) {
        perGroup[r.group].push_back(r);
    }
    for (uint32_t group : groups) {
        // Storage order within a group: by timestamp, data before DDL, then by key. The fake's
        // keys order data by node group before txn, so compare as sorted sets and check the
        // timestamp and stream order separately.
        std::vector<Released> expected = script[group];
        std::vector<Released> actual = perGroup[group];
        for (size_t i = 1; i < actual.size(); ++i) {
            ASSERT(std::tie(actual[i - 1].ts, actual[i - 1].ddl) <=
                   std::tie(actual[i].ts, actual[i].ddl))
                << actual[i - 1] << " then " << actual[i];
        }
        std::sort(expected.begin(), expected.end());
        std::sort(actual.begin(), actual.end());
        ASSERT_EQ(actual.size(), expected.size()) << "group " << group;
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(actual[i], expected[i]);
        }
    }
    ASSERT_EQ(source.emitBound(), 100u);
    ASSERT_EQ(source.bufferedBytes(), 0);
}

TEST(CdcScanSource, ThreeGroupsMergeByTimestampWithInlineCompletion) {
    runThreeGroupMerge(Completion::kInline);
}

TEST(CdcScanSource, ThreeGroupsMergeByTimestampWithAsyncCompletion) {
    runThreeGroupMerge(Completion::kAsync);
}

TEST(CdcScanSource, ThreeGroupsMergeUnderACoroutine) {
    // Every wait yields, and is resumed from the service's answering threads or the timer.
    FakeCoroutine coro;
    runThreeGroupMerge(Completion::kAsync, coro.functors());
    ASSERT_GT(coro.yields(), 0);
    ASSERT_EQ(coro.resumes(), coro.yields());
}

TEST(CdcScanSource, CutPageBoundsScannedTsBelowItsLastTimestamp) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    for (uint64_t txn = 1; txn <= 6; ++txn) {
        log->addData(0, 10, 1, txn, 100);
    }
    log->addData(1, 10, 1, 7, 100);
    log->addData(1, 11, 1, 8, 100);
    log->setBound(0, 20);
    log->setBound(1, 20);

    CdcScanSourceOptions options = fastOptions();
    options.maxBytesPerScan = 150;  // one record per page
    CdcScanSource source(log, options);
    source.startAllAt(1);

    boost::optional<CdcSourceRecord> first = source.next(Date_t::now() + Seconds(5), kNoCoro);
    ASSERT(first);
    ASSERT_EQ(describe(*first), (Released{0, 10, false, 1}));
    // Both pages were cut inside timestamp 10, so only 9 is known complete for either group.
    ASSERT_EQ(source.scannedTs(0), 9u);
    ASSERT_EQ(source.scannedTs(1), 9u);

    std::vector<Released> out{describe(*first)};
    for (const Released& r : drain(source)) {
        out.push_back(r);
    }
    assertMergedByTimestamp(out);
    ASSERT_EQ(out.size(), 8u);
    std::sort(out.begin(), out.end());
    ASSERT(std::adjacent_find(out.begin(), out.end()) == out.end());
    ASSERT_EQ(source.scannedTs(0), 20u);
    ASSERT_EQ(source.emitBound(), 20u);
}

TEST(CdcScanSource, EmissionWaitsForTheLaggingGroup) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    log->addData(0, 50, 1, 1);
    log->setBound(0, 100);
    log->setBound(1, 48);
    log->setCompletion(1, Completion::kHeld);

    CdcScanSource source(log, fastOptions());
    source.startAllAt(1);

    // Group 1 has fetched nothing, so group 0's record at 50 must wait.
    ASSERT(!source.next(Date_t::now() + Milliseconds(50), kNoCoro));
    ASSERT(source.drained());
    ASSERT_EQ(source.emitBound(), 0u);
    ASSERT_EQ(log->heldCount(1), 1u);

    // Its scan answers after the getMore returned; the next one finds it. 48 + 1 < 50 still.
    log->releaseHeld(1);
    ASSERT(!source.next(Date_t::now() + Milliseconds(50), kNoCoro));
    ASSERT_EQ(source.scannedTs(1), 48u);
    ASSERT_EQ(source.emitBound(), 48u);

    // At 49 no record below 50 can still come from group 1.
    log->setBound(1, 49);
    log->releaseHeld(1);
    const Date_t start = Date_t::now();
    boost::optional<CdcSourceRecord> record = source.next(start + Seconds(5), kNoCoro);
    ASSERT(record);
    ASSERT_EQ(describe(*record), (Released{0, 50, false, 1}));
    ASSERT_LT(Date_t::now() - start, Seconds(1));
}

TEST(CdcScanSource, IdleStreamAdvancesEmitBound) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1, 2});
    log->setBound(0, 10);
    log->setBound(1, 20);
    log->setBound(2, 30);

    CdcScanSource source(log, fastOptions());
    source.startAllAt(1);
    ASSERT(drain(source).empty());
    ASSERT_EQ(source.emitBound(), 10u);

    log->setBound(0, 40);
    ASSERT(drain(source).empty());
    ASSERT_EQ(source.emitBound(), 20u);
}

// ------------------------------------------------------------------------------------------------
// Fetch pacing
// ------------------------------------------------------------------------------------------------

/**
 * @brief Waits 500 ms on an idle group whose logger answers every scan at once, and checks that
 * the group is scanned at the backoff rate and that the wait ends at its deadline.
 *
 * @param coro The functors the pull passes.
 */
void runIdleBackoff(const CoroutineFunctors& coro) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 50);

    CdcScanSourceOptions options = fastOptions();
    options.idleBackoffInitial = Milliseconds(20);
    options.idleBackoffMax = Milliseconds(80);
    CdcScanSource source(log, options);
    source.startAllAt(1);
    const Date_t start = Date_t::now();
    ASSERT(!source.next(start + Milliseconds(500), coro));
    ASSERT_GTE(Date_t::now() - start, Milliseconds(495));
    ASSERT_LT(Date_t::now() - start, Seconds(3));

    // Pauses of 20, 40, then 80 ms allow about ten scans in 500 ms; without them the logger's
    // immediate answers would make it thousands.
    const uint64_t scans = source.scansIssued(0);
    ASSERT_GTE(scans, 4u);
    ASSERT_LTE(scans, 15u);
    ASSERT_EQ(source.emitBound(), 50u);
    for (const txlog::CdcScanRequest& request : log->requests(0)) {
        ASSERT_GT(request.wait_ms(), 0u);
        ASSERT_LTE(request.wait_ms(), 500u);
    }
}

TEST(CdcScanSource, IdleGroupAnsweringEarlyIsScannedAtTheBackoffRate) {
    runIdleBackoff(kNoCoro);
}

TEST(CdcScanSource, BackToBackShortWaitsArePacedByTheIdleBackoff) {
    // A client issuing getMores with a 10 ms maxTimeMS back to back. Every scan asks for less
    // than the shortest long poll, so an immediate empty answer counts as early, not as held.
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 50);
    CdcScanSourceOptions options = fastOptions();
    options.idleBackoffInitial = Milliseconds(20);
    options.idleBackoffMax = Milliseconds(80);
    CdcScanSource source(log, options);
    source.startAllAt(1);

    const Date_t end = Date_t::now() + Milliseconds(300);
    int calls = 0;
    while (Date_t::now() < end) {
        ASSERT(!source.next(Date_t::now() + Milliseconds(10), kNoCoro));
        ++calls;
    }
    // Pauses of 20, 40, then 80 ms allow about six scans in 300 ms; without them each call would
    // rescan in a loop for its whole 10 ms.
    ASSERT_GTE(calls, 10);
    ASSERT_LTE(source.scansIssued(0), 12u);
    ASSERT_EQ(source.emitBound(), 50u);
}

TEST(CdcScanSource, NoScanGoesOutInTheLastMillisecondsOfAWait) {
    // The group's pause ends inside the last 20 ms of the wait; that must not start a burst of
    // short scans until the deadline.
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 50);
    CdcScanSourceOptions options = fastOptions();
    options.idleBackoffInitial = Milliseconds(10);
    options.idleBackoffMax = Milliseconds(10);
    CdcScanSource source(log, options);
    source.startAllAt(1);
    const Date_t start = Date_t::now();
    ASSERT(!source.next(start + Milliseconds(40), kNoCoro));
    ASSERT_GTE(Date_t::now() - start, Milliseconds(38));

    ASSERT_LTE(source.scansIssued(0), 6u);
    for (const txlog::CdcScanRequest& request : log->requests(0)) {
        ASSERT_GT(request.wait_ms(), 20u);
    }
}

TEST(CdcScanSource, IdleWaitUnderACoroutineIsPacedByTheTimer) {
    // Each pause is a yield that only the deadline timer, armed for the group's ready time,
    // resumes.
    FakeCoroutine coro;
    runIdleBackoff(coro.functors());
    ASSERT_GTE(coro.yields(), 3);
    ASSERT_EQ(coro.resumes(), coro.yields());
}

/**
 * @brief Runs the buffer-cap scenario: groups 0 and 1 buffer pages at 100 and above, group 2 is
 * idle, and group 3 lags with records at 1 to 10, so nothing else is released until it catches
 * up.
 *
 * @param bufferBytes The cap.
 * @param[out] idleScansWhileLagging Group 2's scans when group 3's last record is released.
 * @param[out] laggingScans Group 3's scans at that point.
 */
void runBufferCap(int64_t bufferBytes, uint64_t* idleScansWhileLagging, uint64_t* laggingScans) {
    const std::vector<uint32_t> groups{0, 1, 2, 3};
    auto log = std::make_shared<CdcFakeLogService>(groups);
    uint64_t txn = 1;
    for (uint64_t ts = 100; ts < 105; ++ts) {
        log->addData(0, ts, 1, txn++, 400);
        log->addData(1, ts, 1, txn++, 400);
    }
    for (uint64_t ts = 1; ts <= 10; ++ts) {
        log->addData(3, ts, 1, txn++, 400);
    }
    for (uint32_t group : groups) {
        log->setBound(group, 200);
    }

    CdcScanSourceOptions options = fastOptions();
    options.maxBytesPerScan = 500;  // one record per page
    options.bufferBytes = bufferBytes;
    // No idle pause, so that only the cap keeps the idle group from being scanned.
    options.idleBackoffInitial = Milliseconds(0);
    options.idleBackoffMax = Milliseconds(0);
    CdcScanSource source(log, options);
    source.startAllAt(1);

    std::vector<Released> out;
    for (;;) {
        boost::optional<CdcSourceRecord> record = source.next(Date_t::now() + Seconds(5), kNoCoro);
        ASSERT(record) << "stalled after " << out.size() << " records";
        out.push_back(describe(*record));
        if (out.back().group == 3 && out.back().ts == 10) {
            break;
        }
    }
    *idleScansWhileLagging = source.scansIssued(2);
    *laggingScans = source.scansIssued(3);

    for (const Released& r : drain(source, Milliseconds(20))) {
        out.push_back(r);
    }
    assertMergedByTimestamp(out);
    ASSERT_EQ(out.size(), 20u);
    ASSERT_EQ(source.bufferedBytes(), 0);
}

TEST(CdcScanSource, BufferCapHoldsBackEveryGroupButTheLaggingOne) {
    uint64_t idleScans = 0;
    uint64_t laggingScans = 0;
    // Two buffered pages reach the cap, so the idle group waits while the lagging one catches up.
    runBufferCap(800, &idleScans, &laggingScans);
    ASSERT_EQ(idleScans, 1u);
    ASSERT_GTE(laggingScans, 10u);

    // Without the cap binding, the same run scans the idle group over and over.
    runBufferCap(64 * 1024 * 1024, &idleScans, &laggingScans);
    ASSERT_GT(idleScans, 1u);
}

// ------------------------------------------------------------------------------------------------
// Start position
// ------------------------------------------------------------------------------------------------

TEST(CdcScanSource, FixStartWaitsForEveryGroupsBound) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1, 2});
    CdcScanSource source(log, fastOptions());

    // No group has completed a checkpoint round.
    ASSERT(!source.fixStart(Date_t::now() + Milliseconds(100), kNoCoro));
    const std::vector<txlog::CdcScanRequest> probes = log->requests(1);
    ASSERT_GTE(probes.size(), 2u);
    ASSERT_EQ(probes[0].from_ts(), std::numeric_limits<uint64_t>::max());
    // A group's first probe asks for its bound at once; once it has answered zero, it is
    // re-probed with a long poll.
    ASSERT_EQ(probes[0].wait_ms(), 0u);
    ASSERT_GT(probes[1].wait_ms(), 0u);

    log->addData(0, 7, 1, 1);
    log->addData(0, 8, 1, 2);
    log->addData(2, 9, 1, 3);
    log->setBound(0, 10);
    log->setBound(1, 7);
    log->setBound(2, 12);
    boost::optional<uint64_t> start = source.fixStart(Date_t::now() + Seconds(5), kNoCoro);
    ASSERT(start);
    ASSERT_EQ(*start, 7u);

    // The stream starts after S: the record at 7 predates it. Group 1's bound must pass 8 before
    // group 2's record at 9 may go.
    log->setBound(1, 12);
    const std::vector<Released> out = drain(source);
    ASSERT_EQ(out.size(), 2u);
    ASSERT_EQ(out[0], (Released{0, 8, false, 2}));
    ASSERT_EQ(out[1], (Released{2, 9, false, 3}));
}

TEST(CdcScanSource, FixStartReturnsWhenAHeldProbeAnswers) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    log->setBound(0, 5);
    log->setCompletion(1, Completion::kHeld);
    CdcScanSource source(log, fastOptions());

    std::thread answerer([log] {
        while (log->heldCount(1) == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        log->setBound(1, 3);
        log->releaseHeld(1);
    });
    const Date_t begin = Date_t::now();
    boost::optional<uint64_t> start = source.fixStart(begin + Seconds(10), kNoCoro);
    answerer.join();
    ASSERT(start);
    ASSERT_EQ(*start, 3u);
    ASSERT_LT(Date_t::now() - begin, Seconds(5));
}

// ------------------------------------------------------------------------------------------------
// Retries
// ------------------------------------------------------------------------------------------------

/**
 * @brief A one-group service with one record, and source options whose retry backoff is long
 * enough to measure.
 *
 * The group has two node IDs: 1, the leader, known both by hostname ("a:1") and by IP address
 * ("10.0.0.1:1"), as the log service may name it either way, and 2 ("b:2").
 */
struct RetryFixture {
    RetryFixture() : log(std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0})) {
        log->addNode(0, 1, {"a:1", "10.0.0.1:1"});
        log->addNode(0, 2, {"b:2"});
        log->setLeader(0, 1);
        log->addData(0, 5, 1, 1);
        log->setBound(0, 10);
        options = fastOptions();
        options.client.retryBackoffInitial = kBackoff;
        options.client.retryBackoffMax = kBackoff * 2;
    }

    /**
     * @brief Pulls the record and measures how long it took.
     *
     * @param expectedScans How many scans it should take.
     * @return The time from the call to the record.
     */
    Milliseconds timeToRecord(uint64_t expectedScans = 2) {
        CdcScanSource source(log, options);
        source.startAllAt(1);
        const Date_t start = Date_t::now();
        boost::optional<CdcSourceRecord> record = source.next(start + Seconds(10), kNoCoro);
        const Milliseconds elapsed = Date_t::now() - start;
        ASSERT(record);
        ASSERT_EQ(describe(*record), (Released{0, 5, false, 1}));
        ASSERT_EQ(source.scansIssued(0), expectedScans);
        return elapsed;
    }

    static constexpr Milliseconds kBackoff{200};
    std::shared_ptr<CdcFakeLogService> log;
    CdcScanSourceOptions options;
};

constexpr Milliseconds RetryFixture::kBackoff;

TEST(CdcScanClient, NotLeaderNamingTheSameNodeBacksOff) {
    RetryFixture f;
    f.log->notLeaderNext(0, "a:1");  // a leader not yet able to serve
    ASSERT_GTE(f.timeToRecord(), RetryFixture::kBackoff - Milliseconds(10));
    ASSERT(f.log->nodesFollowed(0).empty());
    ASSERT_EQ(f.log->refreshCount(0), 0);
}

TEST(CdcScanClient, NotLeaderNamingTheSameNodeByAnotherAddressBacksOff) {
    RetryFixture f;
    // The node just asked, named by its IP address rather than its hostname: still the same node,
    // so it is neither followed nor retried at once.
    f.log->notLeaderNext(0, "10.0.0.1:1");
    ASSERT_GTE(f.timeToRecord(), RetryFixture::kBackoff - Milliseconds(10));
    ASSERT(f.log->nodesFollowed(0).empty());
    ASSERT_EQ(f.log->refreshCount(0), 0);
}

TEST(CdcScanClient, NotLeaderWithNoRedirectBacksOff) {
    RetryFixture f;
    f.log->notLeaderNext(0, "");
    ASSERT_GTE(f.timeToRecord(), RetryFixture::kBackoff - Milliseconds(10));
    ASSERT(f.log->nodesFollowed(0).empty());
}

TEST(CdcScanClient, NotLeaderNamingAnotherNodeRetriesThereAtOnce) {
    RetryFixture f;
    f.log->notLeaderNext(0, "b:2");
    ASSERT_LT(f.timeToRecord(), RetryFixture::kBackoff);
    const std::vector<uint32_t> followed = f.log->nodesFollowed(0);
    ASSERT_EQ(followed.size(), 1u);
    ASSERT_EQ(followed[0], 2u);
    const std::vector<uint32_t> targets = f.log->targets(0);
    ASSERT_EQ(targets.size(), 2u);
    ASSERT_EQ(targets[0], 1u);
    ASSERT_EQ(targets[1], 2u);
}

TEST(CdcScanClient, TwoNodesNamingEachOtherAreFollowedAtTheBackoffRate) {
    RetryFixture f;
    // During an election each of the two nodes names the other.
    f.log->notLeaderNext(0, "b:2");
    f.log->notLeaderNext(0, "a:1");
    f.log->notLeaderNext(0, "b:2");
    // The first hop is taken at once, the second after one backoff, the third after the doubled
    // one; without the cap all three would take no time at all.
    ASSERT_GTE(f.timeToRecord(4), RetryFixture::kBackoff * 3 - Milliseconds(10));
    const std::vector<uint32_t> followed = f.log->nodesFollowed(0);
    ASSERT_EQ(followed.size(), 3u);
    ASSERT_EQ(followed[0], 2u);
    ASSERT_EQ(followed[1], 1u);
    ASSERT_EQ(followed[2], 2u);
    ASSERT_EQ(f.log->refreshCount(0), 0);
}

TEST(CdcScanClient, NotLeaderNamingAnUnknownNodeRefreshesAndBacksOff) {
    RetryFixture f;
    f.log->notLeaderNext(0, "c:3");
    ASSERT_GTE(f.timeToRecord(), RetryFixture::kBackoff - Milliseconds(10));
    ASSERT(f.log->nodesFollowed(0).empty());
    ASSERT_EQ(f.log->refreshCount(0), 1);
}

TEST(CdcScanClient, TransportErrorRefreshesAndBacksOff) {
    RetryFixture f;
    f.log->failNext(0);
    ASSERT_GTE(f.timeToRecord(), RetryFixture::kBackoff - Milliseconds(10));
    ASSERT_EQ(f.log->refreshCount(0), 1);
}

TEST(CdcScanSource, LogStateWithoutHistoryFailsTheStream) {
    // The MEMORY log state answers every scan with ENOMETHOD; retrying would never succeed, so
    // the stream fails at once instead of waiting out its deadline.
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 10);
    log->setFailureCode(0, brpc::ENOMETHOD);
    CdcScanSource source(log, fastOptions());
    source.startAllAt(1);
    const Date_t start = Date_t::now();
    ASSERT_THROWS_CODE(source.next(start + Seconds(10), kNoCoro),
                       AssertionException,
                       ErrorCodes::CommandNotSupported);
    ASSERT_LT(Date_t::now() - start, Seconds(5));
}

TEST(CdcScanSource, LogStateWithoutHistoryFailsTheOpen) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setFailureCode(0, brpc::ENOMETHOD);
    CdcScanSource source(log, fastOptions());
    const Date_t start = Date_t::now();
    ASSERT_THROWS_CODE(source.fixStart(start + Seconds(10), kNoCoro),
                       AssertionException,
                       ErrorCodes::CommandNotSupported);
    ASSERT_LT(Date_t::now() - start, Seconds(5));
}

TEST(CdcScanSource, PositionBelowTheRetentionFloorLosesHistory) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 10);
    log->setRetentionFloor(0, 5);
    CdcScanSource source(log, fastOptions());
    source.startAllAt(3);
    ASSERT_THROWS_CODE(source.next(Date_t::now() + Seconds(5), kNoCoro),
                       AssertionException,
                       ErrorCodes::ChangeStreamHistoryLost);
}

// ------------------------------------------------------------------------------------------------
// Call lifetime
// ------------------------------------------------------------------------------------------------

TEST(CdcScanSource, DestroyingTheSourceCancelsItsCalls) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 10);
    log->setCompletion(0, Completion::kHeld);
    {
        CdcScanSource source(log, fastOptions());
        source.startAllAt(1);
        ASSERT(!source.next(Date_t::now() + Milliseconds(20), kNoCoro));
        ASSERT_EQ(log->heldCount(0), 1u);
    }
    ASSERT_EQ(log->cancelCount(), 1);
    ASSERT_EQ(log->heldCount(0), 0u);
}

TEST(CdcScanSource, CallbackAfterTheSourceIsGoneIsHarmless) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->addData(0, 5, 1, 1);
    log->setBound(0, 10);
    log->setCompletion(0, Completion::kHeld);
    log->setCancelRunsDone(false);
    {
        CdcScanSource source(log, fastOptions());
        source.startAllAt(1);
        ASSERT(!source.next(Date_t::now() + Milliseconds(20), kNoCoro));
    }
    ASSERT_EQ(log->cancelCount(), 1);
    ASSERT_EQ(log->heldCount(0), 1u);
    // The call's state outlives the source: the answer lands in it and the closure drops it.
    log->releaseHeld(0);
    ASSERT_EQ(log->heldCount(0), 0u);
}

}  // namespace
}  // namespace mongo
