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

#include "mongo/db/modules/eloq/src/cdc/cdc_wait_object.h"

#include <bthread/unstable.h>
#include <butil/time.h>

#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

/**
 * @brief What a deadline timer carries: a reference that keeps the wait object alive until the
 * timer has either fired or been deleted.
 */
struct DeadlineTimerArg {
    std::shared_ptr<CdcWaitObject> waitObject;  ///< The object the timer notifies.
};

/**
 * @brief bthread timer callback: notifies the wait object and releases the timer's reference.
 *
 * @param arg The DeadlineTimerArg the timer was armed with; owned by the callback once it runs.
 */
void onDeadline(void* arg) {
    std::unique_ptr<DeadlineTimerArg> timerArg(static_cast<DeadlineTimerArg*>(arg));
    timerArg->waitObject->notify();
}

}  // namespace

std::shared_ptr<CdcWaitObject> CdcWaitObject::make() {
    return std::shared_ptr<CdcWaitObject>(new CdcWaitObject());
}

void CdcWaitObject::notify() {
    const std::function<void()>* resume = nullptr;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        if (_coroutineWaiting) {
            // Unregister before resuming: the waiter is resumed exactly once, and a later notify
            // is recorded for the next wait instead.
            _coroutineWaiting = false;
            resume = _resume;
            _resume = nullptr;
        } else {
            _notified = true;
        }
    }
    if (resume) {
        (*resume)();
    } else {
        _cv.notify_all();
    }
}

void CdcWaitObject::waitUntil(Date_t deadline, const CoroutineFunctors& coro) {
    std::unique_lock<std::mutex> lk(_mutex);
    if (_notified) {
        _notified = false;
        return;
    }
    if (Date_t::now() >= deadline) {
        return;
    }
    if (coro.yieldFuncPtr && coro.resumeFuncPtr) {
        _yieldUntil(std::move(lk), deadline, coro);
        return;
    }
    if (deadline == Date_t::max()) {
        _cv.wait(lk, [this] { return _notified; });
    } else {
        _cv.wait_until(lk, deadline.toSystemTimePoint(), [this] { return _notified; });
    }
    _notified = false;
}

void CdcWaitObject::_yieldUntil(std::unique_lock<std::mutex> lk,
                                Date_t deadline,
                                const CoroutineFunctors& coro) {
    invariant(!_coroutineWaiting);

    bthread_timer_t timer;
    DeadlineTimerArg* timerArg = nullptr;
    if (deadline != Date_t::max()) {
        const int64_t remainingMs =
            std::max<int64_t>(0, durationCount<Milliseconds>(deadline - Date_t::now()));
        timerArg = new DeadlineTimerArg{shared_from_this()};
        if (bthread_timer_add(
                &timer, butil::milliseconds_from_now(remainingMs), onDeadline, timerArg) != 0) {
            // Without a timer nothing would bound the wait; return and let the caller loop.
            delete timerArg;
            return;
        }
    }

    _coroutineWaiting = true;
    _resume = coro.resumeFuncPtr;
    lk.unlock();
    (*coro.yieldFuncPtr)();

    // Resumed by notify(), which unregistered this waiter. A timer deleted before it ran never
    // released its reference; one that ran, or is running, releases it itself.
    if (timerArg && bthread_timer_del(timer) == 0) {
        delete timerArg;
    }
}

}  // namespace mongo
