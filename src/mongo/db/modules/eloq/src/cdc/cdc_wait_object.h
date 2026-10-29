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
 * The per-cursor wait object that stands between the CDC scan path's RPC callbacks and the
 * coroutine waiting for them (docs/cdc-change-stream.md §5.8, "Cancellation ownership").
 */

#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

#include "mongo/db/coro_sync.h"
#include "mongo/util/time_support.h"

namespace mongo {

/**
 * @brief Where scan completions meet the coroutine that waits for them.
 *
 * An RPC callback never holds or resumes a coroutine: the getMore that issued the scan may have
 * returned on its deadline, and resuming a finished coroutine would be a use-after-free (F11,
 * §5.8). The callback calls notify() instead, and only this object resumes anything: a waiter
 * registers before it yields, notify() resumes it if one is registered, and otherwise records
 * that something arrived, so the next wait returns at once. The deadline timer notifies through
 * the same object.
 *
 * A waiter registered here is resumed exactly once. The resume functor only queues the coroutine
 * on its thread group, so a notify() that lands between registration and the yield is safe: the
 * coroutine runs again right after it yields.
 *
 * Callers without coroutine functors (unit tests, plain threads) block on a condition variable
 * instead.
 *
 * Owned through shared_ptr by the cursor's scan state and by every RPC callback and timer that may
 * still notify it, so a late notify() always finds it alive.
 */
class CdcWaitObject : public std::enable_shared_from_this<CdcWaitObject> {
public:
    /**
     * @brief Creates a wait object; the only way to make one, since the deadline timer needs a
     * shared_ptr to it.
     *
     * @return The new wait object.
     */
    static std::shared_ptr<CdcWaitObject> make();

    CdcWaitObject(const CdcWaitObject&) = delete;
    CdcWaitObject& operator=(const CdcWaitObject&) = delete;

    /**
     * @brief Wakes the waiter, or records a wake-up for the next wait if none is waiting.
     *
     * Safe from any thread, including an RPC callback and a bthread timer.
     */
    void notify();

    /**
     * @brief Returns once notify() has been called since the previous wait returned, or at
     * 'deadline'.
     *
     * A wake-up can be spurious (a timer that fired just as a response arrived), so callers
     * re-check their own state after every return.
     *
     * @param deadline When to return if nothing arrives; Date_t::max() waits indefinitely.
     * @param coro The calling coroutine's functors, or CoroutineFunctors::Unavailable to block
     * the calling thread instead.
     */
    void waitUntil(Date_t deadline, const CoroutineFunctors& coro);

private:
    CdcWaitObject() = default;

    /**
     * @brief The coroutine form of waitUntil: registers, arms the deadline timer, yields.
     *
     * @param lk The held lock on _mutex; released before yielding.
     * @param deadline When the timer notifies.
     * @param coro The calling coroutine's functors.
     */
    void _yieldUntil(std::unique_lock<std::mutex> lk,
                     Date_t deadline,
                     const CoroutineFunctors& coro);

    std::mutex _mutex;
    std::condition_variable _cv;     ///< Wakes a thread blocked in waitUntil without coroutines.
    bool _notified = false;          ///< A notify() arrived while nobody waited.
    bool _coroutineWaiting = false;  ///< A coroutine is registered.
    const std::function<void()>* _resume = nullptr;  ///< Its resume functor, while registered.
};

}  // namespace mongo
