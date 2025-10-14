#pragma once

#include <condition_variable>
#include <mutex>

#include "mongo/util/time_support.h"

namespace mongo {

struct CoroutineFunctors {
    const std::function<void()>* yieldFuncPtr{nullptr};
    const std::function<void()>* resumeFuncPtr{nullptr};
    const std::function<void()>* longResumeFuncPtr{nullptr};
    const std::function<void(uint16_t)>* migrateThreadGroupFuncPtr{nullptr};

    const static CoroutineFunctors Unavailable;

    friend bool operator==(const CoroutineFunctors& lhs, const CoroutineFunctors& rhs) {
        return lhs.yieldFuncPtr == rhs.yieldFuncPtr && lhs.resumeFuncPtr == rhs.resumeFuncPtr &&
            lhs.longResumeFuncPtr == rhs.longResumeFuncPtr &&
            lhs.migrateThreadGroupFuncPtr == rhs.migrateThreadGroupFuncPtr;
    }
    friend bool operator!=(const CoroutineFunctors& lhs, const CoroutineFunctors& rhs) {
        return !(lhs == rhs);
    }
};

namespace coro {
/**
 * coro::Mutex can be used in both pthread/coroutine.
 * coro::Mutex can be cast to std::mutex.
 *
 * coro::ConditionVariable can be used in both pthread/coroutine.
 * coro::ConditionVariable can be cast to std::condition_variable.
 *
 * Unlike boost::fiber, EloqDoc is busy-loop, and there is no waiting queue for blocking
 * coroutines.
 */

class Mutex {
public:
    void lock();
    void unlock() {
        _mux.unlock();
    }

    bool try_lock() {
        return _mux.try_lock();
    }

private:
    std::mutex _mux;
};

class ConditionVariable {
public:
    void notify_one() noexcept {
        _cv.notify_one();
    }
    void notify_all() noexcept {
        _cv.notify_all();
    }
    void wait(std::unique_lock<Mutex>& lock);

    template <class Predicate>
    void wait(std::unique_lock<Mutex>& lock, Predicate stop_waiting) {
        while (!stop_waiting()) {
            wait(lock);
        }
    }

    template <class Clock, class Duration>
    std::cv_status wait_until(std::unique_lock<Mutex>& lock,
                              const std::chrono::time_point<Clock, Duration>& timeout_time) {
        wait(lock);
        return Clock::now() < timeout_time ? std::cv_status::no_timeout : std::cv_status::timeout;
    }

    template <class Clock, class Duration, class Predicate>
    bool wait_until(std::unique_lock<Mutex>& lock,
                    const std::chrono::time_point<Clock, Duration>& timeout_time,
                    Predicate stop_waiting) {
        while (!stop_waiting()) {
            if (wait_until(lock, timeout_time) == std::cv_status::timeout) {
                return stop_waiting();
            }
        }
        return true;
    }

    template <class Rep, class Period>
    std::cv_status wait_for(std::unique_lock<Mutex>& lock,
                            const std::chrono::duration<Rep, Period>& rel_time) {
        return wait_until(lock, std::chrono::steady_clock::now() + rel_time);
    }

    template <class Rep, class Period, class Predicate>
    bool wait_for(std::unique_lock<Mutex>& lock,
                  const std::chrono::duration<Rep, Period>& rel_time,
                  Predicate stop_waiting) {
        return wait_until(
            lock, std::chrono::steady_clock::now() + rel_time, std::move(stop_waiting));
    }

    std::cv_status wait_until(std::unique_lock<Mutex>& lock, Date_t timeout_time) {
        wait(lock);
        return Date_t::now() < timeout_time ? std::cv_status::no_timeout : std::cv_status::timeout;
    }

private:
    std::condition_variable _cv;
};
}  // namespace coro
}  // namespace mongo
