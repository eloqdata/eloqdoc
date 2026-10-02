#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault;

#include "mongo/db/coro_sync.h"

#include <algorithm>
#include <bthread/unstable.h>
#include <butil/time.h>

#include "mongo/db/client.h"
#include "mongo/db/local_thread_state.h"
#include "mongo/util/log.h"

namespace mongo {

const CoroutineFunctors CoroutineFunctors::Unavailable{};

namespace coro {

namespace {

/**
 * @brief The bthread timer callback of sleepUntil: resumes the sleeping coroutine.
 *
 * The resume functor only queues the coroutine on its thread group, so it is safe from the timer
 * thread, and safe even if it runs before the coroutine has yielded: the coroutine runs again
 * right after it yields.
 *
 * @param arg The coroutine's resume functor.
 */
void resumeSleeper(void* arg) {
    (*static_cast<const std::function<void()>*>(arg))();
}

}  // namespace

const CoroutineFunctors* currentCoroutine() {
    if (LocalThread::ID() == -1) {
        return nullptr;
    }
    Client* client = Client::getCurrent();
    if (!client) {
        return nullptr;
    }
    const CoroutineFunctors& coro = client->coroutineFunctors();
    if (coro == CoroutineFunctors::Unavailable || !coro.resumeFuncPtr || !*coro.resumeFuncPtr) {
        return nullptr;
    }
    return &coro;
}

void sleepUntil(Date_t deadline) {
    const Date_t now = Date_t::now();
    if (deadline <= now) {
        return;
    }
    const CoroutineFunctors* coro = currentCoroutine();
    if (!coro) {
        sleepmillis(durationCount<Milliseconds>(deadline - now));
        return;
    }
    const int64_t ms = std::max<int64_t>(1, durationCount<Milliseconds>(deadline - now));
    bthread_timer_t timer;
    if (bthread_timer_add(&timer,
                          butil::milliseconds_from_now(ms),
                          resumeSleeper,
                          const_cast<std::function<void()>*>(coro->resumeFuncPtr)) != 0) {
        // No timer: give the thread group a turn until the deadline instead, or, without a way
        // to requeue the coroutine, sleep.
        if (!coro->longResumeFuncPtr || !*coro->longResumeFuncPtr) {
            sleepmillis(ms);
            return;
        }
        while (Date_t::now() < deadline) {
            (*coro->longResumeFuncPtr)();
            (*coro->yieldFuncPtr)();
        }
        return;
    }
    // Only the timer resumes a coroutine that yields here, once.
    (*coro->yieldFuncPtr)();
}

void Mutex::lock() {
    if (LocalThread::ID() != -1) {
        Client* client = Client::getCurrent();
        if (client) {
            const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
            if (coro != CoroutineFunctors::Unavailable) {
                // By C++ standard, the return value of std::mutex::try_lock is undefined when
                // called repeatedly. By GCC/Clang implementation, the return value of
                // std::mutex::try_lock is definitely false when called repeatedly.
                while (!_mux.try_lock()) {
                    (*coro.longResumeFuncPtr)();
                    (*coro.yieldFuncPtr)();
                }
            } else {
                MONGO_LOG(1)
                    << "ThreadGroup " << LocalThread::ID()
                    << " call std::mutex::lock because the coroutine context is unavailable.";
                _mux.lock();
            }
        } else {
            MONGO_LOG(1) << "ThreadGroup " << LocalThread::ID()
                         << " call std::mutex::lock because the client object is unavailable.";
            _mux.lock();
        }
    } else {
        _mux.lock();
    }
}

void ConditionVariable::wait(std::unique_lock<Mutex>& lock) {
    invariant(lock.owns_lock());
    if (!_yieldIfCoroutine(lock)) {
        _cv.wait(reinterpret_cast<std::unique_lock<std::mutex>&>(lock));
    }
}

bool ConditionVariable::_yieldIfCoroutine(std::unique_lock<Mutex>& lock) {
    if (LocalThread::ID() == -1 || !Client::getCurrent())
        return false;
    const auto& coro = Client::getCurrent()->coroutineFunctors();
    if (coro == CoroutineFunctors::Unavailable)
        return false;
    lock.unlock();
    (*coro.longResumeFuncPtr)();
    (*coro.yieldFuncPtr)();
    lock.lock();
    return true;
}
}  // namespace coro
}  // namespace mongo
