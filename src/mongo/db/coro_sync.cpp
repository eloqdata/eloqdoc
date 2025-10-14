#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault;

#include "mongo/db/coro_sync.h"
#include "mongo/db/client.h"
#include "mongo/db/local_thread_state.h"
#include "mongo/util/log.h"

#ifdef ELOQ_MODULE_ENABLED
#include "bthread/bthread.h"
#endif

namespace mongo {

const CoroutineFunctors CoroutineFunctors::Unavailable{};

namespace coro {

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
#ifndef ELOQ_MODULE_ENABLED
                MONGO_LOG(2)
                    << "ThreadGroup " << LocalThread::ID()
                    << " call std::mutex::lock because the coroutine context is unavailable.";
                _mux.lock();
#else
                MONGO_LOG(2) << "ThreadGroup " << LocalThread::ID()
                             << " call bthread_yield because the coroutine context is unavailable.";
                while (!_mux.try_lock()) {
                    bthread_yield();
                }
#endif
            }
        } else {
#ifndef ELOQ_MODULE_ENABLED
            MONGO_LOG(2) << "ThreadGroup " << LocalThread::ID()
                         << " call std::mutex::lock because the client object is unavailable.";
            _mux.lock();
#else
            MONGO_LOG(2) << "ThreadGroup " << LocalThread::ID()
                         << " call bthread_yield because the client object is unavailable.";
            while (!_mux.try_lock()) {
                bthread_yield();
            }
#endif
        }
    } else {
        _mux.lock();
    }
}

void ConditionVariable::wait(std::unique_lock<Mutex>& lock) {
    invariant(lock.owns_lock());
    if (LocalThread::ID() != -1) {
        Client* client = Client::getCurrent();
        if (client) {
            const CoroutineFunctors& coro = Client::getCurrent()->coroutineFunctors();
            if (coro != CoroutineFunctors::Unavailable) {
                lock.unlock();
                (*coro.longResumeFuncPtr)();
                (*coro.yieldFuncPtr)();
                lock.lock();
            } else {
#ifndef ELOQ_MODULE_ENABLED
                MONGO_LOG(2) << "ThreadGroup " << LocalThread::ID()
                             << " call std::condition_variable::wait because the coroutine context "
                                "is unavailable.";
                _cv.wait(reinterpret_cast<std::unique_lock<std::mutex>&>(lock));
#else
                MONGO_LOG(2) << "ThreadGroup " << LocalThread::ID()
                             << " call bthread_yield because the coroutine context is unavailable.";
                lock.unlock();
                bthread_yield();
                lock.lock();
#endif
            }
        } else {
#ifndef ELOQ_MODULE_ENABLED
            MONGO_LOG(2)
                << "ThreadGroup " << LocalThread::ID()
                << " call std::condition_variable::wait because the client object is unavailable.";
            _cv.wait(reinterpret_cast<std::unique_lock<std::mutex>&>(lock));
#else
            MONGO_LOG(2) << "ThreadGroup " << LocalThread::ID()
                         << " call bthread_yield because the client object is unavailable.";
            lock.unlock();
            bthread_yield();
            lock.lock();
#endif
        }

    } else {
        _cv.wait(reinterpret_cast<std::unique_lock<std::mutex>&>(lock));
    }
}
}  // namespace coro
}  // namespace mongo
