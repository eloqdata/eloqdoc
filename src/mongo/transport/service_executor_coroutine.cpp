#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kExecutor;

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <tuple>

#include "mongo/base/string_data.h"
#include "mongo/db/local_thread_state.h"
#include "mongo/db/server_parameters.h"
#include "mongo/transport/service_entry_point_utils.h"
#include "mongo/transport/service_executor_coroutine.h"
#include "mongo/transport/service_executor_task_names.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/log.h"

#include <bthread/bthread.h>

#ifndef EXT_TX_PROC_ENABLED
#define EXT_TX_PROC_ENABLED
#endif

namespace mongo {

extern std::function<std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    getTxServiceFunctors;

namespace transport {
// namespace {

// // Tasks scheduled with MayRecurse may be called recursively if the recursion depth is below this
// // value.
// MONGO_EXPORT_SERVER_PARAMETER(reservedServiceExecutorRecursionLimit, int, 8);

// constexpr auto kThreadsRunning = "threadsRunning"_sd;
// constexpr auto kExecutorLabel = "executor"_sd;
// constexpr auto kExecutorName = "reserved"_sd;
// constexpr auto kReadyThreads = "readyThreads"_sd;
// constexpr auto kStartingThreads = "startingThreads"_sd;
// }  // namespace

#ifdef ELOQ_MODULE_ENABLED
void MongoModule::ExtThdStart(int thd_id) {
    MONGO_LOG(3) << "MongoModule::ExtThdStart " << thd_id;
    ThreadGroup& threadGroup = _threadGroups[thd_id];
    invariant(threadGroup._threadGroupId == thd_id);
    if (!threadGroup._threadNameSet) {
        setThreadName(str::stream() << "thread_group_" << thd_id);
        threadGroup._threadNameSet = true;
    }
    threadGroup._extWorkerActive.store(true, std::memory_order_release);
}

void MongoModule::ExtThdEnd(int thd_id) {
    MONGO_LOG(3) << "MongoModule::ExtThdEnd " << thd_id;
    ThreadGroup& threadGroup = _threadGroups[thd_id];
    invariant(threadGroup._threadGroupId == thd_id);
    threadGroup._extWorkerActive.store(false, std::memory_order_release);
}

void MongoModule::Process(int thd_id) {
    MONGO_LOG(3) << "MongoModule::Process " << thd_id;
    ThreadGroup& threadGroup = _threadGroups[thd_id];
    invariant(threadGroup._threadGroupId == thd_id);
    size_t cnt = 0;
    // process resume task
    cnt = threadGroup._resumeQueue.TryDequeueBulk(
        std::make_move_iterator(threadGroup._taskBulk.begin()), threadGroup._taskBulk.size());
    for (size_t i = 0; i < cnt; ++i) {
        threadGroup._taskBulk[i]();
    }

    // process normal task
    if (cnt < threadGroup._taskBulk.size()) {
        cnt = threadGroup._taskQueue.TryDequeueBulk(
            std::make_move_iterator(threadGroup._taskBulk.begin()), threadGroup._taskBulk.size());
        for (size_t i = 0; i < cnt; ++i) {
            threadGroup._taskBulk[i]();
        }
    }
}

bool MongoModule::HasTask(int thd_id) const {
    ThreadGroup& threadGroup = _threadGroups[thd_id];
    invariant(threadGroup._threadGroupId == thd_id);
    return threadGroup.isBusy();
}
#endif

void ThreadGroup::enqueueTask(Task task) {
    _taskQueue.Enqueue(std::move(task));
    notifyIfAsleep();
}

void ThreadGroup::resumeTask(Task task) {
    _resumeQueue.Enqueue(std::move(task));
    notifyIfAsleep();
}

void ThreadGroup::notifyIfAsleep() {
#ifndef ELOQ_MODULE_ENABLED
    if (_isSleep.load(std::memory_order_relaxed)) {
        std::unique_lock<std::mutex> lk(_sleepMutex);
        _sleepCV.notify_one();
    }
#else
    if (!_extWorkerActive.load(std::memory_order_relaxed)) {
        MongoModule::Instance()->NotifyWorker(_threadGroupId);
    }
#endif
}

void ThreadGroup::setTxServiceFunctors() {
    std::tie(_txProcessorExec, _updateExtProc) = getTxServiceFunctors(_threadGroupId);
}

bool ThreadGroup::isBusy() const {
    return _ongoingCoroutineCnt > 0 || !_taskQueue.IsEmpty() || !_resumeQueue.IsEmpty();
}

#ifndef ELOQ_MODULE_ENABLED
void ThreadGroup::trySleep() {
    // If there are tasks in the , does not sleep.
    // if (isBusy()) {
    //     return;
    // }

    // MONGO_LOG(0) << "idle";
    // wait for kTrySleepTimeOut at most
    // _tickCnt.store(0, std::memory_order_release);
    // while (_tickCnt.load(std::memory_order_relaxed) < kTrySleepTimeOut) {
    //     if (isBusy()) {
    //         return;
    //     }
    // }

    // Sets the sleep flag before entering the critical section. std::memory_order_relaxed is
    // good enough, because the following mutex ensures that this instruction happens before the
    // critical section
    _isSleep.store(true, std::memory_order_relaxed);

    std::unique_lock<std::mutex> lk(_sleepMutex);

    // Double checkes again in the critical section before going to sleep. If additional tasks
    // are enqueued, does not sleep.
    if (isBusy()) {
        _isSleep.store(false, std::memory_order_relaxed);
        return;
    }

    MONGO_LOG(0) << "sleep";
#ifdef EXT_TX_PROC_ENABLED
    _updateExtProc(-1);
#endif
    _sleepCV.wait(lk, [this] { return isBusy(); });

    // Woken up from sleep.
#ifdef EXT_TX_PROC_ENABLED
    _updateExtProc(1);
#endif
    _isSleep.store(false, std::memory_order_relaxed);
}

void ThreadGroup::terminate() {
    _isTerminated.store(true, std::memory_order_relaxed);
    std::unique_lock<std::mutex> lk(_sleepMutex);
    _sleepCV.notify_one();
}
#endif

ServiceExecutorCoroutine::ServiceExecutorCoroutine(ServiceContext* ctx, size_t reservedThreads)
    : _reservedThreads(reservedThreads), _threadGroups(reservedThreads) {
    bthread_setconcurrency(reservedThreads);
    for (int16_t thdGroupId = 0; thdGroupId < reservedThreads; ++thdGroupId) {
        _threadGroups[thdGroupId].setThreadGroupID(thdGroupId);
    }
}

Status ServiceExecutorCoroutine::start() {
    MONGO_LOG(0) << "ServiceExecutorCoroutine::start";
#ifndef ELOQ_MODULE_ENABLED
    for (size_t i = 0; i < _reservedThreads; i++) {
        auto status = _startWorker(static_cast<int16_t>(i));
        if (!status.isOK()) {
            return status;
        }
    }
#else
    MongoModule::Instance()->Init(_threadGroups.data());
    int rc = eloq::register_module(MongoModule::Instance());
    invariant(rc == 0);
#endif
    _stillRunning.store(true, std::memory_order_release);
    return Status::OK();
}

#ifndef ELOQ_MODULE_ENABLED
Status ServiceExecutorCoroutine::_startWorker(int16_t groupId) {
    MONGO_LOG(0) << "Starting new worker thread for " << _name << " service executor. "
                 << " group id: " << groupId;

    return launchServiceWorkerThread([this, threadGroupId = groupId] {
        while (!_stillRunning.load(std::memory_order_acquire)) {
        }
        LocalThread::SetID(threadGroupId);
        setThreadName(str::stream() << "thread_group_" << threadGroupId);

        // std::unique_lock<stdx::mutex> lk(_mutex);
        // _numRunningWorkerThreads.addAndFetch(1);
        // auto numRunningGuard = MakeGuard([&] {
        //     _numRunningWorkerThreads.subtractAndFetch(1);
        //     _shutdownCondition.notify_one();
        // });
        // lk.unlock();

        ThreadGroup& threadGroup = _threadGroups[threadGroupId];

#ifdef EXT_TX_PROC_ENABLED
        threadGroup.setTxServiceFunctors();
        MONGO_LOG(0) << "threadGroup._updateExtProc(1)";
        threadGroup._updateExtProc(1);
#endif

        auto& taskBulk = threadGroup._taskBulk;

        size_t idleCnt = 0;
        std::chrono::steady_clock::time_point idleStartTime;
        while (_stillRunning.load(std::memory_order_relaxed)) {
            if (!_stillRunning.load(std::memory_order_relaxed)) {
                break;
            }

            size_t cnt = 0;
            // process resume task
            cnt = threadGroup._resumeQueue.TryDequeueBulk(std::make_move_iterator(taskBulk.begin()),
                                                          taskBulk.size());
            for (size_t i = 0; i < cnt; ++i) {
                // setThreadName(threadNameSD);
                taskBulk[i]();
            }

            // process normal task
            if (cnt < taskBulk.size()) {
                cnt = threadGroup._taskQueue.TryDequeueBulk(
                    std::make_move_iterator(taskBulk.begin()), taskBulk.size());
                for (size_t i = 0; i < cnt; ++i) {
                    // setThreadName(threadNameSD);
                    taskBulk[i]();
                }
            }
#ifdef EXT_TX_PROC_ENABLED
            // process as a TxProcessor
            (threadGroup._txProcessorExec)();
#endif
            if (cnt == 0) {
                if (idleCnt == 0) {
                    idleStartTime = std::chrono::steady_clock::now();
                    MONGO_LOG(3) << "idleStartTime " << idleStartTime.time_since_epoch().count();
                }
                idleCnt++;
                if ((idleCnt & kIdleCycle) == 0) {
                    // check timeout
                    auto interval = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - idleStartTime)
                                        .count();
                    if (interval > kIdleTimeoutMs) {
                        threadGroup.trySleep();
                    }
                }
            } else {
                idleCnt = 0;
            }
        }

        MONGO_LOG(0) << "Exiting worker thread in " << _name << " service executor";
    });
}
#endif

Status ServiceExecutorCoroutine::shutdown(Milliseconds timeout) {
    MONGO_LOG(0) << "Shutting down coroutine executor";
    _stillRunning.store(false, std::memory_order_release);
#ifndef ELOQ_MODULE_ENABLED
    for (ThreadGroup& thd_group : _threadGroups) {
        thd_group.terminate();
    }
#else
    int rc = eloq::unregister_module(MongoModule::Instance());
    invariant(rc == 0);
#endif
    return Status::OK();
}

Status ServiceExecutorCoroutine::schedule(Task task,
                                          ScheduleFlags flags,
                                          ServiceExecutorTaskName taskName) {
    return schedule(task, flags, taskName, 0);
}

Status ServiceExecutorCoroutine::schedule(Task task,
                                          ScheduleFlags flags,
                                          ServiceExecutorTaskName taskName,
                                          uint16_t threadGroupId) {
    MONGO_LOG(3) << "schedule with group id: " << threadGroupId;
    if (!_stillRunning.load(std::memory_order_relaxed)) {
        return Status{ErrorCodes::ShutdownInProgress, "Executor is not running"};
    }

    _threadGroups[threadGroupId].enqueueTask(std::move(task));

    return Status::OK();
}


std::function<void()> ServiceExecutorCoroutine::coroutineResumeFunctor(uint16_t threadGroupId,
                                                                       const Task& task) {
    invariant(threadGroupId < _threadGroups.size());
    return [thd_group = &_threadGroups[threadGroupId], &task]() { thd_group->resumeTask(task); };
}

std::function<void()> ServiceExecutorCoroutine::coroutineLongResumeFunctor(uint16_t threadGroupId,
                                                                           const Task& task) {
    invariant(threadGroupId < _threadGroups.size());
    return [thd_group = &_threadGroups[threadGroupId], &task]() { thd_group->enqueueTask(task); };
}

void ServiceExecutorCoroutine::ongoingCoroutineCountUpdate(uint16_t threadGroupId, int delta) {
    _threadGroups[threadGroupId]._ongoingCoroutineCnt += delta;
}

void ServiceExecutorCoroutine::appendStats(BSONObjBuilder* bob) const {
    // stdx::lock_guard<stdx::mutex> lk(_mutex);
    // *bob << kExecutorLabel << kExecutorName << kThreadsRunning
    //      << static_cast<int>(_numRunningWorkerThreads.loadRelaxed()) << kReadyThreads;
    //  << static_cast<int>(_numReadyThreads) << kStartingThreads
    //  << static_cast<int>(_numStartingThreads);
}
}  // namespace transport
}  // namespace mongo
