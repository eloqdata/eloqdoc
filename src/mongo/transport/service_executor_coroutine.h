#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <string_view>

#include "mongo/base/status.h"
#include "mongo/db/modules/eloq/data_substrate/tx_service/include/concurrent_queue_wsize.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/transport/service_executor.h"
#include "mongo/transport/service_executor_task_names.h"

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/eloq_module.h>
#endif
#include <bthread/moodycamelqueue.h>

namespace mongo::transport {

class ThreadGroup;

#ifdef ELOQ_MODULE_ENABLED
class MongoModule final : public eloq::EloqModule {
public:
    static MongoModule* Instance() {
        static MongoModule mongoModule;
        return &mongoModule;
    }

    void Init(ThreadGroup* threadGroups) {
        _threadGroups = threadGroups;
    }

    void ExtThdStart(int thd_id) override;

    void ExtThdEnd(int thd_id) override;

    void Process(int thd_id) override;

    bool HasTask(int thd_id) const override;

private:
    MongoModule() = default;

private:
    ThreadGroup* _threadGroups{nullptr};
};
#endif

class ThreadGroup {
    friend class ServiceExecutorCoroutine;
#ifdef ELOQ_MODULE_ENABLED
    friend class MongoModule;
#endif
    using Task = std::function<void()>;

public:
    ThreadGroup() = default;

    void enqueueTask(Task task);
    void resumeTask(Task task);

    void notifyIfAsleep();

#ifndef ELOQ_MODULE_ENABLED
    /**
     * @brief Called by the thread bound to this thread group.
     */
    void trySleep();

    void terminate();
#endif

    void setTxServiceFunctors();

    void setThreadGroupID(int16_t id) {
        _threadGroupId = id;
    }

private:
    bool isBusy() const;

private:
    int16_t _threadGroupId{-1};

#ifdef ELOQ_MODULE_ENABLED
    bool _threadNameSet{false};
    std::atomic<bool> _extWorkerActive{false};
#endif

    constexpr static size_t kTaskBatchSize{100};
    std::array<Task, kTaskBatchSize> _taskBulk;

    txservice::ConcurrentQueueWSize<Task> _taskQueue;
    txservice::ConcurrentQueueWSize<Task> _resumeQueue;

#ifndef ELOQ_MODULE_ENABLED
    std::atomic<bool> _isSleep{false};
    std::mutex _sleepMutex;
    std::condition_variable _sleepCV;
#endif
    std::atomic<bool> _isTerminated{false};
    uint16_t _ongoingCoroutineCnt{0};

    std::atomic<uint64_t> _tickCnt{0};
    static constexpr uint64_t kTrySleepTimeOut = 5;

    std::function<void()> _txProcessorExec;
    std::function<void(int16_t)> _updateExtProc;
};

/**
 * The reserved service executor emulates a thread per connection.
 * Each connection has its own worker thread where jobs get scheduled.
 *
 * The executor will start reservedThreads on start, and create a new thread every time it
 * starts a new thread, ensuring there are always reservedThreads available for work - this
 * means that even when you hit the NPROC ulimit, there will still be threads ready to
 * accept work. When threads exit, they will go back to waiting for work if there are fewer
 * than reservedThreads available.
 */
class ServiceExecutorCoroutine final : public ServiceExecutor {
public:
    explicit ServiceExecutorCoroutine(ServiceContext* ctx, size_t reservedThreads = 1);

    Status start() override;

    Status schedule(Task task, ScheduleFlags flags, ServiceExecutorTaskName taskName) override;
    Status schedule(Task task,
                    ScheduleFlags flags,
                    ServiceExecutorTaskName taskName,
                    uint16_t threadGroupId) override;


    Status shutdown(Milliseconds timeout) override;

    Mode transportMode() const override {
        return Mode::kAsynchronous;
    }
    std::function<void()> coroutineResumeFunctor(uint16_t threadGroupId, const Task& task) override;
    std::function<void()> coroutineLongResumeFunctor(uint16_t threadGroupId,
                                                     const Task& task) override;
    void ongoingCoroutineCountUpdate(uint16_t threadGroupId, int delta) override;
    void appendStats(BSONObjBuilder* bob) const override;

private:
#ifndef ELOQ_MODULE_ENABLED
    Status _startWorker(int16_t groupId);
#endif

    // static thread_local std::deque<Task> _localWorkQueue;
    // static thread_local int _localRecursionDepth;
    // static thread_local int64_t _localThreadIdleCounter;

    std::atomic<bool> _stillRunning{false};

    // mutable stdx::mutex _mutex;
    // stdx::condition_variable _threadWakeup;
    // stdx::condition_variable _shutdownCondition;

    // AtomicUInt32 _numRunningWorkerThreads{0};

    const size_t _reservedThreads;

    std::vector<ThreadGroup> _threadGroups;
    // std::thread _backgroundTimeService;

    constexpr static std::string_view _name{"coroutine"};
    constexpr static uint32_t kIdleCycle = (1 << 10) - 1;  // 2^n-1
    constexpr static uint32_t kIdleTimeoutMs = 1000;
};

}  // namespace mongo::transport
