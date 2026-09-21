/**
 *    Copyright (C) 2017 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault;

#include "mongo/platform/basic.h"

#include "boost/optional.hpp"

#include "mongo/db/local_thread_state.h"
#include "mongo/db/service_context.h"
#include "mongo/stdx/future.h"
#include "mongo/transport/service_executor_adaptive.h"
#include "mongo/transport/service_executor_coroutine.h"
#include "mongo/transport/service_executor_synchronous.h"
#include "mongo/transport/service_executor_task_names.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

#include <asio.hpp>

#ifdef ELOQ_MODULE_ENABLED
#include <bthread/bthread.h>
#include <gflags/gflags.h>

DECLARE_bool(brpc_worker_as_ext_processor);
#endif

namespace mongo {
namespace {
using namespace transport;

namespace {
constexpr Milliseconds kWorkerThreadRunTime{1000};
// Run time + generous scheduling time slice
const Milliseconds kShutdownTime = kWorkerThreadRunTime + Milliseconds{50};
}

struct TestOptions : public ServiceExecutorAdaptive::Options {
    int reservedThreads() const final {
        return 1;
    }

    Milliseconds workerThreadRunTime() const final {
        return kWorkerThreadRunTime;
    }

    int runTimeJitter() const final {
        return 0;
    }

    Milliseconds stuckThreadTimeout() const final {
        return Milliseconds{100};
    }

    Microseconds maxQueueLatency() const final {
        return duration_cast<Microseconds>(Milliseconds{5});
    }

    int idlePctThreshold() const final {
        return 0;
    }

    int recursionLimit() const final {
        return 0;
    }
};

/* This implements the portions of the transport::Reactor based on ASIO, but leaves out
 * the methods not needed by ServiceExecutors.
 *
 * TODO Maybe use TransportLayerASIO's Reactor?
 */
class ASIOReactor : public transport::Reactor {
public:
    ASIOReactor() : _ioContext() {}

    void run() noexcept final {
        asio::io_context::work work(_ioContext);
        _ioContext.run();
    }

    void runFor(Milliseconds time) noexcept final {
        asio::io_context::work work(_ioContext);

        try {
            _ioContext.run_for(time.toSystemDuration());
        } catch (...) {
            severe() << "Uncaught exception in reactor: " << exceptionToStatus();
            fassertFailed(50476);
        }
    }

    void stop() final {
        _ioContext.stop();
    }

    void drain() override final {
        _ioContext.restart();
        while (_ioContext.poll()) {
            MONGO_LOG(1) << "Draining remaining work in reactor.";
        }
        _ioContext.stop();
    }

    std::unique_ptr<ReactorTimer> makeTimer() final {
        MONGO_UNREACHABLE;
    }

    Date_t now() final {
        MONGO_UNREACHABLE;
    }

    void schedule(ScheduleMode mode, Task task) final {
        if (mode == kDispatch) {
            _ioContext.dispatch(std::move(task));
        } else {
            _ioContext.post(std::move(task));
        }
    }

    bool onReactorThread() const final {
        return false;
    }

    operator asio::io_context&() {
        return _ioContext;
    }

private:
    asio::io_context _ioContext;
};

class ServiceExecutorAdaptiveFixture : public unittest::Test {
protected:
    void setUp() override {
        auto scOwned = ServiceContext::make();
        setGlobalServiceContext(std::move(scOwned));

        reactors = {std::make_shared<ASIOReactor>(), std::make_shared<ASIOReactor>()};
        executor = stdx::make_unique<ServiceExecutorAdaptive>(
            getGlobalServiceContext(), std::vector<ReactorHandle>(reactors),
            stdx::make_unique<TestOptions>());
    }

    void tearDown() override {
        ASSERT_OK(executor->shutdown(kShutdownTime));
        executor.reset();
        setGlobalServiceContext({});
    }

    std::vector<ReactorHandle> reactors;
    std::unique_ptr<ServiceExecutorAdaptive> executor;
};

class ServiceExecutorSynchronousFixture : public unittest::Test {
protected:
    void setUp() override {
        auto scOwned = ServiceContext::make();
        setGlobalServiceContext(std::move(scOwned));

        executor = stdx::make_unique<ServiceExecutorSynchronous>(getGlobalServiceContext());
    }

    std::unique_ptr<ServiceExecutorSynchronous> executor;
};

void scheduleBasicTask(ServiceExecutor* exec, bool expectSuccess) {
    struct Completion {
        stdx::condition_variable cond;
        stdx::mutex mutex;
        bool completed = false;
    };
    auto completion = std::make_shared<Completion>();
    auto task = [completion] {
        stdx::unique_lock<stdx::mutex> lk(completion->mutex);
        completion->completed = true;
        completion->cond.notify_all();
    };

    stdx::unique_lock<stdx::mutex> lk(completion->mutex);
    auto status = exec->schedule(
        std::move(task), ServiceExecutor::kEmptyFlags, ServiceExecutorTaskName::kSSMStartSession);
    if (expectSuccess) {
        ASSERT_OK(status);
        ASSERT_TRUE(completion->cond.wait_for(
            lk, std::chrono::seconds(10), [&] { return completion->completed; }));
    } else {
        ASSERT_NOT_OK(status);
    }
}

TEST_F(ServiceExecutorAdaptiveFixture, RunsEachIngressReactor) {
    ASSERT_OK(executor->start());
    for (const auto& reactor : reactors) {
        auto completed = std::make_shared<stdx::promise<void>>();
        auto future = completed->get_future();
        reactor->schedule(Reactor::kPost, [completed] { completed->set_value(); });
        ASSERT_TRUE(future.wait_for(std::chrono::seconds(10)) == stdx::future_status::ready);
        future.get();
    }
    ASSERT_EQ(reactors.size(), static_cast<size_t>(executor->threadsRunning()));
    ASSERT_OK(executor->shutdown(kShutdownTime));
    ASSERT_EQ(0, executor->threadsRunning());
}

TEST_F(ServiceExecutorAdaptiveFixture, ShutdownBeforeStartupIsSafe) {
    ASSERT_OK(executor->shutdown(kShutdownTime));
}

TEST_F(ServiceExecutorAdaptiveFixture, ShutdownIsIdempotent) {
    ASSERT_OK(executor->start());
    ASSERT_OK(executor->shutdown(kShutdownTime));
    ASSERT_OK(executor->shutdown(kShutdownTime));
}

TEST_F(ServiceExecutorAdaptiveFixture, RejectsEmptyReactorList) {
    ServiceExecutorAdaptive empty(getGlobalServiceContext(), {},
                                  stdx::make_unique<TestOptions>());
    ASSERT_EQ(ErrorCodes::InvalidOptions, empty.start().code());
    ASSERT_OK(empty.shutdown(kShutdownTime));
}

DEATH_TEST_F(ServiceExecutorAdaptiveFixture, SchedulingIsUnreachable, "MONGO_UNREACHABLE") {
    ASSERT_OK(executor->start());
    executor->schedule([] {}, ServiceExecutor::kEmptyFlags,
                       ServiceExecutorTaskName::kSSMStartSession).ignore();
}

#ifdef ELOQ_MODULE_ENABLED
TEST(ServiceExecutorCoroutine, ModuleSchedulingAndLifecycle) {
    constexpr int kThreadGroups = 2;
    // Start only brpc's worker pool: no Data Substrate, storage engine, or data-store fixture.
    // This matches the module-backed scheduler used by the EloqDoc server.
    FLAGS_brpc_worker_as_ext_processor = true;
    ASSERT_EQ(0, bthread_setconcurrency(kThreadGroups));
    bthread_t bootstrap;
    ASSERT_EQ(0, bthread_start_background(
        &bootstrap, nullptr, [](void*) -> void* { return nullptr; }, nullptr));
    ASSERT_EQ(0, bthread_join(bootstrap, nullptr));

    auto service = ServiceContext::make();
    ServiceExecutorCoroutine executor(service.get(), kThreadGroups);
    bool rejectedTaskRan = false;
    auto rejectedTask = [&] { rejectedTaskRan = true; };
    const auto flags = ServiceExecutor::kEmptyFlags;
    const auto taskName = ServiceExecutorTaskName::kSSMStartSession;
    ASSERT_EQ(ErrorCodes::ShutdownInProgress,
              executor.schedule(rejectedTask, flags, taskName).code());
    ASSERT_EQ(ErrorCodes::ShutdownInProgress,
              executor.schedule(rejectedTask, flags, taskName, 1).code());

    ASSERT_OK(executor.start());
    bool running = true;
    auto stop = MakeGuard([&] {
        if (running)
            ASSERT_OK(executor.shutdown(kShutdownTime));
    });
    auto checkGroup = [](int16_t expectedGroup, auto submit) {
        auto completed = std::make_shared<stdx::promise<int16_t>>();
        auto future = completed->get_future();
        ServiceExecutor::Task task = [completed] { completed->set_value(LocalThread::ID()); };
        submit(task);
        ASSERT_TRUE(future.wait_for(std::chrono::seconds(10)) == stdx::future_status::ready);
        ASSERT_EQ(expectedGroup, future.get());
    };
    checkGroup(0, [&](const ServiceExecutor::Task& task) {
        ASSERT_OK(executor.schedule(task, flags, taskName));
    });
    for (int16_t group = 0; group < kThreadGroups; ++group) {
        checkGroup(group, [&](const ServiceExecutor::Task& task) {
            ASSERT_OK(executor.schedule(task, flags, taskName, group));
        });
        checkGroup(group, [&](const ServiceExecutor::Task& task) {
            executor.coroutineResumeFunctor(group, task)();
        });
        checkGroup(group, [&](const ServiceExecutor::Task& task) {
            executor.coroutineLongResumeFunctor(group, task)();
        });
        checkGroup(group, [&](const ServiceExecutor::Task& task) {
            executor.deferCallOnMainStack(group, ServiceExecutor::Task(task));
        });
    }
    ASSERT_OK(executor.shutdown(kShutdownTime));
    running = false;
    ASSERT_EQ(ErrorCodes::ShutdownInProgress,
              executor.schedule(rejectedTask, flags, taskName).code());
    ASSERT_EQ(ErrorCodes::ShutdownInProgress,
              executor.schedule(rejectedTask, flags, taskName, 1).code());
    ASSERT_FALSE(rejectedTaskRan);
}
#endif

TEST_F(ServiceExecutorSynchronousFixture, BasicTaskRuns) {
    ASSERT_OK(executor->start());
    auto guard = MakeGuard([this] { ASSERT_OK(executor->shutdown(kShutdownTime)); });

    scheduleBasicTask(executor.get(), true);
}

TEST_F(ServiceExecutorSynchronousFixture, ScheduleFailsBeforeStartup) {
    scheduleBasicTask(executor.get(), false);
}


}  // namespace
}  // namespace mongo
