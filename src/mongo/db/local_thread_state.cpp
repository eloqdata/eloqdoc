#include "mongo/db/local_thread_state.h"

#ifndef ELOQ_MODULE_ENABLED
namespace {
thread_local int16_t localThreadId = -1;
}
#else
#include <bthread/task_group.h>
namespace bthread {
extern BAIDU_THREAD_LOCAL TaskGroup* tls_task_group;
}  // namespace bthread
#endif

namespace mongo {

std::function<std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    getTxServiceFunctors;

int16_t LocalThread::ID() {
#ifndef ELOQ_MODULE_ENABLED
    return localThreadId;
#else
    if (bthread::tls_task_group) {
        return bthread::tls_task_group->group_id_;
    } else {
        return -1;
    }
#endif
}

#ifndef ELOQ_MODULE_ENABLED
void LocalThread::SetID(int16_t id) {
    localThreadId = id;
}
#endif

bool LocalThread::IsBThread() {
#ifndef ELOQ_MODULE_ENABLED
    return false;
#else
    return bthread::tls_task_group != nullptr;
#endif
}
}  // namespace mongo
