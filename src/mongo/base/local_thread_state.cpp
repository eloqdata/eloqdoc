#include "mongo/base/local_thread_state.h"

namespace mongo {

thread_local int16_t localThreadId = -1;

std::function<std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    getTxServiceFunctors;

const CoroutineFunctors CoroutineFunctors::Unavailable{};
}  // namespace mongo
