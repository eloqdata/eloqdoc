#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace mongo {

extern thread_local int16_t localThreadId;

extern std::function<std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    getTxServiceFunctors;

}  // namespace mongo
