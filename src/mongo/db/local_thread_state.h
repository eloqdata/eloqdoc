#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace mongo {

extern std::function<std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    getTxServiceFunctors;

struct LocalThread {
    static int16_t ID();
#ifndef ELOQ_MODULE_ENABLED
    static void SetID(int16_t id);
#endif
    static bool IsBThread();
};

}  // namespace mongo
