#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace mongo {

extern std::function<std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    getTxServiceFunctors;

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

struct LocalThread {
    static int16_t ID();
#ifndef ELOQ_MODULE_ENABLED
    static void SetID(int16_t id);
#endif
    static bool IsBThread();
};

}  // namespace mongo
