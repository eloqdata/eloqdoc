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


}  // namespace mongo
