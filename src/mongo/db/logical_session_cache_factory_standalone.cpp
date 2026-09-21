// CMake-only implementation of the normal mongod logical-session-cache factory.
// The SCons build continues to use logical_session_cache_factory_mongod.cpp.

#include "mongo/platform/basic.h"

#include "mongo/db/logical_session_cache_factory_mongod.h"

#include "mongo/db/logical_session_cache_impl.h"
#include "mongo/db/service_liaison_mongod.h"
#include "mongo/db/sessions_collection_standalone.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"

namespace mongo {

std::unique_ptr<LogicalSessionCache> makeLogicalSessionCacheD(LogicalSessionCacheServer state) {
    invariant(state == LogicalSessionCacheServer::kStandalone);
    return stdx::make_unique<LogicalSessionCacheImpl>(
        stdx::make_unique<ServiceLiaisonMongod>(),
        std::make_shared<SessionsCollectionStandalone>(),
        nullptr,  // MongoDB's transaction reaper is only used by replica sets and shards.
        LogicalSessionCacheImpl::Options{});
}

}  // namespace mongo
