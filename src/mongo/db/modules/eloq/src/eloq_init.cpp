/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the license:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#if defined(__linux__)
#include <sys/vfs.h>
#endif

#include "mongo/platform/basic.h"

#include "mongo/db/service_context.h"
#include "mongo/db/storage/kv/kv_storage_engine.h"
#include "mongo/db/storage/storage_engine_init.h"
#include "mongo/db/storage/storage_engine_lock_file.h"
#include "mongo/db/storage/storage_engine_metadata.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/util/log.h"

#include "mongo/db/modules/eloq/src/cdc/document_source_eloq_change_stream.h"
#include "mongo/db/modules/eloq/src/eloq_kv_engine.h"
#include "src/base/eloq_util.h"

namespace mongo {
namespace {
class EloqFactory : public StorageEngine::Factory {
public:
    ~EloqFactory() override {}

    StorageEngine* create(const StorageGlobalParams& params,
                          const StorageEngineLockFile* lockFile) const override {
        if (lockFile && lockFile->createdByUncleanShutdown()) {
            warning() << "Recovering data from the last clean checkpoint.";
        }
        auto kv = std::make_unique<EloqKVEngine>(params.dbpath);
        KVStorageEngineOptions options;
        // options.directoryPerDB = params.directoryperdb;
        // options.forRepair = params.repair;

        auto storageEngine = std::make_unique<KVStorageEngine>(kv.release(), options);
        storageEngine->setUseNoopLockImpl(true);
        return storageEngine.release();
    }

    StringData getCanonicalName() const override {
        return kEloqEngineName;
    }

    // virtual Status validateCollectionStorageOptions(const BSONObj& options) const override {
    //     return Status::OK();
    // }
    // virtual Status validateIndexStorageOptions(const BSONObj& options) const override {
    //     return Status::OK();
    // }

    Status validateMetadata(const StorageEngineMetadata& metadata,
                            const StorageGlobalParams& params) const override {
        return Status::OK();
    }

    /**
     * Returns a new document suitable for storing in the data directory metadata.
     * This document will be used by validateMetadata() to check startup options
     * on restart.
     */
    BSONObj createMetadataOptions(const StorageGlobalParams& params) const override {
        BSONObjBuilder builder;
        builder.appendBool("demoBSON", 1);
        return builder.obj();
    }
};

ServiceContext::ConstructorActionRegisterer registerEloq(
    "EloqEngineInit", [](ServiceContext* service) {
        registerStorageEngine(service, std::make_unique<EloqFactory>());
        // Change streams read EloqDoc's log service rather than an oplog
        // (docs/cdc-change-stream.md §5.2).
        registerEloqChangeStreams();
    });

}  // namespace
}  // namespace mongo