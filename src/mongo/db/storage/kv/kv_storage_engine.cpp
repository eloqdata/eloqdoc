/**
 *    Copyright (C) 2014 MongoDB Inc.
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


#include <cstdint>
#include <vector>
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage
#define LOG_FOR_RECOVERY(level) \
    MONGO_LOG_COMPONENT(level, ::mongo::logger::LogComponent::kStorageRecovery)

#include "mongo/db/storage/kv/kv_storage_engine.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "mongo/db/catalog/catalog_control.h"
#include "mongo/db/concurrency/eloq_locker_noop.h"
#include "mongo/db/logical_clock.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/storage/kv/kv_catalog_feature_tracker.h"
#include "mongo/db/storage/kv/kv_database_catalog_entry.h"
#include "mongo/db/storage/kv/kv_engine.h"
#include "mongo/db/storage/storage_repair_observer.h"
#include "mongo/db/unclean_shutdown.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

namespace {
const std::string catalogInfo = "_mdb_catalog";
const auto kCatalogLogLevel = logger::LogSeverity::Debug(2);
}  // namespace

class KVStorageEngine::RemoveDBChange : public RecoveryUnit::Change {
public:
    RemoveDBChange(KVStorageEngine* engine, StringData db, KVDatabaseCatalogEntryBase* entry)
        : _engine(engine), _db(db.toString()), _entry(entry) {}

    virtual void commit(boost::optional<Timestamp>) {
        delete _entry;
    }

    virtual void rollback() {
        // stdx::lock_guard<stdx::mutex> lk(_engine->_dbsLock);
        // _engine->_dbs[_db] = _entry;
    }

    KVStorageEngine* const _engine;
    const std::string _db;
    KVDatabaseCatalogEntryBase* const _entry;
};

KVStorageEngine::KVStorageEngine(
    KVEngine* engine,
    const KVStorageEngineOptions& options,
    stdx::function<KVDatabaseCatalogEntryFactory> databaseCatalogEntryFactory)
    : _databaseCatalogEntryFactory(std::move(databaseCatalogEntryFactory)),
      _options(options),
      _engine(engine),
      _supportsDocLocking(_engine->supportsDocLocking()),
      _supportsDBLocking(_engine->supportsDBLocking()),
      _supportsCappedCollections(_engine->supportsCappedCollections()) {
    uassert(28601,
            "Storage engine does not support --directoryperdb",
            !(options.directoryPerDB && !engine->supportsDirectoryPerDB()));

    // OperationContextNoop opCtx(_engine->newRecoveryUnit());
    OperationContext opCtx(nullptr, 0);
    opCtx.setRecoveryUnit(_engine->newRecoveryUnit(),
                          WriteUnitOfWork::RecoveryUnitState::kNotInUnitOfWork);
    opCtx.setLockState(std::make_unique<EloqLockerNoop>());
    loadCatalog(&opCtx);
}

void KVStorageEngine::loadCatalog(OperationContext* opCtx) {
    MONGO_LOG(1) << "KVStorageEngine::loadCatalog";
    bool catalogExists = _engine->hasIdent(opCtx, catalogInfo);
    if (_options.forRepair && catalogExists) {
        auto repairObserver = StorageRepairObserver::get(getGlobalServiceContext());
        invariant(repairObserver->isIncomplete());

        log() << "Repairing catalog metadata";
        Status status = _engine->repairIdent(opCtx, catalogInfo);

        if (status.code() == ErrorCodes::DataModifiedByRepair) {
            warning() << "Catalog data modified by repair: " << status.reason();
            repairObserver->onModification(str::stream()
                                           << "KVCatalog repaired: " << status.reason());
        } else {
            fassertNoTrace(50926, status);
        }
    }

    if (!catalogExists) {
        WriteUnitOfWork uow(opCtx);

        auto status = _engine->createGroupedRecordStore(
            opCtx, catalogInfo, catalogInfo, CollectionOptions(), KVPrefix::kNotPrefixed);

        // BadValue is usually caused by invalid configuration string.
        // We still fassert() but without a stack trace.
        if (status.code() == ErrorCodes::BadValue) {
            fassertFailedNoTrace(28562);
        }
        fassert(28520, status);
        uow.commit();
    }

    _catalogRecordStore = _engine->getGroupedRecordStore(
        opCtx, catalogInfo, catalogInfo, CollectionOptions(), KVPrefix::kNotPrefixed);
    if (shouldLog(::mongo::logger::LogComponent::kStorageRecovery, kCatalogLogLevel)) {
        LOG_FOR_RECOVERY(kCatalogLogLevel) << "loadCatalog:";
        _dumpCatalog(opCtx);
    }

    _catalog = std::make_unique<KVCatalog>(
        _catalogRecordStore.get(), _options.directoryPerDB, _options.directoryForIndexes);
    _catalog->init(opCtx);

    // Every collection the tx service's catalog has is opened, after a clean or an unclean
    // shutdown alike. Stock MongoDB compares the _mdb_catalog here with the idents the storage
    // engine lists and drops a collection whose table is missing, because it creates and drops the
    // two in separate steps. In Eloq a collection's table and its catalog entry are one record,
    // changed by one DDL transaction and recovered with it, so there is nothing to repair; two
    // separate listings can still differ while a recovered DDL finishes its data store step, and
    // dropping on that difference would drop a live table. The listing comes from the data store
    // and may lag the tx service, so each name is checked against the tx service's catalog: a
    // table a recovered drop has removed is skipped, and a table a recovered create has not yet
    // written to the data store is opened on its first lookup (DatabaseImpl::getCollection).
    std::vector<std::string> collectionsKnownToCatalog;
    _catalog->getAllCollections(&collectionsKnownToCatalog);

    KVPrefix maxSeenPrefix = KVPrefix::kNotPrefixed;
    for (const auto& coll : collectionsKnownToCatalog) {
        NamespaceString nss(coll);
        std::string dbName = nss.db().toString();

        if (_catalog->findEntry(opCtx, coll).isEmpty()) {
            MONGO_LOG(1) << "KVStorageEngine::loadCatalog: " << coll
                         << " is no longer in the tx service's catalog; not opened";
            continue;
        }

        // No rollback since this is only for committed dbs.
        for (auto& dbMap : _dbMapVector) {
            if (auto iter = dbMap.find(dbName); iter == dbMap.end()) {
                auto [newIter, _] =
                    dbMap.try_emplace(dbName, _databaseCatalogEntryFactory(dbName, this));
                newIter->second->initCollection(opCtx, coll, _options.forRepair);
            } else {
                iter->second->initCollection(opCtx, coll, _options.forRepair);
            }
        }
        // KVDatabaseCatalogEntryBase*& db = _dbs[dbName];
        // if (!db) {
        //     db = _databaseCatalogEntryFactory(dbName, this).release();
        // }

        auto maxPrefixForCollection = _catalog->getMetaData(opCtx, coll).getMaxPrefix();
        maxSeenPrefix = std::max(maxSeenPrefix, maxPrefixForCollection);

        if (nss.isOrphanCollection()) {
            log() << "Orphaned collection found: " << nss;
        }
    }

    KVPrefix::setLargestPrefix(maxSeenPrefix);
    opCtx->recoveryUnit()->abandonSnapshot();

    // Unset the unclean shutdown flag to avoid executing special behavior if this method is called
    // after startup.
    startingAfterUncleanShutdown(getGlobalServiceContext()) = false;
}

void KVStorageEngine::closeCatalog(OperationContext* opCtx) {
    MONGO_LOG(1) << "KVStorageEngine::closeCatalog";
    dassert(opCtx->lockState()->isLocked());
    if (shouldLog(::mongo::logger::LogComponent::kStorageRecovery, kCatalogLogLevel)) {
        LOG_FOR_RECOVERY(kCatalogLogLevel) << "loadCatalog:";
        _dumpCatalog(opCtx);
    }

    // The slots are indexed by LocalThread::ID() when the catalog is reopened.
    // Clear their entries, not the slots themselves.
    for (auto& dbMap : _dbMapVector) {
        dbMap.clear();
    }

    _catalog.reset(nullptr);
    _catalogRecordStore.reset(nullptr);
}

/**
 * @brief Reconciles the catalog with the storage engine's idents; in Eloq there is nothing to
 * reconcile, so no index needs a rebuild.
 *
 * Stock MongoDB keeps a collection's table and its _mdb_catalog entry as two artifacts, created
 * and dropped in separate steps, and at start-up drops tables the catalog does not reference,
 * fails on a catalog collection whose table is missing, rebuilds a ready index whose table is
 * missing and drops unfinished indexes from the catalog. None of these states exists in Eloq:
 * - A collection's table, its indexes and its catalog entry are one record in the tx service,
 *   changed by one DDL transaction, which the tx service recovers if it was in flight.
 * - The catalog never holds an unfinished index, background secondary builds included:
 *   EloqCatalogRecordStore::updateRecord keeps metadata with an index that is not ready in the
 *   operation's recovery unit and writes the catalog only once every index is ready. An index's
 *   ident comes from the same record, so a ready index never lacks one either.
 * Comparing two listings of that one record would only compare two reads, which differ while a
 * recovered DDL finishes its data store step, and acting on the difference would drop a live table
 * or abort start-up.
 *
 * @param opCtx The operation context; unused.
 * @return An empty list of indexes to rebuild.
 */
StatusWith<std::vector<StorageEngine::CollectionIndexNamePair>>
KVStorageEngine::reconcileCatalogAndIdents(OperationContext* opCtx) {
    return std::vector<CollectionIndexNamePair>{};
}

void KVStorageEngine::cleanShutdown() {
    // for (DBMap::const_iterator it = _dbMapVector.begin(); it != _dbMapVector.end(); ++it) {
    //     delete it->second;
    // }
    _dbMapVector.clear();

    _catalog.reset(nullptr);
    _catalogRecordStore.reset(nullptr);

    _engine->cleanShutdown();
    // intentionally not deleting _engine
}

KVStorageEngine::~KVStorageEngine() {}

void KVStorageEngine::finishInit() {}

RecoveryUnit* KVStorageEngine::newRecoveryUnit() {
    if (!_engine) {
        // shutdown
        return nullptr;
    }
    return _engine->newRecoveryUnit();
}

RecoveryUnit::UPtr KVStorageEngine::newRecoveryUnitUPtr() {
    if (!_engine) {
        // shutdown
        return {nullptr, nullptr};
    }
    return _engine->newRecoveryUnitUPtr();
}

bool KVStorageEngine::supportsRecordVersions() const {
    return _engine->supportsRecordVersions();
}

Status KVStorageEngine::lockCollection(
    OperationContext* opCtx, StringData ns, bool isForWrite, bool* exists, std::string* version) {
    return _engine->lockCollection(opCtx, ns, isForWrite, exists, version);
}

void KVStorageEngine::onAuthzDataChanged(OperationContext* opCtx) {
    return _engine->onAuthzDataChanged(opCtx);
}

void KVStorageEngine::listDatabases(std::vector<std::string>* out) const {
    return _engine->listDatabases(*out);
    // stdx::lock_guard<stdx::mutex> lk(_dbsLock);
    // for (DBMap::const_iterator it = _dbMapVector.begin(); it != _dbMapVector.end(); ++it) {
    //     if (it->second->isEmpty())
    //         continue;
    //     out->push_back(it->first);
    // }
}

bool KVStorageEngine::databaseExists(std::string_view dbName) const {
    return _engine->databaseExists(dbName);
}

void KVStorageEngine::listCollections(std::string_view dbName,
                                      std::vector<std::string>* out) const {
    _engine->listCollections(dbName, *out);
}

void KVStorageEngine::listCollections(std::string_view dbName, std::set<std::string>& out) const {
    _engine->listCollections(dbName, out);
}

KVDatabaseCatalogEntryBase* KVStorageEngine::getDatabaseCatalogEntry(OperationContext* opCtx,
                                                                     StringData dbName) {
    // stdx::lock_guard<stdx::mutex> lk(_dbsLock);
    auto id = static_cast<int16_t>(LocalThread::ID() + 1);
    auto& dbMap = _dbMapVector[id];
    if (auto iter = dbMap.find(dbName); iter == dbMap.end()) {
        auto [newIter, _] =
            dbMap.try_emplace(dbName.toString(), _databaseCatalogEntryFactory(dbName, this));
        return newIter->second.get();
    } else {
        return iter->second.get();
    }

    // KVDatabaseCatalogEntryBase*& db = _dbs[dbName.toString()];
    // if (!db) {
    //     // Not registering change since db creation is implicit and never rolled back.
    //     db = _databaseCatalogEntryFactory(dbName, this).release();
    // }
    // return db;
}

Status KVStorageEngine::closeDatabase(OperationContext* opCtx, StringData db) {
    // This is ok to be a no-op as there is no database layer in kv.
    return Status::OK();
}

Status KVStorageEngine::dropDatabase(OperationContext* opCtx, StringData db) {
    // KVDatabaseCatalogEntryBase* entry;
    // {
    //     stdx::lock_guard<stdx::mutex> lk(_dbsLock);
    //     DBMap::const_iterator it = _dbMapVector.find(db.toString());
    //     if (it == _dbMapVector.end())
    //         return Status(ErrorCodes::NamespaceNotFound, "db not found to drop");
    //     entry = it->second;
    // }

    auto id = static_cast<int16_t>(LocalThread::ID() + 1);
    auto& dbMap = _dbMapVector[id];
    KVDatabaseCatalogEntryBase* entry{nullptr};
    if (auto iter = dbMap.find(db); iter == dbMap.end()) {
        entry = _databaseCatalogEntryFactory(db, this).release();
    } else {
        entry = iter->second.get();
    }

    std::vector<std::string> toDrop;
    entry->getCollectionNamespaces(toDrop);
    return _dropCollections(opCtx, entry, toDrop);
}

Status KVStorageEngine::_dropCollections(OperationContext* opCtx,
                                         KVDatabaseCatalogEntryBase* dbce,
                                         std::vector<std::string>& collections) {
    MONGO_LOG(1) << "KVStorageEngine::_dropCollections";

    Status firstError = Status::OK();
    WriteUnitOfWork wuow(opCtx);
    for (const auto& collection : collections) {
        // NamespaceString nss{std::move(collection)};

        Status result = dbce->dropCollection(opCtx, collection);
        if (!result.isOK() && firstError.isOK()) {
            firstError = result;
        }
    }

    wuow.commit();
    return firstError;
}
/**
 * Returns the first `dropCollection` error that this method encounters. This method will attempt
 * to drop all collections, regardless of the error status.
 */
Status KVStorageEngine::_dropCollectionsNoTimestamp(OperationContext* opCtx,
                                                    KVDatabaseCatalogEntryBase* dbce,
                                                    CollIter begin,
                                                    CollIter end) {
    MONGO_UNREACHABLE;
    // On primaries, this method will be called outside of any `TimestampBlock` state meaning the
    // "commit timestamp" will not be set. For this case, this method needs no special logic to
    // avoid timestamping the upcoming writes.
    //
    // On secondaries, there will be a wrapping `TimestampBlock` and the "commit timestamp" will
    // be set. Carefully save that to the side so the following writes can go through without that
    // context.
    const Timestamp commitTs = opCtx->recoveryUnit()->getCommitTimestamp();
    if (!commitTs.isNull()) {
        opCtx->recoveryUnit()->clearCommitTimestamp();
    }

    // Ensure the method exits with the same "commit timestamp" state that it was called with.
    auto addCommitTimestamp = MakeGuard([&opCtx, commitTs] {
        if (!commitTs.isNull()) {
            opCtx->recoveryUnit()->setCommitTimestamp(commitTs);
        }
    });

    Status firstError = Status::OK();
    WriteUnitOfWork untimestampedDropWuow(opCtx);
    for (auto toDrop = begin; toDrop != end; ++toDrop) {
        std::string coll = *toDrop;
        NamespaceString nss(coll);

        Status result = dbce->dropCollection(opCtx, coll);
        if (!result.isOK() && firstError.isOK()) {
            firstError = result;
        }
    }

    untimestampedDropWuow.commit();
    return firstError;
}

int KVStorageEngine::flushAllFiles(OperationContext* opCtx, bool sync) {
    return _engine->flushAllFiles(opCtx, sync);
}

Status KVStorageEngine::beginBackup(OperationContext* opCtx) {
    // We should not proceed if we are already in backup mode
    if (_inBackupMode)
        return Status(ErrorCodes::BadValue, "Already in Backup Mode");
    Status status = _engine->beginBackup(opCtx);
    if (status.isOK())
        _inBackupMode = true;
    return status;
}

void KVStorageEngine::endBackup(OperationContext* opCtx) {
    // We should never reach here if we aren't already in backup mode
    invariant(_inBackupMode);
    _engine->endBackup(opCtx);
    _inBackupMode = false;
}

bool KVStorageEngine::isDurable() const {
    return _engine->isDurable();
}

bool KVStorageEngine::isEphemeral() const {
    return _engine->isEphemeral();
}

SnapshotManager* KVStorageEngine::getSnapshotManager() const {
    return _engine->getSnapshotManager();
}

Status KVStorageEngine::repairRecordStore(OperationContext* opCtx, const std::string& ns) {
    auto repairObserver = StorageRepairObserver::get(getGlobalServiceContext());
    invariant(!_options.forRepair || repairObserver->isIncomplete());

    Status status = _engine->repairIdent(opCtx, _catalog->getCollectionIdent(ns));
    bool dataModified = status.code() == ErrorCodes::DataModifiedByRepair;
    if (!status.isOK() && !dataModified) {
        return status;
    }

    if (_options.forRepair && dataModified) {
        repairObserver->onModification(str::stream()
                                       << "Collection " << ns << ": " << status.reason());
    }
    // _dbs[nsToDatabase(ns)]->reinitCollectionAfterRepair(opCtx, ns);

    return Status::OK();
}

void KVStorageEngine::setJournalListener(JournalListener* jl) {
    _engine->setJournalListener(jl);
}

void KVStorageEngine::setStableTimestamp(Timestamp stableTimestamp) {
    _engine->setStableTimestamp(stableTimestamp);
}

void KVStorageEngine::setInitialDataTimestamp(Timestamp initialDataTimestamp) {
    _initialDataTimestamp = initialDataTimestamp;
    _engine->setInitialDataTimestamp(initialDataTimestamp);
}

void KVStorageEngine::setOldestTimestamp(Timestamp oldestTimestamp) {
    const bool force = true;
    _engine->setOldestTimestamp(oldestTimestamp, force);
}

bool KVStorageEngine::supportsRecoverToStableTimestamp() const {
    return _engine->supportsRecoverToStableTimestamp();
}

bool KVStorageEngine::supportsRecoveryTimestamp() const {
    return _engine->supportsRecoveryTimestamp();
}

StatusWith<Timestamp> KVStorageEngine::recoverToStableTimestamp(OperationContext* opCtx) {
    invariant(opCtx->lockState()->isW());

    // The "feature document" should not be rolled back. Perform a non-timestamped update to the
    // feature document to lock in the current state.
    KVCatalog::FeatureTracker::FeatureBits featureInfo;
    {
        WriteUnitOfWork wuow(opCtx);
        featureInfo = _catalog->getFeatureTracker()->getInfo(opCtx);
        _catalog->getFeatureTracker()->putInfo(opCtx, featureInfo);
        wuow.commit();
    }

    auto state = catalog::closeCatalog(opCtx);

    StatusWith<Timestamp> swTimestamp = _engine->recoverToStableTimestamp(opCtx);
    if (!swTimestamp.isOK()) {
        return swTimestamp;
    }

    catalog::openCatalog(opCtx, state);

    log() << "recoverToStableTimestamp successful. Stable Timestamp: " << swTimestamp.getValue();
    return {swTimestamp.getValue()};
}

boost::optional<Timestamp> KVStorageEngine::getRecoveryTimestamp() const {
    return _engine->getRecoveryTimestamp();
}

boost::optional<Timestamp> KVStorageEngine::getLastStableCheckpointTimestamp() const {
    return _engine->getLastStableCheckpointTimestamp();
}

bool KVStorageEngine::supportsReadConcernSnapshot() const {
    return _engine->supportsReadConcernSnapshot();
}

bool KVStorageEngine::supportsReadConcernMajority() const {
    return _engine->supportsReadConcernMajority();
}

void KVStorageEngine::replicationBatchIsComplete() const {
    return _engine->replicationBatchIsComplete();
}

Timestamp KVStorageEngine::getAllCommittedTimestamp() const {
    return _engine->getAllCommittedTimestamp();
}

void KVStorageEngine::_dumpCatalog(OperationContext* opCtx) {
    auto catalogRs = _catalogRecordStore.get();
    auto cursor = catalogRs->getCursor(opCtx);
    boost::optional<Record> rec = cursor->next();
    while (rec) {
        // This should only be called by a parent that's done an appropriate `shouldLog` check. Do
        // not duplicate the log level policy.
        LOG_FOR_RECOVERY(kCatalogLogLevel)
            << "\tId: " << rec->id << " Value: " << rec->data.toBson();
        rec = cursor->next();
    }
    opCtx->recoveryUnit()->abandonSnapshot();
}

}  // namespace mongo
