/**
 *    Copyright (C) 2012 10gen Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kIndex

#include "mongo/platform/basic.h"

#include "mongo/db/index_rebuilder.h"

#include <list>
#include <string>

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/client.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

using std::endl;
using std::string;
using std::vector;

namespace {
void checkNS(OperationContext* opCtx, const std::vector<std::string>& nsToCheck) {
    bool firstTime = true;
    for (auto it = nsToCheck.begin(); it != nsToCheck.end();
         ++it) {
        NamespaceString nss(*it);

        LOG(3) << "IndexRebuilder::checkNS: " << nss.ns();

        // This write lock is held throughout the index building process for this namespace.
        Lock::DBLock lk(opCtx, nss.db(), MODE_X);
        OldClientContext ctx(opCtx, nss.ns());

        Collection* collection = ctx.db()->getCollection(opCtx, nss);
        if (collection == NULL)
            continue;

        IndexCatalog* indexCatalog = collection->getIndexCatalog();

        if (collection->ns().isOplog() && indexCatalog->numIndexesTotal(opCtx) > 0) {
            warning() << nss.ns() << " had illegal indexes, removing";
            indexCatalog->dropAllIndexes(opCtx, true);
            continue;
        }


        MultiIndexBlock indexer(opCtx, collection);

        {
            WriteUnitOfWork wunit(opCtx);
            vector<BSONObj> indexesToBuild = indexCatalog->getAndClearUnfinishedIndexes(opCtx);

            // The indexes have now been removed from system.indexes, so the only record is
            // in-memory. If there is a journal commit between now and when insert() rewrites
            // the entry and the db crashes before the new system.indexes entry is journalled,
            // the index will be lost forever. Thus, we must stay in the same WriteUnitOfWork
            // to ensure that no journaling will happen between now and the entry being
            // re-written in MultiIndexBlock::init(). The actual index building is done outside
            // of this WUOW.

            if (indexesToBuild.empty()) {
                continue;
            }

            log() << "found " << indexesToBuild.size() << " interrupted index build(s) on "
                  << nss.ns();

            if (firstTime) {
                log() << "note: restart the server with --noIndexBuildRetry "
                      << "to skip index rebuilds";
                firstTime = false;
            }

            if (!serverGlobalParams.indexBuildRetry) {
                log() << "  not rebuilding interrupted indexes";
                wunit.commit();
                continue;
            }

            uassertStatusOK(indexer.init(indexesToBuild));

            wunit.commit();
        }

        try {
            uassertStatusOK(indexer.insertAllDocumentsInCollection());

            WriteUnitOfWork wunit(opCtx);
            indexer.commit();
            wunit.commit();
        } catch (const DBException& e) {
            error() << "Index rebuilding did not complete: " << redact(e);
            log() << "note: restart the server with --noIndexBuildRetry to skip index rebuilds";
            // If anything went wrong, leave the indexes partially built so that we pick them up
            // again on restart.
            indexer.abortWithoutCleanup();
            fassertFailedNoTrace(26100);
        } catch (...) {
            // If anything went wrong, leave the indexes partially built so that we pick them up
            // again on restart.
            indexer.abortWithoutCleanup();
            throw;
        }
    }
}
}  // namespace

void restartInProgressIndexesFromLastShutdown(OperationContext* opCtx) {
    AuthorizationSession::get(opCtx->getClient())->grantInternalAuthorization();

    std::vector<std::string> dbNames;

    StorageEngine* storageEngine = getGlobalServiceContext()->getStorageEngine();
    storageEngine->listDatabases(&dbNames);

    try {
        std::vector<std::string> collNames;
        for (std::vector<std::string>::const_iterator dbName = dbNames.begin();
             dbName < dbNames.end();
             ++dbName) {
            AutoGetDb autoDb(opCtx, *dbName, MODE_S);

            Database* db = autoDb.getDb();
            db->getDatabaseCatalogEntry()->getCollectionNamespaces(collNames);
        }
        checkNS(opCtx, collNames);
    } catch (const DBException& e) {
        error() << "Index verification did not complete: " << redact(e);
        fassertFailedNoTrace(18643);
    }
    LOG(1) << "checking complete";
}
}
