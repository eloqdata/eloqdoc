/**
 *    Copyright (C) 2016 MongoDB Inc.
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

#pragma once

#include <cstdint>
#include <string>

#include <boost/optional.hpp>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/stdx/functional.h"
#include "mongo/util/assert_util.h"

namespace mongo {

class Database;
class OperationContext;

/**
 * Interface for system.views collection operations associated with view catalog management.
 * All methods must be called from within a WriteUnitOfWork, and with the DBLock held in at
 * least intent mode.
 */
class DurableViewCatalog {
public:
    static constexpr StringData viewsCollectionName() {
        return NamespaceString::kSystemDotViewsCollectionName;
    }

    /**
     * Thread-safe method to mark a catalog name was changed. This will cause the in-memory
     * view catalog to be marked invalid
     */
    static void onExternalChange(OperationContext* opCtx, const NamespaceString& name);

    /**
     * @brief One view's definition as system.views holds it, with the version of the document read.
     */
    struct VersionedView {
        BSONObj definition;    ///< The document, owned: {_id, viewOn, pipeline, collation}.
        uint64_t version = 0;  ///< Commit timestamp of the version read; 0 if the reading
                               ///< transaction wrote it and has not committed.
    };

    using Callback = stdx::function<Status(const BSONObj& view)>;
    virtual Status iterate(OperationContext* opCtx, Callback callback) = 0;

    /**
     * @brief Whether lookup() reports versions, so the view catalog can check a cached definition
     * against system.views on every lookup instead of trusting it until it is invalidated.
     *
     * EloqDoc: a node keeps a view catalog per worker thread, and a view changed on another thread
     * or node invalidates none of them; only system.views is current.
     *
     * @param opCtx Operation context.
     * @return true if the storage engine reports record versions.
     */
    virtual bool supportsVersionedLookup(OperationContext* opCtx) const {
        return false;
    }

    /**
     * @brief Reads one view's definition from system.views in the operation's transaction.
     *
     * Called only when supportsVersionedLookup() is true.
     *
     * @param opCtx Operation context.
     * @param name The view's namespace.
     * @return The definition and its version; none if there is no such view; an error if the
     * stored definition is invalid.
     */
    virtual StatusWith<boost::optional<VersionedView>> lookup(OperationContext* opCtx,
                                                              const NamespaceString& name) {
        MONGO_UNREACHABLE;
    }
    virtual void upsert(OperationContext* opCtx,
                        const NamespaceString& name,
                        const BSONObj& view) = 0;
    virtual void remove(OperationContext* opCtx, const NamespaceString& name) = 0;
    virtual const std::string& getName() const = 0;
    virtual ~DurableViewCatalog() = default;
};

/**
 * Actual implementation of DurableViewCatalog for use by the Database class.
 * Implements durability through database operations on the system.views collection.
 */
class DurableViewCatalogImpl final : public DurableViewCatalog {
public:
    explicit DurableViewCatalogImpl(Database* db) : _db(db) {}

    Status iterate(OperationContext* opCtx, Callback callback);
    bool supportsVersionedLookup(OperationContext* opCtx) const override;
    StatusWith<boost::optional<VersionedView>> lookup(OperationContext* opCtx,
                                                      const NamespaceString& name) override;
    void upsert(OperationContext* opCtx, const NamespaceString& name, const BSONObj& view);
    void remove(OperationContext* opCtx, const NamespaceString& name);
    const std::string& getName() const;

private:
    /**
     * @brief Checks a system.views document's structure: only the expected fields, names valid and
     * in this database, a pipeline array and, if any, a collation object.
     *
     * @param viewDef The document.
     * @return OK, or InvalidViewDefinition.
     */
    Status _validate(const BSONObj& viewDef) const;

    Database* const _db;
};
}  // namespace mongo
