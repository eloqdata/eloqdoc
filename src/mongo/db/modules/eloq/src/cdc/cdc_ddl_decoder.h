/**
 *    Copyright (C) 2026-present EloqData Inc.
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
 */

/**
 * @file
 * Decodes the schema op a CDC DDL record carries into what its change event reports
 * (docs/cdc-change-stream.md §5.4 "Per DDL record", §5.4a; plan C7).
 */

#pragma once

#include <boost/optional.hpp>
#include <cstdint>
#include <string>

#include "log.pb.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"

namespace mongo {

/**
 * @brief The values of txservice::OperationType that a CDC DDL record's table op can carry
 * (UpsertTableMessage.op_type). Mirrored here because the tx service's type.h pulls in glog,
 * whose macros clash with the server's.
 */
enum class CdcDdlOpType : uint32_t {
    kCreateTable = 5,  ///< txservice::OperationType::CreateTable.
    kDropTable = 6,    ///< txservice::OperationType::DropTable.
    kAddIndex = 8,     ///< txservice::OperationType::AddIndex.
    kDropIndex = 9,    ///< txservice::OperationType::DropIndex.
};

/**
 * @brief What one DDL record reports: the event's operationType, namespace and
 * operationDescription (§5.4a).
 */
struct CdcDdlChange {
    /** "create", "drop", "createIndexes" or "dropIndexes". */
    StringData operationType;
    /** The table name from the record, verbatim: the collection's "db.coll" (§5.4 step 4). */
    std::string ns;
    /**
     * create: the collection's options as listCollections reports them; createIndexes:
     * {indexes: [<spec>, ...]}; dropIndexes: {indexes: [{v, key, name}, ...]}. Empty for drop,
     * whose event has no operationDescription.
     */
    BSONObj operationDescription;
    /** The record drops the collection: the change that invalidates a stream on it (§5.1). */
    bool dropsCollection = false;
};

/**
 * @brief Decodes a DDL record's schema op into the change its event reports.
 *
 * Reads only the record: the op type, the rolled-back mark, the table name, and the catalog
 * metadata at the head of each schema image (`size_t len | BSON | size_t len | kv info |
 * size_t len | key schema versions`, EloqDS::SerializeSchemaImage, native byte order). An index
 * op's indexes are those whose names one image has and the other lacks: the added ones, in the
 * new image, for an add index; the dropped ones, in the old image, for a drop index. An add index
 * whose build broke a constraint has a second record, marked rolled back, which reports its
 * indexes dropped.
 *
 * @param op The record's schema op.
 * @return The change; none for an op no event reports: another engine, an op type other than
 * create, drop, add index or drop index, another rolled-back op, the server's feature document,
 * or a table name that is not a collection namespace. The log service records none of these
 * today, so skipping them only keeps the stream working past a log service that one day does.
 * @throws ChangeStreamFatalError if a schema image the change needs cannot be decoded.
 */
boost::optional<CdcDdlChange> decodeCdcDdl(const txlog::SchemaOpMessage& op);

}  // namespace mongo
