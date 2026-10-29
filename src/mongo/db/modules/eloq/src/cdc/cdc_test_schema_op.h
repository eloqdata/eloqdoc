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
 * Test-only builders for the schema ops CDC DDL records carry, with schema images in the layout
 * EloqDoc writes (EloqDS::SerializeSchemaImage, CollectionCatalogEntry::MetaData::toBSON).
 */

#pragma once

#include <string>
#include <vector>

#include "log.pb.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_ddl_decoder.h"

namespace mongo {
namespace cdc_test {

/**
 * @brief An index spec as the catalog stores it.
 *
 * @param ns The collection.
 * @param name The index name.
 * @param key The key pattern.
 * @param extra Further options, such as unique.
 * @return {v: 2, key, name, ns, ...extra}.
 */
inline BSONObj indexSpec(StringData ns,
                         StringData name,
                         const BSONObj& key,
                         const BSONObj& extra = BSONObj()) {
    BSONObjBuilder spec;
    spec.append("v", 2);
    spec.append("key", key);
    spec.append("name", name);
    spec.append("ns", ns);
    spec.appendElements(extra);
    return spec.obj();
}

/**
 * @brief The _id index's spec.
 *
 * @param ns The collection.
 * @return The spec.
 */
inline BSONObj idIndexSpec(StringData ns) {
    return indexSpec(ns, "_id_", BSON("_id" << 1));
}

/**
 * @brief A schema image whose catalog metadata describes a collection.
 *
 * @param ns The collection.
 * @param options The stored options, the server's own (uuid, catalogVersion) included.
 * @param specs The index specs, _id_'s first.
 * @return size_t len | {md: {ns, options, indexes}, idxIdent, ns, ident} | size_t len | kv
 * info | size_t len | key schema versions, every length in native byte order.
 */
inline std::string schemaImage(StringData ns,
                               const BSONObj& options,
                               const std::vector<BSONObj>& specs) {
    BSONObjBuilder metadata;
    {
        BSONObjBuilder md(metadata.subobjStart("md"));
        md.append("ns", ns);
        md.append("options", options);
        BSONArrayBuilder indexes(md.subarrayStart("indexes"));
        for (const BSONObj& spec : specs) {
            indexes.append(
                BSON("spec" << spec << "ready" << true << "multikey" << false << "head" << 0LL));
        }
    }
    {
        BSONObjBuilder idents(metadata.subobjStart("idxIdent"));
        for (const BSONObj& spec : specs) {
            idents.append(spec["name"].str(), ns.toString() + "." + spec["name"].str());
        }
    }
    metadata.append("ns", ns);
    metadata.append("ident", ns);
    const BSONObj obj = metadata.obj();

    std::string image;
    const auto appendPart = [&image](const char* data, size_t length) {
        image.append(reinterpret_cast<const char*>(&length), sizeof(length));
        image.append(data, length);
    };
    appendPart(obj.objdata(), static_cast<size_t>(obj.objsize()));
    appendPart("kv-info", 7);
    appendPart("", 0);
    return image;
}

/**
 * @brief A schema op as the log service keeps it in a CDC DDL record.
 *
 * @param ns The table name.
 * @param type The table op's type.
 * @param oldImage The old schema image; empty for a create.
 * @param newImage The new schema image; empty for a drop.
 * @param rolledBack The commit log's rolled-back mark.
 * @return The op.
 */
inline txlog::SchemaOpMessage schemaOp(StringData ns,
                                       CdcDdlOpType type,
                                       std::string oldImage,
                                       std::string newImage,
                                       bool rolledBack = false) {
    txlog::SchemaOpMessage op;
    op.set_table_name_str(ns.toString());
    op.set_table_engine(txlog::TableEngine::EloqDoc);
    op.set_old_catalog_blob(std::move(oldImage));
    op.set_new_catalog_blob(std::move(newImage));
    op.mutable_table_op()->set_op_type(static_cast<uint32_t>(type));
    op.set_rolled_back(rolledBack);
    return op;
}

/**
 * @brief A create's schema op, with default options.
 *
 * @param ns The collection.
 * @return The op.
 */
inline txlog::SchemaOpMessage createOp(StringData ns) {
    return schemaOp(ns,
                    CdcDdlOpType::kCreateTable,
                    "",
                    schemaImage(ns, BSON("catalogVersion" << "1"), {idIndexSpec(ns)}));
}

/**
 * @brief A drop's schema op.
 *
 * @param ns The collection.
 * @return The op.
 */
inline txlog::SchemaOpMessage dropOp(StringData ns) {
    return schemaOp(ns,
                    CdcDdlOpType::kDropTable,
                    schemaImage(ns, BSON("catalogVersion" << "1"), {idIndexSpec(ns)}),
                    "");
}

/**
 * @brief An add index's schema op adding one index on {a: 1} named "a_1".
 *
 * @param ns The collection.
 * @param rolledBack The commit log's rolled-back mark.
 * @return The op.
 */
inline txlog::SchemaOpMessage addIndexOp(StringData ns, bool rolledBack = false) {
    const BSONObj options = BSON("catalogVersion" << "1");
    return schemaOp(
        ns,
        CdcDdlOpType::kAddIndex,
        schemaImage(ns, options, {idIndexSpec(ns)}),
        schemaImage(ns, options, {idIndexSpec(ns), indexSpec(ns, "a_1", BSON("a" << 1))}),
        rolledBack);
}

}  // namespace cdc_test
}  // namespace mongo
