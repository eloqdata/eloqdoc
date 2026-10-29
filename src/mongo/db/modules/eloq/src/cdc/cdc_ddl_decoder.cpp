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

#include "mongo/platform/basic.h"

#include "mongo/db/modules/eloq/src/cdc/cdc_ddl_decoder.h"

#include <cstring>
#include <set>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bson_validate.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/namespace_string.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace {

/**
 * @brief The catalog metadata at the head of a schema image.
 *
 * @param image The image.
 * @param which "old" or "new", for the error message.
 * @param op The record's schema op, for the error message.
 * @return The metadata, owned; the catalog entry {md: {ns, options, indexes}, idxIdent, ...}.
 * @throws ChangeStreamFatalError if the image is cut short or the metadata is not a BSON object
 * with an md object.
 */
BSONObj catalogMetadata(const std::string& image,
                        StringData which,
                        const txlog::SchemaOpMessage& op) {
    const auto fail = [&](const std::string& why) {
        uasserted(ErrorCodes::ChangeStreamFatalError,
                  str::stream() << "The change stream cannot decode the " << which
                                << " schema image of the DDL record for " << op.table_name_str()
                                << ": " << why);
    };
    size_t length = 0;
    if (image.size() < sizeof(length)) {
        fail("the image is shorter than its first length");
    }
    std::memcpy(&length, image.data(), sizeof(length));
    if (length > image.size() - sizeof(length)) {
        fail("the catalog metadata's length runs past the image");
    }
    const char* data = image.data() + sizeof(length);
    const Status valid = validateBSON(data, length, BSONVersion::kLatest);
    if (!valid.isOK()) {
        fail(str::stream() << "the catalog metadata is not BSON: " << valid.reason());
    }
    BSONObj metadata(data);
    if (!metadata["md"].isABSONObj()) {
        fail("the catalog metadata has no md object");
    }
    return metadata.getOwned();
}

/**
 * @brief The specs of a catalog entry's indexes, in the entry's order.
 *
 * @param metadata The catalog entry.
 * @return The specs, views into 'metadata'.
 */
std::vector<BSONObj> indexSpecs(const BSONObj& metadata) {
    std::vector<BSONObj> specs;
    const BSONElement indexes = metadata["md"]["indexes"];
    if (indexes.type() != BSONType::Array) {
        return specs;
    }
    for (const BSONElement& index : indexes.Obj()) {
        if (index.isABSONObj() && index.Obj()["spec"].isABSONObj()) {
            specs.push_back(index.Obj()["spec"].Obj());
        }
    }
    return specs;
}

/**
 * @brief The specs of the indexes one catalog entry has and another lacks, matched by name.
 *
 * @param from The entry whose indexes are reported.
 * @param without The entry whose indexes are left out.
 * @return The specs, in 'from''s order.
 */
std::vector<BSONObj> indexesOnlyIn(const BSONObj& from, const BSONObj& without) {
    std::set<std::string> names;
    for (const BSONObj& spec : indexSpecs(without)) {
        names.insert(spec["name"].str());
    }
    std::vector<BSONObj> only;
    for (const BSONObj& spec : indexSpecs(from)) {
        if (names.count(spec["name"].str()) == 0) {
            only.push_back(spec);
        }
    }
    return only;
}

/**
 * @brief createIndexes' operationDescription: each spec as listIndexes reports it, less ns,
 * which the event's own ns carries (and which 4.4 and later specs no longer have).
 *
 * @param specs The specs.
 * @return {indexes: [<spec>, ...]}.
 */
BSONObj createdIndexes(const std::vector<BSONObj>& specs) {
    BSONObjBuilder description;
    BSONArrayBuilder indexes(description.subarrayStart("indexes"));
    for (const BSONObj& spec : specs) {
        indexes.append(spec.removeField("ns"));
    }
    indexes.done();
    return description.obj();
}

/**
 * @brief dropIndexes' operationDescription: each index's version, key pattern and name.
 *
 * @param specs The specs.
 * @return {indexes: [{v, key, name}, ...]}.
 */
BSONObj droppedIndexes(const std::vector<BSONObj>& specs) {
    BSONObjBuilder description;
    BSONArrayBuilder indexes(description.subarrayStart("indexes"));
    for (const BSONObj& spec : specs) {
        BSONObjBuilder index(indexes.subobjStart());
        for (StringData field : {"v"_sd, "key"_sd, "name"_sd}) {
            if (const BSONElement e = spec[field]) {
                index.append(e);
            }
        }
    }
    indexes.done();
    return description.obj();
}

}  // namespace

boost::optional<CdcDdlChange> decodeCdcDdl(const txlog::SchemaOpMessage& op) {
    if (op.table_engine() != txlog::TableEngine::EloqDoc || !op.has_table_op()) {
        return boost::none;
    }
    // The server's feature document is a table of the engine too, named "featureDocument".
    if (!NamespaceString(op.table_name_str()).isValid()) {
        return boost::none;
    }

    CdcDdlChange change;
    change.ns = op.table_name_str();
    switch (static_cast<CdcDdlOpType>(op.table_op().op_type())) {
        case CdcDdlOpType::kCreateTable: {
            if (op.rolled_back()) {
                return boost::none;
            }
            const BSONObj metadata = catalogMetadata(op.new_catalog_blob(), "new"_sd, op);
            // As listCollections reports them: the UUID and EloqDoc's catalog version are stored
            // as options but are not the user's.
            BSONObjBuilder options;
            const BSONElement stored = metadata["md"]["options"];
            if (stored.isABSONObj()) {
                for (const BSONElement& option : stored.Obj()) {
                    const StringData name = option.fieldNameStringData();
                    if (name != "uuid"_sd && name != "catalogVersion"_sd) {
                        options.append(option);
                    }
                }
            }
            change.operationType = "create"_sd;
            change.operationDescription = options.obj();
            return change;
        }
        case CdcDdlOpType::kDropTable:
            if (op.rolled_back()) {
                return boost::none;
            }
            change.operationType = "drop"_sd;
            change.dropsCollection = true;
            return change;
        case CdcDdlOpType::kAddIndex: {
            const BSONObj before = catalogMetadata(op.old_catalog_blob(), "old"_sd, op);
            const BSONObj after = catalogMetadata(op.new_catalog_blob(), "new"_sd, op);
            const std::vector<BSONObj> added = indexesOnlyIn(after, before);
            if (op.rolled_back()) {
                // The build broke a constraint after its prepare log, which reported the indexes
                // created; this second record takes them away (plan B8).
                change.operationType = "dropIndexes"_sd;
                change.operationDescription = droppedIndexes(added);
            } else {
                change.operationType = "createIndexes"_sd;
                change.operationDescription = createdIndexes(added);
            }
            return change;
        }
        case CdcDdlOpType::kDropIndex: {
            if (op.rolled_back()) {
                return boost::none;
            }
            const BSONObj before = catalogMetadata(op.old_catalog_blob(), "old"_sd, op);
            const BSONObj after = catalogMetadata(op.new_catalog_blob(), "new"_sd, op);
            change.operationType = "dropIndexes"_sd;
            change.operationDescription = droppedIndexes(indexesOnlyIn(before, after));
            return change;
        }
    }
    return boost::none;
}

}  // namespace mongo
