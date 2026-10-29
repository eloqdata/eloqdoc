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
 * Test-only builders for redo-log blobs and primary keys, in the layout the tx service writes
 * (docs/cdc-change-stream.md F1-F3; TransactionExecution::FillDataLogRequest).
 */

#pragma once

#include <cstdint>
#include <string>

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/ordering.h"
#include "mongo/db/modules/eloq/src/base/cdc_blob_decoder.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace cdc_test {

/** txservice::TableEngine::EloqDoc, as the blob's engine byte spells it. */
constexpr uint8_t kEngineEloqDoc = 3;
/** txservice::TableEngine::EloqKv. */
constexpr uint8_t kEngineEloqKv = 2;
/** txservice::TableType::Primary. */
constexpr uint8_t kTypePrimary = 0;
/** txservice::TableType::Secondary. */
constexpr uint8_t kTypeSecondary = 1;

/**
 * @brief Appends a host-order integer, as the writer's reinterpret_cast and append do.
 *
 * @param[in,out] out The bytes.
 * @param value The integer.
 */
template <typename T>
void appendNative(std::string* out, T value) {
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
}

/**
 * @brief The KeyString bytes of a primary key, as EloqDoc logs them (without type bits).
 *
 * @param idValue A BSON object holding the _id value as its only field, named "".
 * @return The bytes.
 */
inline std::string idKeyString(const BSONObj& idValue) {
    const KeyString ks(KeyString::kLatestVersion, idValue, Ordering::make(BSONObj()));
    return std::string(ks.getBuffer(), ks.getSize());
}

/**
 * @brief The KeyString bytes of an integer _id.
 *
 * @param id The _id.
 * @return The bytes.
 */
inline std::string idKeyString(long long id) {
    return idKeyString(BSON("" << id));
}

/**
 * @brief Builds a node group's redo-log blob section by section, in the writer's layout:
 * sections of `u8 name_len | name | u8 engine | u8 type | u32 kv_len | entries`, entries of
 * `u16 key_len | KeyString | u8 op | record` with the record absent for a delete, and a record
 * serialized as `size_t | unpack_info | size_t | BSON`.
 */
class BlobBuilder {
public:
    /**
     * @brief Starts a section.
     *
     * @param tableName The table name; for an EloqDoc primary table, the namespace.
     * @param engine The engine byte.
     * @param type The table-type byte.
     * @return This builder.
     */
    BlobBuilder& section(StringData tableName, uint8_t engine, uint8_t type) {
        _finishSection();
        appendNative<uint8_t>(&_blob, static_cast<uint8_t>(tableName.size()));
        _blob.append(tableName.rawData(), tableName.size());
        appendNative<uint8_t>(&_blob, engine);
        appendNative<uint8_t>(&_blob, type);
        _kvLenAt = _blob.size();
        appendNative<uint32_t>(&_blob, 0);
        _inSection = true;
        return *this;
    }

    /**
     * @brief Starts a section for an EloqDoc collection.
     *
     * @param ns The namespace.
     * @return This builder.
     */
    BlobBuilder& collection(StringData ns) {
        return section(ns, kEngineEloqDoc, kTypePrimary);
    }

    /**
     * @brief Adds an insert of a document keyed by its integer _id.
     *
     * @param doc The document; its _id must be an integer.
     * @return This builder.
     */
    BlobBuilder& insert(const BSONObj& doc) {
        return _entry(idKeyString(BSON("" << doc["_id"])), CdcOperation::kInsert, doc);
    }

    /**
     * @brief Adds an update (a replace event) of a document keyed by its _id.
     *
     * @param doc The document.
     * @return This builder.
     */
    BlobBuilder& update(const BSONObj& doc) {
        return _entry(idKeyString(BSON("" << doc["_id"])), CdcOperation::kUpdate, doc);
    }

    /**
     * @brief Adds a delete of the document with this _id.
     *
     * @param idValue {"": <_id>}.
     * @return This builder.
     */
    BlobBuilder& remove(const BSONObj& idValue) {
        return _entry(idKeyString(idValue), CdcOperation::kDelete, BSONObj());
    }

    /**
     * @brief Adds raw bytes inside a section, for an engine whose entries the decoder skips.
     *
     * @param bytes The bytes.
     * @return This builder.
     */
    BlobBuilder& opaqueBytes(StringData bytes) {
        invariant(_inSection);
        _blob.append(bytes.rawData(), bytes.size());
        return *this;
    }

    /**
     * @brief Finishes the blob.
     *
     * @return Its bytes.
     */
    std::string done() {
        _finishSection();
        return _blob;
    }

private:
    BlobBuilder& _entry(const std::string& keyString, CdcOperation op, const BSONObj& doc) {
        invariant(_inSection);
        appendNative<uint16_t>(&_blob, static_cast<uint16_t>(keyString.size()));
        _blob.append(keyString);
        appendNative<uint8_t>(&_blob, static_cast<uint8_t>(op));
        if (op != CdcOperation::kDelete) {
            appendNative<size_t>(&_blob, 0);  // empty unpack info
            appendNative<size_t>(&_blob, static_cast<size_t>(doc.objsize()));
            _blob.append(doc.objdata(), doc.objsize());
        }
        return *this;
    }

    void _finishSection() {
        if (!_inSection) {
            return;
        }
        const uint32_t kvLen = static_cast<uint32_t>(_blob.size() - _kvLenAt - sizeof(uint32_t));
        std::string encoded;
        appendNative<uint32_t>(&encoded, kvLen);
        _blob.replace(_kvLenAt, sizeof(uint32_t), encoded);
        _inSection = false;
    }

    std::string _blob;
    size_t _kvLenAt = 0;
    bool _inSection = false;
};

}  // namespace cdc_test
}  // namespace mongo
