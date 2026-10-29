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
 * The change stream's redo-log blob decoder and stream scopes (docs/cdc-change-stream.md F1-F3,
 * §5.1, §5.4 steps 1-2).
 */

#pragma once

#include <cstdint>
#include <string>

#include "mongo/base/string_data.h"

namespace mongo {

/**
 * @brief The operation a logged entry records, as the blob's one-byte op field spells it
 * (txservice::OperationType).
 *
 * Upsert is accepted but does not occur in a data log: the write set resolves to Insert, Update
 * or Delete before the log is written, which the replay path asserts.
 */
enum class CdcOperation : uint8_t {
    kUpdate = 1,  ///< A replace of an existing document.
    kDelete = 2,  ///< A delete; no record follows it.
    kInsert = 3,  ///< An insert.
    kUpsert = 4,  ///< Accepted, never logged.
};

/**
 * @brief Which namespaces a stream covers (docs/cdc-change-stream.md, §5.1).
 *
 * `local`, `admin` and `config` are excluded from every scope, deployment-level included: a
 * deployment stream is defined to skip them, and the reference consumer keeps its own progress
 * document in `admin` precisely because no source event can name it (§6.1).
 */
class CdcStreamScope {
public:
    /**
     * @brief Every collection outside local/admin/config. The CDC mode.
     *
     * @return The scope.
     */
    static CdcStreamScope deployment() {
        return CdcStreamScope(Kind::kDeployment, "");
    }

    /**
     * @brief Every collection of one database.
     *
     * @param db The database.
     * @return The scope.
     */
    static CdcStreamScope database(StringData db) {
        return CdcStreamScope(Kind::kDatabase, db.toString());
    }

    /**
     * @brief One namespace.
     *
     * @param ns The namespace, "db.coll".
     * @return The scope.
     */
    static CdcStreamScope collection(StringData ns) {
        return CdcStreamScope(Kind::kCollection, ns.toString());
    }

    /**
     * @brief Whether the scope covers a namespace.
     *
     * @param ns A namespace, "db.coll".
     * @return true if the stream reports changes to it.
     */
    bool includes(StringData ns) const;

    /**
     * @brief Whether this is a collection-level scope on the given namespace, the one scope a
     * drop invalidates (§5.1).
     *
     * @param ns A namespace, "db.coll".
     * @return true if the scope is collection(ns).
     */
    bool isCollection(StringData ns) const {
        return _kind == Kind::kCollection && ns == StringData(_target);
    }

private:
    /** @brief The scope's level. */
    enum class Kind {
        kDeployment,  ///< Every collection outside local/admin/config.
        kDatabase,    ///< One database's collections.
        kCollection,  ///< One collection.
    };

    /**
     * @brief Makes a scope.
     *
     * @param kind Its level.
     * @param target The database or namespace; empty for the deployment.
     */
    CdcStreamScope(Kind kind, std::string target) : _kind(kind), _target(std::move(target)) {}

    Kind _kind;           ///< The level.
    std::string _target;  ///< The database or namespace; empty for the deployment.
};

/**
 * @brief Walks one node group's redo-log blob and yields the document changes a change stream
 * cares about (docs/cdc-change-stream.md, F1-F3 and §5.4 steps 1-2).
 *
 * The blob is a sequence of per-table sections, each
 *
 *     u8 name_len | name | u8 engine | u8 type | u32 kv_len | kv_len bytes of entries
 *
 * and each entry is
 *
 *     u16 key_len | KeyString bytes | u8 op | record, present iff op != Delete
 *
 * with every integer in native byte order and a record serialized as
 * `size_t | unpack_info | size_t | BSON`. All of this is verified against the writer,
 * TransactionExecution::FillDataLogRequest, and against the reader the recovery path uses:
 * ReplayLogCc's section walk in tx_service/include/cc/cc_request.h and its entry loop in
 * tx_service/include/cc/template_cc_map.h. That a record follows every non-Delete op is a
 * contract, not a convenience: nothing delimits an entry, so replay would misparse the rest of
 * a section without it. The same loop asserts the op is Insert, Update or Delete, which is why
 * Upsert is accepted here but never actually occurs in a data log.
 *
 * Sections the stream does not want -- another engine's tables, secondary indexes, namespaces
 * outside the scope -- are skipped by their length without looking inside, which is what lets a
 * blob carrying EloqKV object-command sections pass through untouched.
 *
 * **Positions count everything, including what was skipped.** A resume token records an entry as
 * the ordinal of its section in the blob and of the entry within that section (§5.6), and that
 * position has to mean the same thing to a stream watching one collection and to one watching
 * the deployment. So the ordinals are properties of the record alone: filtering changes which
 * entries are produced, never what they are numbered.
 */
class CdcBlobDecoder {
public:
    /**
     * @brief One logged change.
     *
     * Its StringData fields are views into the blob the decoder was constructed over, not
     * copies, so the blob must outlive any entry taken from it.
     */
    struct Entry {
        /** Ordinal of the section within the blob, counting sections that were skipped. */
        uint32_t section = 0;
        /**
         * Ordinal of the entry within its section, over every entry the section holds. Entries
         * are never individually skipped -- filtering happens a whole section at a time -- so
         * this simply counts up from zero at each new section.
         */
        uint32_t entry = 0;
        /** The Mongo namespace: for an EloqDoc primary table the table name is the namespace (F2).
         */
        StringData ns;
        CdcOperation op = CdcOperation::kInsert;  ///< The logged operation.
        /**
         * The logged KeyString of _id with its length prefix removed (F3). This is what an event
         * carries verbatim as eloq.keyBytes, and what CdcKeyDecoder turns into a documentKey.
         */
        StringData keyString;
        /** The serialized record, `size_t | unpack_info | size_t | BSON`. Empty for a delete. */
        StringData record;

        /**
         * @brief The BSON document inside the record.
         *
         * @return The document's bytes, or an empty StringData for a delete.
         */
        StringData documentBson() const;
    };

    /**
     * @brief Starts walking a blob.
     *
     * @param blob The blob; it must outlive the decoder and every entry taken from it.
     * @param scope The namespaces whose sections are walked; the rest are skipped by length.
     */
    CdcBlobDecoder(StringData blob, CdcStreamScope scope);

    /**
     * @brief The next entry in the stream's scope.
     *
     * @param[out] out The entry, when there is one.
     * @return true if 'out' was filled; false once the blob is exhausted.
     * @throws AssertionException if the blob is malformed or truncated; the bytes come from the
     * log, so a failure here is corruption, not user input.
     */
    bool next(Entry* out);

private:
    /**
     * @brief Positions _sectionEntries at the next in-scope section, or leaves it empty at the
     * end.
     */
    void advanceToNextInScopeSection();

    StringData _blob;       ///< The blob being walked.
    CdcStreamScope _scope;  ///< The namespaces whose sections are walked.

    size_t _blobOffset = 0;      ///< Where the next section header starts.
    StringData _sectionEntries;  ///< Entries of the section being walked.
    size_t _entryOffset = 0;     ///< Where the next entry starts inside _sectionEntries.
    StringData _sectionNs;       ///< The namespace of the section being walked.
    /**
     * Ordinal of the section currently being walked; differs from _sectionsSeen, which also
     * counts the sections that were skipped.
     */
    uint32_t _currentSection = 0;
    uint32_t _sectionsSeen = 0;  ///< Sections seen so far, skipped ones included.
    uint32_t _entryOrdinal = 0;  ///< Ordinal of the next entry within the section.
};

}  // namespace mongo
