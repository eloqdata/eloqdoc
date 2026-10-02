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

#include "mongo/db/modules/eloq/src/cdc/cdc_event_stream.h"

#include <algorithm>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/modules/eloq/src/base/cdc_key_decoder.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

/**
 * What an event document adds around the document itself, rounded well up: field names,
 * operationType, clusterTime, ns and eloq.keyBytes' overhead.
 */
const int kEventEnvelopeBytes = 512;

/**
 * @brief The timestamp a storage key or DDL position begins with.
 *
 * @param key The key; empty when its stream produced nothing.
 * @return The big-endian timestamp in its first eight bytes, or 0 for a shorter key.
 */
uint64_t keyTimestamp(const std::string& key) {
    if (key.size() < 8) {
        return 0;
    }
    uint64_t ts = 0;
    for (int i = 0; i < 8; ++i) {
        ts = (ts << 8) | static_cast<unsigned char>(key[i]);
    }
    return ts;
}

/**
 * @brief The bytes that identify a change for dedup (§5.5): the transaction, the namespace and
 * the logged key.
 *
 * @param txn The transaction number.
 * @param entry The change.
 * @return The bytes.
 */
std::string dedupKey(uint64_t txn, const CdcBlobDecoder::Entry& entry) {
    std::string key;
    key.reserve(8 + entry.ns.size() + 1 + entry.keyString.size());
    for (int shift = 56; shift >= 0; shift -= 8) {
        key.push_back(static_cast<char>((txn >> shift) & 0xff));
    }
    key.append(entry.ns.rawData(), entry.ns.size());
    // The namespace cannot contain a NUL, so the key cannot be confused with another split.
    key.push_back('\0');
    key.append(entry.keyString.rawData(), entry.keyString.size());
    return key;
}

/**
 * @brief The operationType of a logged change (§5.4a).
 *
 * @param op The logged operation.
 * @return "insert", "replace" or "delete".
 */
StringData operationType(CdcOperation op) {
    switch (op) {
        case CdcOperation::kInsert:
            return "insert"_sd;
        case CdcOperation::kUpdate:
        case CdcOperation::kUpsert:
            return "replace"_sd;
        case CdcOperation::kDelete:
            return "delete"_sd;
    }
    MONGO_UNREACHABLE;
}

/**
 * @brief An event's clusterTime (§5.4a): Timestamp(secs = ts / 1e6, inc = ts % 1e6), lossless
 * for microsecond timestamps.
 *
 * @param ts The event's timestamp.
 * @return The clusterTime.
 */
Timestamp clusterTime(uint64_t ts) {
    return Timestamp(static_cast<unsigned>(ts / 1000000), static_cast<unsigned>(ts % 1000000));
}

}  // namespace

CdcEventStream::CdcEventStream(std::shared_ptr<CdcScanTransport> transport,
                               CdcStreamScope scope,
                               CdcEventStreamOptions options)
    : _source(std::move(transport), options.source), _scope(std::move(scope)), _options(options) {}

bool CdcEventStream::openFresh(Date_t deadline, const CoroutineFunctors& coro) {
    invariant(_openingToken.isEmpty());
    const boost::optional<uint64_t> start = _source.fixStart(deadline, coro);
    if (!start) {
        return false;
    }
    _openingToken = CdcResumeToken::makeWatermark(*start).toBSON();
    return true;
}

void CdcEventStream::openAt(const CdcResumeToken& token) {
    invariant(_openingToken.isEmpty());
    // Throws for a token no real stream can issue, before any group is started.
    const uint64_t absentStart = token.startTimestampForAbsentGroup();

    if (token.kind() == CdcResumeToken::Kind::kEvent) {
        // The drop of the watched collection would be at a form-0 entry's DDL key at the token's
        // own timestamp (a DDL event's group uses form 0). That group restarts at the timestamp
        // and re-reads the record, which _begin checks; an invalidate token denotes the position
        // after the invalidate and is never checked.
        for (const CdcResumeTokenEntry& entry : token.entries()) {
            if (entry.form == CdcResumeTokenEntry::Form::kAfterKey &&
                keyTimestamp(entry.ddlKey) == token.timestamp()) {
                _ownDropKeys.emplace(entry.logGroupId, entry.ddlKey);
            }
        }
        if (!_ownDropKeys.empty()) {
            _resumedFrom = token;
        }
    }

    for (uint32_t id : _source.logGroupIds()) {
        const CdcResumeTokenEntry* entry = token.entryFor(id);
        const uint64_t dataTs = entry ? keyTimestamp(entry->dataKey) : 0;
        const uint64_t ddlTs = entry ? keyTimestamp(entry->ddlKey) : 0;
        if (std::max(dataTs, ddlTs) < absentStart) {
            // Not named, named with nothing produced, or named at a position the token's own
            // timestamp covers: it restarts there. Its key would send the scan below the token,
            // into history the log may have purged although the token covers it; tokens this
            // stream issues leave such entries out (_entries), others may carry them.
            _source.startAt(id, absentStart);
            continue;
        }

        Group& group = _groups[id];
        group.hasCursor = true;
        group.dataKey = entry->dataKey;
        group.ddlKey = entry->ddlKey;
        group.partial = entry->form == CdcResumeTokenEntry::Form::kAtKeyAfterPosition;
        group.section = entry->section;
        group.entry = entry->entry;

        ResumeSkip skip;
        skip.ts = std::max(dataTs, ddlTs);
        skip.dataKey = entry->dataKey;
        skip.ddlKey = entry->ddlKey;
        skip.dataAtTs = dataTs == skip.ts;
        skip.ddlAtTs = ddlTs == skip.ts;
        skip.atPosition = group.partial;
        skip.section = entry->section;
        skip.entry = entry->entry;
        group.skip = std::move(skip);
        _source.startAt(id, group.skip->ts);
    }
    _openingToken = token.toBSON();
}

boost::optional<Document> CdcEventStream::next(Date_t deadline, const CoroutineFunctors& coro) {
    invariant(!_openingToken.isEmpty());
    for (;;) {
        if (_invalidated) {
            return boost::none;
        }
        if (_pendingInvalidate) {
            boost::optional<Document> invalidate;
            invalidate.swap(_pendingInvalidate);
            _invalidateToken = (*invalidate)["_id"].getDocument().toBson();
            _invalidated = true;
            return invalidate;
        }
        if (_current) {
            Current& current = *_current;
            Group& group = _groups[current.record.logGroupId()];
            CdcBlobDecoder::Entry entry;
            while (current.decoder->next(&entry)) {
                const std::string key = dedupKey(current.txn, entry);
                if (current.consumeThrough &&
                    std::make_pair(entry.section, entry.entry) <= *current.consumeThrough) {
                    // Produced before the resume token.
                    _seen(group, current.ts, key);
                    continue;
                }
                if (_seen(group, current.ts, key)) {
                    ++_duplicatesSuppressed;
                    continue;
                }
                group.hasCursor = true;
                group.dataKey = current.key;
                group.partial = true;
                group.section = entry.section;
                group.entry = entry.entry;
                return _event(current, entry);
            }
            // Every change of the record is produced, consumed or out of scope.
            group.hasCursor = true;
            group.dataKey = current.key;
            group.partial = false;
            _current.reset();
            continue;
        }

        boost::optional<CdcSourceRecord> record = _source.next(deadline, coro);
        if (!record) {
            return boost::none;
        }
        if (boost::optional<Document> event = _begin(std::move(*record))) {
            return event;
        }
    }
}

CdcResumeToken CdcEventStream::watermark() const {
    const uint64_t bound = _source.emitBound();
    return CdcResumeToken::makeWatermark(bound, _entries(bound));
}

boost::optional<Document> CdcEventStream::_begin(CdcSourceRecord record) {
    Group& group = _groups[record.logGroupId()];
    const uint64_t ts = record.timestamp();
    if (group.skip && ts > group.skip->ts) {
        group.skip.reset();
    }

    if (record.stream() == CdcSourceRecord::Stream::kDdl) {
        const std::string key = record.ddlKey();
        if (group.skip) {
            // At T_K: a DDL record at or below the entry's DDL key was produced before the token.
            // If the data key set T_K, none of T_K's DDL records was, since they follow its data.
            if (group.skip->ddlAtTs && key <= group.skip->ddlKey) {
                const auto ownDrop = _ownDropKeys.find(record.logGroupId());
                if (ownDrop != _ownDropKeys.end() && ownDrop->second == key) {
                    _ownDropKeys.erase(ownDrop);
                    const boost::optional<CdcDdlChange> change =
                        decodeCdcDdl(record.ddl().schema_op());
                    if (_dropsWatchedCollection(change)) {
                        // Resumed from the drop's own token: the drop was delivered, and the
                        // invalidate that followed it is all that is left (§5.6).
                        _pendingInvalidate =
                            _invalidateEvent(CdcResumeToken(_resumedFrom->timestamp(),
                                                            CdcResumeToken::Kind::kInvalidate,
                                                            _resumedFrom->entries()),
                                             change->ns);
                    }
                }
                return boost::none;
            }
            // Every data record at T_K precedes this one, so nothing more is left to skip.
            group.skip.reset();
        }
        group.hasCursor = true;
        group.ddlKey = key;
        return _ddlEvent(record);
    }

    Current current(record);
    current.key = record.dataKey();
    current.ts = ts;
    current.txn = record.data().txn_number();
    const StringData blob(record.data().blob());

    if (group.skip) {
        const ResumeSkip& skip = *group.skip;
        // At T_K every data record precedes every DDL record, so all of them were produced when
        // the DDL key is at T_K, whatever the data key says; otherwise those up to the data key.
        const bool producedBefore = skip.ddlAtTs || !skip.dataAtTs || current.key < skip.dataKey ||
            (current.key == skip.dataKey && !skip.atPosition);
        if (producedBefore) {
            // Consumed without producing; its changes go into the dedup set, since a repeat of one
            // of them later at T_K was produced before the token too.
            CdcBlobDecoder decoder(blob, _scope);
            CdcBlobDecoder::Entry entry;
            while (decoder.next(&entry)) {
                _seen(group, ts, dedupKey(current.txn, entry));
            }
            return boost::none;
        }
        if (current.key == skip.dataKey) {
            current.consumeThrough = std::make_pair(skip.section, skip.entry);
        }
    }

    current.decoder = std::make_unique<CdcBlobDecoder>(blob, _scope);
    _current.emplace(std::move(current));
    return boost::none;
}

boost::optional<Document> CdcEventStream::_ddlEvent(const CdcSourceRecord& record) {
    const boost::optional<CdcDdlChange> change = decodeCdcDdl(record.ddl().schema_op());
    if (!change || !_scope.includes(change->ns)) {
        return boost::none;
    }
    const uint64_t ts = record.timestamp();
    // The record's own group has just moved its DDL key to it, so the entry is form 0 (§5.6).
    const std::vector<CdcResumeTokenEntry> entries = _entries(ts - 1);
    const NamespaceString nss(change->ns);

    MutableDocument event;
    event.addField("_id",
                   Value(CdcResumeToken(ts, CdcResumeToken::Kind::kEvent, entries).toBSON()));
    event.addField("operationType", Value(change->operationType));
    event.addField("clusterTime", Value(clusterTime(ts)));
    event.addField("ns", Value(Document{{"db", nss.db()}, {"coll", nss.coll()}}));
    if (!change->dropsCollection) {
        event.addField("operationDescription", Value(change->operationDescription));
    }
    if (_dropsWatchedCollection(change)) {
        _pendingInvalidate = _invalidateEvent(
            CdcResumeToken(ts, CdcResumeToken::Kind::kInvalidate, entries), change->ns);
    }
    return event.freeze();
}

bool CdcEventStream::_dropsWatchedCollection(const boost::optional<CdcDdlChange>& change) const {
    return change && change->dropsCollection && _scope.isCollection(change->ns);
}

Document CdcEventStream::_invalidateEvent(const CdcResumeToken& token, StringData ns) {
    const NamespaceString nss(ns);
    const uint64_t ts = token.timestamp();
    MutableDocument event;
    event.addField("_id", Value(token.toBSON()));
    event.addField("operationType", Value("invalidate"_sd));
    event.addField("clusterTime", Value(clusterTime(ts)));
    event.addField("ns", Value(Document{{"db", nss.db()}, {"coll", nss.coll()}}));
    return event.freeze();
}

bool CdcEventStream::_seen(Group& group, uint64_t ts, const std::string& key) {
    if (group.dedupTs != ts) {
        // Two changes to one key never share a timestamp (F4), so only the current one matters.
        group.dedup.clear();
        group.dedupBytes = 0;
        group.dedupTs = ts;
    }
    if (group.dedup.count(key) != 0) {
        return true;
    }
    const int64_t bytes = static_cast<int64_t>(key.size()) + 32;
    if (group.dedupBytes + bytes > _options.dedupSetBytes) {
        ++_dedupOverflows;
        return false;
    }
    group.dedup.insert(key);
    group.dedupBytes += bytes;
    return false;
}

Document CdcEventStream::_event(const Current& current, const CdcBlobDecoder::Entry& entry) {
    const BSONObj token =
        CdcResumeToken(current.ts, CdcResumeToken::Kind::kEvent, _entries(current.ts - 1)).toBSON();
    const bool isDelete = entry.op == CdcOperation::kDelete;

    // A view into the page; copied once below, into the event.
    BSONObj fullDocument;
    if (!isDelete) {
        fullDocument = BSONObj(entry.documentBson().rawData());
    }
    // An insert's or replace's key is the document's own _id, as the client wrote it; a delete's
    // comes from the logged key, value-preserving but possibly in another numeric type (§5.4).
    const BSONObj documentKey = !isDelete && fullDocument.hasField("_id")
        ? BSON("_id" << fullDocument["_id"])
        : CdcKeyDecoder::decodeIdKey(entry.keyString);
    const NamespaceString nss(entry.ns);

    MutableDocument event;
    event.addField("_id", Value(token));
    event.addField("operationType", Value(operationType(entry.op)));
    event.addField("clusterTime", Value(clusterTime(current.ts)));
    if (!isDelete) {
        const int64_t estimate = static_cast<int64_t>(fullDocument.objsize()) + token.objsize() +
            documentKey.objsize() + static_cast<int64_t>(entry.keyString.size()) +
            static_cast<int64_t>(entry.ns.size()) + kEventEnvelopeBytes;
        if (estimate > BSONObjMaxUserSize) {
            // Past the limit the event could not be returned at all and would wedge the stream
            // (§5.8); the consumer reads the document by documentKey instead.
            event.addField("fullDocumentOmitted", Value(true));
        } else {
            event.addField("fullDocument", Value(fullDocument.getOwned()));
        }
    }
    event.addField("ns", Value(Document{{"db", nss.db()}, {"coll", nss.coll()}}));
    event.addField("documentKey", Value(documentKey));
    event.addField("eloq",
                   Value(Document{{"keyBytes",
                                   Value(BSONBinData(entry.keyString.rawData(),
                                                     static_cast<int>(entry.keyString.size()),
                                                     BinDataGeneral))}}));
    return event.freeze();
}

std::vector<CdcResumeTokenEntry> CdcEventStream::_entries(uint64_t coveredThrough) const {
    std::vector<CdcResumeTokenEntry> entries;
    for (const auto& [id, group] : _groups) {
        // Every record of a group at or below 'coveredThrough' has been produced: a watermark is
        // issued only once drained, and an event at T only after every group's records below T,
        // since the merge releases the lowest timestamp first. A group whose keys are no later
        // resumes exactly from the token's timestamp.
        if (!group.hasCursor ||
            std::max(keyTimestamp(group.dataKey), keyTimestamp(group.ddlKey)) <= coveredThrough) {
            continue;
        }
        CdcResumeTokenEntry entry;
        entry.logGroupId = id;
        entry.form = group.partial ? CdcResumeTokenEntry::Form::kAtKeyAfterPosition
                                   : CdcResumeTokenEntry::Form::kAfterKey;
        entry.dataKey = group.dataKey;
        entry.ddlKey = group.ddlKey;
        if (group.partial) {
            entry.section = group.section;
            entry.entry = group.entry;
        }
        entries.push_back(std::move(entry));
    }
    return entries;
}

}  // namespace mongo
