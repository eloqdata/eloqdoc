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
 * Event stream tests (docs/cdc-change-stream.md §5.1, §5.4, §5.4a, §5.5, §5.6; plan C6, C7, C8a,
 * C8b, C10), against the scripted log service with redo-log blobs and schema ops built in the
 * writers' layouts. They check the DML and DDL event documents, the stream scopes, the tokens
 * every event and the watermark carry, forward-write dedup, the oversized-document rule, a
 * collection stream's invalidate, and resume: from every event token, from watermarks with and
 * without entries, at a timestamp set by either stream's key, at a collection stream's own drop,
 * and after its invalidate.
 */

#include "mongo/platform/basic.h"

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "mongo/base/error_codes.h"
#include "mongo/bson/timestamp.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_event_stream.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_fake_log_service.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_test_blob_builder.h"
#include "mongo/db/modules/eloq/src/cdc/cdc_test_schema_op.h"
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using cdc_test::BlobBuilder;

/** Tests block the calling thread instead of yielding a coroutine. */
const CoroutineFunctors& kNoCoro = CoroutineFunctors::Unavailable;

/**
 * @brief Options with short timings and no idle pause, so that draining is quick.
 *
 * @return The options.
 */
CdcEventStreamOptions fastOptions() {
    CdcEventStreamOptions options;
    options.source.maxWait = Milliseconds(1000);
    options.source.rpcSlack = Milliseconds(1000);
    options.source.idleBackoffInitial = Milliseconds(5);
    options.source.idleBackoffMax = Milliseconds(10);
    options.source.client.retryBackoffInitial = Milliseconds(5);
    options.source.client.retryBackoffMax = Milliseconds(10);
    return options;
}

/**
 * @brief Pulls events until the stream drains.
 *
 * @param stream The stream.
 * @param idle How long each pull may wait.
 * @return The events, in pull order.
 */
std::vector<Document> drain(CdcEventStream& stream, Milliseconds idle = Milliseconds(50)) {
    std::vector<Document> events;
    while (boost::optional<Document> event = stream.next(Date_t::now() + idle, kNoCoro)) {
        events.push_back(*event);
    }
    ASSERT(stream.drained());
    return events;
}

/**
 * @brief Pulls events until the stream drains or has returned its invalidate event.
 *
 * @param stream The stream.
 * @param idle How long each pull may wait.
 * @return The events, in pull order.
 */
std::vector<Document> pullAll(CdcEventStream& stream, Milliseconds idle = Milliseconds(50)) {
    std::vector<Document> events;
    while (boost::optional<Document> event = stream.next(Date_t::now() + idle, kNoCoro)) {
        events.push_back(*event);
    }
    ASSERT(stream.drained() || stream.invalidated());
    return events;
}

/**
 * @brief The operationType of each event.
 *
 * @param events The events.
 * @return The types, in order.
 */
std::vector<std::string> operationTypes(const std::vector<Document>& events) {
    std::vector<std::string> types;
    for (const Document& event : events) {
        types.push_back(event["operationType"].getString());
    }
    return types;
}

/**
 * @brief What identifies an event across runs: everything but its token, which records
 * positions that page cuts can reorder within a timestamp.
 *
 * @param event The event.
 * @return A printable descriptor.
 */
std::string describe(const Document& event) {
    MutableDocument copy(event);
    copy.remove("_id");
    return copy.freeze().toString();
}

/**
 * @brief The descriptors of events, sorted, for comparing the events two runs produced.
 *
 * @param events The events.
 * @return The sorted descriptors.
 */
std::vector<std::string> sortedDescriptors(const std::vector<Document>& events) {
    std::vector<std::string> out;
    for (const Document& event : events) {
        out.push_back(describe(event));
    }
    std::sort(out.begin(), out.end());
    return out;
}

/**
 * @brief The token an event carries as its _id.
 *
 * @param event The event.
 * @return The decoded token.
 */
CdcResumeToken tokenOf(const Document& event) {
    return uassertStatusOK(CdcResumeToken::parse(event["_id"].getDocument().toBson()));
}

/**
 * @brief A document with a unique version, so every change in a script is distinguishable.
 *
 * @param id The _id.
 * @param v The version.
 * @return The document.
 */
BSONObj doc(long long id, int v) {
    return BSON("_id" << id << "v" << v);
}

// ------------------------------------------------------------------------------------------------
// Event documents and scopes
// ------------------------------------------------------------------------------------------------

TEST(CdcEventStream, ProducesInsertReplaceAndDeleteEvents) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 4);
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));

    const long long bigId = 5000000000LL;  // needs int64: the delete key must keep it exact
    log->addDataBlob(0,
                     5000123,
                     1,
                     7,
                     BlobBuilder()
                         .collection("test.c")
                         .insert(doc(1, 1))
                         .update(doc(2, 2))
                         .remove(BSON("" << bigId))
                         .done());
    log->setBound(0, 6000000);
    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 3u);

    for (const Document& event : events) {
        ASSERT_VALUE_EQ(event["clusterTime"], Value(Timestamp(5, 123)));
        ASSERT_VALUE_EQ(event["ns"], Value(Document{{"db", "test"_sd}, {"coll", "c"_sd}}));
    }
    ASSERT_VALUE_EQ(events[0]["operationType"], Value("insert"_sd));
    ASSERT_VALUE_EQ(events[0]["fullDocument"], Value(doc(1, 1)));
    ASSERT_VALUE_EQ(events[0]["documentKey"], Value(BSON("_id" << 1LL)));
    ASSERT_VALUE_EQ(events[1]["operationType"], Value("replace"_sd));
    ASSERT_VALUE_EQ(events[1]["fullDocument"], Value(doc(2, 2)));
    ASSERT_VALUE_EQ(events[1]["documentKey"], Value(BSON("_id" << 2LL)));
    ASSERT_VALUE_EQ(events[2]["operationType"], Value("delete"_sd));
    ASSERT(events[2]["fullDocument"].missing());
    ASSERT_VALUE_EQ(events[2]["documentKey"], Value(BSON("_id" << bigId)));

    // eloq.keyBytes is the logged KeyString, byte for byte.
    const std::string key = cdc_test::idKeyString(BSON("" << bigId));
    const Value keyBytes = events[2]["eloq"]["keyBytes"];
    ASSERT_EQ(keyBytes.getType(), BSONType::BinData);
    ASSERT_EQ(std::string(static_cast<const char*>(keyBytes.getBinData().data),
                          keyBytes.getBinData().length),
              key);
}

TEST(CdcEventStream, ScopesSelectTheirNamespaces) {
    auto makeLog = [] {
        auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
        log->setBound(0, 4);
        return log;
    };
    auto run = [&](CdcStreamScope scope) {
        auto log = makeLog();
        CdcEventStream stream(log, scope, fastOptions());
        ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
        log->addDataBlob(0,
                         10,
                         1,
                         1,
                         BlobBuilder()
                             .collection("test.c")
                             .insert(doc(1, 1))
                             .collection("test.d")
                             .insert(doc(2, 2))
                             .collection("other.c")
                             .insert(doc(3, 3))
                             .collection("admin.x")
                             .insert(doc(4, 4))
                             .done());
        log->setBound(0, 20);
        std::vector<std::string> namespaces;
        for (const Document& event : drain(stream)) {
            namespaces.push_back(event["ns"]["db"].getString() + "." +
                                 event["ns"]["coll"].getString());
        }
        return namespaces;
    };
    ASSERT(run(CdcStreamScope::collection("test.c")) == std::vector<std::string>({"test.c"}));
    ASSERT(run(CdcStreamScope::database("test")) == std::vector<std::string>({"test.c", "test.d"}));
    ASSERT(run(CdcStreamScope::deployment()) ==
           std::vector<std::string>({"test.c", "test.d", "other.c"}));
}

TEST(CdcEventStream, OversizedDocumentOmitsFullDocument) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 4);
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));

    // A document just under the limit: with the event envelope around it, it would exceed it.
    const BSONObj big = BSON("_id" << 1 << "big" << std::string(BSONObjMaxUserSize - 200, 'x'));
    log->addDataBlob(
        0, 10, 1, 1, BlobBuilder().collection("test.c").insert(big).insert(doc(2, 2)).done());
    log->setBound(0, 20);
    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 2u);
    ASSERT(events[0]["fullDocument"].missing());
    ASSERT_VALUE_EQ(events[0]["fullDocumentOmitted"], Value(true));
    ASSERT_VALUE_EQ(events[0]["documentKey"], Value(BSON("_id" << 1)));
    ASSERT_VALUE_EQ(events[1]["fullDocument"], Value(doc(2, 2)));
    ASSERT(events[1]["fullDocumentOmitted"].missing());
}

// ------------------------------------------------------------------------------------------------
// Tokens and dedup
// ------------------------------------------------------------------------------------------------

TEST(CdcEventStream, TokensCarryTheCursorsBeyondTheirTimestamp) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    log->setBound(0, 4);
    log->setBound(1, 4);
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    ASSERT_BSONOBJ_EQ(stream.openingToken(), CdcResumeToken::makeWatermark(4).toBSON());

    log->addDataBlob(0, 10, 1, 1, BlobBuilder().collection("t.a").insert(doc(1, 1)).done());
    log->addDataBlob(1,
                     11,
                     2,
                     2,
                     BlobBuilder()
                         .collection("t.b")
                         .insert(doc(2, 2))
                         .collection("t.c")
                         .insert(doc(3, 3))
                         .done());
    log->setBound(0, 20);
    log->setBound(1, 20);
    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 3u);

    // The first event: only its own group has a cursor, mid-record.
    const CdcResumeToken first = tokenOf(events[0]);
    ASSERT(first.kind() == CdcResumeToken::Kind::kEvent);
    ASSERT_EQ(first.timestamp(), 10u);
    ASSERT_EQ(first.entries().size(), 1u);
    ASSERT(first.entries()[0].form == CdcResumeTokenEntry::Form::kAtKeyAfterPosition);
    ASSERT_EQ(first.entries()[0].dataKey, CdcFakeLogService::dataKey(10, 1, 1));
    ASSERT_EQ(first.entries()[0].section, 0u);
    ASSERT_EQ(first.entries()[0].entry, 0u);

    // The third: group 1 is in its second section. Group 0 finished its record at 10, which the
    // token's own timestamp covers (every record below 11 was produced before any at 11), so it
    // is left out and resumes from 11.
    const CdcResumeToken third = tokenOf(events[2]);
    ASSERT_EQ(third.timestamp(), 11u);
    ASSERT_EQ(third.entries().size(), 1u);
    ASSERT(!third.entryFor(0));
    const CdcResumeTokenEntry* g1 = third.entryFor(1);
    ASSERT(g1);
    ASSERT(g1->form == CdcResumeTokenEntry::Form::kAtKeyAfterPosition);
    ASSERT_EQ(g1->dataKey, CdcFakeLogService::dataKey(11, 2, 2));
    ASSERT_EQ(g1->section, 1u);
    ASSERT_EQ(g1->entry, 0u);

    // Drained: the watermark is at emit_bound and certifies both groups through it, so neither
    // cursor, both at or below 20, is carried.
    const CdcResumeToken watermark = stream.watermark();
    ASSERT(watermark.kind() == CdcResumeToken::Kind::kWatermark);
    ASSERT_EQ(watermark.timestamp(), 20u);
    ASSERT(watermark.entries().empty());
}

TEST(CdcEventStream, CoveredCursorsDoNotSendTheResumeBelowTheRetentionFloor) {
    // Group 1 writes once, at 10; group 0 writes at 100; the stream drains at 120. A token past
    // 10 covers group 1's cursor, so resuming from it must not scan from 10: once the log purges
    // history through 50, such a scan fails with ChangeStreamHistoryLost although the token
    // covers everything the purge removed.
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    log->setBound(0, 4);
    log->setBound(1, 4);
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    log->addDataBlob(1, 10, 2, 1, BlobBuilder().collection("t.a").insert(doc(1, 1)).done());
    log->addDataBlob(0, 100, 1, 2, BlobBuilder().collection("t.a").insert(doc(2, 2)).done());
    log->setBound(0, 120);
    log->setBound(1, 120);
    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 2u);

    // The tokens leave the covered cursor out.
    const CdcResumeToken atHundred = tokenOf(events[1]);
    ASSERT_EQ(atHundred.timestamp(), 100u);
    ASSERT_EQ(atHundred.entries().size(), 1u);
    ASSERT(atHundred.entryFor(0));
    const CdcResumeToken watermark = stream.watermark();
    ASSERT_EQ(watermark.timestamp(), 120u);
    ASSERT(watermark.entries().empty());

    // The same positions with the covered cursor carried, as a token written another way may be.
    CdcResumeTokenEntry covered;
    covered.logGroupId = 1;
    covered.form = CdcResumeTokenEntry::Form::kAfterKey;
    covered.dataKey = CdcFakeLogService::dataKey(10, 2, 1);
    const CdcResumeToken eventCarryingIt(
        100, CdcResumeToken::Kind::kEvent, {*atHundred.entryFor(0), covered});
    const CdcResumeToken watermarkCarryingIt = CdcResumeToken::makeWatermark(120, {covered});

    log->setRetentionFloor(0, 50);
    log->setRetentionFloor(1, 50);
    log->addDataBlob(1, 130, 2, 3, BlobBuilder().collection("t.a").insert(doc(3, 3)).done());
    log->setBound(0, 140);
    log->setBound(1, 140);
    for (const CdcResumeToken& token :
         {atHundred, watermark, eventCarryingIt, watermarkCarryingIt}) {
        CdcEventStream resumed(log, CdcStreamScope::deployment(), fastOptions());
        resumed.openAt(token);
        const std::vector<Document> rest = drain(resumed);
        ASSERT_EQ(rest.size(), 1u) << token.toBSON();
        ASSERT_VALUE_EQ(rest[0]["fullDocument"], Value(doc(3, 3)));
    }
}

TEST(CdcEventStream, ForwardWriteDuplicatesAreProducedOnce) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 4);
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));

    // One transaction logged the same entry in both node groups' records during a migration.
    const std::string blob = BlobBuilder().collection("t.a").insert(doc(1, 1)).done();
    log->addDataBlob(0, 10, 1, 9, blob);
    log->addDataBlob(0, 10, 2, 9, blob);
    // The same key changed again later is a distinct change.
    log->addDataBlob(0, 11, 1, 10, BlobBuilder().collection("t.a").update(doc(1, 2)).done());
    log->setBound(0, 20);

    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 2u);
    ASSERT_VALUE_EQ(events[0]["fullDocument"], Value(doc(1, 1)));
    ASSERT_VALUE_EQ(events[1]["fullDocument"], Value(doc(1, 2)));
    ASSERT_EQ(stream.duplicatesSuppressed(), 1u);
    ASSERT_EQ(stream.dedupOverflows(), 0u);
}

TEST(CdcEventStream, FullDedupSetProducesTheRepeatAgain) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 4);
    CdcEventStreamOptions options = fastOptions();
    options.dedupSetBytes = 1;  // records nothing
    CdcEventStream stream(log, CdcStreamScope::deployment(), options);
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));

    const std::string blob = BlobBuilder().collection("t.a").insert(doc(1, 1)).done();
    log->addDataBlob(0, 10, 1, 9, blob);
    log->addDataBlob(0, 10, 2, 9, blob);
    log->setBound(0, 20);

    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 2u);
    ASSERT_EQ(describe(events[0]), describe(events[1]));
    ASSERT_EQ(stream.dedupOverflows(), 2u);
}

TEST(CdcEventStream, StatsFollowThePulls) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 4);
    CdcEventStreamOptions options = fastOptions();
    options.dedupSetBytes = 1;  // records nothing: every change counts as an overflow
    CdcEventStream stream(log, CdcStreamScope::deployment(), options);
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    ASSERT_EQ(stream.stats().lastPullMillis.load(), 0);

    log->addDataBlob(0,
                     5000123,
                     1,
                     7,
                     BlobBuilder().collection("test.c").insert(doc(1, 1)).insert(doc(2, 2)).done());
    log->setBound(0, 6000000);
    const boost::optional<Document> first = stream.next(Date_t::now() + Seconds(5), kNoCoro);
    ASSERT(first);
    ASSERT_EQ(stream.stats().producedThrough.load(), 5000123u);
    ASSERT_GT(stream.stats().lastPullMillis.load(), 0);

    // Drained, the stream has produced through its emit bound: no lag.
    ASSERT_EQ(drain(stream).size(), 1u);
    ASSERT_EQ(stream.stats().producedThrough.load(), stream.stats().emitBound.load());
    ASSERT_GTE(stream.stats().emitBound.load(), 5000123u);
    ASSERT_EQ(stream.dedupOverflows(), 2u);
    ASSERT_EQ(stream.stats().dedupOverflows.load(), 2u);
}

// ------------------------------------------------------------------------------------------------
// DDL events and invalidate
// ------------------------------------------------------------------------------------------------

/**
 * @brief Scripts a collection's life in group 0: t.a created at 10, written at 11, indexed at 12,
 * dropped at 13, created again at 14 and written at 15; and t.b written at 12.
 *
 * @param log The service.
 */
void scriptLifecycle(CdcFakeLogService& log) {
    log.addDdl(0, 10, 1, cdc_test::createOp("t.a"));
    log.addDataBlob(0, 11, 1, 2, BlobBuilder().collection("t.a").insert(doc(1, 1)).done());
    log.addDataBlob(0, 12, 1, 4, BlobBuilder().collection("t.b").insert(doc(2, 2)).done());
    log.addDdl(0, 12, 3, cdc_test::addIndexOp("t.a"));
    log.addDdl(0, 13, 5, cdc_test::dropOp("t.a"));
    log.addDdl(0, 14, 6, cdc_test::createOp("t.a"));
    log.addDataBlob(0, 15, 1, 7, BlobBuilder().collection("t.a").insert(doc(3, 3)).done());
    log.setBound(0, 20);
}

/**
 * @brief Opens a stream fresh on a single-group service, scripts the lifecycle, and pulls every
 * event.
 *
 * @param log The service, with group 0 only.
 * @param scope The stream's scope.
 * @return The events.
 */
std::vector<Document> lifecycleEvents(const std::shared_ptr<CdcFakeLogService>& log,
                                      CdcStreamScope scope) {
    log->setBound(0, 4);
    CdcEventStream stream(log, std::move(scope), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    scriptLifecycle(*log);
    return pullAll(stream);
}

TEST(CdcEventStream, DdlRecordsProduceEvents) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    const std::vector<Document> events = lifecycleEvents(log, CdcStreamScope::deployment());
    ASSERT(operationTypes(events) ==
           std::vector<std::string>(
               {"create", "insert", "insert", "createIndexes", "drop", "create", "insert"}));

    const Document& create = events[0];
    ASSERT_VALUE_EQ(create["clusterTime"], Value(Timestamp(0, 10)));
    ASSERT_VALUE_EQ(create["ns"], Value(Document{{"db", "t"_sd}, {"coll", "a"_sd}}));
    ASSERT_VALUE_EQ(create["operationDescription"], Value(BSONObj()));
    // DDL events carry neither a document key nor the key bytes.
    ASSERT(create["documentKey"].missing());
    ASSERT(create["eloq"].missing());

    const Document& createIndexes = events[3];
    ASSERT_VALUE_EQ(createIndexes["clusterTime"], Value(Timestamp(0, 12)));
    ASSERT_VALUE_EQ(
        createIndexes["operationDescription"],
        Value(BSON("indexes" << BSON_ARRAY(BSON("v" << 2 << "key" << BSON("a" << 1) << "name"
                                                    << "a_1")))));
    // Its group's entry is form 0, after the DDL record, with the data record finished before it
    // at the same timestamp.
    const CdcResumeToken token = tokenOf(createIndexes);
    ASSERT(token.kind() == CdcResumeToken::Kind::kEvent);
    ASSERT_EQ(token.timestamp(), 12u);
    ASSERT_EQ(token.entries().size(), 1u);
    ASSERT(token.entries()[0].form == CdcResumeTokenEntry::Form::kAfterKey);
    ASSERT_EQ(token.entries()[0].dataKey, CdcFakeLogService::dataKey(12, 1, 4));
    ASSERT_EQ(token.entries()[0].ddlKey, CdcFakeLogService::ddlKey(12, 3));

    const Document& drop = events[4];
    ASSERT_VALUE_EQ(drop["clusterTime"], Value(Timestamp(0, 13)));
    ASSERT_VALUE_EQ(drop["ns"], Value(Document{{"db", "t"_sd}, {"coll", "a"_sd}}));
    ASSERT(drop["operationDescription"].missing());
}

TEST(CdcEventStream, DdlEventsFollowTheScope) {
    auto run = [](CdcStreamScope scope) {
        auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
        log->setBound(0, 4);
        CdcEventStream stream(log, std::move(scope), fastOptions());
        ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
        log->addDdl(0, 10, 1, cdc_test::createOp("u.x"));
        log->addDdl(0, 11, 2, cdc_test::createOp("admin.y"));
        log->addDdl(0, 12, 3, cdc_test::createOp("t.a"));
        log->addDdl(0, 13, 4, cdc_test::createOp("featureDocument"));
        log->setBound(0, 20);
        std::vector<std::string> namespaces;
        for (const Document& event : drain(stream)) {
            namespaces.push_back(event["ns"]["db"].getString() + "." +
                                 event["ns"]["coll"].getString());
        }
        return namespaces;
    };
    ASSERT(run(CdcStreamScope::deployment()) == std::vector<std::string>({"u.x", "t.a"}));
    ASSERT(run(CdcStreamScope::database("t")) == std::vector<std::string>({"t.a"}));
    ASSERT(run(CdcStreamScope::collection("u.x")) == std::vector<std::string>({"u.x"}));
}

TEST(CdcEventStream, CollectionStreamEndsWithAnInvalidateAfterItsDrop) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->setBound(0, 4);
    CdcEventStream stream(log, CdcStreamScope::collection("t.a"), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    ASSERT(stream.invalidateToken().isEmpty());
    scriptLifecycle(*log);
    const std::vector<Document> events = pullAll(stream);
    ASSERT(operationTypes(events) ==
           std::vector<std::string>({"create", "insert", "createIndexes", "drop", "invalidate"}));
    ASSERT(stream.invalidated());
    // What the batch loop reports when the stream closes, delivered or filtered out.
    ASSERT_BSONOBJ_EQ(stream.invalidateToken(), events[4]["_id"].getDocument().toBson());
    // Nothing follows, the next incarnation's events included.
    ASSERT_FALSE(stream.next(Date_t::now() + Milliseconds(50), kNoCoro));

    // The invalidate's token is the drop's position with kind 2.
    const CdcResumeToken drop = tokenOf(events[3]);
    const Document& invalidate = events[4];
    ASSERT_BSONOBJ_EQ(
        invalidate["_id"].getDocument().toBson(),
        CdcResumeToken(drop.timestamp(), CdcResumeToken::Kind::kInvalidate, drop.entries())
            .toBSON());
    ASSERT_VALUE_EQ(invalidate["clusterTime"], Value(Timestamp(0, 13)));
    ASSERT_VALUE_EQ(invalidate["ns"], Value(Document{{"db", "t"_sd}, {"coll", "a"_sd}}));

    // A database stream is not invalidated: it reports the next incarnation too.
    auto other = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    ASSERT(operationTypes(lifecycleEvents(other, CdcStreamScope::database("t"))) ==
           std::vector<std::string>(
               {"create", "insert", "insert", "createIndexes", "drop", "create", "insert"}));
    // Nor is a collection stream by another collection's drop.
    auto third = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    ASSERT(operationTypes(lifecycleEvents(third, CdcStreamScope::collection("t.b"))) ==
           std::vector<std::string>({"insert"}));
}

TEST(CdcEventStream, ResumingACollectionStreamFromEveryTokenEndsAtItsInvalidate) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    const std::vector<Document> all = lifecycleEvents(log, CdcStreamScope::collection("t.a"));
    ASSERT_EQ(all.size(), 5u);

    // From each event's token: the rest, down to the invalidate. From the drop's own token that
    // is the invalidate alone, with the drop's position (§5.6).
    for (size_t i = 0; i + 1 < all.size(); ++i) {
        CdcEventStream resumed(log, CdcStreamScope::collection("t.a"), fastOptions());
        resumed.openAt(tokenOf(all[i]));
        const std::vector<Document> rest = pullAll(resumed);
        ASSERT(resumed.invalidated()) << "resuming after event " << i;
        ASSERT_BSONOBJ_EQ(resumed.invalidateToken(), all[4]["_id"].getDocument().toBson());
        ASSERT_EQ(rest.size(), all.size() - i - 1) << "resuming after event " << i;
        for (size_t j = 0; j < rest.size(); ++j) {
            ASSERT_DOCUMENT_EQ(rest[j], all[i + 1 + j]);
        }
    }

    // From the invalidate's token, as startAfter carries it: the next incarnation.
    CdcEventStream started(log, CdcStreamScope::collection("t.a"), fastOptions());
    started.openAt(tokenOf(all[4]));
    const std::vector<Document> next = drain(started);
    ASSERT(operationTypes(next) == std::vector<std::string>({"create", "insert"}));
    ASSERT_VALUE_EQ(next[1]["fullDocument"], Value(doc(3, 3)));
    ASSERT_FALSE(started.invalidated());
}

TEST(CdcEventStream, OnlyACollectionStreamOnTheDroppedCollectionResumesToAnInvalidate) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    const std::vector<Document> all = lifecycleEvents(log, CdcStreamScope::deployment());
    const CdcResumeToken drop = tokenOf(all[4]);
    ASSERT_EQ(drop.timestamp(), 13u);

    // A deployment stream reports the drop as an ordinary event and goes on past it.
    CdcEventStream deployment(log, CdcStreamScope::deployment(), fastOptions());
    deployment.openAt(drop);
    ASSERT(operationTypes(drain(deployment)) == std::vector<std::string>({"create", "insert"}));

    // A collection stream on another collection has nothing after it.
    CdcEventStream other(log, CdcStreamScope::collection("t.b"), fastOptions());
    other.openAt(drop);
    ASSERT(drain(other).empty());

    // An invalidate token is never checked for the drop: it resumes after the invalidate.
    CdcEventStream started(log, CdcStreamScope::collection("t.a"), fastOptions());
    started.openAt(
        CdcResumeToken(drop.timestamp(), CdcResumeToken::Kind::kInvalidate, drop.entries()));
    ASSERT(operationTypes(drain(started)) == std::vector<std::string>({"create", "insert"}));
}

// ------------------------------------------------------------------------------------------------
// Resume
// ------------------------------------------------------------------------------------------------

/**
 * @brief Scripts three groups with several records per timestamp, several changes per record,
 * forward-write duplicates, a record entirely out of the deployment scope, and a DDL record.
 *
 * @param log The service.
 */
void scriptThreeGroups(CdcFakeLogService& log) {
    int v = 0;
    long long id = 0;
    uint64_t txn = 100;
    for (uint64_t ts = 10; ts < 16; ++ts) {
        for (uint32_t group = 0; group < 3; ++group) {
            if ((ts + group) % 4 == 0) {
                continue;  // this group is quiet at this timestamp
            }
            for (uint32_t ng = 1; ng <= 2; ++ng) {
                // A transaction's write set holds one entry per key, so every key in a record,
                // and across the records of one transaction, is distinct.
                BlobBuilder blob;
                blob.collection("db.a").insert(doc(++id, ++v)).update(doc(++id, ++v));
                blob.collection("db.b").insert(doc(++id, ++v));
                if (ng == 2) {
                    blob.remove(BSON("" << id - 2));
                }
                log.addDataBlob(group, ts, ng, txn, blob.done());
            }
            ++txn;
        }
    }
    // Two records of one transaction carrying the same entry (a migration forward write).
    const std::string dup = BlobBuilder().collection("db.a").insert(doc(1000, 1000)).done();
    log.addDataBlob(1, 13, 1, 500, dup);
    log.addDataBlob(1, 13, 2, 500, dup);
    // A record with nothing a deployment stream reports.
    log.addDataBlob(2, 14, 1, 501, BlobBuilder().collection("admin.x").insert(doc(1, 1)).done());
    // DDL records: one no stream reports, two in one group at one timestamp, and others in other
    // groups. Their events' positions, and the unreported record's, must stay exact.
    log.addDdl(0, 12, 502);
    log.addDdl(0, 12, 503, cdc_test::addIndexOp("db.a"));
    log.addDdl(1, 13, 504, cdc_test::createOp("db.c"));
    log.addDdl(2, 15, 505, cdc_test::dropOp("db.c"));
    log.addDdl(2, 15, 506, cdc_test::createOp("db.d"));
}

TEST(CdcEventStream, ResumingFromEveryEventTokenYieldsExactlyTheRest) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1, 2});
    for (uint32_t group = 0; group < 3; ++group) {
        log->setBound(group, 4);
    }
    CdcEventStreamOptions options = fastOptions();
    options.source.maxBytesPerScan = 150;  // cut pages inside timestamps
    CdcEventStream stream(log, CdcStreamScope::deployment(), options);
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    scriptThreeGroups(*log);
    for (uint32_t group = 0; group < 3; ++group) {
        log->setBound(group, 30);
    }
    const std::vector<Document> all = drain(stream);
    ASSERT_GT(all.size(), 50u);
    ASSERT_EQ(stream.duplicatesSuppressed(), 1u);

    for (size_t i = 0; i < all.size(); ++i) {
        CdcEventStream resumed(log, CdcStreamScope::deployment(), options);
        resumed.openAt(tokenOf(all[i]));
        const std::vector<Document> rest = drain(resumed);
        const std::vector<Document> expected(all.begin() + i + 1, all.end());
        ASSERT(sortedDescriptors(rest) == sortedDescriptors(expected))
            << "resuming after event " << i << " of " << all.size() << " produced " << rest.size()
            << " events, expected " << expected.size();
    }
}

TEST(CdcEventStream, ResumingFromTheOpeningWatermarkStartsAboveS) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    log->addDataBlob(0, 3, 1, 1, BlobBuilder().collection("t.a").insert(doc(1, 1)).done());
    log->setBound(0, 4);
    log->setBound(1, 6);
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    const BSONObj opening = stream.openingToken();

    // A change after S, then a reopen from the opening token: it arrives exactly once, and the
    // change at or below S never does.
    log->addDataBlob(1, 5, 1, 2, BlobBuilder().collection("t.a").insert(doc(2, 2)).done());
    log->setBound(0, 10);
    log->setBound(1, 10);
    CdcEventStream reopened(log, CdcStreamScope::deployment(), fastOptions());
    reopened.openAt(uassertStatusOK(CdcResumeToken::parse(opening)));
    ASSERT_BSONOBJ_EQ(reopened.openingToken(), opening);
    const std::vector<Document> events = drain(reopened);
    ASSERT_EQ(events.size(), 1u);
    ASSERT_VALUE_EQ(events[0]["fullDocument"], Value(doc(2, 2)));
}

TEST(CdcEventStream, ResumingFromAWatermarkWhoseEntryIsAboveItsTimestamp) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    log->setBound(0, 4);
    log->setBound(1, 4);
    CdcEventStreamOptions options = fastOptions();
    options.source.maxBytesPerScan = 60;  // one record per page
    CdcEventStream stream(log, CdcStreamScope::deployment(), options);
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));

    for (uint64_t txn = 1; txn <= 3; ++txn) {
        log->addDataBlob(
            0, 10, 1, txn, BlobBuilder().collection("t.a").insert(doc(txn, txn)).done());
    }
    log->setBound(0, 20);
    log->setBound(1, 20);

    // The first page is cut inside timestamp 10; then group 0's scans are held, so the stream
    // drains with the rest of 10 unfetched: emit_bound is 9 while group 0's cursor is at 10.
    boost::optional<Document> first = stream.next(Date_t::now() + Seconds(5), kNoCoro);
    ASSERT(first);
    log->setCompletion(0, CdcFakeLogService::Completion::kHeld);
    ASSERT(drain(stream, Milliseconds(30)).empty());
    const CdcResumeToken watermark = stream.watermark();
    ASSERT_EQ(watermark.timestamp(), 9u);
    const CdcResumeTokenEntry* entry = watermark.entryFor(0);
    ASSERT(entry);
    ASSERT_EQ(entry->dataKey, CdcFakeLogService::dataKey(10, 1, 1));

    // Resuming from it yields the rest of 10 and nothing twice.
    log->setCompletion(0, CdcFakeLogService::Completion::kInline);
    CdcEventStream resumed(log, CdcStreamScope::deployment(), options);
    resumed.openAt(watermark);
    const std::vector<Document> rest = drain(resumed);
    ASSERT_EQ(rest.size(), 2u);
    ASSERT_VALUE_EQ(rest[0]["fullDocument"], Value(doc(2, 2)));
    ASSERT_VALUE_EQ(rest[1]["fullDocument"], Value(doc(3, 3)));
}

TEST(CdcEventStream, DdlKeyAtTheResumeTimestampConsumesItsDataRecords) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->addDataBlob(0, 15, 1, 1, BlobBuilder().collection("t.a").insert(doc(1, 1)).done());
    log->addDataBlob(0, 20, 1, 2, BlobBuilder().collection("t.a").insert(doc(2, 2)).done());
    log->addDdl(0, 20, 3);
    log->addDataBlob(0, 21, 1, 4, BlobBuilder().collection("t.a").insert(doc(4, 4)).done());
    log->setBound(0, 30);

    // The DDL record at 20 was produced, so every data record at 20, which precedes it, was too.
    CdcResumeTokenEntry entry;
    entry.logGroupId = 0;
    entry.dataKey = CdcFakeLogService::dataKey(15, 1, 1);
    std::string ddlKey;
    for (uint64_t value : {uint64_t{20}, uint64_t{3}}) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            ddlKey.push_back(static_cast<char>((value >> shift) & 0xff));
        }
    }
    entry.ddlKey = ddlKey;
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    stream.openAt(CdcResumeToken(20, CdcResumeToken::Kind::kEvent, {entry}));
    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 1u);
    ASSERT_VALUE_EQ(events[0]["fullDocument"], Value(doc(4, 4)));
}

TEST(CdcEventStream, DataKeyAtTheResumeTimestampLeavesLaterRecords) {
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0});
    log->addDataBlob(0, 20, 1, 1, BlobBuilder().collection("t.a").insert(doc(1, 1)).done());
    log->addDataBlob(0, 20, 2, 1, BlobBuilder().collection("t.a").insert(doc(2, 2)).done());
    log->addDdl(0, 20, 3);
    log->setBound(0, 30);

    CdcResumeTokenEntry entry;
    entry.logGroupId = 0;
    entry.dataKey = CdcFakeLogService::dataKey(20, 1, 1);
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    stream.openAt(CdcResumeToken(20, CdcResumeToken::Kind::kEvent, {entry}));
    const std::vector<Document> events = drain(stream);
    ASSERT_EQ(events.size(), 1u);
    ASSERT_VALUE_EQ(events[0]["fullDocument"], Value(doc(2, 2)));
}

TEST(CdcEventStream, EventTokenAtTimestampZeroFailsTheOpen) {
    // It would restart the groups it does not name from 0, which the scan source refuses with an
    // invariant; the open must fail with a user error first.
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1});
    CdcEventStream stream(log, CdcStreamScope::deployment(), fastOptions());
    ASSERT_THROWS_CODE(stream.openAt(CdcResumeToken(0, CdcResumeToken::Kind::kEvent, {})),
                       AssertionException,
                       ErrorCodes::FailedToParse);
}

TEST(CdcEventStream, MutatedTokensOpenOrFailWithAUserErrorNeverAnAbort) {
    // Tokens come from clients. Take the tokens a real stream issues, mutate their bytes, and
    // open and drain a stream at every mutation that still decodes: each must work or throw a
    // user error. An invariant anywhere on that path aborts this test.
    auto log = std::make_shared<CdcFakeLogService>(std::vector<uint32_t>{0, 1, 2});
    for (uint32_t group = 0; group < 3; ++group) {
        log->setBound(group, 4);
    }
    CdcEventStreamOptions options = fastOptions();
    options.source.maxBytesPerScan = 150;  // cut pages inside timestamps: form-1 entries
    CdcEventStream stream(log, CdcStreamScope::deployment(), options);
    ASSERT(stream.openFresh(Date_t::now() + Seconds(5), kNoCoro));
    std::vector<std::string> seeds{
        CdcResumeToken::parse(stream.openingToken()).getValue().encode()};
    scriptThreeGroups(*log);
    for (uint32_t group = 0; group < 3; ++group) {
        log->setBound(group, 30);
    }
    const std::vector<Document> all = drain(stream);
    ASSERT_GT(all.size(), 50u);
    for (size_t i = 0; i < all.size(); i += all.size() / 3) {
        seeds.push_back(tokenOf(all[i]).encode());
    }
    seeds.push_back(stream.watermark().encode());

    size_t undecodable = 0;
    size_t opened = 0;
    auto tryToken = [&](const std::string& bytes) {
        StatusWith<CdcResumeToken> token = CdcResumeToken::decode(bytes);
        if (!token.isOK()) {
            ++undecodable;
            return;
        }
        try {
            CdcEventStream resumed(log, CdcStreamScope::deployment(), options);
            resumed.openAt(token.getValue());
            drain(resumed, Milliseconds(5));
            ++opened;
        } catch (const DBException&) {
            // A user error fails the open or the stream, as for any bad input.
        }
    };
    for (const std::string& seed : seeds) {
        for (size_t at = 0; at < seed.size(); ++at) {
            for (int value : {0x00, 0xff, (seed[at] ^ 0x01) & 0xff}) {
                std::string mutated = seed;
                mutated[at] = static_cast<char>(value);
                tryToken(mutated);
            }
        }
        for (size_t length = 0; length < seed.size(); ++length) {
            tryToken(seed.substr(0, length));
        }
    }
    // Zeroing a timestamp byte of these small fake timestamps makes an event token at 0, which
    // the decoder refuses; many other mutations decode and reach the open and the drain.
    ASSERT_GT(undecodable, 0u);
    ASSERT_GT(opened, 0u);
}

}  // namespace
}  // namespace mongo
