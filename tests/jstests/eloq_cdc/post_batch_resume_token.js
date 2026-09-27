/**
 * postBatchResumeToken on every reply (docs/cdc-change-stream.md §5.3 step 4; plan C9 matrix rows
 * 1, 2, 5, 6 and 9): the opening watermark with batchSize 0, the supplied token for a token-based
 * open, a watermark that passes events a user $match filtered out, a watermark that advances
 * while the stream is idle, and no token at all on an ordinary cursor.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_pbrt");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));
    const coll = testDB.c;

    function getMore(id) {
        return assert.commandWorked(
            testDB.runCommand({getMore: id, collection: "c", maxTimeMS: 1000}));
    }

    // Row 1: batchSize 0 pulls nothing and still reports the opening watermark.
    const opened =
        assert.commandWorked(EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), "c", {}, [], {
            batchSize: 0
        }));
    assert.eq(opened.cursor.firstBatch, []);
    const opening = opened.cursor.postBatchResumeToken;
    assert.eq(EloqCdcUtil.tokenKind(opening), 1, tojson(opened));

    // Row 2: a token-based open reports the supplied token.
    const resumed = assert.commandWorked(EloqCdcUtil.openRaw(
        db.getMongo(), testDB.getName(), "c", {resumeAfter: opening}, [], {batchSize: 0}));
    assert.eq(resumed.cursor.postBatchResumeToken, opening);

    // Row 5: every event is filtered by a user stage; the watermark still passes them. An
    // unfiltered stream opened at the same point says where the events are.
    const filtered = assert.commandWorked(EloqCdcUtil.openRaw(db.getMongo(),
                                                              testDB.getName(),
                                                              "c",
                                                              {resumeAfter: opening},
                                                              [{$match: {operationType: "none"}}]));
    assert.commandWorked(coll.insert({_id: 1}));
    assert.commandWorked(coll.insert({_id: 2}));
    const events = EloqCdcUtil.nextEvents(
        new DBCommandCursor(testDB, resumed, /*batchSize*/ 0), 2);
    const eventsTs = EloqCdcUtil.tokenTimestamp(events[1]._id);
    assert.soon(function() {
        const reply = getMore(filtered.cursor.id);
        assert.eq(reply.cursor.nextBatch, []);
        return EloqCdcUtil.tokenTimestamp(reply.cursor.postBatchResumeToken) >= eventsTs;
    }, "the filtered stream's watermark never passed its events", EloqCdcUtil.kEventTimeoutMS, 100);

    // Row 6: idle getMores keep reporting a watermark, and it advances with the checkpoints.
    const idleStart = EloqCdcUtil.tokenTimestamp(getMore(filtered.cursor.id).cursor.postBatchResumeToken);
    let previous = idleStart;
    assert.soon(function() {
        const token = getMore(filtered.cursor.id).cursor.postBatchResumeToken;
        assert.eq(EloqCdcUtil.tokenKind(token), 1, tojson(token));
        const ts = EloqCdcUtil.tokenTimestamp(token);
        assert.gte(ts, previous, "the watermark went backwards");
        previous = ts;
        return ts > idleStart;
    }, "the idle watermark never advanced", EloqCdcUtil.kEventTimeoutMS, 100);

    // Row 9: ordinary cursors carry no token.
    const plain = assert.commandWorked(
        testDB.runCommand({aggregate: "c", pipeline: [{$match: {}}], cursor: {batchSize: 1}}));
    assert(!plain.cursor.hasOwnProperty("postBatchResumeToken"), tojson(plain));
    const found = assert.commandWorked(testDB.runCommand({find: "c", batchSize: 1}));
    assert(!found.cursor.hasOwnProperty("postBatchResumeToken"), tojson(found));

    for (let id of [opened.cursor.id, resumed.cursor.id, filtered.cursor.id]) {
        testDB.runCommand({killCursors: "c", cursors: [id]});
    }
})();
