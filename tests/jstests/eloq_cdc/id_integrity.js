/**
 * The end-of-pipeline check on an event's _id (docs/cdc-change-stream.md §5.3 step 4; plan C9
 * matrix rows 10-13): a user stage that removes or replaces _id, which holds the resume token,
 * fails the stream with ChangeStreamFatalError (280), while stages that keep _id work, including
 * a $match that drops some events.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_id_integrity");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));
    // The stream must not report the setup above (§5.1: it starts at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());
    const coll = testDB.c;
    const kChangeStreamFatalError = 280;

    function open(userStages) {
        const reply = EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), "c", {}, userStages);
        assert.commandWorked(reply);
        return reply.cursor.id;
    }

    const removed = open([{$project: {_id: 0}}]);
    const replaced = open([{$addFields: {_id: "$documentKey"}}]);
    const nested = open([{$addFields: {"_id._data": "tampered"}}]);
    const kept = open([{$match: {"documentKey._id": {$gte: 2}}}, {$project: {_id: 1, operationType: 1}}]);

    assert.commandWorked(coll.insert({_id: 1}));
    assert.commandWorked(coll.insert({_id: 2}));
    assert.commandWorked(coll.insert({_id: 3}));

    // Rows 10-12: the first event that reaches the client fails the stream.
    for (let id of [removed, replaced, nested]) {
        assert.soon(function() {
            const reply = testDB.runCommand({getMore: id, collection: "c", maxTimeMS: 1000});
            if (reply.ok) {
                assert.eq(reply.cursor.nextBatch, [], "a rewritten _id reached the client");
                return false;
            }
            assert.eq(reply.code, kChangeStreamFatalError, tojson(reply));
            return true;
        }, "the stream never failed", EloqCdcUtil.kEventTimeoutMS, 100);
    }

    // Row 13: stages that keep _id deliver the events they pass, token intact.
    const events = EloqCdcUtil.nextEvents(
        new DBCommandCursor(testDB, {ok: 1, cursor: {id: kept, ns: coll.getFullName(), firstBatch: []}}, 0),
        2);
    assert.eq(events.map(e => Object.keys(e).sort()), [["_id", "operationType"], ["_id", "operationType"]]);
    for (let e of events) {
        assert.eq(EloqCdcUtil.tokenKind(e._id), 0, tojson(e));
    }
    testDB.runCommand({killCursors: "c", cursors: [kept]});
})();
