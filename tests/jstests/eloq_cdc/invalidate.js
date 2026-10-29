/**
 * A collection-level stream's invalidate (docs/cdc-change-stream.md §5.1, §5.4a, §5.6; plan C9
 * rows 7 and 8, C10). After the watched collection's drop the stream delivers an invalidate,
 * whose token is the drop's position with kind 2, and closes its cursor in the same reply. A
 * stream resumed from the drop's own token delivers that invalidate alone and closes, although a
 * collection of the same name exists again; resumeAfter refuses the invalidate token, and
 * startAfter on it reaches the next incarnation. A database-level stream is not invalidated. A
 * user stage that filters the invalidate out still sees the cursor close, and the closing reply
 * still carries the invalidate's token, not the drop's, from which startAfter goes on into the
 * next incarnation instead of closing again.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_invalidate");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));
    // The stream must not report the setup above (§5.1: it starts at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());
    const coll = testDB.c;

    /** Opens a stream on c, or on the database for coll 1; returns the opening reply. */
    function open(spec, collName) {
        return EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), collName || "c", spec || {});
    }

    /**
     * Reads replies until 'n' events have arrived or the cursor closes.
     *
     * @return {events, closed, token}: the events, whether the server closed the cursor, and the
     * last reply's postBatchResumeToken.
     */
    function read(reply, n) {
        const cursorColl = reply.cursor.ns.substring(reply.cursor.ns.indexOf(".") + 1);
        let events = reply.cursor.firstBatch;
        let id = reply.cursor.id;
        let token = reply.cursor.postBatchResumeToken;
        assert.soon(function() {
            if (id == 0 || events.length >= n) {
                return true;
            }
            const more = assert.commandWorked(
                testDB.runCommand({getMore: id, collection: cursorColl, maxTimeMS: 1000}));
            events = events.concat(more.cursor.nextBatch);
            id = more.cursor.id;
            token = more.cursor.postBatchResumeToken;
            return id == 0 || events.length >= n;
        }, () => "events: " + tojson(events), EloqCdcUtil.kEventTimeoutMS, 100);
        if (id != 0) {
            assert.commandWorked(testDB.runCommand({killCursors: cursorColl, cursors: [id]}));
        }
        return {events: events, closed: id == 0, token: token};
    }

    const opened = assert.commandWorked(open());
    assert.commandWorked(coll.insert({_id: 1}));
    assert(coll.drop());
    // Recreates c: a new incarnation, which the invalidated stream never reports.
    assert.commandWorked(coll.insert({_id: 2}));

    const live = read(opened, 4);
    assert.eq(live.events.map(e => e.operationType), ["insert", "drop", "invalidate"],
              tojson(live.events));
    assert(live.closed, "the invalidate left the cursor open");
    const drop = live.events[1];
    const invalidate = live.events[2];
    assert.eq(EloqCdcUtil.tokenKind(invalidate._id), 2, tojson(invalidate));
    assert.eq(EloqCdcUtil.tokenTimestamp(invalidate._id), EloqCdcUtil.tokenTimestamp(drop._id));
    assert.eq(invalidate.clusterTime, drop.clusterTime);
    // The reply that closed the cursor reports the invalidate's token.
    assert.eq(live.token, invalidate._id);

    // Resuming from the drop's own token, with either option: the same invalidate, alone, and the
    // cursor closes, although c exists again.
    for (let option of ["resumeAfter", "startAfter"]) {
        const resumed = read(assert.commandWorked(open({[option]: drop._id})), 2);
        assert.eq(resumed.events, [invalidate], option);
        assert(resumed.closed, option + ": the invalidate left the cursor open");
    }

    // An invalidate token is refused by resumeAfter; startAfter on it reaches the new incarnation.
    assert.commandFailedWithCode(open({resumeAfter: invalidate._id}),
                                 ErrorCodes.InvalidResumeToken);
    const started = read(assert.commandWorked(open({startAfter: invalidate._id})), 2);
    assert.eq(started.events.map(e => e.operationType), ["create", "insert"],
              tojson(started.events));
    assert.eq(started.events[1].documentKey, {_id: 2});
    assert(!started.closed);

    // A database stream reports the drop as an ordinary event and goes on past it.
    const database = read(assert.commandWorked(open({resumeAfter: drop._id}, 1)), 2);
    assert.eq(database.events.map(e => e.operationType), ["create", "insert"],
              tojson(database.events));
    assert(!database.closed);

    // The invalidate filtered out by a user stage: the cursor closes after the drop all the same,
    // with the invalidate's token, so startAfter reaches the next incarnation.
    assert.commandWorked(testDB.createCollection("f"));
    EloqCdcUtil.settleDdl(db.getMongo());
    const filteredOpen = assert.commandWorked(EloqCdcUtil.openRaw(
        db.getMongo(), testDB.getName(), "f", {}, [{$match: {operationType: {$ne: "invalidate"}}}]));
    assert.commandWorked(testDB.f.insert({_id: 1}));
    assert(testDB.f.drop());
    assert.commandWorked(testDB.f.insert({_id: 2}));
    const filtered = read(filteredOpen, 3);
    assert.eq(filtered.events.map(e => e.operationType), ["insert", "drop"],
              tojson(filtered.events));
    assert(filtered.closed, "filtering the invalidate out left the cursor open");
    assert.eq(EloqCdcUtil.tokenKind(filtered.token), 2, tojson(filtered.token));
    assert.eq(EloqCdcUtil.tokenTimestamp(filtered.token),
              EloqCdcUtil.tokenTimestamp(filtered.events[1]._id));
    const next = read(assert.commandWorked(open({startAfter: filtered.token}, "f")), 2);
    assert.eq(next.events.map(e => e.operationType), ["create", "insert"], tojson(next.events));
    assert.eq(next.events[1].documentKey, {_id: 2});
    assert(!next.closed);

    assert.commandWorked(testDB.dropDatabase());
})();
