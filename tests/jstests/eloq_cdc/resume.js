/**
 * Resuming an EloqDoc change stream (docs/cdc-change-stream.md §5.6; plan C8a, C8b, C13 ii):
 * from the opening watermark, from event tokens (including one inside a multi-document insert),
 * with resumeAfter and startAfter, and from a watermark taken after events. Each resume must
 * deliver exactly the changes after its token: none before it, none twice.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_resume");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));
    const coll = testDB.c;

    function open(spec) {
        const reply = EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), "c", spec);
        assert.commandWorked(reply);
        assert(reply.cursor.postBatchResumeToken, tojson(reply));
        return reply.cursor;
    }

    function kill(id) {
        assert.commandWorked(testDB.runCommand({killCursors: "c", cursors: [id]}));
    }

    function getMore(id) {
        return assert.commandWorked(
            testDB.runCommand({getMore: id, collection: "c", maxTimeMS: 1000}));
    }

    /** The _ids of the next 'n' changed documents, waiting for them to arrive. */
    function readKeys(id, n) {
        let events = [];
        assert.soon(function() {
            events = events.concat(getMore(id).cursor.nextBatch);
            return events.length >= n;
        }, () => "events: " + tojson(events), EloqCdcUtil.kEventTimeoutMS, 100);
        assert.eq(events.length, n, tojson(events));
        return events;
    }

    /**
     * Reads on until the stream's watermark passes 'ts', and fails on any event meanwhile: nothing
     * at or below 'ts' may be left to deliver.
     */
    function assertQuietThrough(id, ts) {
        assert.soon(function() {
            const reply = getMore(id);
            assert.eq(reply.cursor.nextBatch, [], "unexpected events");
            const token = reply.cursor.postBatchResumeToken;
            return EloqCdcUtil.tokenKind(token) === 1 && EloqCdcUtil.tokenTimestamp(token) >= ts;
        }, "the watermark never passed " + ts, EloqCdcUtil.kEventTimeoutMS, 100);
    }

    function keysOf(events) {
        return events.map(e => e.documentKey._id);
    }

    function lastTs(events) {
        return EloqCdcUtil.tokenTimestamp(events[events.length - 1]._id);
    }

    // C8a / C13 ii: the cursor is lost before the first event; a change committed meanwhile
    // arrives exactly once after reopening from the opening watermark. A fresh open would have
    // started past it.
    const first = open({});
    const opening = first.postBatchResumeToken;
    kill(first.id);
    assert.commandWorked(coll.insert({_id: "E"}));
    const second = open({resumeAfter: opening});
    // A token-based open reports the supplied token (§5.1).
    assert.eq(second.postBatchResumeToken, opening);
    const e = readKeys(second.id, 1);
    assert.eq(keysOf(e), ["E"]);
    assertQuietThrough(second.id, lastTs(e));

    // C8b: after events, reopen from each event token.
    assert.commandWorked(coll.insert({_id: "A1"}));
    assert.commandWorked(coll.insert({_id: "A2"}));
    assert.commandWorked(coll.insert({_id: "A3"}));
    const a = readKeys(second.id, 3);
    assert.eq(keysOf(a), ["A1", "A2", "A3"]);

    const afterA1 = open({resumeAfter: a[0]._id});
    assert.eq(keysOf(readKeys(afterA1.id, 2)), ["A2", "A3"]);
    assertQuietThrough(afterA1.id, lastTs(a));
    kill(afterA1.id);

    const afterA2 = open({startAfter: a[1]._id});
    assert.eq(keysOf(readKeys(afterA2.id, 1)), ["A3"]);
    assertQuietThrough(afterA2.id, lastTs(a));
    kill(afterA2.id);

    // Inside one multi-document insert: every resume point between its changes.
    const batch = [{_id: "B1"}, {_id: "B2"}, {_id: "B3"}, {_id: "B4"}];
    assert.commandWorked(coll.insert(batch));
    const b = readKeys(second.id, batch.length);
    assert.eq(keysOf(b), ["B1", "B2", "B3", "B4"]);
    for (let i = 0; i < b.length; ++i) {
        const resumed = open({resumeAfter: b[i]._id});
        if (i + 1 < b.length) {
            assert.eq(keysOf(readKeys(resumed.id, b.length - i - 1)),
                      keysOf(b.slice(i + 1)),
                      "resuming after " + b[i].documentKey._id);
        }
        assertQuietThrough(resumed.id, lastTs(b));
        kill(resumed.id);
    }

    // A watermark taken after the events: nothing before it comes back, later changes do.
    let watermark;
    assert.soon(function() {
        const reply = getMore(second.id);
        assert.eq(reply.cursor.nextBatch, []);
        watermark = reply.cursor.postBatchResumeToken;
        return EloqCdcUtil.tokenKind(watermark) === 1 &&
            EloqCdcUtil.tokenTimestamp(watermark) >= lastTs(b);
    }, "no watermark past the events", EloqCdcUtil.kEventTimeoutMS, 100);
    const fromWatermark = open({resumeAfter: watermark});
    assert.commandWorked(coll.insert({_id: "C"}));
    assert.eq(keysOf(readKeys(fromWatermark.id, 1)), ["C"]);
    kill(fromWatermark.id);

    kill(second.id);
})();
