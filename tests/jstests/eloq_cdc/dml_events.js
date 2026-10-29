/**
 * Insert, replace and delete events of an EloqDoc change stream (docs/cdc-change-stream.md §5.4,
 * §5.4a): every field of each event, the opening reply's watermark, and a delete whose _id needs
 * 64 bits, which the canonical key decoder must keep exact.
 *
 * Driven with raw aggregate/getMore commands, so it checks the wire contract whatever the shell.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_dml");
    assert.commandWorked(testDB.dropDatabase());
    const coll = testDB.events;
    assert.commandWorked(testDB.createCollection(coll.getName()));
    // The stream must not report the setup above (§5.1: it starts at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());

    // Open: the reply carries a watermark even though nothing is returned (§5.1, F13).
    const opened = EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), coll.getName(), {});
    assert.commandWorked(opened);
    assert.eq(opened.cursor.firstBatch.length, 0, tojson(opened));
    const openingToken = opened.cursor.postBatchResumeToken;
    assert(openingToken, "the opening reply must carry a postBatchResumeToken: " + tojson(opened));
    assert.eq(EloqCdcUtil.tokenKind(openingToken), 1, tojson(openingToken));
    const cursorId = opened.cursor.id;
    assert.neq(cursorId, 0, "a change stream cursor stays open");

    const bigId = NumberLong("5000000000");  // above 32 bits
    assert.commandWorked(coll.insert({_id: 1, a: 1}));
    assert.commandWorked(coll.update({_id: 1}, {$set: {a: 2}}));
    assert.commandWorked(coll.insert({_id: bigId, b: "x"}));
    assert.commandWorked(coll.remove({_id: bigId}));
    assert.commandWorked(coll.remove({_id: 1}));

    // Pull until the five events arrive; every getMore reply carries a token.
    let events = [];
    assert.soon(function() {
        const reply = assert.commandWorked(testDB.runCommand(
            {getMore: cursorId, collection: coll.getName(), maxTimeMS: 1000}));
        assert(reply.cursor.postBatchResumeToken, tojson(reply));
        events = events.concat(reply.cursor.nextBatch);
        return events.length >= 5;
    }, () => "events so far: " + tojson(events), EloqCdcUtil.kEventTimeoutMS, 100);
    assert.eq(events.length, 5, tojson(events));

    const expected = [
        {op: "insert", key: 1, doc: {_id: 1, a: 1}},
        {op: "replace", key: 1, doc: {_id: 1, a: 2}},
        {op: "insert", key: bigId, doc: {_id: bigId, b: "x"}},
        {op: "delete", key: bigId},
        {op: "delete", key: 1},
    ];
    let lastTs = EloqCdcUtil.tokenTimestamp(openingToken);
    for (let i = 0; i < events.length; ++i) {
        const e = events[i];
        const want = expected[i];
        assert.eq(e.operationType, want.op, tojson(e));
        assert.eq(e.ns, {db: testDB.getName(), coll: coll.getName()}, tojson(e));
        // Value equality: a delete's key comes back value-preserved, possibly in another numeric
        // type, and must still match the document.
        assert.eq(bsonWoCompare({k: e.documentKey._id}, {k: want.key}), 0, tojson(e));
        if (want.doc) {
            assert.docEq(e.fullDocument, want.doc, tojson(e));
        } else {
            assert(!e.hasOwnProperty("fullDocument"), tojson(e));
        }
        assert.eq(typeof e._id._data, "string", tojson(e));
        assert.eq(EloqCdcUtil.tokenKind(e._id), 0, tojson(e));
        const ts = EloqCdcUtil.tokenTimestamp(e._id);
        assert.gt(ts, EloqCdcUtil.tokenTimestamp(openingToken), tojson(e));
        assert.gte(ts, lastTs, tojson(e));
        lastTs = ts;
        // clusterTime is the event's timestamp, seconds and microseconds.
        assert.eq(e.clusterTime, Timestamp(Math.floor(ts / 1e6), ts % 1e6), tojson(e));
        // eloq.keyBytes is the logged key, identical for every change of one document.
        assert.eq(e.eloq.keyBytes.subtype(), 0, tojson(e));
    }
    assert.eq(events[0].eloq.keyBytes, events[1].eloq.keyBytes);
    assert.eq(events[0].eloq.keyBytes, events[4].eloq.keyBytes);
    assert.eq(events[2].eloq.keyBytes, events[3].eloq.keyBytes);

    assert.commandWorked(testDB.runCommand({killCursors: coll.getName(), cursors: [cursorId]}));
})();
