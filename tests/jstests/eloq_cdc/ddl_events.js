/**
 * DDL events (docs/cdc-change-stream.md §5.4, §5.4a, §5.6; plan C7, C8b, B8): a database-level
 * stream reports a collection's create, with the options listCollections reports; an index build
 * as one createIndexes with every spec listIndexes reports; an index drop; an index build rolled
 * back by a key over the size limit, as its createIndexes and then a dropIndexes; the drop; and
 * the next incarnation's create, made implicitly by an insert. A stream resumed from each DDL
 * event's token, with resumeAfter or startAfter, delivers exactly the events after it.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_ddl_events");
    assert.commandWorked(testDB.dropDatabase());
    const coll = testDB.c;

    /** Opens a database-level stream; returns its cursor id. */
    function open(spec) {
        const reply = assert.commandWorked(
            EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), 1, spec || {}));
        return reply.cursor.id;
    }

    function getMore(id) {
        return assert.commandWorked(
            testDB.runCommand({getMore: id, collection: "$cmd.aggregate", maxTimeMS: 1000}));
    }

    /**
     * Reads exactly 'n' events, then reads on until the watermark passes the last of them and
     * fails on anything more: nothing at or below it may be left to deliver.
     */
    function readExactly(id, n) {
        let events = [];
        assert.soon(function() {
            events = events.concat(getMore(id).cursor.nextBatch);
            return events.length >= n;
        }, () => "events: " + tojson(events), EloqCdcUtil.kEventTimeoutMS, 100);
        assert.eq(events.length, n, tojson(events));
        const lastTs = n > 0 ? EloqCdcUtil.tokenTimestamp(events[n - 1]._id) : 0;
        assert.soon(function() {
            const reply = getMore(id);
            assert.eq(reply.cursor.nextBatch, [], "unexpected events after " + tojson(events));
            const token = reply.cursor.postBatchResumeToken;
            return EloqCdcUtil.tokenKind(token) === 1 &&
                EloqCdcUtil.tokenTimestamp(token) >= lastTs;
        }, "the watermark never passed " + lastTs, EloqCdcUtil.kEventTimeoutMS, 100);
        assert.commandWorked(
            testDB.runCommand({killCursors: "$cmd.aggregate", cursors: [id]}));
        return events;
    }

    /** An index spec as the events report it: listIndexes' spec without its ns. */
    function specOf(name) {
        const spec = coll.getIndexes().find(ix => ix.name === name);
        assert(spec, name + " not in " + tojson(coll.getIndexes()));
        const copy = Object.assign({}, spec);
        delete copy.ns;
        return copy;
    }

    // The stream must not report the drop above (§5.1: it starts at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());
    const id = open();

    assert.commandWorked(testDB.createCollection("c", {validator: {a: {$exists: true}}}));
    const listed = testDB.getCollectionInfos({name: "c"})[0].options;
    // EloqDoc builds a unique index only on an empty collection.
    assert.commandWorked(testDB.runCommand({
        createIndexes: "c",
        indexes: [{key: {a: 1}, name: "a_1"}, {key: {b: -1}, name: "b_-1", unique: true}]
    }));
    const createdSpecs = [specOf("a_1"), specOf("b_-1")];
    assert.commandWorked(coll.insert({_id: 1, a: 1, b: 1}));
    assert.commandWorked(coll.dropIndex("b_-1"));
    // A key over the 1024-byte limit fails the build after its prepare log, which rolls it back.
    assert.commandWorked(coll.insert({_id: 2, a: 2, big: "x".repeat(1100)}));
    assert.commandFailedWithCode(
        testDB.runCommand({createIndexes: "c", indexes: [{key: {big: 1}, name: "big_1"}]}),
        ErrorCodes.KeyTooLong);
    assert(coll.drop());
    assert.commandWorked(coll.insert({_id: 3, a: 3}));

    const events = readExactly(id, 10);
    assert.eq(events.map(e => e.operationType),
              [
                  "create",
                  "createIndexes",
                  "insert",
                  "dropIndexes",
                  "insert",
                  "createIndexes",
                  "dropIndexes",
                  "drop",
                  "create",
                  "insert"
              ],
              tojson(events));

    let lastTs = 0;
    for (let e of events) {
        assert.eq(e.ns, {db: testDB.getName(), coll: "c"}, tojson(e));
        assert.eq(EloqCdcUtil.tokenKind(e._id), 0, tojson(e));
        const ts = EloqCdcUtil.tokenTimestamp(e._id);
        assert.eq(e.clusterTime, Timestamp(Math.floor(ts / 1e6), ts % 1e6), tojson(e));
        assert.gte(ts, lastTs, "events out of timestamp order: " + tojson(events));
        lastTs = ts;
        if (e.operationType !== "insert") {
            // DDL events carry neither a document key nor the key bytes.
            assert(!e.hasOwnProperty("documentKey"), tojson(e));
            assert(!e.hasOwnProperty("eloq"), tojson(e));
        }
    }

    // create: the options as listCollections reports them; none for the implicit create.
    assert.docEq(events[0].operationDescription, listed);
    assert.docEq(events[8].operationDescription, {});
    // One createIndexes for the op that built two indexes, with their specs as listIndexes has
    // them; the drop names the index by version, key pattern and name.
    assert.docEq(events[1].operationDescription, {indexes: createdSpecs});
    assert.docEq(events[3].operationDescription,
                 {indexes: [{v: createdSpecs[1].v, key: {b: -1}, name: "b_-1"}]});
    // The rolled-back build: its createIndexes at its prepare log, then a dropIndexes, later.
    const built = events[5].operationDescription.indexes;
    assert.eq(built.length, 1, tojson(events[5]));
    assert.eq([built[0].name, built[0].key], ["big_1", {big: 1}], tojson(events[5]));
    assert.docEq(events[6].operationDescription,
                 {indexes: [{v: built[0].v, key: {big: 1}, name: "big_1"}]});
    assert.lt(EloqCdcUtil.tokenTimestamp(events[5]._id),
              EloqCdcUtil.tokenTimestamp(events[6]._id));
    assert(!events[7].hasOwnProperty("operationDescription"), tojson(events[7]));

    // Resuming from each DDL event delivers exactly the rest.
    events.forEach((e, i) => {
        if (e.operationType === "insert") {
            return;
        }
        const option = i % 2 === 0 ? "resumeAfter" : "startAfter";
        const rest = readExactly(open({[option]: e._id}), events.length - i - 1);
        assert.eq(rest, events.slice(i + 1), option + " event " + i);
    });

    assert.commandWorked(testDB.dropDatabase());
})();
