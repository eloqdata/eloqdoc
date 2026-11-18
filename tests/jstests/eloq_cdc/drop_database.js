/**
 * dropDatabase must emit one drop per collection, including empty collections. Database and
 * deployment streams see all drops and can resume between them; each collection stream is
 * invalidated. A later insert into a recreated collection bounds the drops without relying on
 * collection enumeration order or a fixed sleep.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_drop_database");
    const admin = db.getSiblingDB("admin");
    const names = ["empty", "indexed", "populated"];
    const marker = {_id: "after-drop", value: 2};
    const deploymentFilter = [{$match: {"ns.db": testDB.getName()}}];
    const streams = [];

    assert.commandWorked(testDB.dropDatabase());
    for (let name of names) {
        assert.commandWorked(testDB.createCollection(name));
    }
    assert.commandWorked(testDB.indexed.createIndex({value: 1}));
    assert.commandWorked(testDB.populated.insert({_id: "before-drop", value: 1}));
    EloqCdcUtil.settleDdl(db.getMongo());

    function open(database, collection, spec, pipeline) {
        const reply = assert.commandWorked(EloqCdcUtil.openRaw(
            db.getMongo(), database.getName(), collection, spec, pipeline, {batchSize: 0}));
        assert.eq(reply.cursor.firstBatch, [], tojson(reply));
        const stream = {
            db: database,
            collection: reply.cursor.ns.substring(reply.cursor.ns.indexOf(".") + 1),
            id: reply.cursor.id,
            events: []
        };
        streams.push(stream);
        return stream;
    }

    function getMore(stream) {
        const reply = assert.commandWorked(stream.db.runCommand(
            {getMore: stream.id, collection: stream.collection, maxTimeMS: 1000}));
        stream.id = reply.cursor.id;
        stream.events = stream.events.concat(reply.cursor.nextBatch);
        return reply.cursor;
    }

    function isMarker(event) {
        return event.operationType === "insert" && event.ns.coll === "populated" &&
            event.documentKey._id === marker._id;
    }

    // The marker follows every drop. Read through its watermark too, so duplicate or unexpected
    // events fail the test rather than being left in an unread batch.
    function readThroughMarker(stream) {
        assert.soon(function() {
            assert.neq(stream.id, 0, "dropDatabase closed a database/deployment stream");
            getMore(stream);
            return stream.events.some(isMarker);
        }, () => "marker never arrived; events: " + tojson(stream.events),
                    EloqCdcUtil.kEventTimeoutMS, 100);
        const last = stream.events[stream.events.length - 1];
        assert(isMarker(last), tojson(stream.events));
        const markerTs = EloqCdcUtil.tokenTimestamp(last._id);
        assert.soon(function() {
            assert.neq(stream.id, 0, "dropDatabase closed a database/deployment stream");
            const cursor = getMore(stream);
            assert.eq(cursor.nextBatch, [], "unexpected events: " + tojson(cursor.nextBatch));
            const token = cursor.postBatchResumeToken;
            return EloqCdcUtil.tokenKind(token) === 1 &&
                EloqCdcUtil.tokenTimestamp(token) >= markerTs;
        }, "the watermark never passed the marker", EloqCdcUtil.kEventTimeoutMS, 100);
        return stream.events;
    }

    try {
        const database = open(testDB, 1, {});
        const deployment = open(admin, 1, {allChangesForCluster: true}, deploymentFilter);
        const collections = names.map(name => open(testDB, name, {}));

        assert.commandWorked(testDB.dropDatabase());
        assert.eq(testDB.getCollectionNames(), []);
        // Repeating the drop on an empty/missing database must not emit more drops.
        assert.commandWorked(testDB.dropDatabase());
        assert.commandWorked(testDB.populated.insert(marker));

        const events = readThroughMarker(database);
        assert.eq(events.map(e => e.operationType),
                  ["drop", "drop", "drop", "create", "insert"], tojson(events));
        const drops = events.slice(0, names.length);
        assert.eq(drops.map(e => e.ns.coll).sort(), names, tojson(drops));
        for (let event of events) {
            assert.eq(event.ns.db, testDB.getName(), tojson(event));
        }
        assert.eq(events[3].ns.coll, "populated", tojson(events[3]));
        assert.eq(events[4].fullDocument, marker, tojson(events[4]));
        assert.eq(readThroughMarker(deployment), events);

        // The first emitted drop is not necessarily the first name in 'names'. Resume from the
        // actual event, and require all remaining drops plus the new incarnation, exactly once.
        const resumeAfter = drops[0]._id;
        assert.eq(readThroughMarker(open(testDB, 1, {resumeAfter: resumeAfter})), events.slice(1));
        assert.eq(readThroughMarker(open(admin, 1,
                                         {allChangesForCluster: true, resumeAfter: resumeAfter},
                                         deploymentFilter)),
                  events.slice(1));

        // All original collection streams close, even the one whose namespace was recreated.
        for (let stream of collections) {
            assert.soon(function() {
                if (stream.id != 0) {
                    getMore(stream);
                }
                return stream.id == 0;
            }, () => "collection stream did not close: " + tojson(stream.events),
                        EloqCdcUtil.kEventTimeoutMS, 100);
            assert.eq(stream.events.map(e => e.operationType), ["drop", "invalidate"],
                      tojson(stream.events));
            assert.eq(stream.events[0], drops.find(e => e.ns.coll === stream.collection));
        }
        assert.eq(testDB.populated.find().toArray(), [marker]);
    } finally {
        for (let stream of streams) {
            if (stream.id != 0) {
                assert.commandWorked(stream.db.runCommand(
                    {killCursors: stream.collection, cursors: [stream.id]}));
            }
        }
    }
    assert.commandWorked(testDB.dropDatabase());
})();
