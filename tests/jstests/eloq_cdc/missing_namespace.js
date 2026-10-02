/**
 * Change streams on namespaces that do not exist yet (docs/cdc-change-stream.md §5.1). MongoDB
 * opens them, and so does EloqDoc: the aggregate runs with no collection, the path run_aggregate
 * takes for a missing namespace. Each stream opens with a watermark and then reports the create
 * of its collection, which the first insert makes implicitly, and that insert.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const existingDB = db.getSiblingDB("eloq_cdc_missing_ns");
    const absentDB = db.getSiblingDB("eloq_cdc_missing_ns_db");
    const absentForDbStream = db.getSiblingDB("eloq_cdc_missing_ns_dbstream");
    for (let d of [existingDB, absentDB, absentForDbStream]) {
        assert.commandWorked(d.dropDatabase());
    }
    assert.commandWorked(existingDB.createCollection("other"));
    // The stream must not report the setup above (§5.1: it starts at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());

    /**
     * Opens a stream on a namespace that does not exist and checks the opening reply.
     *
     * @param testDB The database to open it on.
     * @param coll The collection name, or 1 for a database-level stream.
     * @return A cursor over the stream.
     */
    function open(testDB, coll) {
        const reply =
            assert.commandWorked(EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), coll, {}));
        assert.eq(reply.cursor.firstBatch, [], tojson(reply));
        assert.eq(EloqCdcUtil.tokenKind(reply.cursor.postBatchResumeToken), 1, tojson(reply));
        return new DBCommandCursor(testDB, reply);
    }

    const cases = [
        {
            name: "a missing collection in an existing database",
            cursor: open(existingDB, "absent"),
            target: existingDB.absent
        },
        {
            name: "a collection in a missing database",
            cursor: open(absentDB, "c"),
            target: absentDB.c
        },
        {
            name: "a database-level stream on a missing database",
            cursor: open(absentForDbStream, 1),
            target: absentForDbStream.c
        },
    ];
    assert.eq(existingDB.getCollectionNames().indexOf("absent"), -1, "opening created it");
    assert.eq(absentDB.getCollectionNames(), [], "opening created the database");
    assert.eq(absentForDbStream.getCollectionNames(), [], "opening created the database");

    cases.forEach((c, i) => assert.commandWorked(c.target.insert({_id: i, name: c.name})));
    cases.forEach((c, i) => {
        const [create, insert] = EloqCdcUtil.nextEvents(c.cursor, 2);
        const ns = {db: c.target.getDB().getName(), coll: c.target.getName()};
        assert.eq(create.operationType, "create", c.name + ": " + tojson(create));
        assert.eq(create.ns, ns, c.name);
        assert.eq(insert.operationType, "insert", c.name + ": " + tojson(insert));
        assert.eq(insert.ns, ns, c.name);
        assert.eq(insert.documentKey, {_id: i}, c.name);
        assert.eq(insert.fullDocument, {_id: i, name: c.name}, c.name);
        c.cursor.close();
    });
})();
