/**
 * postBatchResumeToken in the aggregate's own batch loop (run_aggregate.cpp handleCursorCommand;
 * plan C9 matrix rows 3 and 4, docs/cdc-change-stream.md §10 item 9). On a real server the
 * aggregate that opens a change stream returns at once, before its scans answer, so its first
 * batch never holds an event. The eloqChangeStreamFirstBatchWait fail point lets it wait for them.
 *
 *  - Row 3: a first batch cut by its batchSize reports the _id of the last event it holds, and
 *    the first getMore carries on with the next event.
 *  - Row 4: a first batch cut because the next event would take the reply past 16 MB stashes that
 *    event and reports the _id of the last event it holds; the first getMore starts with the
 *    stashed event.
 *
 * Both start from a token fixed before the events, once another stream has seen every one of
 * them, so they are below the checkpoint bound when the stream under test scans.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_pbrt_first_batch");
    const admin = db.getSiblingDB("admin");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("small"));
    assert.commandWorked(testDB.createCollection("big"));
    // The streams must not report the setup above (§5.1: they start at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());

    /**
     * Runs a change stream's opening aggregate on a test collection.
     *
     * @param coll The collection.
     * @param spec The $changeStream spec.
     * @param extraPipeline Stages after $changeStream.
     * @param cursorOptions The aggregate's cursor options.
     * @return The reply.
     */
    function open(coll, spec, extraPipeline, cursorOptions) {
        return assert.commandWorked(EloqCdcUtil.openRaw(
            db.getMongo(), testDB.getName(), coll, spec, extraPipeline, cursorOptions));
    }

    /**
     * Runs one getMore on a change stream cursor.
     *
     * @param coll The collection.
     * @param reply The reply that returned the cursor.
     * @return The getMore reply.
     */
    function getMore(coll, reply) {
        return assert.commandWorked(
            testDB.runCommand({getMore: reply.cursor.id, collection: coll, maxTimeMS: 5000}));
    }

    /**
     * Fixes a start before any change on a collection, makes the changes, and waits until a
     * stream from that start has seen all of them.
     *
     * @param coll The collection.
     * @param docs The documents to insert.
     * @return {start, ids}: the start token and the events' _ids, in order.
     */
    function eventsAfterStart(coll, docs) {
        const watcher = open(coll, {}, [{$project: {fullDocument: 0}}], {batchSize: 0});
        const start = watcher.cursor.postBatchResumeToken;
        docs.forEach((doc) => assert.commandWorked(testDB[coll].insert(doc)));
        const ids = EloqCdcUtil.nextEvents(new DBCommandCursor(testDB, watcher, 0), docs.length)
                        .map((e) => e._id);
        testDB.runCommand({killCursors: coll, cursors: [watcher.cursor.id]});
        return {start: start, ids: ids};
    }

    /**
     * Checks a reply's token is the drained watermark, at or above an event's position.
     *
     * @param reply The reply.
     * @param id The event's _id.
     * @param what The reply, for messages.
     */
    function assertWatermarkCovers(reply, id, what) {
        const token = reply.cursor.postBatchResumeToken;
        assert.eq(EloqCdcUtil.tokenKind(token), 1, what + "'s token: " + tojson(token));
        assert.gte(EloqCdcUtil.tokenTimestamp(token), EloqCdcUtil.tokenTimestamp(id),
                   what + "'s token: " + tojson(token));
    }

    const small = eventsAfterStart("small", [0, 1, 2, 3, 4].map((i) => ({_id: i})));
    const pad = "x".repeat(6 * 1024 * 1024);
    const big = eventsAfterStart("big", [0, 1, 2].map((i) => ({_id: i, pad: pad})));

    const original = assert.commandWorked(admin.runCommand({getParameter: 1, cdc_scan_max_bytes: 1}))
                         .cdc_scan_max_bytes;
    assert.commandWorked(admin.runCommand({
        configureFailPoint: "eloqChangeStreamFirstBatchWait",
        mode: "alwaysOn",
        data: {waitMs: 10000}
    }));
    try {
        // Row 3: the first batch is cut by its batchSize.
        const cut = open("small", {resumeAfter: small.start}, [], {batchSize: 2});
        assert.eq(cut.cursor.firstBatch.map((e) => e._id), small.ids.slice(0, 2), "first batch");
        assert.eq(cut.cursor.postBatchResumeToken, small.ids[1], "first batch's token");
        const rest = getMore("small", cut);
        assert.eq(rest.cursor.nextBatch.map((e) => e._id), small.ids.slice(2), "first getMore");
        assertWatermarkCovers(rest, small.ids[4], "first getMore");
        testDB.runCommand({killCursors: "small", cursors: [cut.cursor.id]});

        // Row 4: one scan page holds all three events of about 6 MB each (streams read the
        // parameter when they open); two fit in the first batch, and the third is stashed.
        assert.commandWorked(
            admin.runCommand({setParameter: 1, cdc_scan_max_bytes: 32 * 1024 * 1024}));
        const full = open("big", {resumeAfter: big.start}, [], {});
        assert.eq(full.cursor.firstBatch.map((e) => e._id), big.ids.slice(0, 2), "first batch");
        assert.eq(full.cursor.postBatchResumeToken, big.ids[1], "first batch's token");
        const stashed = getMore("big", full);
        assert.eq(stashed.cursor.nextBatch.map((e) => e._id), [big.ids[2]], "the stashed event");
        assert.eq(stashed.cursor.nextBatch[0].fullDocument._id, 2);
        // After the stashed event the loop pulls again and finds nothing more: the reply reports
        // the drained watermark, which covers the stashed event too.
        assertWatermarkCovers(stashed, big.ids[2], "first getMore");
        testDB.runCommand({killCursors: "big", cursors: [full.cursor.id]});
    } finally {
        assert.commandWorked(
            admin.runCommand({configureFailPoint: "eloqChangeStreamFirstBatchWait", mode: "off"}));
        assert.commandWorked(admin.runCommand({setParameter: 1, cdc_scan_max_bytes: original}));
    }
})();
