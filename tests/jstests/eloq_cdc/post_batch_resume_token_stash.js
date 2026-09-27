/**
 * postBatchResumeToken when a reply fills up (plan C9 matrix row 4, getMore loop): when the next
 * event would take the reply past 16 MB, the batch loop stashes it; the reply reports the _id of
 * the last event it holds, and the next getMore starts with the stashed event and reports its
 * _id.
 *
 * A getMore stops waiting at its first result, so a reply holds only what is already buffered:
 * about one scan page per log group. The test raises cdc_scan_max_bytes to 32 MB so one page
 * holds all three events of about 6 MB each. It first waits until another stream has seen every
 * change, so they are below the checkpoint bound when the stream under test scans.
 *
 * The aggregate loop is not covered: an aggregate's first batch has no time to wait for its scans,
 * so it holds no events on a real server (row 3 cannot occur either).
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_pbrt_stash");
    const admin = db.getSiblingDB("admin");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("big"));

    /**
     * Runs a change stream's opening aggregate on the test collection.
     *
     * @param spec The $changeStream spec.
     * @param extraPipeline Stages after $changeStream.
     * @param cursorOptions The aggregate's cursor options.
     * @return The reply.
     */
    function open(spec, extraPipeline, cursorOptions) {
        return assert.commandWorked(EloqCdcUtil.openRaw(
            db.getMongo(), testDB.getName(), "big", spec, extraPipeline, cursorOptions));
    }

    /**
     * Runs one getMore on a change stream cursor.
     *
     * @param reply The reply that returned the cursor.
     * @return The getMore reply.
     */
    function getMore(reply) {
        return assert.commandWorked(
            testDB.runCommand({getMore: reply.cursor.id, collection: "big", maxTimeMS: 5000}));
    }

    const original = assert.commandWorked(admin.runCommand({getParameter: 1, cdc_scan_max_bytes: 1}))
                         .cdc_scan_max_bytes;
    try {
        // Fix a start before any change, insert three events of about 6 MB each, and wait until
        // a stream from that start has seen all of them, projecting fullDocument away to keep
        // its replies small.
        const watcher = open({}, [{$project: {fullDocument: 0}}], {batchSize: 0});
        const start = watcher.cursor.postBatchResumeToken;
        const pad = "x".repeat(6 * 1024 * 1024);
        for (let i = 0; i < 3; ++i) {
            assert.commandWorked(testDB.big.insert({_id: i, pad: pad}));
        }
        const ids =
            EloqCdcUtil.nextEvents(new DBCommandCursor(testDB, watcher, 0), 3).map((e) => e._id);
        testDB.runCommand({killCursors: "big", cursors: [watcher.cursor.id]});

        // Streams read the parameter when they open.
        assert.commandWorked(
            admin.runCommand({setParameter: 1, cdc_scan_max_bytes: 32 * 1024 * 1024}));
        const opened = open({resumeAfter: start}, [], {batchSize: 0});
        assert.eq(opened.cursor.postBatchResumeToken, start);

        // Two events fit in the first getMore's reply; the third is stashed for the next one.
        const first = getMore(opened);
        assert.eq(first.cursor.nextBatch.map((e) => e._id), ids.slice(0, 2), "first reply");
        assert.eq(first.cursor.postBatchResumeToken, ids[1], "first reply's token");
        const second = getMore(opened);
        assert.eq(second.cursor.nextBatch.map((e) => e._id), [ids[2]], "the stashed event");
        assert.eq(second.cursor.nextBatch[0].fullDocument._id, 2);
        // After appending the stashed event the loop pulls again, the source has nothing more,
        // and the reply reports the drained watermark, which covers the stashed event too. The
        // event's own _id would be the token only if more events were pending.
        const token = second.cursor.postBatchResumeToken;
        assert.eq(EloqCdcUtil.tokenKind(token), 1, "second reply's token: " + tojson(token));
        assert.gte(EloqCdcUtil.tokenTimestamp(token), EloqCdcUtil.tokenTimestamp(ids[2]),
                   "second reply's token: " + tojson(token));
        testDB.runCommand({killCursors: "big", cursors: [opened.cursor.id]});
    } finally {
        assert.commandWorked(admin.runCommand({setParameter: 1, cdc_scan_max_bytes: original}));
    }
})();
