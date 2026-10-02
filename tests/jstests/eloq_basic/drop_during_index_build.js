/**
 * A drop issued while a foreground index build runs on the same collection finishes promptly.
 *
 * The drop's catalog write intent conflicts with the build's, so the drop retries until the build
 * ends (writeConflictRetry). Its backoff used to sleep the thread, which in EloqDoc is a brpc
 * worker that also drives a core's transaction processor: retries sleeping back to back stalled
 * that core, and with it the build the drop was waiting for, which then took minutes instead of
 * seconds. The backoff now yields the coroutine (WriteConflictException::logAndBackoffYielding).
 *
 * The test builds an index over enough documents to take a while, issues the drop once the build
 * has run a moment, and checks that both commands end well within a minute and the collection is
 * gone.
 */
(function() {
    "use strict";

    const kDbName = "eloq_basic_drop_during_index_build";
    const testDB = db.getSiblingDB(kDbName);
    assert.commandWorked(testDB.dropDatabase());
    const coll = testDB.c;
    for (let batch = 0; batch < 10; batch++) {
        const bulk = coll.initializeUnorderedBulkOp();
        for (let i = 0; i < 10000; i++) {
            const id = batch * 10000 + i;
            bulk.insert({_id: id, a: id % 997, s: "x".repeat(64) + id});
        }
        assert.commandWorked(bulk.execute());
    }

    const awaitBuild = startParallelShell(function() {
        const res = db.getSiblingDB("eloq_basic_drop_during_index_build")
                        .runCommand({createIndexes: "c", indexes: [{key: {a: 1, s: 1}, name: "a_s"}]});
        // The drop may win the race to the catalog, in which case the build fails cleanly.
        assert(res.ok === 1 || res.code !== undefined, tojson(res));
    }, db.getMongo().port);

    // Drop once the build has been running a moment.
    assert.soon(function() {
        const ops = db.currentOp({"command.createIndexes": "c"}).inprog;
        return ops.length > 0 && ops[0].microsecs_running > 100 * 1000;
    }, "the index build never started", 60 * 1000, 5);
    const start = Date.now();
    assert.commandWorked(testDB.runCommand({drop: "c"}));
    const dropMs = Date.now() - start;
    awaitBuild();
    jsTestLog("the drop took " + dropMs + " ms");

    assert.lt(dropMs, 60 * 1000, "the drop starved the index build it was waiting for");
    assert.eq(-1, testDB.getCollectionNames().indexOf("c"));
    assert.commandWorked(testDB.dropDatabase());
})();
