// Real concurrent writers must retry the current document without replaying earlier $inc writes.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
    "use strict";
    load("tests/jstests/libs/parallelTester.js");
    const coll = db.getCollection("update_many_write_conflict_retry");
    coll.drop();
    const documentCount = 32;
    const workerCount = 4;
    const rounds = 8;
    const documents = [];
    for (let i = 0; i < documentCount; ++i) {
        documents.push({_id: i, group: "all", counter: 0});
    }
    assert.writeOK(coll.insert(documents));
    const before = db.serverStatus().metrics.operation.writeConflicts;
    const barrier = new CountDownLatch(workerCount + 1);
    const workers = [];

    function updateWorker(host, database, collection, rounds, count, barrier) {
        const workerDB = new Mongo(host).getDB(database);
        barrier.countDown();
        while (barrier.getCount() > 0) {}
        for (let i = 0; i < rounds; ++i) {
            const result = workerDB.runCommand({
                update: collection,
                updates: [{q: {group: "all"}, u: {$inc: {counter: 1}}, multi: true}],
                maxTimeMS: 30000
            });
            assert.commandWorked(result);
            assert.eq(undefined, result.writeErrors, tojson(result));
            assert.eq(count, result.n, tojson(result));
            assert.eq(count, result.nModified, tojson(result));
        }
        return {ok: 1};
    }

    for (let i = 0; i < workerCount; ++i) {
        const worker = new ScopedThread(updateWorker, db.getMongo().host, db.getName(),
                                        coll.getName(), rounds, documentCount, barrier);
        workers.push(worker);
        worker.start();
    }
    assert.soon(function() { return barrier.getCount() === 1; }, "workers did not start", 30000);
    barrier.countDown();
    workers.forEach(function(worker) {
        worker.join();
        assert.commandWorked(worker.returnData());
    });
    assert.eq(documentCount, coll.find({counter: workerCount * rounds}).itcount());
    const after = db.serverStatus().metrics.operation.writeConflicts;
    assert.gt(after, before, "test must exercise the server write-conflict retry path");
    print("updateMany retried " + (after - before) + " write conflicts");
    coll.drop();
}());
