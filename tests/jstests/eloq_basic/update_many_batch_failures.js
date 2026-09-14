// Exercise rollback, replay and concurrent changes at real write-set batch boundaries.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
"use strict";
load("tests/jstests/libs/parallelTester.js");
const coll = db.getCollection("update_many_batch_failures");
const ns = coll.getFullName();
const payload = "x".repeat(1024 * 1024);
const count = 72;

function populate() {
    coll.drop();
    for (let i = 0; i < count; ++i) {
        assert.writeOK(
            coll.insert({_id: i, counter: 0, eligible: true, uniqueKey: i, payload: payload}));
    }
}
function configure(name, mode, data) {
    assert.commandWorked(db.adminCommand({configureFailPoint: name, mode: mode, data: data || {}}));
}
function stats() {
    return db.serverStatus().metrics.updateMany;
}
function startUpdate(maxTimeMS) {
    const thread = new ScopedThread(function(host, database, collection, timeout) {
        return new Mongo(host).getDB(database).runCommand({
            update: collection,
            updates: [{q: {eligible: true}, u: {$inc: {counter: 1}}, multi: true}],
            maxTimeMS: timeout
        });
    }, db.getMongo().host, db.getName(), coll.getName(), maxTimeMS);
    thread.start();
    return thread;
}
function waitForBoundary() {
    assert.soon(function() {
        return db.currentOp({"ns": ns, "msg": "hangAfterUpdateManyBatchCommit"}).inprog.length > 0;
    }, "update did not reach the committed batch boundary", 30000);
}

populate();
let before = stats();
configure("failUpdateManyBatch",
          {times: 1},
          {namespace: ns, batch: 2, errorCode: ErrorCodes.WriteConflict});
try {
    const result = coll.updateMany({}, {$inc: {counter: 1}});
    assert.eq(count, result.matchedCount);
    assert.eq(count, result.modifiedCount);
    assert.eq(count, coll.find({counter: 1}).itcount());
    assert.eq(2, stats().committedBatches - before.committedBatches);
    assert.eq(1, stats().retriedBatches - before.retriedBatches);
} finally {
    configure("failUpdateManyBatch", "off");
}

// The next candidate was read before the commit. It must be read and matched again, and
// documents deleted or excluded by another writer must be skipped.
populate();
configure("hangAfterUpdateManyBatchCommit", "alwaysOn", {namespace: ns, batch: 1});
let worker = startUpdate(30000);
try {
    waitForBoundary();
    assert.eq(61, coll.find({counter: 1}).itcount());
    assert.eq(1, coll.updateOne({_id: 61}, {$inc: {counter: 10}}).modifiedCount);
    assert.writeOK(coll.remove({_id: 62}));
    assert.eq(1, coll.updateOne({_id: 63}, {$set: {eligible: false}}).modifiedCount);
} finally {
    configure("hangAfterUpdateManyBatchCommit", "off");
}
worker.join();
let response = worker.returnData();
assert.commandWorked(response);
assert.eq(undefined, response.writeErrors, tojson(response));
assert.eq(70, response.n, tojson(response));
assert.eq(70, response.nModified, tojson(response));
assert.eq(11, coll.findOne({_id: 61}).counter);
assert.eq(null, coll.findOne({_id: 62}));
assert.eq(0, coll.findOne({_id: 63}).counter);
assert.eq(69, coll.find({counter: 1}).itcount());

// A deadline after the first commit must leave that prefix committed and stop the command.
populate();
configure("hangAfterUpdateManyBatchCommit", "alwaysOn", {namespace: ns, batch: 1});
worker = startUpdate(5000);
try {
    waitForBoundary();
    worker.join();
    response = worker.returnData();
    assert.commandFailedWithCode(response, ErrorCodes.MaxTimeMSExpired);
} finally {
    configure("hangAfterUpdateManyBatchCommit", "off");
}
assert.eq(61, coll.find({counter: 1}).itcount());
assert.eq(11, coll.find({counter: 0}).itcount());

// Simulate loss of a successful commit acknowledgement: the command must report the error
// without replaying the committed prefix, including non-idempotent updates.
populate();
before = stats();
configure("failAfterUpdateManyBatchCommit",
          {times: 1},
          {namespace: ns, batch: 1, errorCode: ErrorCodes.InternalError});
try {
    const error = assert.throws(function() {
        coll.updateMany({}, {$inc: {counter: 1}});
    });
    assert.eq(ErrorCodes.InternalError, error.code, tojson(error));
} finally {
    configure("failAfterUpdateManyBatchCommit", "off");
}
assert.eq(1, stats().committedBatches - before.committedBatches);
assert.eq(0, stats().retriedBatches - before.retriedBatches);
assert.eq(61, coll.find({counter: 1}).itcount());
assert.eq(11, coll.find({counter: 0}).itcount());

// A schema change by another operation invalidates the saved plan after a commit.
populate();
configure("hangAfterUpdateManyBatchCommit", "alwaysOn", {namespace: ns, batch: 1});
worker = startUpdate(30000);
try {
    waitForBoundary();
    assert.commandWorked(coll.createIndex({uniqueKey: 1}));
} finally {
    configure("hangAfterUpdateManyBatchCommit", "off");
}
worker.join();
response = worker.returnData();
assert.eq(1, response.ok, tojson(response));
assert.eq(ErrorCodes.QueryPlanKilled, response.writeErrors[0].code, tojson(response));
assert.eq(61, coll.find({counter: 1}).itcount());
assert.eq(11, coll.find({counter: 0}).itcount());

[true, false].forEach(function(ordered) {
    populate();
    // Precompute destinations so an ordinary modifier update collides only in batch two.
    for (let i = 0; i < count; ++i) {
        assert.writeOK(coll.update({_id: i}, {$set: {nextUniqueKey: i < 61 ? i : 999}}));
    }
    assert.commandWorked(coll.createIndex({uniqueKey: 1}, {unique: true}));
    response = db.runCommand({
        update: coll.getName(),
        ordered: ordered,
        updates: [
            {
                q: {},
                multi: true,
                u: {$inc: {counter: 1}, $rename: {nextUniqueKey: "uniqueKey"}}
            },
            {q: {_id: 71}, u: {$set: {afterError: true}}}
        ]
    });
    assert.eq(1, response.ok, tojson(response));
    assert.eq(1, response.writeErrors.length, tojson(response));
    assert.eq(0, response.writeErrors[0].index, tojson(response));
    assert.eq(ErrorCodes.DuplicateKey, response.writeErrors[0].code, tojson(response));
    assert.eq(61, coll.find({counter: 1}).itcount());
    assert.eq(11, coll.find({counter: 0}).itcount());
    assert.eq(0, coll.find({uniqueKey: 999}).hint({uniqueKey: 1}).itcount());
    assert.eq(count, coll.find().hint({uniqueKey: 1}).itcount());
    assert.eq(ordered ? 0 : 1, coll.find({afterError: true}).itcount());
});
coll.drop();
}());
