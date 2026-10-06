// A normal multi-update must commit whole documents in batches when its write set exceeds
// the storage engine's capacity. Each individual document here fits in a transaction.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
"use strict";

const coll = db.getCollection("update_many_write_set_batches");
coll.drop();
const payload = "x".repeat(1024 * 1024);
const isEloq = db.serverStatus().storageEngine.name === "eloq";
const limit = 62 * 1024 * 1024;

function batchStats() {
    return db.serverStatus().metrics.updateMany || {};
}

function checkBatches(before, expectedCount, minimumBytes) {
    if (!isEloq) {
        return;
    }
    const after = batchStats();
    assert.eq(expectedCount, after.committedBatches - before.committedBatches, tojson(after));
    const bytes = after.committedBytes - before.committedBytes;
    assert.gte(bytes, minimumBytes, tojson(after));
    assert.lte(bytes, expectedCount * limit, tojson(after));
}

function populate(count, size) {
    coll.drop();
    for (let i = 0; i < count; ++i) {
        assert.writeOK(coll.insert({_id: i, counter: 0, payload: size}));
    }
}

// Start with the minimal failing case so an unpatched server fails on the original symptom,
// before assertions about the new observability counters are reached.
[62, 61].forEach(function(count) {
    populate(count, payload);
    const before = batchStats();
    const result = coll.updateMany({}, {$inc: {counter: 1}});
    assert.eq(count, result.matchedCount, tojson(result));
    assert.eq(count, result.modifiedCount, tojson(result));
    assert.eq(count, coll.find({counter: 1}).itcount());
    assert.eq(0, coll.find({counter: {$ne: 1}}).itcount());
    checkBatches(before, count <= 61 ? 1 : 2, count * payload.length);
});

// Reading another window must not commit a partially filled write set.
coll.drop();
const smallPayload = "y".repeat(10240);
for (let start = 0; start < 2050; start += 50) {
    const documents = [];
    for (let i = start; i < start + 50; ++i) {
        documents.push({_id: i, counter: 0, payload: smallPayload});
    }
    assert.writeOK(coll.insert(documents));
}
let before = batchStats();
let result = coll.updateMany({}, {$inc: {counter: 1}});
assert.eq(2050, result.matchedCount);
assert.eq(2050, result.modifiedCount);
assert.eq(2050, coll.find({counter: 1}).itcount());
checkBatches(before, 1, 2050 * smallPayload.length);

// A no-op still contributes to matchedCount without filling the write set.
before = batchStats();
result = coll.updateMany({}, {$set: {counter: 1}});
assert.eq(2050, result.matchedCount);
assert.eq(0, result.modifiedCount);
checkBatches(before, 1, 0);
if (isEloq) {
    assert.eq(0, batchStats().committedBytes - before.committedBytes);
}

// Admission uses the new document size, including growth.
populate(72, smallPayload);
before = batchStats();
result = coll.updateMany({}, {$set: {payload: payload}, $inc: {counter: 1}});
assert.eq(72, result.matchedCount);
assert.eq(72, result.modifiedCount);
assert.eq(72, coll.find({counter: 1}).itcount());
checkBatches(before, 2, 72 * payload.length);

// Moving the scanned index key forwards must not apply $inc a second time after a commit.
assert.commandWorked(coll.createIndex({counter: 1}));
const query = {
    counter: {$gte: 0, $lt: 1000}
};
const plan = coll.find(query).explain().queryPlanner.winningPlan;
assert.neq(-1, tojson(plan).indexOf("IXSCAN"), tojson(plan));
before = batchStats();
result = coll.updateMany(query, {$inc: {counter: 1}});
assert.eq(72, result.matchedCount);
assert.eq(72, result.modifiedCount);
assert.eq(72, coll.find({counter: 2}).hint({counter: 1}).itcount());
checkBatches(before, 2, 72 * payload.length);

result = coll.updateMany({_id: -1}, {$inc: {counter: 1}}, {upsert: true});
assert.eq(-1, result.upsertedId);
assert.eq(1, coll.findOne({_id: -1}).counter);
coll.drop();
}());
