// New multikey metadata, partial unique keys, and encoded numeric types survive batch retries.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
"use strict";
const coll = db.getCollection("update_many_batch_schema");
coll.drop();
const payload = "x".repeat(1024 * 1024);
for (let i = 0; i < 72; ++i) {
    assert.writeOK(coll.insert({
        _id: NumberLong(i),
        counter: 0,
        value: NumberDecimal(i + ".25"),
        uniqueKey: i,
        eligible: i % 2 === 0,
        payload: payload
    }));
}
assert.commandWorked(coll.createIndex({value: 1}));
assert.commandWorked(
    coll.createIndex({uniqueKey: 1}, {unique: true, partialFilterExpression: {eligible: true}}));
const before = db.serverStatus().metrics.updateMany;
assert.commandWorked(db.adminCommand({
    configureFailPoint: "failUpdateManyBatch",
    mode: {times: 1},
    data: {namespace: coll.getFullName(), batch: 1, errorCode: ErrorCodes.WriteConflict}
}));
try {
    const result = db.runCommand({
        update: coll.getName(),
        updates: [{
            q: {},
            multi: true,
            u: {
                $inc: {counter: 1, uniqueKey: 100},
                $set: {value: [NumberDecimal("1000.25"), NumberDecimal("2000.25")], eligible: true}
            }
        }]
    });
    assert.commandWorked(result);
    assert.eq(undefined, result.writeErrors, tojson(result));
    assert.eq(72, result.n);
    assert.eq(72, result.nModified);
} finally {
    assert.commandWorked(db.adminCommand({configureFailPoint: "failUpdateManyBatch", mode: "off"}));
}
const after = db.serverStatus().metrics.updateMany;
assert.eq(2, after.committedBatches - before.committedBatches);
assert.eq(1, after.retriedBatches - before.retriedBatches);
assert.eq(72, coll.find({counter: 1}).itcount());
assert.eq(72, coll.find({value: NumberDecimal("1000.25")}).hint({value: 1}).itcount());
assert.eq(72, coll.find({value: NumberDecimal("2000.25")}).hint({value: 1}).itcount());
assert.eq(0, coll.find({value: NumberDecimal("0.25")}).hint({value: 1}).itcount());
assert.eq(0, coll.find({eligible: true, uniqueKey: {$lt: 100}}).hint({uniqueKey: 1}).itcount());
assert.eq(72, coll.find({eligible: true, uniqueKey: {$gte: 100}}).hint({uniqueKey: 1}).itcount());
coll.drop();
}());
