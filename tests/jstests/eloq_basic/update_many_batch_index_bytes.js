// Index deletes, index inserts and the primary record share one document admission decision.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
"use strict";
const coll = db.getCollection("update_many_batch_index_bytes");
const limit = 62 * 1024 * 1024;

function keys(prefix, count) {
    const result = [];
    const tail = "x".repeat(600);
    for (let i = 0; i < count; ++i) {
        result.push(prefix + i + tail);
    }
    return result;
}
function indexes() {
    for (let i = 0; i < 4; ++i) {
        const pattern = {values: 1};
        pattern["suffix" + i] = 1;
        assert.commandWorked(coll.createIndex(pattern));
    }
}
function checkIndexes(expected, firstKey) {
    for (let i = 0; i < 4; ++i) {
        const pattern = {values: 1};
        pattern["suffix" + i] = 1;
        assert.eq(expected, coll.find({values: firstKey}).hint(pattern).itcount());
    }
}

coll.drop();
const oldKeys = keys("old", 1200);
const newKeys = keys("new", 1200);
for (let i = 0; i < 12; ++i) {
    assert.writeOK(coll.insert({_id: NumberLong(i), counter: 0, values: oldKeys}));
}
indexes();
let before = db.serverStatus().metrics.updateMany;
const result = coll.updateMany({}, {$set: {values: newKeys}, $inc: {counter: 1}});
assert.eq(12, result.matchedCount);
assert.eq(12, result.modifiedCount);
assert.eq(12, coll.find({counter: 1}).itcount());
let after = db.serverStatus().metrics.updateMany;
assert.eq(2, after.committedBatches - before.committedBatches, tojson(after));
assert.gt(after.committedBytes - before.committedBytes, limit, tojson(after));
checkIndexes(12, newKeys[0]);
checkIndexes(0, oldKeys[0]);

// A single BSON document fits under 16 MiB, but its old/new multikey entries cannot fit
// in one 62 MiB transaction. It must fail without changing its body or any index.
coll.drop();
const largeOldKeys = keys("old", 13000);
const largeNewKeys = keys("new", 13000);
const document = {
    _id: 0,
    counter: 0,
    values: largeOldKeys
};
assert.lt(Object.bsonsize(document), 16 * 1024 * 1024);
assert.writeOK(coll.insert(document));
indexes();
before = db.serverStatus().metrics.updateMany;
const error = assert.throws(function() {
    coll.updateMany({}, {$set: {values: largeNewKeys}, $inc: {counter: 1}});
});
assert.eq(ErrorCodes.TransactionTooLarge, error.code, tojson(error));
after = db.serverStatus().metrics.updateMany;
assert.eq(0, after.committedBatches - before.committedBatches);
assert.eq(0, coll.findOne({_id: 0}).counter);
assert.eq(largeOldKeys.length, coll.findOne({_id: 0}).values.length);
checkIndexes(1, largeOldKeys[0]);
checkIndexes(0, largeNewKeys[0]);
coll.drop();
}());
