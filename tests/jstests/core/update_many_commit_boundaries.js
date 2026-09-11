// Standalone multi-updates commit each document, including its index changes, atomically.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
    "use strict";

    const coll = db.getCollection("update_many_commit_boundaries");
    coll.drop();
    assert.commandWorked(coll.createIndex({k: 1}));
    for (let i = 0; i < 64; ++i) {
        assert.writeOK(coll.insert({_id: i, k: i, counter: 0}));
    }
    // Move keys forward while they still match the index scan. Each document must be
    // updated exactly once even when its new key is encountered later in the scan.
    const query = {k: {$gte: 0, $lt: 1000}};
    const plan = coll.find(query).explain().queryPlanner.winningPlan;
    assert.neq(-1, tojson(plan).indexOf("IXSCAN"), tojson(plan));
    let result = coll.updateMany(query, {$inc: {k: 1, counter: 1}});
    assert.eq(64, result.matchedCount, tojson(result));
    assert.eq(64, result.modifiedCount, tojson(result));
    assert.eq(64, coll.find({counter: 1}).itcount());
    assert.eq(64, coll.find({k: {$gte: 1, $lte: 64}}).hint({k: 1}).itcount());

    result = coll.updateMany(query, {$set: {counter: 1}});
    assert.eq(64, result.matchedCount, tojson(result));
    assert.eq(0, result.modifiedCount, tojson(result));
    result = coll.updateMany({_id: -1}, {$inc: {counter: 1}}, {upsert: true});
    assert.eq(-1, result.upsertedId, tojson(result));
    assert.eq(1, coll.findOne({_id: -1}).counter);

    coll.drop();
    assert.commandWorked(coll.createIndex({uniqueKey: 1}, {unique: true}));
    assert.writeOK(coll.insert([
        {_id: 0, uniqueKey: 0, counter: 0},
        {_id: 1, uniqueKey: 1, counter: 0},
        {_id: 2, uniqueKey: 2, counter: 0}
    ]));
    const error = assert.throws(function() {
        coll.updateMany({}, {$set: {uniqueKey: 10}, $inc: {counter: 1}});
    });
    assert.eq(ErrorCodes.DuplicateKey, error.code, tojson(error));
    // The successful document stays committed; the failing document's body and indexes
    // must both roll back.
    assert.eq(1, coll.find({counter: 1}).itcount());
    assert.eq(2, coll.find({counter: 0}).itcount());
    assert.eq(1, coll.find({uniqueKey: 10}).hint({uniqueKey: 1}).itcount());
    assert.eq(3, coll.find().hint({uniqueKey: 1}).itcount());

    [true, false].forEach(function(ordered) {
        const response = db.runCommand({
            update: coll.getName(),
            ordered: ordered,
            updates: [
                {q: {_id: 20}, u: {$set: {uniqueKey: 20}}, upsert: true},
                {q: {_id: 21}, u: {$set: {uniqueKey: 10}}, upsert: true},
                {q: {_id: 22}, u: {$set: {uniqueKey: 22}}, upsert: true}
            ]
        });
        assert.eq(1, response.ok, tojson(response));
        assert.eq(1, response.writeErrors.length, tojson(response));
        assert.eq(1, response.writeErrors[0].index, tojson(response));
        assert.eq(ErrorCodes.DuplicateKey, response.writeErrors[0].code, tojson(response));
        assert.neq(null, coll.findOne({_id: 20}));
        assert.eq(null, coll.findOne({_id: 21}));
        assert.eq(ordered ? 0 : 1, coll.find({_id: 22}).itcount());
        assert.writeOK(coll.remove({_id: {$gte: 20}}));
    });
    coll.drop();
}());
