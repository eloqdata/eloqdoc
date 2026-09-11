// A multi-update outside an explicit transaction must not accumulate all documents in one
// storage transaction. Each document fits comfortably below the transaction write-set limit.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
    "use strict";

    const coll = db.getCollection("update_many_large_write_set");
    coll.drop();

    const count = 72;
    const payload = "x".repeat(1024 * 1024);
    for (let i = 0; i < count; ++i) {
        assert.writeOK(coll.insert({_id: i, counter: 0, payload: payload}));
    }

    const result = coll.updateMany({}, {$inc: {counter: 1}});
    assert.eq(count, result.matchedCount, tojson(result));
    assert.eq(count, result.modifiedCount, tojson(result));
    assert.eq(count, coll.count({counter: 1}));
    assert.eq(0, coll.count({counter: {$ne: 1}}));
    coll.drop();
}());
