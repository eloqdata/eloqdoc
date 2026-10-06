// OP_UPDATE must use capacity batches and report a later failure through getLastError.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
"use strict";
const connection = db.getMongo();
const originalMode = connection.writeMode();
const coll = db.getCollection("update_many_batch_legacy");
const count = 72;
const payload = "x".repeat(1024 * 1024);
function stats() {
    return db.serverStatus().metrics.updateMany;
}
try {
    connection.forceWriteMode("commands");
    coll.drop();
    for (let i = 0; i < count; ++i) {
        assert.writeOK(coll.insert({_id: i, counter: 0, payload: payload}));
    }
    connection.forceWriteMode("legacy");
    let before = stats();
    coll.update({}, {$inc: {counter: 1}}, false, true);
    let result = db.getLastErrorObj();
    assert.commandWorked(result);
    assert.eq(null, result.err, tojson(result));
    assert.eq(count, result.n);
    assert.eq(count, coll.find({counter: 1}).itcount());
    assert.eq(2, stats().committedBatches - before.committedBatches);

    // Preserve the committed prefix exactly once and expose the error on the legacy protocol.
    before = stats();
    assert.commandWorked(db.adminCommand({
        configureFailPoint: "failUpdateManyBatch",
        mode: {times: 1},
        data: {namespace: coll.getFullName(), batch: 2, errorCode: ErrorCodes.InternalError}
    }));
    try {
        coll.update({}, {$inc: {counter: 1}}, false, true);
        result = db.getLastErrorObj();
        assert.eq(1, result.ok, tojson(result));
        assert.eq(ErrorCodes.InternalError, result.code, tojson(result));
        assert.neq(null, result.err, tojson(result));
    } finally {
        assert.commandWorked(
            db.adminCommand({configureFailPoint: "failUpdateManyBatch", mode: "off"}));
    }
    assert.eq(61, coll.find({counter: 2}).itcount());
    assert.eq(count - 61, coll.find({counter: 1}).itcount());
    assert.eq(1, stats().committedBatches - before.committedBatches);
} finally {
    connection.forceWriteMode(originalMode);
    coll.drop();
}
}());
