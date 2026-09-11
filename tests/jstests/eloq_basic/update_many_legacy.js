// OP_UPDATE uses the same per-document transaction boundary as the update command.
// @tags: [requires_non_retryable_writes, assumes_unsharded_collection]
(function() {
    "use strict";
    const connection = db.getMongo();
    const originalMode = connection.writeMode();
    const coll = db.getCollection("update_many_legacy");
    const count = 72;
    try {
        connection.forceWriteMode("commands");
        coll.drop();
        const payload = "x".repeat(1024 * 1024);
        for (let i = 0; i < count; ++i) {
            assert.writeOK(coll.insert({_id: i, counter: 0, payload: payload}));
        }
        connection.forceWriteMode("legacy");
        coll.update({}, {$inc: {counter: 1}}, {multi: true});
        // Legacy writes have no immediate reply; getLastError reports their outcome.
        const result = db.runCommand({getLastError: 1});
        assert.commandWorked(result);
        assert.eq(null, result.err, tojson(result));
        assert.eq(count, result.n, tojson(result));
        connection.forceWriteMode("commands");
        assert.eq(count, coll.find({counter: 1}).itcount());
    } finally {
        connection.forceWriteMode(originalMode);
        coll.drop();
    }
}());
