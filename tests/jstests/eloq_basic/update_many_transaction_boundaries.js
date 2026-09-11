// Explicit transactions retain atomic multi-update semantics and the Eloq write-set limit.
// @tags: [uses_transactions, requires_non_retryable_writes]
(function() {
    "use strict";
    const coll = db.getCollection("update_many_transaction_boundaries");
    coll.drop();
    assert.commandWorked(coll.createIndex({counter: 1}));
    assert.writeOK(coll.insert([{_id: 0, counter: 0}, {_id: 1, counter: 0}]));
    const session = db.getMongo().startSession({causalConsistency: false});
    const sessionColl = session.getDatabase(db.getName()).getCollection(coll.getName());
    try {
        session.startTransaction();
        assert.eq(2, sessionColl.updateMany({}, {$inc: {counter: 1}}).modifiedCount);
        assert.eq(2, sessionColl.find({counter: 1}).itcount());
        assert.eq(2, coll.find({counter: 0}).itcount());
        session.abortTransaction();
        assert.eq(2, coll.find({counter: 0}).hint({counter: 1}).itcount());

        session.startTransaction();
        assert.eq(2, sessionColl.updateMany({}, {$inc: {counter: 1}}).modifiedCount);
        session.commitTransaction();
        assert.eq(2, coll.find({counter: 1}).hint({counter: 1}).itcount());

        // Individually legal documents exceed the aggregate transaction limit.
        assert.writeOK(coll.remove({}));
        const payload = "x".repeat(1024 * 1024);
        for (let i = 0; i < 64; ++i) {
            assert.writeOK(coll.insert({_id: i, counter: 0, payload: payload}));
        }
        session.startTransaction();
        const error = assert.throws(function() {
            sessionColl.updateMany({}, {$inc: {counter: 1}});
        });
        assert.eq(ErrorCodes.TransactionTooLarge, error.code, tojson(error));
        // The command aborts the transaction, including every previously staged document.
        assert.eq(64, coll.find({counter: 0}).itcount());
        assert.eq(0, coll.find({counter: 1}).hint({counter: 1}).itcount());
    } finally {
        session.endSession();
        coll.drop();
    }
}());
