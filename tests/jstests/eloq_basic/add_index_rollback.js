/**
 * An index build that meets a key over the size limit rolls back after its prepare log
 * (docs/cdc-change-stream.md §4.4(b); plan B8): createIndex fails with KeyTooLong, the index does
 * not exist, and the collection stays usable, including for later index builds. The rollback
 * commits at its commit-stage timestamp: the table keeps its current schema, its catalog entry and
 * kv catalog row move to that timestamp, and no table's version changes, so every write the test
 * makes afterwards passes the per-request schema version check.
 */
(function() {
    "use strict";

    const testDB = db.getSiblingDB("eloq_basic_add_index_rollback");
    assert.commandWorked(testDB.dropDatabase());
    const coll = testDB.c;

    for (let i = 0; i < 50; i++) {
        assert.commandWorked(coll.insert({_id: i, small: i % 5, big: "b" + i}));
    }
    // One key of the new index is past the 1024-byte limit.
    assert.commandWorked(coll.insert({_id: "long", small: 1, big: "x".repeat(1100)}));
    assert.commandWorked(coll.createIndex({small: 1}));

    const res = testDB.runCommand({createIndexes: "c", indexes: [{key: {big: 1}, name: "big_1"}]});
    assert.commandFailedWithCode(res, ErrorCodes.KeyTooLong);
    assert(!coll.getIndexes().some(ix => ix.name === "big_1"), tojson(coll.getIndexes()));

    // The collection and its other index keep working at the rolled-back version.
    assert.eq(51, coll.find().itcount());
    assert.commandWorked(coll.insert({_id: 100, small: 2, big: "after"}));
    assert.commandWorked(coll.update({_id: 1}, {$set: {small: 4}}));
    assert.commandWorked(coll.remove({_id: 2}));
    assert.eq(11, coll.find({small: 4}).hint({small: 1}).itcount());

    // A later index build on the same table succeeds, and so does the one that failed once the
    // offending document is gone.
    assert.commandWorked(coll.createIndex({small: 1, _id: -1}));
    assert.commandWorked(coll.remove({_id: "long"}));
    assert.commandWorked(coll.createIndex({big: 1}));
    assert.eq(1, coll.find({big: "after"}).hint({big: 1}).itcount());

    assert.commandWorked(testDB.dropDatabase());
})();
