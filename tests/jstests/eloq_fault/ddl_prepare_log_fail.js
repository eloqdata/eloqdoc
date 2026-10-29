/**
 * CREATE and DROP TABLE whose prepare log fails, driven by the upsert_table_prepare_log_fail data
 * substrate fault point through eloqFaultInject (tx_upsert_table_operation.cpp,
 * UpsertTableOp::RunUntilPrepareLog). The prepare log is the op's point of no return, written after
 * the op takes its write intents and before it installs the dirty schema. When it fails, the op
 * rolls back: it releases the write intents, has no dirty schema to reject, cleans its log, and the
 * command fails.
 *
 * The fault fails the prepare log without writing it, on every op, until it is removed: unlike the
 * one-shot faults of ddl_kv_retry.js it does not disarm itself, so the test removes it in a
 * finally block. The API layer may retry the op (EloqDoc retries a drop, a bounded number of
 * times); every attempt fails the same way.
 *
 * - CREATE: fails; the name stays free, and a create of it once the fault is removed is usable.
 * - DROP: fails; the collection stays, with its documents and index, and drops once the fault is
 *   removed.
 *
 * The server needs the fault points compiled in (EloqDoc Debug builds, WITH_FAULT_INJECT); on any
 * other build eloqFaultInject fails with CommandNotSupported, and so does this test.
 */
(function() {
    "use strict";

    const fault = "upsert_table_prepare_log_fail";
    const admin = db.getSiblingDB("admin");
    const testDB = db.getSiblingDB("eloq_fault_ddl_prepare_log_fail");
    assert.commandWorked(testDB.dropDatabase());

    /**
     * Whether the fault point is armed on this node.
     *
     * @return true when armed.
     */
    function faultArmed() {
        return assert.commandWorked(admin.runCommand({eloqFaultInject: fault, query: true})).armed;
    }

    /**
     * Checks that a collection holds the documents and index the test gave it.
     *
     * @param coll The collection.
     */
    function checkIntact(coll) {
        assert.eq(20, coll.find().itcount());
        assert.eq(5, coll.find({k: 2}).hint({k: 1}).itcount());
    }

    // A collection that exists before the fault, for the drop.
    const kept = testDB.kept;
    for (let i = 0; i < 20; i++) {
        assert.commandWorked(kept.insert({_id: i, k: i % 4}));
    }
    assert.commandWorked(kept.createIndex({k: 1}));
    checkIntact(kept);

    const created = testDB.created;
    assert.commandWorked(admin.runCommand({eloqFaultInject: fault}));
    try {
        assert(faultArmed(), fault + " is not armed");

        assert.commandFailed(testDB.runCommand({create: created.getName()}));
        // Still armed: the fault fired, and fires again on the next op.
        assert(faultArmed(), fault + " disarmed itself");
        assert.eq(-1, testDB.getCollectionNames().indexOf(created.getName()));

        assert.commandFailed(testDB.runCommand({drop: kept.getName()}));
        assert.neq(-1, testDB.getCollectionNames().indexOf(kept.getName()));
    } finally {
        assert.commandWorked(admin.runCommand({eloqFaultInject: fault, params: "remove"}));
    }
    assert(!faultArmed(), fault + " is still armed");

    // The failed drop changed nothing.
    checkIntact(kept);

    // Both ops go through once the fault is removed.
    assert.commandWorked(testDB.createCollection(created.getName()));
    for (let i = 0; i < 10; i++) {
        assert.commandWorked(created.insert({_id: i}));
    }
    assert.commandWorked(created.createIndex({x: 1}));
    assert.eq(10, created.find().itcount());

    assert(kept.drop());
    assert.eq(-1, testDB.getCollectionNames().indexOf(kept.getName()));

    assert.commandWorked(testDB.dropDatabase());
})();
