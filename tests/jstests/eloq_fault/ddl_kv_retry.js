/**
 * CREATE and DROP TABLE on a data store failure, driven by data substrate fault points through
 * eloqFaultInject (tx_upsert_table_operation.cpp; docs/cdc-change-stream.md §4.4(b)). A failure
 * before the commit log rolls the op back at once; a failure after it, when the op can only roll
 * forward, is retried until the step succeeds, ddl_kv_retry_interval_seconds apart (30 s by default;
 * the test fixtures set 1 s).
 *
 * - upsert_table_create_kv_table_fail: creating the new table's kv tables, after the prepare log
 *   and before the commit log, fails once. The create drops what it created, fails with a commit
 *   log at timestamp 0, and the command fails; the name stays free, and a later create of it
 *   succeeds. A table_name parameter aims the fault at one table; without it, the first create
 *   to reach it takes it, which may be the server's own (its feature document).
 * - upsert_table_update_sequence_fail on a create: the sequence table update, also before the
 *   commit log, fails once, with the same outcome.
 * - upsert_table_kv_write_fail on a create: the catalog row, written after the commit log at the
 *   commit-stage timestamp, fails once and is retried.
 * - upsert_table_update_sequence_fail and upsert_table_kv_write_fail on a drop: the sequence table
 *   update and the deletion of the kv table, both after the commit log, fail once and are retried.
 *
 * Each fault disarms itself when it fires, so reading it disarmed after the DDL proves the path
 * ran. The server needs the fault points compiled in (EloqDoc Debug builds, WITH_FAULT_INJECT); on
 * any other build eloqFaultInject fails with CommandNotSupported, and so does this test.
 */
(function() {
    "use strict";

    const admin = db.getSiblingDB("admin");
    const testDB = db.getSiblingDB("eloq_fault_ddl_kv_retry");
    assert.commandWorked(testDB.dropDatabase());

    /**
     * Whether a fault point is armed on this node.
     *
     * @param name The fault point.
     * @return true when armed.
     */
    function faultArmed(name) {
        return assert.commandWorked(admin.runCommand({eloqFaultInject: name, query: true})).armed;
    }

    /**
     * Arms a one-shot fault point and checks it took.
     *
     * @param name The fault point.
     */
    function arm(name) {
        assert.commandWorked(admin.runCommand({eloqFaultInject: name}));
        assert(faultArmed(name), name + " is not armed");
    }

    /**
     * Checks that a collection is usable: it takes writes and an index, and reads them back.
     *
     * @param coll The collection.
     */
    function checkUsable(coll) {
        for (let i = 0; i < 20; i++) {
            assert.commandWorked(coll.insert({_id: i, k: i % 4}));
        }
        assert.commandWorked(coll.createIndex({k: 1}));
        assert.eq(20, coll.find().itcount());
        assert.eq(5, coll.find({k: 2}).hint({k: 1}).itcount());
    }

    /**
     * Arms a fault that fails a create before its commit log, and checks that the create rolls
     * back: the command fails, the name stays free, and a new create of it is usable.
     *
     * @param fault The fault point.
     * @param coll The collection to create.
     */
    function checkCreateRolledBack(fault, coll) {
        arm(fault);
        assert.commandFailed(testDB.runCommand({create: coll.getName()}));
        assert(!faultArmed(fault), fault + " never fired");
        assert.eq(-1, testDB.getCollectionNames().indexOf(coll.getName()));
        assert.commandWorked(testDB.createCollection(coll.getName()));
        checkUsable(coll);
    }

    checkCreateRolledBack("upsert_table_create_kv_table_fail", testDB.kv_tables_failed);
    checkCreateRolledBack("upsert_table_update_sequence_fail", testDB.sequence_failed);

    // The catalog row of a new collection fails once after the commit log; the create retries.
    const cataloged = testDB.catalog_row_retried;
    arm("upsert_table_kv_write_fail");
    assert.commandWorked(testDB.createCollection(cataloged.getName()));
    assert(!faultArmed("upsert_table_kv_write_fail"), "the catalog row fault never fired");
    checkUsable(cataloged);

    // A drop's sequence table update, then its kv table deletion, fail once after the commit log;
    // the drop retries each.
    for (let fault of ["upsert_table_update_sequence_fail", "upsert_table_kv_write_fail"]) {
        const dropped = testDB.kv_tables_failed;
        arm(fault);
        assert(dropped.drop());
        assert(!faultArmed(fault), "the drop's " + fault + " never fired");
        assert.eq(-1, testDB.getCollectionNames().indexOf(dropped.getName()));
        // The name is free again, and a new collection under it starts empty.
        assert.commandWorked(testDB.createCollection(dropped.getName()));
        assert.eq(0, dropped.find().itcount());
    }

    assert.commandWorked(testDB.dropDatabase());
})();
