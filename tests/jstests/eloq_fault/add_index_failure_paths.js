/**
 * Add-index failure paths, driven by data substrate fault points through eloqFaultInject
 * (tx_index_operation.cpp, UpsertTableIndexOp).
 *
 * - alter_table_index_create_kv_table_fail: creating the index's kv table fails, before the
 *   prepare log. createIndex fails, the tables are dropped, and a retry builds the index.
 * - alter_table_index_prepare_log_fail: the prepare log fails after the kv tables exist. Same
 *   outcome.
 * - alter_table_index_update_schema_image_fail: the kv catalog write after the commit log fails
 *   once (the fault then disarms itself) and is retried. createIndex succeeds.
 *
 * Each case checks that its fault fired: the two failing builds must report that fault's error,
 * and the one-shot fault must read disarmed after the build. No parallel shell is used. The server
 * needs the fault points compiled in (EloqDoc Debug builds, WITH_FAULT_INJECT); on any other
 * build eloqFaultInject fails with CommandNotSupported, and so does this test.
 */
(function() {
    "use strict";

    const admin = db.getSiblingDB("admin");
    const kDocs = 5000;
    const kKeys = 100;

    function faultArmed(name) {
        return assert.commandWorked(admin.runCommand({eloqFaultInject: name, query: true})).armed;
    }

    function arm(name) {
        assert.commandWorked(admin.runCommand({eloqFaultInject: name}));
        assert(faultArmed(name), name + " is not armed");
    }

    function disarm(name) {
        assert.commandWorked(admin.runCommand({eloqFaultInject: name, params: "remove"}));
        assert(!faultArmed(name), name + " is still armed");
    }

    function load(coll) {
        coll.drop();
        const bulk = coll.initializeUnorderedBulkOp();
        for (let i = 0; i < kDocs; i++) {
            bulk.insert({_id: i, k: i % kKeys});
        }
        assert.commandWorked(bulk.execute());
    }

    function createKIndex(coll) {
        return db.runCommand({createIndexes: coll.getName(), indexes: [{key: {k: 1}, name: "k_1"}]});
    }

    function hasKIndex(coll) {
        return coll.getIndexes().some(ix => ix.name === "k_1");
    }

    function checkKIndex(coll) {
        assert(hasKIndex(coll), tojson(coll.getIndexes()));
        assert.eq(kDocs, coll.find({}, {_id: 1}).hint("k_1").itcount());
        for (let k = 0; k < kKeys; k += 17) {
            assert.eq(kDocs / kKeys, coll.find({k: k}, {_id: 1}).hint("k_1").itcount(), "k=" + k);
        }
    }

    // A build that the fault fails before the index is committed: the error is that fault's, the
    // index is absent, the collection stays writable, and a retry builds a complete index.
    function failedBuild(coll, fault, expectedError) {
        load(coll);
        arm(fault);
        const res = createKIndex(coll);
        disarm(fault);
        assert.commandFailed(res);
        assert(res.errmsg.includes(expectedError),
               fault + " did not fail the build as expected: " + tojson(res));
        assert(!hasKIndex(coll), tojson(coll.getIndexes()));

        assert.commandWorked(coll.insert({_id: kDocs, k: 0}));
        assert.commandWorked(coll.remove({_id: kDocs}));
        assert.commandWorked(createKIndex(coll));
        checkKIndex(coll);
    }

    failedBuild(db.eloq_fault_create_kv_table,
                "alter_table_index_create_kv_table_fail",
                "Data storage is not available");

    failedBuild(db.eloq_fault_prepare_log,
                "alter_table_index_prepare_log_fail",
                "Failed at prepare phase");

    // The kv catalog write after the commit log fails once and is retried; the fault disarms itself
    // when it fires, so reading it disarmed after the build proves the retried path ran.
    (function catalogWriteRetried() {
        const fault = "alter_table_index_update_schema_image_fail";
        const coll = db.eloq_fault_catalog_write;
        load(coll);
        arm(fault);
        assert.commandWorked(createKIndex(coll));
        assert(!faultArmed(fault), fault + " never fired");
        checkKIndex(coll);
    })();

    db.eloq_fault_create_kv_table.drop();
    db.eloq_fault_prepare_log.drop();
    db.eloq_fault_catalog_write.drop();
})();
