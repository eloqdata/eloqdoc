/**
 * A range split whose first UpsertRanges fails is retried with the same ranges
 * (tx_operation.cpp, SplitFlushRangeOp, ds_upsert_range_op_).
 *
 * split_flush_upsert_ranges_fail fails one upsert before it writes, then disarms itself. The test
 * loads one collection past the 256 MB range limit, so that a checkpoint splits its range. The
 * fault reading disarmed proves the split reached the failed upsert. Every document must still be
 * found once the retry has run.
 *
 * The server needs the fault points compiled in (EloqDoc Debug builds, WITH_FAULT_INJECT); on any
 * other build eloqFaultInject fails with CommandNotSupported, and so does this test.
 */
(function() {
    "use strict";

    const admin = db.getSiblingDB("admin");
    const fault = "split_flush_upsert_ranges_fail";
    const kDocs = 350 * 1000;
    const kBatch = 2000;
    const pad = "x".repeat(1000);
    const coll = db.eloq_fault_range_split;

    function faultArmed() {
        return assert.commandWorked(admin.runCommand({eloqFaultInject: fault, query: true})).armed;
    }

    coll.drop();
    assert.commandWorked(admin.runCommand({eloqFaultInject: fault}));
    assert(faultArmed(), fault + " is not armed");

    for (let base = 0; base < kDocs; base += kBatch) {
        const bulk = coll.initializeUnorderedBulkOp();
        for (let i = base; i < base + kBatch; i++) {
            bulk.insert({_id: i, pad: pad});
        }
        assert.commandWorked(bulk.execute());
    }

    // A checkpoint splits the range; the split's first upsert hits the fault, which disarms itself.
    assert.soon(() => !faultArmed(), "the range split never reached its upsert", 10 * 60 * 1000, 1000);

    // The retry follows after a back-off of a few seconds.
    sleep(15 * 1000);

    assert.eq(kDocs, coll.find({}, {_id: 1}).itcount());
    for (let n = 0; n < 1000; n++) {
        const id = Math.floor(Math.random() * kDocs);
        assert.neq(null, coll.findOne({_id: id}, {_id: 1}), "_id " + id);
    }

    coll.drop();
})();
