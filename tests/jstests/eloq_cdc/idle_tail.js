/**
 * The tail of a quiet period (docs/cdc-change-stream.md §5.7, §10.10; plan F3): one change, then
 * silence. The change must still arrive, carried past the bound by the checkpoint rounds that run
 * with no traffic, and afterwards the watermark must keep advancing round after round.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_idle_tail");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));
    // The stream must not report the setup above (§5.1: it starts at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());

    const opened = assert.commandWorked(EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), "c", {}));
    const id = opened.cursor.id;
    function getMore() {
        return assert.commandWorked(
            testDB.runCommand({getMore: id, collection: "c", maxTimeMS: 1000}));
    }

    // One change, then nothing else writes.
    assert.commandWorked(testDB.c.insert({_id: "tail"}));
    let tail;
    assert.soon(function() {
        const batch = getMore().cursor.nextBatch;
        if (batch.length > 0) {
            assert.eq(batch.length, 1, tojson(batch));
            tail = batch[0];
            return true;
        }
        return false;
    }, "the tail change never arrived", EloqCdcUtil.kEventTimeoutMS, 100);
    assert.eq(tail.documentKey._id, "tail");

    // Two more watermark advances past it, with no traffic in between.
    let last = EloqCdcUtil.tokenTimestamp(tail._id);
    let advances = 0;
    assert.soon(function() {
        const reply = getMore();
        assert.eq(reply.cursor.nextBatch, [], "no change was made");
        const token = reply.cursor.postBatchResumeToken;
        const ts = EloqCdcUtil.tokenTimestamp(token);
        if (EloqCdcUtil.tokenKind(token) === 1 && ts > last) {
            last = ts;
            ++advances;
        }
        return advances >= 2;
    }, "the idle watermark stopped advancing", EloqCdcUtil.kEventTimeoutMS, 100);

    testDB.runCommand({killCursors: "c", cursors: [id]});
})();
