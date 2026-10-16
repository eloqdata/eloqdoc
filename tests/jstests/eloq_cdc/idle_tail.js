/**
 * The tail of a quiet period (docs/cdc-change-stream.md §5.7, §10.10; plan F3): one change, then
 * silence. The change must still arrive, carried past the stream's bound by the checkpoint rounds
 * that run with no traffic, and with the first round that can carry it; afterwards the watermark
 * must keep advancing with no events.
 *
 * The test counts checkpoint rounds, not seconds: every round's report advances the stream's
 * watermark once, and under load the rounds and the change slow down alike. Before the change
 * arrives the watermark may advance once for a round that took its checkpoint timestamp before the
 * insert committed, and once more for every checkpoint round the checkpoint delay spans: while a
 * snapshot transaction that started before the insert is still running, a round's checkpoint
 * timestamp may sit up to the delay (checkpointer_delay_seconds, 5 s by default) below the round's
 * own, below the commit.
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

    // The default checkpointer_delay_seconds, which the server does not report.
    const kDelayMS = 5000;
    const section = assert.commandWorked(db.adminCommand({serverStatus: 1})).eloqChangeStreams;
    const roundMS = section.checkpointIntervalMillis;
    assert.gt(roundMS, 0, tojson(section));
    const kAllowedAdvances = 1 + Math.ceil(kDelayMS / roundMS);

    // The timestamp of the reply's watermark token if it is above 'last', else undefined.
    function advanced(reply, last) {
        const token = reply.cursor.postBatchResumeToken;
        const ts = EloqCdcUtil.tokenTimestamp(token);
        return EloqCdcUtil.tokenKind(token) === 1 && ts > last ? ts : undefined;
    }

    // The watermark just before the insert, so that only the rounds reported after it count.
    const before = getMore();
    assert.eq(before.cursor.nextBatch, [], "no change was made yet");
    let last = EloqCdcUtil.tokenTimestamp(before.cursor.postBatchResumeToken);

    // One change, then nothing else writes.
    assert.commandWorked(testDB.c.insert({_id: "tail"}));
    const insertedAt = Date.now();
    let tail;
    let advancesBefore = 0;
    assert.soon(function() {
        const reply = getMore();
        const batch = reply.cursor.nextBatch;
        if (batch.length > 0) {
            assert.eq(batch.length, 1, tojson(batch));
            tail = batch[0];
            return true;
        }
        const ts = advanced(reply, last);
        if (ts !== undefined) {
            last = ts;
            ++advancesBefore;
        }
        return false;
    }, "the tail change never arrived", EloqCdcUtil.kEventTimeoutMS, 100);
    assert.eq(tail.documentKey._id, "tail");
    print("the tail change arrived " + (Date.now() - insertedAt) + " ms after its insert, after " +
          advancesBefore + " watermark advance(s) that did not carry it");
    assert.lte(advancesBefore,
               kAllowedAdvances,
               "the watermark advanced " + advancesBefore + " times before the tail change " +
                   "arrived; checkpoint round " + roundMS + " ms, delay " + kDelayMS + " ms");

    // Two more watermark advances past it, with no traffic in between.
    last = EloqCdcUtil.tokenTimestamp(tail._id);
    let advancesAfter = 0;
    assert.soon(function() {
        const reply = getMore();
        assert.eq(reply.cursor.nextBatch, [], "no change was made");
        const ts = advanced(reply, last);
        if (ts !== undefined) {
            last = ts;
            ++advancesAfter;
        }
        return advancesAfter >= 2;
    }, "the idle watermark stopped advancing", EloqCdcUtil.kEventTimeoutMS, 100);

    testDB.runCommand({killCursors: "c", cursors: [id]});
})();
