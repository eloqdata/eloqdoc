/**
 * serverStatus' eloqChangeStreams section (docs/cdc-change-stream.md §8; plan C12): an open stream
 * is listed with its scope and namespace; its lag is how far what it has produced lies below the
 * emit bound it last saw, 0 once a getMore finds nothing more; dedupSetOverflows counts the changes
 * a full dedup set did not record, per stream and over the server's lifetime; a closed stream
 * leaves the list and keeps its overflows in the total. The section also reports the node's
 * checkpoint round and checkpoint delay, which bound how soon a change reaches the streams.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const admin = db.getSiblingDB("admin");
    const testDB = db.getSiblingDB("eloq_cdc_server_status");
    assert.commandWorked(testDB.dropDatabase());

    function section() {
        const status = assert.commandWorked(admin.runCommand({serverStatus: 1}));
        assert(status.eloqChangeStreams, "serverStatus has no eloqChangeStreams section");
        return status.eloqChangeStreams;
    }

    function mine() {
        return section().streams.filter(s => s.ns === testDB.getName());
    }

    function getMore(id) {
        return assert.commandWorked(
            testDB.runCommand({getMore: id, collection: "$cmd.aggregate", maxTimeMS: 1000}));
    }

    // At 0 bytes the dedup set records nothing, so every change the stream produces counts as an
    // overflow. Read when a stream opens.
    const originalDedup =
        assert.commandWorked(admin.runCommand({getParameter: 1, cdc_dedup_set_bytes: 1}))
            .cdc_dedup_set_bytes;
    assert.commandWorked(admin.runCommand({setParameter: 1, cdc_dedup_set_bytes: 0}));
    try {
        EloqCdcUtil.settleDdl(db.getMongo());
        const before = section();
        assert.gt(before.checkpointIntervalMillis, 0, tojson(before));
        const id = assert.commandWorked(EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), 1, {}))
                       .cursor.id;
        assert.eq(mine().length, 1, tojson(section()));
        assert.eq(mine()[0].scope, "database", tojson(mine()));
        assert.eq(section().open, before.open + 1, tojson(section()));

        assert.commandWorked(testDB.c.insert([{_id: 1}, {_id: 2}, {_id: 3}]));
        let inserts = 0;
        assert.soon(function() {
            inserts += getMore(id).cursor.nextBatch.filter(e => e.operationType === "insert").length;
            return inserts >= 3;
        }, "the inserts never arrived", EloqCdcUtil.kEventTimeoutMS, 100);
        const reading = mine()[0];
        assert.gte(reading.dedupSetOverflows, 3, tojson(reading));
        assert.gt(reading.producedThrough, 0, tojson(reading));
        assert.gte(reading.lagMicros, 0, tojson(reading));
        assert.gte(reading.idleMillis, 0, tojson(reading));

        // A getMore that finds nothing more leaves the stream produced through its bound.
        assert.soon(function() {
            assert.eq(getMore(id).cursor.nextBatch, [], "unexpected events");
            const caughtUp = mine()[0];
            // The section's numbers are NumberLongs, which === compares by identity.
            return Number(caughtUp.lagMicros) === 0 &&
                Number(caughtUp.producedThrough) === Number(caughtUp.emitBound);
        }, () => "the lag never reached 0: " + tojson(mine()), EloqCdcUtil.kEventTimeoutMS, 100);
        assert.gte(section().maxLagMicros, 0, tojson(section()));
        assert.gte(section().dedupSetOverflows, before.dedupSetOverflows + 3, tojson(section()));

        // Closed, the stream leaves the list and its overflows stay in the total.
        assert.commandWorked(testDB.runCommand({killCursors: "$cmd.aggregate", cursors: [id]}));
        assert.soon(() => mine().length === 0, () => tojson(section()));
        assert.eq(section().open, before.open, tojson(section()));
        assert.gte(section().dedupSetOverflows, before.dedupSetOverflows + 3, tojson(section()));
    } finally {
        assert.commandWorked(
            admin.runCommand({setParameter: 1, cdc_dedup_set_bytes: originalDedup}));
    }
    assert.commandWorked(testDB.dropDatabase());
})();
