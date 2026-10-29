/**
 * A drop's position in the change stream (docs/cdc-change-stream.md §4.4(b), §5.5, F7; plan B6
 * (ii)). Writes that commit while a drop is between its prepare log and its commit-stage write
 * locks belong to the dropped collection, so every one of them must sit below the drop's
 * commit-stage timestamp P, and the next create of the name above it: the stream relies on that
 * order alone and filters nothing by incarnation.
 *
 * The upsert_table_hold_before_commit_lock fault point holds the drop in that window
 * (tx_upsert_table_operation.cpp) and arms upsert_table_held_before_commit_lock once it does.
 * MongoDB's locks exclude nothing in EloqDoc (every operation runs with EloqLockerNoop), so inserts
 * on this node commit while it is held, after its prepare log, as inserts on another node would. The test checks, in a database-level stream, that those
 * inserts come before the drop with timestamps below P, the drop event's timestamp, and that the
 * create and insert of the next incarnation come after it. Their timestamps need not be above the
 * prepare log's: a write that read the old schema may commit on either side of it (F7); the hold
 * marker, armed before the inserts are sent, is what places them in the window.
 *
 * The server needs the fault points compiled in (EloqDoc Debug builds, WITH_FAULT_INJECT); on any
 * other build eloqFaultInject fails with CommandNotSupported, and so does this test.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const kHold = "upsert_table_hold_before_commit_lock";
    const kHeld = "upsert_table_held_before_commit_lock";
    const kDbName = "eloq_fault_ddl_drop_order";
    const admin = db.getSiblingDB("admin");
    const testDB = db.getSiblingDB(kDbName);
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));
    assert.commandWorked(testDB.c.insert({_id: "before"}));
    // The stream must not report the setup above (§5.1: it starts at the checkpoint bound).
    EloqCdcUtil.settleDdl(db.getMongo());

    /**
     * Whether a fault point is armed on this node.
     *
     * @param name The fault point.
     * @return true when armed.
     */
    function faultArmed(name) {
        return assert.commandWorked(admin.runCommand({eloqFaultInject: name, query: true})).armed;
    }

    const opened =
        assert.commandWorked(EloqCdcUtil.openRaw(db.getMongo(), kDbName, 1, {}));
    const cursorId = opened.cursor.id;

    // Hold the drop between its logs and write while it is held.
    const held = ["held0", "held1", "held2", "held3", "held4"];
    assert.commandWorked(admin.runCommand({eloqFaultInject: kHold}));
    let awaitDrop;
    try {
        assert(faultArmed(kHold), kHold + " is not armed");
        awaitDrop = startParallelShell(function() {
            assert(db.getSiblingDB("eloq_fault_ddl_drop_order").c.drop());
        }, db.getMongo().port);
        assert.soon(() => faultArmed(kHeld), "the drop never reached its hold", 60 * 1000, 100);
        for (let id of held) {
            assert.commandWorked(testDB.c.insert({_id: id}));
        }
    } finally {
        // Armed, the fault would hold every later DDL of the suite for a minute.
        assert.commandWorked(admin.runCommand({eloqFaultInject: kHold, params: "remove"}));
    }
    awaitDrop();
    assert(!faultArmed(kHeld), kHeld + " is still armed after the drop");

    // The next incarnation, created by its first insert.
    assert.commandWorked(testDB.c.insert({_id: "after"}));

    let events = [];
    assert.soon(function() {
        const reply = assert.commandWorked(testDB.runCommand(
            {getMore: cursorId, collection: "$cmd.aggregate", maxTimeMS: 1000}));
        events = events.concat(reply.cursor.nextBatch);
        return events.length >= held.length + 3;
    }, () => "events: " + tojson(events), EloqCdcUtil.kEventTimeoutMS, 100);
    assert.commandWorked(testDB.runCommand({killCursors: "$cmd.aggregate", cursors: [cursorId]}));

    // In stream order: the held inserts, the drop, then the next incarnation.
    assert.eq(events.map(e => e.operationType + (e.documentKey ? ":" + e.documentKey._id : "")),
              held.map(id => "insert:" + id).concat(["drop", "create", "insert:after"]),
              tojson(events));
    const ts = events.map(e => EloqCdcUtil.tokenTimestamp(e._id));
    const dropIndex = held.length;
    const commitStageTs = ts[dropIndex];

    for (let i = 0; i < held.length; i++) {
        assert.lt(
            ts[i], commitStageTs, "insert " + held[i] + " is not below the drop: " + tojson(ts));
    }
    assert.gt(ts[dropIndex + 1], commitStageTs, "the next create is not above the drop");
    assert.gt(ts[dropIndex + 2], ts[dropIndex + 1]);

    assert.commandWorked(testDB.dropDatabase());
})();
