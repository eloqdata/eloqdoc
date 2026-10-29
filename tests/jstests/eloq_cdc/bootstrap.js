/**
 * What change data capture bootstrap needs from the server (docs/cdc-change-stream.md §6.2; plan
 * D3, D8, E2), driven from the shell the way the reference consumer drives it:
 *
 * - eloqClusterGate pauses schema changes and replies with the pause's timestamp; a second pause
 *   fails with ConflictingOperationInProgress; bad arguments, the reserved mode "ddl_dml" and a
 *   database other than admin are refused;
 * - while the gate is paused, every kind of DDL (create, an insert's implicit create, drop,
 *   dropDatabase, createIndexes, dropIndexes, collMod, and a view's create, collMod and drop) fails
 *   at once with ClusterGatePaused and changes nothing, and DML goes on;
 * - a stream opened during the pause reaches a watermark at or above the pause's timestamp: that
 *   watermark is T0, the position the copy is taken against;
 * - resumed from T0, a stream delivers exactly what happened after it: the DML made after T0 and,
 *   once the gate resumes, the DDL; none of the refused DDL.
 *
 * The gate is cluster-wide state that outlives a test, so every path out of this one resumes it.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const admin = db.getSiblingDB("admin");
    const testDB = db.getSiblingDB("eloq_cdc_bootstrap");
    assert.commandWorked(testDB.dropDatabase());
    const coll = testDB.c;

    // ClusterGatePaused, in EloqDoc's own range of error codes, which a stock shell has no name for.
    const kClusterGatePaused = 5001;
    // A refusal comes back at once; a DDL retried as a write conflict would take tens of seconds.
    const kRefusalMS = 5000;

    function gate(op) {
        return assert.commandWorked(admin.runCommand({eloqClusterGate: {op: op, mode: "ddl"}}));
    }

    function getMore(id) {
        return assert.commandWorked(
            testDB.runCommand({getMore: id, collection: "$cmd.aggregate", maxTimeMS: 1000}));
    }

    /** Runs 'ddl' and checks that the gate refused it at once. */
    function assertRefused(ddl, what) {
        const start = Date.now();
        const res = ddl();
        assert.commandFailedWithCode(res, kClusterGatePaused, what);
        assert.lt(Date.now() - start, kRefusalMS, what + " was not refused at once");
    }

    assert.commandWorked(testDB.createCollection("c"));
    assert.commandWorked(coll.createIndex({a: 1}));
    assert.commandWorked(coll.insert([{_id: 1, a: 1}, {_id: 2, a: 2}]));
    assert.commandWorked(testDB.createCollection("other"));
    assert.commandWorked(testDB.createView("v", "c", []));
    const indexesBefore = coll.getIndexes().map(ix => ix.name).sort();
    const viewsBefore = testDB.system.views.find().toArray();

    // Arguments are checked before anything is locked.
    assert.commandFailedWithCode(admin.runCommand({eloqClusterGate: {op: "close"}}),
                                 ErrorCodes.BadValue);
    assert.commandFailedWithCode(admin.runCommand({eloqClusterGate: {op: "pause", when: 1}}),
                                 ErrorCodes.BadValue);
    assert.commandFailedWithCode(admin.runCommand({eloqClusterGate: "pause"}), ErrorCodes.BadValue);
    assert.commandFailedWithCode(
        admin.runCommand({eloqClusterGate: {op: "pause", mode: "ddl_dml"}}),
        ErrorCodes.CommandNotSupported);
    assert.commandFailedWithCode(testDB.runCommand({eloqClusterGate: {op: "pause"}}),
                                 ErrorCodes.Unauthorized);

    EloqCdcUtil.settleDdl(db.getMongo());
    const pauseTs = gate("pause").pauseTs;
    let resumed = false;
    try {
        assert.gt(pauseTs, 0);
        assert.commandFailedWithCode(admin.runCommand({eloqClusterGate: {op: "pause"}}),
                                     ErrorCodes.ConflictingOperationInProgress);

        assertRefused(() => testDB.createCollection("refused"), "a create");
        assertRefused(() => testDB.runCommand({insert: "implicit", documents: [{_id: 1}]}),
                      "an insert's implicit create");
        assertRefused(() => testDB.runCommand({drop: "other"}), "a drop");
        assertRefused(() => testDB.runCommand({dropDatabase: 1}), "a dropDatabase");
        assertRefused(
            () => testDB.runCommand({createIndexes: "c", indexes: [{key: {b: 1}, name: "b_1"}]}),
            "an index build");
        assertRefused(() => testDB.runCommand({dropIndexes: "c", index: "a_1"}), "an index drop");
        assertRefused(() => testDB.runCommand({collMod: "c", validator: {a: {$exists: true}}}),
                      "a collMod");
        assertRefused(() => testDB.runCommand({create: "refusedView", viewOn: "c", pipeline: []}),
                      "a view create");
        assertRefused(
            () => testDB.runCommand({collMod: "v", viewOn: "other", pipeline: [{$match: {a: 1}}]}),
            "a view collMod");
        assertRefused(() => testDB.runCommand({drop: "v"}), "a view drop");
        assert.eq(testDB.getCollectionNames().sort(), ["c", "other", "v"]);
        assert.eq(testDB.system.views.find().toArray(), viewsBefore);
        assert.eq(coll.getIndexes().map(ix => ix.name).sort(), indexesBefore);
        assert(!testDB.getCollectionInfos({name: "c"})[0].options.hasOwnProperty("validator"));

        // DML is never gated.
        assert.commandWorked(coll.insert({_id: 3, a: 3}));
        assert.commandWorked(coll.update({_id: 1}, {$set: {a: 10}}));
        assert.commandWorked(coll.remove({_id: 2}));

        // T0: the first watermark at or above the pause's timestamp (§6.2 step 3). DDL committed
        // before the pause is below it, and none can commit above it until the resume.
        const watcher = assert.commandWorked(EloqCdcUtil.openRaw(db.getMongo(),
                                                                 testDB.getName(), 1, {}))
                            .cursor.id;
        let t0;
        assert.soon(function() {
            const reply = getMore(watcher);
            const token = reply.cursor.postBatchResumeToken;
            if (EloqCdcUtil.tokenKind(token) === 1 && EloqCdcUtil.tokenTimestamp(token) >= pauseTs) {
                t0 = token;
            }
            return t0 !== undefined;
        }, "no watermark reached the pause at " + pauseTs, EloqCdcUtil.kEventTimeoutMS, 100);
        assert.commandWorked(testDB.runCommand({killCursors: "$cmd.aggregate", cursors: [watcher]}));

        assert.commandWorked(coll.insert({_id: "after-t0"}));
        const resumeTs = gate("resume").resumeTs;
        resumed = true;
        assert.gt(resumeTs, pauseTs);

        assert.commandWorked(coll.createIndex({b: 1}));
        assert.commandWorked(coll.insert({_id: "after-resume"}));

        // From T0: the insert after it, the index build after the resume, the last insert, and
        // nothing else; the refused DDL never committed, so it has no event.
        const id = assert.commandWorked(EloqCdcUtil.openRaw(
                                            db.getMongo(), testDB.getName(), 1, {resumeAfter: t0}))
                       .cursor.id;
        let events = [];
        assert.soon(function() {
            events = events.concat(getMore(id).cursor.nextBatch);
            return events.length >= 3;
        }, () => "events: " + tojson(events), EloqCdcUtil.kEventTimeoutMS, 100);
        assert.eq(events.map(e => [e.operationType, e.ns.coll]),
                  [["insert", "c"], ["createIndexes", "c"], ["insert", "c"]],
                  tojson(events));
        assert.eq(events[0].documentKey, {_id: "after-t0"});
        assert.eq(events[1].operationDescription.indexes.map(ix => ix.name), ["b_1"]);
        assert.eq(events[2].documentKey, {_id: "after-resume"});
        assert.gt(EloqCdcUtil.tokenTimestamp(events[1]._id), resumeTs);
        // Nothing more up to a watermark past the last event.
        const lastTs = EloqCdcUtil.tokenTimestamp(events[2]._id);
        assert.soon(function() {
            const reply = getMore(id);
            assert.eq(reply.cursor.nextBatch, [], "unexpected events after " + tojson(events));
            const token = reply.cursor.postBatchResumeToken;
            return EloqCdcUtil.tokenKind(token) === 1 && EloqCdcUtil.tokenTimestamp(token) >= lastTs;
        }, "the watermark never passed " + lastTs, EloqCdcUtil.kEventTimeoutMS, 100);
        assert.commandWorked(testDB.runCommand({killCursors: "$cmd.aggregate", cursors: [id]}));
    } finally {
        if (!resumed) {
            gate("resume");
        }
    }

    // Resuming an open gate is harmless, and DDL runs again.
    gate("resume");
    assert.commandWorked(testDB.dropDatabase());
})();
