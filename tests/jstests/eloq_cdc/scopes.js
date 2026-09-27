/**
 * The three stream scopes (docs/cdc-change-stream.md §5.1): a collection stream sees its
 * namespace only, a database stream its database, and a deployment stream every collection
 * outside local, admin and config.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const dbA = db.getSiblingDB("eloq_cdc_scope_a");
    const dbB = db.getSiblingDB("eloq_cdc_scope_b");
    assert.commandWorked(dbA.dropDatabase());
    assert.commandWorked(dbB.dropDatabase());
    for (let [d, c] of [[dbA, "c"], [dbA, "d"], [dbB, "e"]]) {
        assert.commandWorked(d.createCollection(c));
    }

    function open(dbName, coll, spec) {
        const reply = EloqCdcUtil.openRaw(db.getMongo(), dbName, coll, spec);
        assert.commandWorked(reply);
        // getMore names the cursor's collection, "$cmd.aggregate" for a database or deployment
        // stream.
        const ns = reply.cursor.ns;
        return {
            db: db.getSiblingDB(dbName),
            coll: ns.substring(ns.indexOf(".") + 1),
            id: reply.cursor.id
        };
    }
    const streams = {
        collection: open(dbA.getName(), "c", {}),
        database: open(dbA.getName(), 1, {}),
        deployment: open("admin", 1, {allChangesForCluster: true}),
    };

    assert.commandWorked(dbA.c.insert({_id: "a.c"}));
    assert.commandWorked(dbA.d.insert({_id: "a.d"}));
    assert.commandWorked(dbB.e.insert({_id: "b.e"}));
    assert.commandWorked(db.getSiblingDB("admin").eloq_cdc_scope.insert({_id: "admin"}));
    // The marker, written last, bounds what each stream must have seen by the time it arrives.
    assert.commandWorked(dbB.e.insert({_id: "marker"}));

    // The deployment stream reads up to the marker, which gives its timestamp.
    let markerTs;
    function namespacesUntilMarker(stream) {
        const seen = [];
        assert.soon(function() {
            const reply = assert.commandWorked(stream.db.runCommand(
                {getMore: stream.id, collection: stream.coll, maxTimeMS: 1000}));
            for (let e of reply.cursor.nextBatch) {
                if (e.documentKey._id === "marker") {
                    markerTs = EloqCdcUtil.tokenTimestamp(e._id);
                    return true;
                }
                seen.push(e.ns.db + "." + e.ns.coll);
            }
            return false;
        }, () => "the marker never arrived; saw " + tojson(seen), EloqCdcUtil.kEventTimeoutMS, 100);
        return seen;
    }

    // The others never see the marker: they read until their resume token reaches its timestamp,
    // which certifies that everything up to it has been delivered (§5.3 step 4).
    function namespacesThroughMarkerTs(stream) {
        const seen = [];
        assert.soon(function() {
            const reply = assert.commandWorked(stream.db.runCommand(
                {getMore: stream.id, collection: stream.coll, maxTimeMS: 1000}));
            for (let e of reply.cursor.nextBatch) {
                seen.push(e.ns.db + "." + e.ns.coll);
            }
            return EloqCdcUtil.tokenTimestamp(reply.cursor.postBatchResumeToken) >= markerTs;
        }, () => "never passed the marker; saw " + tojson(seen), EloqCdcUtil.kEventTimeoutMS, 100);
        return seen;
    }

    assert.eq(namespacesUntilMarker(streams.deployment),
              [dbA.getName() + ".c", dbA.getName() + ".d", dbB.getName() + ".e"]);
    assert.eq(namespacesThroughMarkerTs(streams.database),
              [dbA.getName() + ".c", dbA.getName() + ".d"]);
    assert.eq(namespacesThroughMarkerTs(streams.collection), [dbA.getName() + ".c"]);

    for (let s of [streams.collection, streams.database, streams.deployment]) {
        assert.commandWorked(s.db.runCommand({killCursors: s.coll, cursors: [s.id]}));
    }
})();
