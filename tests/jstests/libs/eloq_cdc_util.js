/**
 * Helpers for the eloq_cdc suite (docs/cdc-change-stream.md): reading events from a change stream
 * with a deadline, asserting silence, and reading the timestamp and kind inside a resume token.
 *
 * A change surfaces once every log group's checkpoint bound has passed it (§4.2, §5.7): one
 * checkpointer round at worst, plus the MVCC delay. The waits below allow for the default 10 s
 * interval several times over, so the suite also runs against an untuned server.
 */
var EloqCdcUtil = (function() {
    "use strict";

    // Longest wait for events that are expected to arrive.
    const kEventTimeoutMS = 120 * 1000;

    /**
     * Pulls exactly 'n' events from a change stream cursor, waiting up to kEventTimeoutMS, and
     * fails if they do not all arrive.
     */
    function nextEvents(cursor, n) {
        const events = [];
        assert.soon(
            function() {
                while (events.length < n && cursor.hasNext()) {
                    events.push(cursor.next());
                }
                return events.length >= n;
            },
            function() {
                return "expected " + n + " events, got " + tojson(events);
            },
            kEventTimeoutMS,
            100);
        return events;
    }

    /**
     * Asserts that no event arrives on 'cursor' for 'ms' milliseconds. Each hasNext() issues an
     * awaitData getMore, which returns an empty batch after its own wait.
     */
    function assertNoEvents(cursor, ms) {
        const end = Date.now() + ms;
        while (Date.now() < end) {
            if (cursor.hasNext()) {
                doassert("unexpected event: " + tojson(cursor.next()));
            }
        }
    }

    /**
     * The timestamp, in microseconds, a token records: its _data is the hex of
     * 0x02 | u64 timestamp | u8 kind | ... (§5.6). Timestamps stay below 2^53, so a double holds
     * them exactly.
     */
    function tokenTimestamp(token) {
        assert.eq(typeof token._data, "string", tojson(token));
        return parseInt(token._data.substr(2, 16), 16);
    }

    /** The kind a token records: 0 event, 1 watermark, 2 invalidate. */
    function tokenKind(token) {
        return parseInt(token._data.substr(18, 2), 16);
    }

    /**
     * Runs the opening aggregate of a change stream directly, so a test can look at the raw
     * reply. 'dbName' and 'coll' select the scope (coll 1 for a database or deployment stream).
     */
    function openRaw(conn, dbName, coll, spec, extraPipeline, cursorOptions) {
        return conn.getDB(dbName).runCommand({
            aggregate: coll,
            pipeline: [{$changeStream: spec || {}}].concat(extraPipeline || []),
            cursor: cursorOptions || {}
        });
    }

    /**
     * Waits until a change stream opened from now on starts above every DDL made so far, so that
     * it does not report the collections and indexes a test set up before opening it. A stream
     * opened without a token starts at the checkpoint bound (§5.1), which can lag a DDL that has
     * already returned by a checkpoint round, so such a stream would report it.
     *
     * Makes DDL of its own, a create of eloq_cdc_util.settle and the drop of its database, reads
     * the drop's timestamp from a deployment stream opened before them, and waits for the opening
     * watermark of a fresh stream to reach it. On the suite's one-node fixture every earlier DDL
     * has a lower timestamp. It leaves nothing behind: the drop is its last DDL, and a stream
     * opened afterwards starts above it.
     */
    function settleDdl(conn) {
        const settleDB = conn.getDB("eloq_cdc_util");
        const admin = conn.getDB("admin");
        assert.commandWorked(settleDB.dropDatabase());  // left by an interrupted earlier call
        const watcher =
            assert.commandWorked(openRaw(conn, "admin", 1, {allChangesForCluster: true})).cursor.id;
        assert.commandWorked(settleDB.createCollection("settle"));
        assert.commandWorked(settleDB.dropDatabase());

        let target;
        assert.soon(function() {
            const reply = assert.commandWorked(
                admin.runCommand({getMore: watcher, collection: "$cmd.aggregate", maxTimeMS: 1000}));
            for (let e of reply.cursor.nextBatch) {
                if (e.operationType === "drop" && e.ns.db === "eloq_cdc_util" &&
                    e.ns.coll === "settle") {
                    target = tokenTimestamp(e._id);
                }
            }
            return target !== undefined;
        }, "the drop of eloq_cdc_util.settle never arrived", kEventTimeoutMS, 100);
        assert.commandWorked(admin.runCommand({killCursors: "$cmd.aggregate", cursors: [watcher]}));

        // Delivery does not put the drop below every log group's bound: the merge releases a
        // record once the other groups are within a microsecond of it. Wait until a fresh stream
        // starts at or above it.
        assert.soon(function() {
            const reply =
                assert.commandWorked(openRaw(conn, "admin", 1, {allChangesForCluster: true}));
            assert.commandWorked(
                admin.runCommand({killCursors: "$cmd.aggregate", cursors: [reply.cursor.id]}));
            return tokenTimestamp(reply.cursor.postBatchResumeToken) >= target;
        }, "streams never started above the DDL at " + target, kEventTimeoutMS, 200);
    }

    return {
        kEventTimeoutMS: kEventTimeoutMS,
        nextEvents: nextEvents,
        assertNoEvents: assertNoEvents,
        tokenTimestamp: tokenTimestamp,
        tokenKind: tokenKind,
        openRaw: openRaw,
        settleDdl: settleDdl,
    };
})();
