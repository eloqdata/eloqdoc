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

    return {
        kEventTimeoutMS: kEventTimeoutMS,
        nextEvents: nextEvents,
        assertNoEvents: assertNoEvents,
        tokenTimestamp: tokenTimestamp,
        tokenKind: tokenKind,
        openRaw: openRaw,
    };
})();
