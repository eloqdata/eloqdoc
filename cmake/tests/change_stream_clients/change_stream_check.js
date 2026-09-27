/**
 * @file EloqDoc change streams through the Node.js driver's watch() (docs/cdc-change-stream.md
 * §10.2, plan C13). Run by cmake/tests/change_stream_drivers.py against a fresh server:
 *
 *     node change_stream_check.js <port> <scenario>...
 *
 * with scenarios "opening" (i), "resume" (ii) and "not-ready" (iii), as described in that
 * script. Prints one PASS or SKIP line per scenario and exits non-zero on the first failure.
 * Needs mongodb@6.16.0, the last Node.js driver that accepts a MongoDB 4.0 server (wire
 * version 7).
 */
"use strict";

const assert = require("assert");
const {MongoClient, MongoServerError} = require("mongodb");

/** EloqDoc's ChangeStreamStartNotReady. */
const kChangeStreamStartNotReady = 5000;
/** CommandNotSupported: the server was built without fault points. */
const kCommandNotSupported = 115;
/** Longest wait for an expected event or watermark. */
const kEventTimeoutMs = 120 * 1000;

/**
 * The microsecond timestamp a resume token records: _data = 02 | u64 ts | kind | ...
 *
 * @param {object} token A resume token.
 * @return {bigint} Its timestamp.
 */
function tokenTimestamp(token) {
    return BigInt("0x" + token._data.substr(2, 16));
}

/**
 * The kind a resume token records: 0 event, 1 watermark, 2 invalidate.
 *
 * @param {object} token A resume token.
 * @return {number} Its kind.
 */
function tokenKind(token) {
    return parseInt(token._data.substr(18, 2), 16);
}

/**
 * (i) watch() on a server with no completed checkpoint round returns a watermark token once the
 * first round completes.
 *
 * @param {MongoClient} client A client of the server.
 */
async function opening(client) {
    const coll = client.db("cdc_drivers").collection("opening");
    const start = Date.now();
    const stream = coll.watch([], {maxAwaitTimeMS: 1000});
    try {
        assert.strictEqual(await stream.tryNext(), null);
        const elapsed = (Date.now() - start) / 1000;
        const token = stream.resumeToken;
        assert.ok(token, "the opening reply carried no postBatchResumeToken");
        assert.strictEqual(tokenKind(token), 1, JSON.stringify(token));
        console.log(`PASS node (i) watch() on a fresh server returned a token after ` +
                    `${elapsed.toFixed(1)} s`);
    } finally {
        await stream.close();
    }
}

/**
 * (ii) A killed cursor resumes from the opening watermark, and a change E made meanwhile
 * arrives exactly once.
 *
 * @param {MongoClient} client A client of the server.
 */
async function resume(client) {
    const db = client.db("cdc_drivers");
    const coll = db.collection("resume");
    const stream = coll.watch([], {maxAwaitTimeMS: 1000});
    try {
        assert.strictEqual(await stream.tryNext(), null);
        assert.ok(stream.resumeToken);
        // Kill the server-side cursor: the next getMore fails with CursorNotFound, which the
        // driver resumes from with the token it holds.
        await db.command({killCursors: coll.collectionName, cursors: [stream.cursor.id]});
        await coll.insertOne({_id: "E"});
        const deadline = Date.now() + kEventTimeoutMs;
        let change = null;
        while (change === null && Date.now() < deadline) {
            change = await stream.tryNext();
        }
        assert.ok(change, "E never arrived");
        assert.strictEqual(change.documentKey._id, "E", JSON.stringify(change));
        const eventTs = tokenTimestamp(change._id);
        // Nothing more at or below E: read on until the watermark passes it.
        for (;;) {
            assert.ok(Date.now() < deadline, "the watermark never passed E");
            const more = await stream.tryNext();
            assert.strictEqual(more, null, `E arrived twice or something else arrived: ` +
                                               JSON.stringify(more));
            const token = stream.resumeToken;
            if (tokenKind(token) === 1 && tokenTimestamp(token) >= eventTs) {
                break;
            }
        }
        console.log("PASS node (ii) resumed from the opening watermark; E arrived exactly once");
    } finally {
        await stream.close();
    }
}

/**
 * (iii) With checkpoint rounds skipped, watch() fails with ChangeStreamStartNotReady once
 * cdc_open_wait_ms runs out, and succeeds when retried after the rounds resume.
 *
 * @param {MongoClient} client A client of the server.
 */
async function notReady(client) {
    const admin = client.db("admin");
    try {
        await admin.command({eloqFaultInject: "checkpointer_skip_ckpt"});
    } catch (e) {
        if (e instanceof MongoServerError && e.code === kCommandNotSupported) {
            console.log("SKIP node (iii) the server was built without WITH_FAULT_INJECT");
            return;
        }
        throw e;
    }
    await admin.command({setParameter: 1, cdc_open_wait_ms: 3000});
    const coll = client.db("cdc_drivers").collection("not_ready");
    const failing = coll.watch();
    try {
        await failing.tryNext();
        assert.fail("the open succeeded with every checkpoint round skipped");
    } catch (e) {
        assert.ok(e instanceof MongoServerError, String(e));
        assert.strictEqual(e.code, kChangeStreamStartNotReady, String(e));
    } finally {
        await failing.close();
    }
    await admin.command({eloqFaultInject: "checkpointer_skip_ckpt", params: "remove"});
    await admin.command({setParameter: 1, cdc_open_wait_ms: 120000});
    const retried = coll.watch([], {maxAwaitTimeMS: 1000});
    try {
        assert.strictEqual(await retried.tryNext(), null);
        assert.ok(retried.resumeToken);
    } finally {
        await retried.close();
    }
    console.log("PASS node (iii) ChangeStreamStartNotReady surfaced; the retry succeeded");
}

/** Scenario functions by the names the harness passes. */
const kScenarios = {"opening": opening, "resume": resume, "not-ready": notReady};

/**
 * Runs the scenarios named on the command line against the server on the given port.
 */
async function main() {
    const [port, ...names] = process.argv.slice(2);
    const client = new MongoClient(`mongodb://127.0.0.1:${port}/?directConnection=true`,
                                   {serverSelectionTimeoutMS: 5000});
    await client.connect();
    try {
        for (const name of names) {
            assert.ok(kScenarios[name], `unknown scenario ${name}`);
            await kScenarios[name](client);
        }
    } finally {
        await client.close();
    }
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
