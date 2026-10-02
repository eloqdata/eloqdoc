/**
 * Options an EloqDoc change stream refuses, and the cdc_open_wait_ms parameter
 * (docs/cdc-change-stream.md §5.1, §5.2, §5.6): malformed tokens, an operation-time start,
 * both resume options at once, and an invalidate token with resumeAfter, which accepts it only
 * with startAfter.
 */
(function() {
    "use strict";

    load("tests/jstests/libs/eloq_cdc_util.js");

    const testDB = db.getSiblingDB("eloq_cdc_open_errors");
    assert.commandWorked(testDB.dropDatabase());
    assert.commandWorked(testDB.createCollection("c"));

    function open(spec) {
        return EloqCdcUtil.openRaw(db.getMongo(), testDB.getName(), "c", spec);
    }

    // A token is {_data: <hex>}: 0x02, u64 timestamp, u8 kind, u16 entry count, entries (§5.6).
    const invalidateToken = {_data: "02" + "0000000000000005" + "02" + "0000"};

    assert.commandFailed(open({resumeAfter: {_data: "not hex"}}));
    assert.commandFailed(open({resumeAfter: {_data: "03" + "0000000000000005" + "01" + "0000"}}),
                         "a token of another format version");

    // Well-formed bytes that name no change: an event or invalidate token at timestamp 0 (every
    // commit timestamp is positive), and a watermark at the largest timestamp (no successor to
    // resume from). Each once aborted the server, or could have; now each fails the open.
    const zeroEvent = {_data: "02" + "0000000000000000" + "00" + "0000"};
    const zeroInvalidate = {_data: "02" + "0000000000000000" + "02" + "0000"};
    const maxWatermark = {_data: "02" + "ffffffffffffffff" + "01" + "0000"};
    for (let token of [zeroEvent, zeroInvalidate, maxWatermark]) {
        for (let option of ["resumeAfter", "startAfter"]) {
            assert.commandFailedWithCode(
                open({[option]: token}), ErrorCodes.FailedToParse, option + " " + tojson(token));
        }
    }
    assert.commandWorked(db.adminCommand({ping: 1}), "the server survived the malformed tokens");
    assert.commandFailedWithCode(open({startAtOperationTime: Timestamp(1, 1)}),
                                 ErrorCodes.InvalidOptions);
    const watermark = {_data: "02" + "0000000000000005" + "01" + "0000"};
    assert.commandFailedWithCode(open({resumeAfter: watermark, startAfter: watermark}), 40674);
    // As in MongoDB: an invalidate token resumes only with startAfter.
    const viaResumeAfter = assert.commandFailedWithCode(open({resumeAfter: invalidateToken}),
                                                        ErrorCodes.InvalidResumeToken);
    assert(/startAfter/.test(viaResumeAfter.errmsg), tojson(viaResumeAfter));
    const viaStartAfter = assert.commandWorked(open({startAfter: invalidateToken}));
    assert.commandWorked(testDB.runCommand({killCursors: "c", cursors: [viaStartAfter.cursor.id]}));

    // cdc_open_wait_ms is a positive, runtime-settable parameter.
    const admin = db.getSiblingDB("admin");
    const original =
        assert.commandWorked(admin.runCommand({getParameter: 1, cdc_open_wait_ms: 1}))
            .cdc_open_wait_ms;
    assert.commandFailed(admin.runCommand({setParameter: 1, cdc_open_wait_ms: 0}));
    assert.commandWorked(admin.runCommand({setParameter: 1, cdc_open_wait_ms: 5000}));
    assert.commandWorked(admin.runCommand({setParameter: 1, cdc_open_wait_ms: original}));

    // cdc_scan_max_bytes is runtime-settable within 1 byte and the log service's 32 MB cap.
    const originalPage =
        assert.commandWorked(admin.runCommand({getParameter: 1, cdc_scan_max_bytes: 1}))
            .cdc_scan_max_bytes;
    assert.eq(originalPage, 4 * 1024 * 1024);
    assert.commandFailed(admin.runCommand({setParameter: 1, cdc_scan_max_bytes: 0}));
    assert.commandFailed(
        admin.runCommand({setParameter: 1, cdc_scan_max_bytes: 32 * 1024 * 1024 + 1}));
    assert.commandWorked(admin.runCommand({setParameter: 1, cdc_scan_max_bytes: 32 * 1024 * 1024}));
    assert.commandWorked(admin.runCommand({setParameter: 1, cdc_scan_max_bytes: originalPage}));
})();
