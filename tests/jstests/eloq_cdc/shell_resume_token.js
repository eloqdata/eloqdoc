/**
 * DBCommandCursor's resume-token bookkeeping (src/mongo/shell/query.js), exercised against
 * canned server replies through a stubbed db. No server takes part, so this runs in any suite
 * and pins the shell-side contract that docs/cdc-change-stream.md §5.3 step 4 relies on:
 * getResumeToken() never points past a document the caller has not received.
 *
 * The first case is the one that matters most: a getMore issued by hasNext() replaces the
 * cursor's batch token, but nothing from that batch has been returned yet, so the token
 * reported must still be the one from before the fetch.
 */
(function() {
    "use strict";

    const cursorId = NumberLong(42);

    function token(data) {
        return {_data: data};
    }

    function event(data, key) {
        return {_id: token(data), operationType: "insert", documentKey: {_id: key}};
    }

    function opening(firstBatch, postBatchResumeToken) {
        const reply = {ok: 1, cursor: {id: cursorId, ns: "test.c", firstBatch: firstBatch}};
        if (postBatchResumeToken !== undefined) {
            reply.cursor.postBatchResumeToken = postBatchResumeToken;
        }
        return reply;
    }

    function getMoreReply(nextBatch, postBatchResumeToken, id) {
        const reply = {
            ok: 1,
            cursor: {id: id === undefined ? cursorId : id, ns: "test.c", nextBatch: nextBatch}
        };
        if (postBatchResumeToken !== undefined) {
            reply.cursor.postBatchResumeToken = postBatchResumeToken;
        }
        return reply;
    }

    // A db whose runCommand answers each getMore with the next canned reply.
    function stubDb(replies) {
        const mongo = {
            useReadCommands: () => true,
            cursorHandleFromId: () => ({zeroCursorId: () => {}}),
        };
        return {
            getMongo: () => mongo,
            getName: () => "test",
            runCommand: function(cmd) {
                assert.eq(cmd.getMore, cursorId, "only getMore is expected: " + tojson(cmd));
                assert.gt(replies.length, 0, "no canned reply left for " + tojson(cmd));
                return replies.shift();
            },
        };
    }

    // A batch fetched by hasNext() but not yet read must not move the token.
    (function prefetchDoesNotAdvanceTheToken() {
        const replies = [getMoreReply([event("A", 1), event("B", 2)], token("afterB"))];
        const cursor = new DBCommandCursor(stubDb(replies), opening([], token("S")), 0);
        assert.eq(cursor.getResumeToken(), token("S"), "opening watermark");

        assert(cursor.hasNext(), "A and B are fetched but nothing is returned");
        assert.eq(cursor.getResumeToken(), token("S"), "still S: nothing has been consumed");

        assert.eq(cursor.next()._id, token("A"));
        assert.eq(cursor.getResumeToken(), token("A"), "after consuming A");

        assert.eq(cursor.next()._id, token("B"));
        assert.eq(cursor.getResumeToken(), token("afterB"), "batch consumed: server token");
        assert.eq(replies.length, 0);
    })();

    // Empty batches carry watermarks, and each one advances the token once received.
    (function emptyBatchesAdvanceToTheirWatermark() {
        const replies = [getMoreReply([], token("W1")), getMoreReply([], token("W2"))];
        const cursor = new DBCommandCursor(stubDb(replies), opening([], token("S")), 0);
        assert(!cursor.hasNext());
        assert.eq(cursor.getResumeToken(), token("W1"));
        assert(!cursor.hasNext());
        assert.eq(cursor.getResumeToken(), token("W2"));
    })();

    // An unread, non-empty opening batch has no honest token: the caller's own resumeAfter is
    // the position, and the cursor must not fabricate one from the batch's token.
    (function unreadOpeningBatchReportsNothing() {
        const cursor =
            new DBCommandCursor(stubDb([]), opening([event("A", 1)], token("afterA")), 0);
        assert.eq(cursor.getResumeToken(), undefined);
        assert.eq(cursor.next()._id, token("A"));
        assert.eq(cursor.getResumeToken(), token("afterA"), "consumed: server token");
    })();

    // A cursor the server reports no token for never reports one either.
    (function ordinaryCursorsReportNothing() {
        const replies = [getMoreReply([{_id: 2}], undefined)];
        const cursor = new DBCommandCursor(stubDb(replies), opening([{_id: 1}]), 0);
        assert.eq(cursor.getResumeToken(), undefined);
        assert.eq(cursor.next()._id, 1);
        assert.eq(cursor.getResumeToken(), undefined);
        assert(cursor.hasNext());
        assert.eq(cursor.next()._id, 2);
        assert.eq(cursor.getResumeToken(), undefined);
    })();

    // The batch that delivers 'invalidate' closes the cursor; its token is the invalidate's.
    (function invalidateClosesWithItsToken() {
        const replies = [getMoreReply([event("INV", 0)], token("INV"), NumberLong(0))];
        const cursor = new DBCommandCursor(stubDb(replies), opening([], token("S")), 0);
        assert(cursor.hasNext());
        assert.eq(cursor.getResumeToken(), token("S"), "fetched, not yet returned");
        assert.eq(cursor.next()._id, token("INV"));
        assert.eq(cursor.getResumeToken(), token("INV"));
        assert(cursor.isExhausted());
    })();
})();
