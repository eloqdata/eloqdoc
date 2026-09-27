import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.BsonDocument;
import org.bson.Document;

/**
 * EloqDoc change streams through the Java sync driver's watch() (docs/cdc-change-stream.md
 * §10.2, plan C13).
 *
 * <p>Run by cmake/tests/change_stream_drivers.py against a fresh server:
 *
 * <pre>java -cp &lt;driver jars&gt;:. ChangeStreamCheck &lt;port&gt; &lt;scenario&gt;...</pre>
 *
 * with scenarios "opening" (i), "resume" (ii) and "not-ready" (iii), as described in that
 * script. Prints one PASS or SKIP line per scenario and exits non-zero on the first failure.
 * Needs driver 5.4.0, the last Java driver that accepts a MongoDB 4.0 server (wire version 7).
 */
public final class ChangeStreamCheck {
    /** EloqDoc's ChangeStreamStartNotReady. */
    private static final int CHANGE_STREAM_START_NOT_READY = 5000;
    /** CommandNotSupported: the server was built without fault points. */
    private static final int COMMAND_NOT_SUPPORTED = 115;
    /** Longest wait for an expected event or watermark, in milliseconds. */
    private static final long EVENT_TIMEOUT_MS = 120_000;

    private ChangeStreamCheck() {}

    /**
     * Fails the scenario unless a condition holds.
     *
     * @param condition The condition.
     * @param message What went wrong otherwise.
     */
    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * The microsecond timestamp a resume token records: _data = 02 | u64 ts | kind | ...
     *
     * @param token A resume token.
     * @return Its timestamp.
     */
    private static long tokenTimestamp(BsonDocument token) {
        return Long.parseUnsignedLong(token.getString("_data").getValue().substring(2, 18), 16);
    }

    /**
     * The kind a resume token records: 0 event, 1 watermark, 2 invalidate.
     *
     * @param token A resume token.
     * @return Its kind.
     */
    private static int tokenKind(BsonDocument token) {
        return Integer.parseInt(token.getString("_data").getValue().substring(18, 20), 16);
    }

    /**
     * (i) watch() on a server with no completed checkpoint round returns a watermark token once
     * the first round completes.
     *
     * @param client A client of the server.
     */
    private static void opening(MongoClient client) {
        MongoCollection<Document> coll = client.getDatabase("cdc_drivers").getCollection("opening");
        long start = System.nanoTime();
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> stream =
                 coll.watch().maxAwaitTime(1, TimeUnit.SECONDS).cursor()) {
            double elapsed = (System.nanoTime() - start) / 1e9;
            check(stream.tryNext() == null, "an event arrived on an empty collection");
            BsonDocument token = stream.getResumeToken();
            check(token != null, "the opening reply carried no postBatchResumeToken");
            check(tokenKind(token) == 1, "not a watermark: " + token.toJson());
            System.out.printf(
                "PASS java (i) watch() on a fresh server returned a token after %.1f s%n", elapsed);
        }
    }

    /**
     * (ii) A killed cursor resumes from the opening watermark, and a change E made meanwhile
     * arrives exactly once.
     *
     * @param client A client of the server.
     */
    private static void resume(MongoClient client) {
        MongoDatabase db = client.getDatabase("cdc_drivers");
        MongoCollection<Document> coll = db.getCollection("resume");
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> stream =
                 coll.watch().maxAwaitTime(1, TimeUnit.SECONDS).cursor()) {
            check(stream.tryNext() == null, "an event arrived on an empty collection");
            check(stream.getResumeToken() != null, "no opening token");
            // Kill the server-side cursor: the next getMore fails with CursorNotFound, which the
            // driver resumes from with the token it holds.
            long cursorId = stream.getServerCursor().getId();
            db.runCommand(new Document("killCursors", "resume")
                              .append("cursors", Arrays.asList(cursorId)));
            coll.insertOne(new Document("_id", "E"));
            long deadline = System.currentTimeMillis() + EVENT_TIMEOUT_MS;
            ChangeStreamDocument<Document> change = null;
            while (change == null && System.currentTimeMillis() < deadline) {
                change = stream.tryNext();
            }
            check(change != null, "E never arrived");
            check(change.getDocumentKey().getString("_id").getValue().equals("E"),
                  "unexpected change: " + change);
            long eventTs = tokenTimestamp(change.getResumeToken());
            // Nothing more at or below E: read on until the watermark passes it.
            for (;;) {
                check(System.currentTimeMillis() < deadline, "the watermark never passed E");
                ChangeStreamDocument<Document> more = stream.tryNext();
                check(more == null, "E arrived twice or something else arrived: " + more);
                BsonDocument token = stream.getResumeToken();
                if (tokenKind(token) == 1 && Long.compareUnsigned(tokenTimestamp(token), eventTs) >= 0) {
                    break;
                }
            }
        }
        System.out.println("PASS java (ii) resumed from the opening watermark; E arrived exactly once");
    }

    /**
     * (iii) With checkpoint rounds skipped, watch() fails with ChangeStreamStartNotReady once
     * cdc_open_wait_ms runs out, and succeeds when retried after the rounds resume.
     *
     * @param client A client of the server.
     */
    private static void notReady(MongoClient client) {
        MongoDatabase admin = client.getDatabase("admin");
        try {
            admin.runCommand(new Document("eloqFaultInject", "checkpointer_skip_ckpt"));
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == COMMAND_NOT_SUPPORTED) {
                System.out.println("SKIP java (iii) the server was built without WITH_FAULT_INJECT");
                return;
            }
            throw e;
        }
        admin.runCommand(new Document("setParameter", 1).append("cdc_open_wait_ms", 3000));
        MongoCollection<Document> coll = client.getDatabase("cdc_drivers").getCollection("not_ready");
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> stream = coll.watch().cursor()) {
            throw new AssertionError("the open succeeded with every checkpoint round skipped");
        } catch (MongoCommandException e) {
            check(e.getErrorCode() == CHANGE_STREAM_START_NOT_READY, "unexpected error: " + e);
        }
        admin.runCommand(new Document("eloqFaultInject", "checkpointer_skip_ckpt")
                             .append("params", "remove"));
        admin.runCommand(new Document("setParameter", 1).append("cdc_open_wait_ms", 120000));
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> stream =
                 coll.watch().maxAwaitTime(1, TimeUnit.SECONDS).cursor()) {
            // The driver caches a batch's postBatchResumeToken when it iterates the batch.
            check(stream.tryNext() == null, "an event arrived on an empty collection");
            check(stream.getResumeToken() != null, "the retried open carried no token");
        }
        System.out.println("PASS java (iii) ChangeStreamStartNotReady surfaced; the retry succeeded");
    }

    /**
     * Runs the scenarios named on the command line against the server on the given port.
     *
     * @param args The port, then scenario names.
     */
    public static void main(String[] args) {
        List<String> names = Arrays.asList(args).subList(1, args.length);
        MongoClientSettings settings =
            MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(
                    "mongodb://127.0.0.1:" + args[0] + "/?directConnection=true"))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(5, TimeUnit.SECONDS))
                .build();
        try (MongoClient client = MongoClients.create(settings)) {
            for (String name : names) {
                switch (name) {
                    case "opening":
                        opening(client);
                        break;
                    case "resume":
                        resume(client);
                        break;
                    case "not-ready":
                        notReady(client);
                        break;
                    default:
                        throw new IllegalArgumentException("unknown scenario " + name);
                }
            }
        }
    }
}
