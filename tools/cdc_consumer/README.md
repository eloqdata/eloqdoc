# EloqDoc CDC reference consumer

`eloq_cdc_consumer.py` keeps a MongoDB-compatible database (the **sink**) a copy of an EloqDoc
deployment (the **source**). On its first start it copies the source's data into the sink (the
**bootstrap**). From then on it reads the source's deployment-level change stream and applies every
change to the sink. It stores no events: the sink holds the data itself, plus one checkpoint
document that records how far the consumer has applied the stream.

It is the reference implementation of the consumer protocol in
[cdc-change-stream.md](../../docs/cdc-change-stream.md) §6: it shows how a consumer must use the
stream to stay correct. It is one Python process and has not been load-tested. Incidents on the
source side (a pause that was never resumed, retention, lag alerts) are covered in the runbook,
[cdc-operations.md](../../docs/cdc-operations.md).

## Requirements

**Source.** An EloqDoc deployment whose change streams work: the write-ahead log enabled, a log
service, and a log state that keeps history (RocksDB or RocksDB-Cloud, not MEMORY). Otherwise
opening or reading the stream fails with `CommandNotSupported` and the consumer stops. Set
`txlog_cdc_retention_seconds` above the longest the consumer may be stopped or behind (see
[Limits](#limits-of-this-version)).

**Sink.** A MongoDB-compatible server that supports views. Only EloqDoc has been tested as a sink.
Put the write concern you want in the sink's connection string, for example
`w=majority&journal=true` on a replica set. Every sink write uses it, including the checkpoint,
which is saved only after the writes it covers, so the checkpoint never claims writes the sink
could lose. A sink with `w=0` is refused.

**Python.** Python 3.8 or later with PyMongo 4. Tested with PyMongo 4.8.0 (CI) and 4.13. The
consumer is the single file `eloq_cdc_consumer.py` and can be copied anywhere PyMongo is installed.

## Privileges

Skip this section if the deployments run without authentication.

**Source user.** It needs:

- `readAnyDatabase`, for the change stream over the whole deployment and to list and read the
  databases it copies, views included;
- the `eloqClusterGate` action on the cluster, to pause schema changes during the bootstrap.
  `clusterManager`, `clusterAdmin` and `root` include it.

The built-in pair is enough:

```js
db.getSiblingDB("admin").createUser({user: "cdc", pwd: "<password>",
                                     roles: ["readAnyDatabase", "clusterManager"]})
```

`clusterManager` also allows other cluster administration. To grant only the gate, use a role
like this instead:

```js
use admin
db.createRole({
  role: "eloqCdcSource",
  privileges: [{resource: {cluster: true}, actions: ["eloqClusterGate"]}],
  roles: [{role: "readAnyDatabase", db: "admin"}]
})
db.createUser({user: "cdc", pwd: "<password>", roles: ["eloqCdcSource"]})
```

Both have been tested end to end.

**Sink user.** `readWriteAnyDatabase` and `dbAdminAnyDatabase` are enough; this pair was tested
with an EloqDoc sink. The consumer drops and recreates the databases it mirrors, writes documents
with `bypassDocumentValidation`, creates and drops collections, indexes and views, and keeps its
checkpoint in `admin.__eloq_cdc_progress`.

## Quick start

```sh
python3 -m pip install 'pymongo>=4.8'
python3 tools/cdc_consumer/eloq_cdc_consumer.py \
    --source 'mongodb://cdc:<password>@eloqdoc-host:27017/?authSource=admin' \
    --sink 'mongodb://cdc_sink:<password>@sink-host:27017/?authSource=admin&w=majority&journal=true' \
    --databases shop
```

On a first start the log shows the bootstrap (timestamps left out):

```text
INFO bootstrap: dropping shop on the sink
INFO bootstrap: gate paused at 1791530000000000
INFO bootstrap: T0 0200065d631543064001 (ts 1791530000123456)
INFO bootstrap: copying shop.items
INFO bootstrap: gate resumed
```

After that the consumer logs only schema changes (`applying createIndexes on shop.items`,
`applying a view replace on shop`), warnings when it retries, and the reason it stops. A healthy
consumer that is just applying documents logs nothing; measure its lag instead (see
[Monitoring](#monitoring)).

## Command-line options

| Option | Default | Meaning |
|---|---|---|
| `--source` | required | The EloqDoc connection string. |
| `--sink` | required | The sink's connection string. Its write concern applies to every sink write. |
| `--databases db ...` | every database except `admin`, `local`, `config` | The databases to mirror, separated by spaces. Without this option, databases created on the source later are mirrored too. |
| `--slots` | 8 | Parallel writers to the sink. All changes to one document go through the same slot. |
| `--batch` | 1000 | Most events applied between two checkpoints. |
| `--max-await-ms` | 1000 | How long one read of the stream waits for new events before it returns empty. |
| `--progress-name` | `progress` | The `_id` of this consumer's checkpoint document in `admin.__eloq_cdc_progress`. |
| `--max-rebootstraps` | 3 | Lost-history re-bootstraps allowed in a row before the stream has caught up once (see [When it halts](#when-it-halts)). |
| `--max-copy-retries` | 3 | Times in a row a bootstrap whose copy failed starts over before the consumer halts (see [The bootstrap](#the-bootstrap)). |

## What reaches the sink

Mirrored:

- every collection in the databases in scope, with its creation options (validator, collation and
  so on), except capped settings and the source's collection UUID;
- documents, written with `bypassDocumentValidation`;
- secondary indexes, but **never unique and never TTL** (below);
- views: their creation, redefinition (`collMod`) and drop;
- stored JavaScript (`system.js`).

Not mirrored:

- the `admin`, `local` and `config` databases, so no users or roles;
- system collections other than `system.js` and views, such as `system.profile`;
- **uniqueness.** Unique indexes are built without `unique` on the sink. The source enforces
  uniqueness. On the sink, a change applied a second time after a restart could clash with a later
  state of another document and fail for good, so the sink only mirrors (design §6.3);
- **TTL.** TTL indexes are built without `expireAfterSeconds`. Documents the source expires reach
  the sink as deletes, so the sink never expires anything on its own (design §6.4);
- **changes to a collection's options after it was created** (`collMod` of a validator, of
  validation settings, or of an index's TTL). This version's stream does not report them; the sink
  keeps the options it got when it created the collection. A fresh bootstrap picks up the current
  ones.

Treat the mirrored databases on the sink as read-only. A bootstrap drops them. A document written
there is overwritten by the source's next change to it, and a schema change made there can
contradict the stream and halt the consumer.

## What the sink guarantees

- **Per-document order.** Each document's changes are applied in the order the source made them.
- **Schema changes alone.** A create, drop, index build, index drop or view change is applied after
  every change before it and before any change after it.
- **Parallel across documents.** Within a batch, the slots apply different documents at the same
  time. A reader of the sink can see part of a batch, including part of one source transaction. The
  stream carries no transaction boundaries in this version.
- **Restarts are safe.** After a restart, the changes since the last checkpoint are applied again.
  Each change is applied as a whole-document replace or a delete by `_id`, so applying it twice
  gives the same result.
- **Convergence.** Once the source stops changing, the sink ends up holding the same data, apart
  from the exceptions listed above.

## Running it

### The bootstrap

A consumer bootstraps when its checkpoint is missing or says `bootstrapping`, which is the case on
its first start. The steps:

1. It marks its checkpoint `bootstrapping`.
2. **It drops every database in scope on the sink.**
3. It pauses schema changes on the whole source cluster (`eloqClusterGate`). Until step 5, every
   schema change on the source fails with `ClusterGatePaused` (5001): creating and dropping
   collections, including the implicit create of an insert into a new collection, index builds and
   drops, `collMod`, and view changes. Document reads and writes are not affected.
4. It copies collection definitions, indexes, documents and views, one collection at a time.
5. It resumes schema changes.
6. It saves the checkpoint as `streaming` at the position the copy reflects, and starts applying the
   stream from there.

The pause lasts as long as the copy, so start a bootstrap when schema changes can wait. Only one
bootstrap at a time can run against a source cluster. A second one halts (runbook §3).

If the copy in step 4 fails with any error, for example because the source server the consumer
reads from restarts, the consumer resumes schema changes, waits, and starts over from step 1. The sink is
dropped again and everything is copied afresh: a copy that broke midway is never repaired, only
done again. The first wait is 10 seconds and each later one twice as long; schema changes on the
source are allowed while it waits. After `--max-copy-retries` failures in a row it halts with the
last error.

### Stopping and restarting

Stop it with Ctrl-C or `kill -INT <pid>`. It finishes the sink writes in flight and stops, possibly
in the middle of a batch; the next start applies that batch again from the checkpoint. If it is
stopped during a bootstrap, it resumes the gate before exiting, and the next start repeats the
bootstrap from step 1.

`SIGTERM` and `SIGKILL` end the process at once, without that cleanup. During a bootstrap that
leaves schema changes paused until someone resumes them (runbook §2). Under systemd set
`KillSignal=SIGINT`; in a container set `STOPSIGNAL SIGINT`.

A restart resumes from the checkpoint; no options need to change.

### Exit status

| Status | Meaning |
|---|---|
| 0 | Stopped with Ctrl-C or `SIGINT`. |
| 1 | An unexpected error; Python prints a traceback. An example is the source staying unreachable while the stream is read. The checkpoint is intact; fix the cause and start it again. |
| 2 | Halted: the log's last line is `ERROR halted: <reason>` (see below). Also used for invalid command-line arguments. |

If a supervisor restarts the consumer, restart it on status 1, with a delay, but not on status 2. A
halt waits for an operator, and a restart forgets how many re-bootstraps it has already made.

### When it halts

Writes to the sink, checkpoint saves, gate commands and opening the stream retry errors that clear
up on their own: network errors, primary step-downs, write conflicts and write concern timeouts.
They back off up to 5 seconds between attempts, retry indefinitely and log each attempt as a
warning. While the sink is unreachable, the warnings repeat and lag grows. A bootstrap whose copy
fails starts over (see [The bootstrap](#the-bootstrap)). A failed read of the open stream is not
retried: it ends the run with status 2 or 1, and a restart resumes from the checkpoint. Any other
error halts the consumer with its checkpoint intact:

| `halted:` message | Meaning and fix |
|---|---|
| `the sink's write concern is unacknowledged (w: 0)` | Remove `w=0` from the sink's connection string. |
| `the cluster gate is already paused: ...` | Another bootstrap is running or one was abandoned. Runbook §3. |
| `bootstrap: the copy failed: <error>; gave up after <N> attempts in a row` | Every attempt's copy failed; `<error>` is the last one's. Fix its cause, then start the consumer again, which bootstraps. |
| `create <ns>: the sink already holds it with other options ...` or `createIndexes on <ns>: ...; the cluster gate was bypassed` | The sink's definitions contradict the stream, usually after a schema change made on the sink. Force a fresh bootstrap. |
| `..., again, after <N> bootstraps in a row that never caught up with the source` | The source's history retention is shorter than a bootstrap takes, or the consumer is too slow. Raise `txlog_cdc_retention_seconds` or speed the consumer up, then start it again. Runbook §4. |
| `opening the change stream: ...` | The source cannot serve change streams (see [Requirements](#requirements)) or refused the user. |
| `reading the change stream: ...` | The source failed a read of the stream. Fix the cause and start it again; it resumes from the checkpoint. |
| `<operation> on <ns>: <error>` | The source or the sink refused an operation, for example the sink being out of space or refusing a privilege. Fix the cause and start it again. |
| `invalidate on a deployment-level stream` or `unknown event type` | Not expected from this version of EloqDoc: report it as a bug. |

### Forcing a fresh bootstrap

Stop the consumer, then mark its checkpoint on the sink (use the `--progress-name` value as
`_id`):

```js
db.getSiblingDB("admin").__eloq_cdc_progress.updateOne(
    {_id: "progress"}, {$set: {state: "bootstrapping"}})
```

Deleting the document works too. Start the consumer and it bootstraps. Remember that a bootstrap
drops the mirrored databases on the sink and pauses schema changes on the source while it copies.

### Monitoring

The consumer's lag is the wall clock minus the timestamp of its last checkpoint. In the
`mongo` shell, on the sink:

```js
var p = db.getSiblingDB("admin").__eloq_cdc_progress.findOne({_id: "progress"});
// While the state is "streaming": the token's timestamp is the 16 hex digits after the first
// two of _data, in microseconds since the epoch.
var ts = parseInt(p.token._data.substr(2, 16), 16);
print((Date.now() * 1000 - ts) / 1e6 + " s behind");
```

Changes reach the stream only after the source's next checkpoint round, so even an idle, healthy
consumer reads as a little behind: normally up to about two checkpoint rounds
(`checkpointer_interval`, 10 s by default). Alert when the lag grows past that, and long before it
nears `txlog_cdc_retention_seconds`. The source's own metrics (history lag, gate state, open
streams in `serverStatus`'s `eloqChangeStreams`) are described in runbook §4.

## Several consumers into one sink

Several consumers can share a sink if:

- each one has its own `--progress-name`;
- their `--databases` do not overlap, because a bootstrap drops every database in its scope on the
  sink;
- they are first started one at a time, because bootstraps cannot run at the same time.

Each one reads the whole deployment's stream and discards what is out of its scope, so every extra
consumer adds a full read of the source's history.

## Tuning

- `--slots`: raise it when the sink has spare capacity and the consumer falls behind.
- `--batch`: a larger batch saves fewer checkpoints and gives the slots more to share, and a restart
  re-applies more.
- `--max-await-ms`: raising it makes fewer reads of an idle stream.
- The source's change stream settings (`cdc_cursor_buffer_bytes`, `cdc_dedup_set_bytes`,
  `cdc_scan_max_wait_ms`) are described under "Change Stream Parameters" in
  [configuration-description.md](../../docs/configuration-description.md).

## Limits of this version

- **A consumer that falls behind the source's retained history is not told.** Its stream resumes at
  the oldest record the source still holds, and the changes in between never reach the sink.
  `txlog_cdc_retention_seconds` is the only protection: set it above the longest the consumer may
  be stopped or behind, and bootstrap again if it ever was (runbook §4).
- Changes to a collection's options after it was created are not mirrored (see
  [What reaches the sink](#what-reaches-the-sink)).
- The bootstrap copies one collection at a time, with schema changes paused throughout.
- It has been tested against single-node EloqDoc sources and sinks only.
