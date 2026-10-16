# EloqDoc CDC: operations runbook

Conditions in a deployment that serves change streams which need an operator, how to recognize
each one, and what to do. The design behind them is in
[cdc-change-stream.md](cdc-change-stream.md); its §8 lists every failure mode, most of which need
no action. How to run the reference consumer is in its manual,
[tools/cdc_consumer/README.md](../tools/cdc_consumer/README.md).

## 1. A schema change's data-store step keeps failing: every stream stalls

### What happens

Every schema change (DDL) has a point of no return: the commit log for a create, a drop or a
truncate, and the prepare log for an index build or an index drop. A data-store failure before
that point rolls the DDL back and fails the command. After it the DDL cannot be rolled back, so
the step that failed is retried, every `ddl_kv_retry_interval_seconds` (30 s by default, set at
start-up), until it succeeds, and the DDL keeps the lock it holds on its table's catalog entry
throughout (design §4.4(b)).

Which lock that is decides whether the streams stall. Every step after a DDL's commit log runs
under its catalog **write lock**, taken just before the commit log: a create's catalog row, a
drop's kv tables, sequence table entry and catalog row, a truncate's catalog row, and an index
op's kv index drop and catalog row. An index build's steps before its commit log, building the
index data, run under a write **intent**, which holds nothing back; a failure there leaves the
table writable and the streams flowing, but the build does not finish and no other DDL can run on
the table until the store answers.

A held write lock stops the checkpoint of the DDL's node group: the checkpoint timestamp cannot
pass the lock. A change stream delivers a change only once every log group's checkpoint bound
has passed it (design §4.2, §5.7), so:

- **Every change stream in the cluster stops delivering**, on every collection, not only the
  DDL's. Writes elsewhere keep committing; the streams deliver them, in order and without loss,
  once the stall ends.
- The DDL's table is unavailable: writes to it wait behind the lock.
- The redo log is not truncated while the checkpoint is held, so the log service's disk use grows
  with the write rate. Dirty data of that node group cannot be flushed either, so its memory use
  grows too.

There is no way to cancel the DDL. Restarting the node does not undo it: recovery reads its log
and finishes it (see "If the store cannot be fixed soon" below). Any other node restarted
meanwhile stays down with it: its recovery restores the DDL's write lock on its copy of the
table's catalog entry, and its start-up reads every collection's catalog, so it serves nothing
until the DDL commits (data substrate `docs/range-split-schema-op-recovery.md` §5).

### Symptoms

- In the server log of the DDL's coordinator, on every attempt:

  ```text
  Schema op on table <db.collection>: the data store failed the <step> (attempt <n>, error:
  <error>); retrying in 30 s while holding the table's catalog lock, txn: <txn>
  ```

  `<step>` names what failed, for example the kv table drop, the catalog row update, the sequence
  table update, or the index data flush.
- When the step runs under the write lock: once the node group's checkpoint has been stuck for
  `ckpt_stall_warn_rounds` rounds, once a minute:

  ```text
  Checkpoint of node group #<ng> has not advanced for <n> consecutive rounds: ckpt_ts=... <=
  last_ckpt_ts=.... Pinned by txn <txn> on core #<core>, ...
  ```

  with the same `<txn>` as the retry warning.
- The `checkpoint_stall_rounds` gauge of that node group rises round after round.
- Change streams return empty batches whose `postBatchResumeToken` stops advancing; consumer
  lag grows. The DDL command itself may still be waiting, or its client may have timed out; the
  DDL goes on regardless.

### Tell it apart from

- **The data store is down altogether.** Checkpoints fail with flush errors and nothing is
  pinned by a schema op; streams stall for the same reason, and the fix is the store's. If a DDL
  was past its point of no return, its retry warnings appear too, and it completes on its own
  once the store is back.
- **A long user transaction holds a write lock.** The stall warning names a txn that has no
  `Schema op on table` warnings. It ends when that transaction commits or aborts.
- **An old MVCC snapshot.** The stall warning says "No write-lock-holding transaction found".

### Fix

1. Read the error in the retry warning and fix its cause in the data store: reachability,
   credentials or permissions, disk space or quota, or the state of the named table in the store.
2. Nothing else is needed. The next attempt succeeds, the DDL finishes and releases its lock, the
   next checkpoint round advances, and every stream catches up. Consumers resume on their own;
   their tokens stay valid.

### If the store cannot be fixed soon

A DDL that stalls the streams has written its commit log, since only the steps after it hold the
write lock. It can be moved off the lock by restarting the node that coordinates it (the node
whose log has the retry warnings), or by moving the leadership of its node group to another
node. Recovery finds the commit log and resumes the DDL under a write intent on the table's
catalog entry instead of the write lock (design §8, the row for a DDL kv write that keeps
failing). An intent does not hold back the checkpoint, so the streams resume while the DDL keeps
retrying; it still keeps every other DDL off the table, and the retry warnings go on until the
store recovers. Check after the restart that `checkpoint_stall_rounds` returns to zero and the
streams' tokens advance again.

A DDL whose retries do not stall the streams, an index build retrying its data under its intent,
needs no restart: only the store's recovery finishes it.

Do not delete or roll back the log service's data to get rid of the DDL: its logs are the only
record that it happened, and the catalog, the store and every stream's DDL events depend on them.

## 2. Schema changes stay paused: a bootstrap's pause was never resumed

### What happens

A consumer's bootstrap pauses schema changes cluster-wide with `eloqClusterGate {op: "pause"}`
while it copies the catalog, and resumes them when the copy ends (design §6.2). The pause has no
timeout: it lasts until someone resumes it, on every node group, across restarts and failovers.
The reference consumer resumes it itself when its copy fails with an error, before it starts the
bootstrap over; only a consumer that dies during the copy (killed, its host lost) leaves it in
place.

### Symptoms

- Every schema change fails at once with `ClusterGatePaused` (5001), "Schema changes are paused
  by the cluster gate; retry after it resumes": create and drop, implicit creates, index builds
  and drops, `collMod`, and a view's create, `collMod` and drop. Reads and writes of documents are
  not affected.
- The gauge `cluster_gate_paused{ng_id}` reads 1 on the node groups' leaders, and the gate-age
  alert (§4) fires.

### Tell it apart from

A bootstrap still copying. Its consumer logs `bootstrap: copying <db>.<collection>` lines, and its
checkpoint in the sink, `admin.__eloq_cdc_progress`, reads `{state: "bootstrapping"}`. Resuming the
gate under a running copy abandons that copy: DDL can then reach the source during the copy, and
the sink can end up with a definition the stream contradicts. Wait for it, or stop it first.

### Fix

1. Make sure no consumer is copying: stop the consumer whose bootstrap paused the gate, if it is
   still running.
2. Resume the gate, as a user with the `eloqClusterGate` action on the cluster (`clusterManager`,
   `clusterAdmin` or `root`): `db.adminCommand({eloqClusterGate: {op: "resume"}})`. It replies with
   the resume's timestamp; resuming an open gate is harmless.
3. Start the consumer again. Its checkpoint still says `bootstrapping`, so it bootstraps from the
   start, pausing the gate again for its copy.

## 3. A bootstrap cannot start: the gate is already paused

### What happens

`eloqClusterGate {op: "pause"}` fails with `ConflictingOperationInProgress` when the gate is
already paused. Pauses do not stack, so that a second bootstrap cannot share a window the first one
will close. The reference consumer then halts with "the cluster gate is already paused: another
bootstrap is running, an earlier one was abandoned, or this consumer's own pause was completed
after its reply was lost".

### Fix

Find which of the three it is. If another consumer is copying (§2, "Tell it apart from"), wait for
it to finish, or stop it. Otherwise the pause is abandoned, or the reply to this consumer's own
pause was lost and the pause completed anyway: resume the gate (§2, step 2) and start the consumer
again. Two bootstraps at the same time are an operator error.

## 4. Retention, lag and the alerts to set

### History retention

A change stream reads the log service's history, which the log service purges once it is
checkpointed. `txlog_cdc_retention_seconds`, in the `[local]` section of the data substrate
configuration (default 0), keeps every record from the last that many seconds before the
checkpoint, on both log states (see
[configuration-description.md](configuration-description.md#history-retention-for-change-streams)).
**Set it above the longest a consumer may stay down or behind.** In this version a consumer whose
position falls behind the retained history is not told: its stream resumes at the oldest record
still kept, and the changes in between are lost to it (design §8). Retention is the only
protection; when a consumer did fall behind, bootstrap it again, which drops the namespaces it
mirrors on the sink and copies them afresh: stop the reference consumer, set its checkpoint
document in `admin.__eloq_cdc_progress` to `{state: "bootstrapping"}` (or delete it), and start it.
It also bootstraps again on its own when the source reports `ChangeStreamHistoryLost`, which a
future log service will do once it tracks where its history begins; since each bootstrap drops the
sink's data, it does so at most three times (`--max-rebootstraps`) before its stream has caught up
with the source once, which takes a watermark above where the stream started and within a minute of
the consumer's clock, and then halts: the history is kept for less time than a bootstrap takes, or
the consumer is too slow for it. Raise `txlog_cdc_retention_seconds`, or speed the consumer up,
before starting it again.

### Lag

- `cdc_arrival_complete_ts{lg_id}` is how far each log group's history is complete, in
  microseconds; `time() - cdc_arrival_complete_ts / 1e6` is how far behind the wall clock every
  stream must be at least. It normally stays within two checkpoint rounds (`checkpointer_interval`,
  default 10 s); when it grows, a checkpoint is stalling, which `checkpoint_stall_rounds{ng_id}`
  confirms (§1).
- A consumer's own lag is the wall clock minus the timestamp of the token it saved last. The
  reference consumer saves it in `admin.__eloq_cdc_progress` on the sink: the timestamp is the 16
  hexadecimal digits after the first two of the token's `_data`, in microseconds.
- `serverStatus`'s `eloqChangeStreams` section lists the server's open streams with each one's
  lag (`lagMicros`: how far what it has produced lies below what it could already have) and
  `idleMillis`, the time since its consumer last read from it. A stream's positions move only when
  it is read, so a consumer that stopped reading shows as a lag that stands still while
  `idleMillis` grows. `maxLagMicros` is the largest lag of all. `checkpointIntervalMillis` is the
  node's checkpoint round: a change normally reaches the streams within one round, plus up to
  `checkpointer_delay_seconds` (default 5 s) with MVCC, the most a round's checkpoint timestamp
  sits below the round's own.
- `cdc_scan_parked_requests{lg_id}` counts the streams waiting for new history on a log group's
  leader; past `cdc_scan_max_parked` (default 1024) new waits are answered at once and the streams
  poll instead.

### Alerts

The metrics are on the data substrate's metrics endpoint (`/eloq_metrics`, `[metrics]
enable_metrics`):

- **Gate age**: `max(cluster_gate_paused) == 1` held for longer than the longest copy you expect
  (a duration rule: in Prometheus, `for:`). Nothing else surfaces a forgotten pause (§2).
- **History lag**: `time() - min(cdc_arrival_complete_ts) / 1e6` above a few checkpoint rounds.
- **Consumer lag**: the consumer's own lag above a margin well under `txlog_cdc_retention_seconds`,
  so that it is noticed before history it still needs is purged.
