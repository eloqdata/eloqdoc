# EloqDoc CDC: operations runbook

Conditions in a deployment that serves change streams which need an operator, how to recognize
each one, and what to do. The design behind them is in
[cdc-change-stream.md](cdc-change-stream.md); its §8 lists every failure mode, most of which need
no action.

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
and finishes it (see "If the store cannot be fixed soon" below).

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
