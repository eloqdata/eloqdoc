# EloqDoc temporary collections: cleanup after a node dies

**Status: future feature, not implemented.** This document records what temporary collections
EloqDoc can have today, why MongoDB's way of cleaning them up is wrong for EloqDoc, how EloqSQL
cleans up its temporary tables, and the design to adopt. When the implementation lands, turn the
design sections into a description of it and fix this document in the same change.

**Path conventions.** Paths starting with `src/mongo/` are relative to the repository root. EloqSQL
paths (`sql/`, `storage/eloq/`) are relative to the EloqSQL repository.

## 1. Temporary collections in MongoDB

A collection whose options carry `temp: true` is scratch output that MongoDB builds and then renames
into place, or a collection that is dropped at the next restart. MongoDB's temporary collections
are not bound to a session: each operation that creates one drops it when it ends, whether it
succeeds (the rename consumes it) or fails, and a restart drops the ones a process left behind
when it died. The restart sweep is `DatabaseImpl::clearTmpCollections`
(`src/mongo/db/catalog/database_impl.cpp`), which `repairDatabasesAndCheckVersion`
(`src/mongo/db/repair_database_and_check_version.cpp`) calls for every database of a standalone
server: it lists the database's collections and drops each one whose options carry `temp: true`.

## 2. Where EloqDoc has them

| Source | Temporary collection | State in EloqDoc |
|---|---|---|
| `$out` (`src/mongo/db/pipeline/document_source_out.cpp`) | `<db>.tmp.agg_out.<n>`: created with `temp: true`, given the target's indexes, filled with the results, then renamed onto the target | Created and filled, then the rename fails (EloqDoc does not support renameCollection yet), and the stage's destructor drops it. Every `$out` fails today. |
| `create` with `temp: true` | Any name the client chooses | Succeeds; the collection stays until a restart sweep drops it. |
| mapReduce, convertToCapped, cloneCollectionAsCapped | `tmp.mr.*`, the capped copy | Rejected before they create anything (mapReduce and capped collections are unsupported). |
| Initial sync's oplog buffer | `local.temp_oplog_buffer` | Replica sets only; never in EloqDoc. |

Rename is planned, which makes `$out` work: its temporary collection then lives for the length of
one aggregation, as in MongoDB. A temporary collection is left behind only when the node running
the operation (or holding the `temp: true` collection) dies.

## 3. Why MongoDB's restart sweep is wrong for EloqDoc

A mongod owns its catalog, so at its start no operation can be using a temporary collection, and
dropping all of them is safe. EloqDoc's catalog is shared by every node: each node lists the same
tables (`EloqKVEngine::listCollections`, from the data store's table list). The sweep therefore
drops, at any node's start, every temporary collection in the cluster, including the ones that
`$out` operations on other, live nodes are writing; those operations then fail at their rename.
Only a single-node deployment is safe.

The sweep also reads two sources: the names from the data store's table list, and each entry from
the tx service's catalog. A drop that has committed but not yet deleted its catalog row from the
store is listed but has no entry; DDL recovery rolls such a drop forward after the node has
started, while the sweep runs. The sweep now skips such a name (it crashed on the missing entry
before).

Until the design below lands, the sweep keeps MongoDB's behavior: any node's restart drops every
temporary collection in the cluster.

## 4. How EloqSQL does it

EloqSQL stores a session's temporary tables as Eloq tables in the shared catalog and ties each to
the node that created it:

1. **The owner is in the name.** `make_tmp_table_name` (`sql/sql_table.cc`) appends the creating
   node's id to every temporary table's name: `<tmpdir>/#sql-temptable-<pid>-<thread>-<n>-<node id>`.
2. **Hidden from listings.** `SHOW TABLES` lists only names that start with `./`
   (`storage/eloq/ha_eloq.cc`), and a temporary table's name is an absolute tmpdir path.
3. **Dropped at session end.** MariaDB closes a session's temporary tables when the session ends,
   and `ha_eloq::delete_table` runs each drop as an ordinary drop-table schema operation.
4. **Dropped at the owner's next start.** `drop_orphan_tmp_tables` (`storage/eloq/ha_eloq.cc`),
   called during engine initialization, lists every table and drops those whose name carries this
   node's id (`is_tmp_table_of`, `storage/eloq/eloq_catalog_name.h`). Before each drop it reads the
   table's catalog entry and skips a table that no longer exists.

Only the owner cleans up, because only the owner's sessions could have used its temporary tables.
Two gaps remain:

- A node that never comes back, or that rejoins under another id, leaves its temporary tables in
  the catalog forever: nothing reaps them cluster-wide.
- `make_tmp_table_name` writes the node id in hexadecimal (`%x`), but `is_tmp_table_of` parses it
  back in decimal (`strtoul(..., 10)`). Ids 0 to 9 agree; from 10 on they do not: node 10 writes
  `a`, which parses as 0, so node 0's next start drops node 10's live temporary tables and node 10
  never cleans up its own.

## 5. Design for EloqDoc

EloqDoc adopts EloqSQL's rule: **the node that created a temporary collection drops it at its next
start, and no other node touches it.**

1. **Record the owner in the catalog.** Temporary collection names are visible and, for `create`,
   chosen by the client, so the owner cannot be part of the name. When a collection is created with
   `temp: true`, the server adds the creating node's id (the data substrate's node id, the one
   EloqSQL uses) to its options as `tempOwner`. A client cannot set `tempOwner` itself.
2. **Sweep only this node's temporary collections.** At start, `clearTmpCollections` drops a
   collection only if its `tempOwner` is this node, and keeps skipping a name whose entry is gone.
   A temporary collection without an owner (created by a build before this design) is left alone
   and logged, for an operator to drop.
3. **Rename clears both.** When rename promotes a temporary collection to its final name (`$out`'s
   rename, with `stayTemp` false), it removes `temp` and `tempOwner` together, as MongoDB's rename
   removes `temp`.
4. **Orphans of a node that never returns.** When a node is removed from the cluster's
   configuration, the node that applies the change drops the temporary collections owned by the
   removed id. An administrative command can drop them by owner as well. A new node that reuses a
   removed node's id adopts its leftovers and drops them at its first start.

## 6. Interactions to settle during implementation

- **The cluster gate.** While schema changes are paused (`eloqClusterGate`, `docs/cdc-change-stream.md`
  section 6.2), the sweep's drops are refused. The sweep should then retry in the background
  after the gate resumes, rather than wait for the next restart.
- **Change streams.** A temporary collection's create, writes and drop go through the redo log and
  the DDL history like any collection's. Decide whether change streams report them; MongoDB's
  behavior for `tmp.agg_out.*` is the reference to check.
- **Catalog option compatibility.** A build without `tempOwner` must still read a catalog entry
  that has it, and a build with it must read entries that lack it.
- **Visibility.** MongoDB lists temporary collections (with `temp: true` in their options); EloqDoc
  keeps listing them, unlike EloqSQL, whose session-private tables are hidden.

## 7. Tests

- A node killed while it holds a `temp: true` collection: after its restart the collection is gone,
  and another node's temporary collection is untouched.
- Another node's restart does not drop the first node's temporary collection.
- Once rename is supported: a node killed in the middle of `$out` leaves `tmp.agg_out.<n>`, which
  its restart drops; `$out` on a live node survives another node's restart.
- A node removed from the cluster: its temporary collections are dropped.
- An entry that vanishes between the listing and the lookup is skipped (the DDL recovery test's
  drop killed after its commit log covers it: `cmake/tests/ddl_recovery.py`).
