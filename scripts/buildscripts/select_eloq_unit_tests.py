#!/usr/bin/env python3
"""Select SCons native tests for EloqDoc, with an explicit, auditable skip report.

This filters the SCons unit-test manifest, not the SCons target definitions. Unknown or
missing binaries are retained: a build failure must not silently turn into a test skip.
Only confirmed unsupported engines, runtimes, and legacy fixtures are excluded.
"""

import argparse
import json
from pathlib import Path


EXCLUDED_PREFIXES = {
    "mongo/db/repl/": "MongoDB replication tests are outside EloqDoc validation",
    "mongo/db/storage/mmap_v1/": "MMAPv1 is not an EloqDoc storage engine",
    "mongo/db/storage/wiredtiger/": "WiredTiger is not an EloqDoc storage engine",
    "mongo/embedded/": "embedded runtime is not the EloqDoc server",
}

# These upstream test targets still use interfaces removed by the Eloq fork. Do not
# exclude their whole directories: the other query, pipeline, and catalog tests matter.
OBSOLETE_TARGETS = {
    "mongo/db/catalog/rename_collection_test": "old Collection ownership interface",
    "mongo/db/commands/mr_test": "old Collection ownership interface",
    "mongo/db/pipeline/document_source_test": "change-stream test uses old Collection ownership",
    "mongo/db/sessions_test": "old Session constructor",
    "mongo/db/storage/ephemeral_for_test/storage_ephemeral_for_test_engine_test":
        "KV harness uses removed catalog namespace interface",
    "mongo/db/storage/kv/kv_database_catalog_entry_test":
        "removed catalog namespace interface",
    "mongo/db/storage/mobile/storage_mobile_kv_engine_test":
        "KV harness uses removed catalog namespace interface",
    "mongo/db/views/views_test": "removed catalog namespace interface",
    "mongo/s/query/cluster_find_test": "old pooled query ownership interface",
    "mongo/s/sharding_routing_table_test": "old query/namespace test interface",
}

# These fixtures create MongoDB's replication oplog during setup. EloqDoc does not
# implement that oplog; changing production catalog semantics to run them would be
# incorrect. Outside the excluded repl/ directory, keep exact targets rather than
# excluding all of s/ or catalog/: those directories retain applicable coverage.
UNSUPPORTED_RUNTIME_FIXTURES = dict.fromkeys((
    "mongo/db/catalog/database_test",
    "mongo/db/catalog/drop_database_test",
    "mongo/db/keys_collection_manager_sharding_test",
    "mongo/db/logical_clock_test",
    "mongo/db/logical_time_validator_test",
    "mongo/db/op_observer_impl_test",
    "mongo/db/ops/write_ops_retryability_test",
    "mongo/db/s/balancer_test",
    "mongo/db/s/collection_sharding_runtime_test",
    "mongo/db/s/config_server_op_observer_test",
    "mongo/db/s/session_catalog_migration_destination_test",
    "mongo/db/s/session_catalog_migration_source_test",
    "mongo/db/s/shard_server_test",
    "mongo/db/s/sharding_catalog_manager_test",
    "mongo/db/transaction_history_iterator_test",
    "mongo/s/catalog/replset_dist_lock_manager_test",
), "fixture requires MongoDB's replication oplog")
UNSUPPORTED_RUNTIME_FIXTURES.update({
    "mongo/db/catalog/capped_utils_test":
        "capped collection conversion is explicitly unsupported by EloqDoc",
    "mongo/db/catalog/create_collection_test":
        "fixture uses removed local-catalog renameCollection API",
    "mongo/db/storage/kv/kv_storage_engine_test":
        "fixture uses removed local-catalog ident reconciliation/repair APIs",
    "mongo/s/client/shard_local_test":
        "MongoDB local-shard fixture assumes MongoDB write-unit/lock semantics",
})


def target_name(binary):
    parts = Path(binary).parts
    # The manifest can contain absolute paths or build-directory-relative paths.
    try:
        return "/".join(parts[parts.index("mongo"):])
    except ValueError:
        return binary


def skip_reason(binary):
    name = target_name(binary)
    for prefix, reason in EXCLUDED_PREFIXES.items():
        if name.startswith(prefix):
            return reason
    return OBSOLETE_TARGETS.get(name) or UNSUPPORTED_RUNTIME_FIXTURES.get(name)


def select_tests(binaries):
    selected, skipped = [], []
    for binary in binaries:
        reason = skip_reason(binary)
        if reason:
            skipped.append({"test": binary, "reason": reason})
        else:
            selected.append(binary)
    return selected, skipped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--suite", type=Path, help="also write a resmoke suite (JSON/YAML)")
    args = parser.parse_args()
    binaries = [line.strip() for line in args.manifest.read_text().splitlines() if line.strip()]
    selected, skipped = select_tests(binaries)
    args.output.write_text("".join(binary + "\n" for binary in selected))
    args.report.write_text(json.dumps({"selected": selected, "skipped": skipped}, indent=2) + "\n")
    if args.suite:
        args.suite.write_text(json.dumps({
            "test_kind": "cpp_unit_test",
            "selector": {"root": str(args.output.resolve())},
            "executor": {"config": {}},
        }, indent=2) + "\n")
    print("Selected %d tests; skipped %d (reasons: %s)" %
          (len(selected), len(skipped), args.report))


if __name__ == "__main__":
    main()
