#!/usr/bin/env python3
"""Extract the minimal EloqDoc source closure from MongoDB 4.0 SConscript files.

The build itself is CMake/Ninja-only.  This small configure-time translator keeps
the source manifest tied to MongoDB's fine-grained library declarations while
selecting the normal standalone server, the Eloq storage engine, and their
transitive dependencies.  It never imports or executes SCons.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import os
import platform
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, MutableMapping, Set


Target = Dict[str, Any]

FORBIDDEN_TARGET_PREFIXES = (
    "mongo/mongos",
    "mongo/db/repl/bgsync",
    "mongo/db/repl/initial_syncer",
    "mongo/db/repl/repl_coordinator_impl",
    "mongo/db/repl/serveronly_repl",
    "mongo/db/storage/mobile/storage_mobile",
    "mongo/db/storage/mmap_v1/storage_mmapv1",
    "mongo/db/storage/wiredtiger/",
    "third_party/wiredtiger/",
    "mongo/embedded/embedded",
    "mongo/embedded/mongoed",
    "mongo/db/s/balancer",
    "mongo/db/s/commands_db_s",
    "mongo/db/s/sharding_catalog_manager",
    "mongo/s/commands/",
    "mongo/s/sharding_legacy_api",
)

EXCLUDED_TEST_TARGET_PREFIXES = (
    "mongo/db/repl/",
    "mongo/db/s/",
    "mongo/db/storage/mmap_v1/",
    "mongo/db/storage/mobile/",
    "mongo/db/storage/wiredtiger/",
    "mongo/s/",
    "mongo/shell/",
)

# Distributed-only suites outside the repl/ and sharding directories. Their libraries
# may still supply shared types to the standalone server; that does not make these
# feature-specific tests part of EloqDoc validation. SCons targets remain unchanged.
EXCLUDED_DISTRIBUTED_TEST_TARGETS = {
    "mongo/client/dbclient_rs_test": "replica-set client routing/failover",
    "mongo/client/replica_set_monitor_test": "replica-set topology monitoring",
    "mongo/db/keys_collection_document_test": "cluster-time signing keys",
    "mongo/db/operation_time_tracker_test": "operation times from remote MongoDB servers",
    "mongo/db/time_proof_service_test": "cluster-time signing/verification",
    "mongo/rpc/config_server_metadata_test": "config-server metadata",
    "mongo/rpc/oplog_query_metadata_test": "replication oplog-query metadata",
    "mongo/rpc/repl_set_metadata_test": "replica-set metadata",
}

SOURCE_REPLACEMENTS = {
    "mongo/db/ftdc/ftdc_system_stats_${TARGET_OS}.cpp": "mongo/db/ftdc/ftdc_system_stats_linux.cpp",
    "mongo/db/storage/mmap_v1/mmap_${TARGET_OS_FAMILY}.cpp": "mongo/db/storage/mmap_v1/mmap_posix.cpp",
    "mongo/crypto/sha_block_${MONGO_CRYPTO}.cpp": "mongo/crypto/sha_block_tom.cpp",
    "mongo/db/storage/storage_engine_lock_file_${TARGET_OS_FAMILY}.cpp":
        "mongo/db/storage/storage_engine_lock_file_posix.cpp",
    "mongo/platform/shared_library_${TARGET_OS_FAMILY}.cpp":
        "mongo/platform/shared_library_posix.cpp",
    "mongo/platform/stack_locator_${TARGET_OS}.cpp": "mongo/platform/stack_locator_linux.cpp",
    "mongo/util/processinfo_${TARGET_OS}.cpp": "mongo/util/processinfo_linux.cpp",
    "mongo/util/stacktrace_${TARGET_OS_FAMILY}.cpp": "mongo/util/stacktrace_posix.cpp",
}

GENERATED_WITHOUT_SOURCE_FILE = {
    "mongo/base/error_codes.cpp",
    "mongo/db/auth/action_type.cpp",
    "mongo/db/fts/stop_words_list.cpp",
    "mongo/db/fts/unicode/codepoints_casefold.cpp",
    "mongo/db/fts/unicode/codepoints_delimiter_list.cpp",
    "mongo/db/fts/unicode/codepoints_diacritic_list.cpp",
    "mongo/shell/mongo.cpp",
    "mongo/scripting/mozjs/mongohelpers_js.cpp",
    "mongo/util/icu_init.cpp",
}


def _flatten(value: Any) -> List[str]:
    if isinstance(value, list):
        result: List[str] = []
        for item in value:
            result.extend(_flatten(item))
        return result
    return [value] if isinstance(value, str) else []


class StaticEvaluator:
    """Evaluate the literal subset used for SConscript source/dependency lists."""

    def __init__(self, variables: MutableMapping[str, Any]):
        self.variables = variables

    def eval(self, node: ast.AST | None, locals_: Dict[str, Any] | None = None) -> Any:
        if node is None:
            return []
        local_values = locals_ or {}
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, (ast.List, ast.Tuple)):
            return [self.eval(item, local_values) for item in node.elts]
        if isinstance(node, ast.Name):
            return local_values.get(node.id, self.variables.get(node.id, []))
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not bool(self.eval(node.operand, local_values))
        if isinstance(node, ast.BinOp):
            left = self.eval(node.left, local_values)
            right = self.eval(node.right, local_values)
            if isinstance(node.op, ast.Add):
                if isinstance(left, list) or isinstance(right, list):
                    return _flatten(left) + _flatten(right)
                return left + right
            if isinstance(node.op, ast.Mod):
                try:
                    return left % right
                except (TypeError, ValueError):
                    return []
        if isinstance(node, ast.IfExp):
            branch = node.body if self.eval(node.test, local_values) else node.orelse
            return self.eval(branch, local_values)
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            left = self.eval(node.left, local_values)
            right = self.eval(node.comparators[0], local_values)
            if isinstance(node.ops[0], ast.Eq):
                return left == right
            if isinstance(node.ops[0], ast.NotEq):
                return left != right
            if isinstance(node.ops[0], ast.In):
                return left in right
            if isinstance(node.ops[0], ast.NotIn):
                return left not in right
        if isinstance(node, ast.BoolOp):
            values = [bool(self.eval(item, local_values)) for item in node.values]
            return all(values) if isinstance(node.op, ast.And) else any(values)
        if isinstance(node, ast.Subscript):
            value = self.eval(node.value, local_values)
            index = self.eval(node.slice, local_values)
            try:
                return value[index]
            except (IndexError, KeyError, TypeError):
                return []
        if isinstance(node, ast.ListComp) and len(node.generators) == 1:
            generator = node.generators[0]
            if not isinstance(generator.target, ast.Name):
                return []
            result = []
            for item in _flatten(self.eval(generator.iter, local_values)):
                nested = dict(local_values)
                nested[generator.target.id] = item
                result.append(self.eval(node.elt, nested))
            return result
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "use_system_version_of_library":
                # Match SCons' pinned dependencies, including ICU: its version is stored in
                # index collation specifications, so using the host's ICU changes compatibility.
                return False
            if isinstance(node.func, ast.Attribute):
                method = node.func.attr
                if method == "TargetOSIs":
                    return any(self.eval(arg, local_values) in ("linux", "posix") for arg in node.args)
                if method == "ToolchainIs":
                    return any(self.eval(arg, local_values) in ("GCC", "gcc") for arg in node.args)
                if method == "Idlc":
                    source = Path(str(self.eval(node.args[0], local_values)))
                    generated_base = source.with_suffix("").as_posix() + "_gen"
                    # SCons returns [generated_cpp, generated_header]; Mongo's declarations select
                    # element zero. Preserve that shape so the surrounding AST subscript works.
                    return [generated_base + ".cpp", generated_base + ".h"]
                if method == "WindowsResourceFile":
                    return []
        return []


def _active_statements(
    statements: Iterable[ast.stmt], evaluator: StaticEvaluator
) -> Iterable[ast.stmt]:
    """Yield statements from branches which are active in the Linux build profile."""
    for statement in statements:
        if isinstance(statement, ast.If):
            branch = statement.body if evaluator.eval(statement.test) else statement.orelse
            yield from _active_statements(branch, evaluator)
        else:
            yield statement


def _collect_variables(tree: ast.Module, target_arch: str) -> Dict[str, Any]:
    variables: Dict[str, Any] = {
        "debugBuild": False,
        "env": {
            "MONGO_ALLOCATOR": "system",
            "MONGO_BUILD_SASL_CLIENT": True,
            "MONGO_CRYPTO": "tom",
            "MONGO_HAVE_LIBMONGOC": False,
            "MONGO_MODULES": ["eloq"],
            "MODULE_BANNERS": [],
            "TARGET_ARCH": target_arch,
            "TARGET_OS": "linux",
        },
        "free_monitoring": "off",
        "mmapv1": False,
        "serverJs": True,
        "usemozjs": True,
        "wiredtiger": False,
    }
    evaluator = StaticEvaluator(variables)
    # All source variables in this MongoDB branch are literal lists declared before use. Repeating
    # the walk resolves lists which refer to an earlier list. Walking only the selected branch is
    # important: several scripts assign Linux and Windows dependencies to the same variable.
    for _ in range(3):
        for statement in _active_statements(tree.body, evaluator):
            for node in ast.walk(statement):
                if isinstance(node, ast.If):
                    # Nested conditionals are handled by _active_statements, not ast.walk.
                    continue
                if not isinstance(node, ast.Assign) or len(node.targets) != 1:
                    continue
                target = node.targets[0]
                if isinstance(target, ast.Name):
                    value = evaluator.eval(node.value)
                    if value != []:
                        variables[target.id] = value
    return variables


def _canonical_dependency(base: str, dependency: str) -> str:
    if dependency.startswith("$BUILD_DIR/"):
        return dependency[len("$BUILD_DIR/") :]
    if dependency.startswith("#/"):
        return dependency[2:]
    return os.path.normpath((PurePosixPath(base) / dependency).as_posix())


def _canonical_source(base: str, source: str) -> str:
    if source.startswith("$BUILD_DIR/"):
        return source[len("$BUILD_DIR/") :]
    return (PurePosixPath(base) / source).as_posix()


def load_targets(source_root: Path, target_arch: str = platform.machine()) -> Dict[str, Target]:
    targets: Dict[str, Target] = {}
    for script in source_root.rglob("SConscript"):
        tree = ast.parse(script.read_text(encoding="utf-8"), filename=str(script))
        evaluator = StaticEvaluator(_collect_variables(tree, target_arch))
        base = script.parent.relative_to(source_root).as_posix()
        for statement in _active_statements(tree.body, evaluator):
            for node in ast.walk(statement):
                if isinstance(node, ast.If):
                    # Nested conditionals are handled by _active_statements, not ast.walk.
                    continue
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    continue
                kind = node.func.attr
                if kind not in ("Library", "Program", "CppUnitTest", "CppIntegrationTest"):
                    continue
                keywords = {item.arg: item.value for item in node.keywords}
                target_node = keywords.get("target", node.args[0] if node.args else None)
                source_node = keywords.get("source", node.args[1] if len(node.args) > 1 else None)
                names = _flatten(evaluator.eval(target_node))
                sources = _flatten(evaluator.eval(source_node))
                dependencies: List[str] = []
                for field in ("LIBDEPS", "LIBDEPS_PRIVATE", "LIBDEPS_INTERFACE"):
                    dependencies.extend(_flatten(evaluator.eval(keywords.get(field))))
                if kind == "CppUnitTest":
                    dependencies.append("$BUILD_DIR/mongo/unittest/unittest_main")
                elif kind == "CppIntegrationTest":
                    dependencies.append("$BUILD_DIR/mongo/unittest/integration_test_main")
                for name in names:
                    canonical_name = (PurePosixPath(base) / name).as_posix()
                    record = targets.setdefault(
                        canonical_name, {"sources": [], "dependencies": [], "kind": kind}
                    )
                    record["sources"].extend(_canonical_source(base, item) for item in sources)
                    record["dependencies"].extend(
                        _canonical_dependency(base, item) for item in dependencies
                    )
    return targets


def select_minimal_graph(targets: Dict[str, Target], source_root: Path | None = None,
                         target_arch: str = platform.machine()) -> Set[str]:
    # Keep db.cpp, ServiceEntryPointMongod, authentication, maintenance and JavaScript.
    # Only the replication/sharding boundaries and alternative engines are removed.
    distributed = {
        "mongo/db/repl/bgsync", "mongo/db/repl/oplog_buffer_blocking_queue",
        "mongo/db/repl/oplog_buffer_collection", "mongo/db/repl/oplog_buffer_proxy",
        "mongo/db/repl/repl_coordinator_impl", "mongo/db/repl/rs_rollback",
        "mongo/db/repl/rslog", "mongo/db/repl/serveronly_repl",
        "mongo/db/repl/oplog_application", "mongo/db/repl/topology_coordinator",
        "mongo/db/repl/repl_set_commands", "mongo/db/s/commands_db_s",
        "mongo/db/s/balancer", "mongo/db/s/op_observer_sharding_impl",
        "mongo/db/s/sharding_catalog_manager", "mongo/s/sharding_legacy_api",
        "mongo/s/catalog/sharding_catalog_client_impl",
        "mongo/s/commands/shared_cluster_commands",
        "mongo/db/storage/mmap_v1/storage_mmapv1",
        "mongo/db/storage/devnull/storage_devnull",
        "mongo/db/storage/ephemeral_for_test/storage_ephemeral_for_test",
    }
    for name in ("mongo/mongodmain", "mongo/db/serveronly", "mongo/db/commands/mongod",
                 "mongo/db/commands/mongod_fsync", "mongo/db/commands/servers"):
        targets[name]["dependencies"] = [
            dep for dep in targets[name]["dependencies"] if dep not in distributed
        ]
    targets["mongo/mongodmain"]["dependencies"].extend([
        "mongo/db/modules/eloq/storage_eloq", "mongo/db/repl/repl_coordinator_standalone",
        "mongo/db/op_observer_impl", "mongo/db/repl/replication_info",
    ])
    # This target also registers the standalone isMaster handshake and diagnostics. It does
    # not use OplogReader; the old dependency must not retain a replica-set oplog client.
    targets["mongo/db/repl/replication_info"]["dependencies"] = [
        dep for dep in targets["mongo/db/repl/replication_info"]["dependencies"]
        if dep != "mongo/db/repl/oplogreader"
    ]
    # The interface supplies shared write-concern constants without the catalog client runtime.
    targets["mongo/db/keys_collection_client_direct"]["dependencies"].append(
        "mongo/s/catalog/sharding_catalog_client")
    targets["mongo/db/repl/repl_coordinator_standalone"] = {
        "sources": ["mongo/db/repl/replication_coordinator_standalone.cpp"],
        "dependencies": ["mongo/db/repl/repl_coordinator_interface",
                         "mongo/db/repl/repl_settings", "mongo/db/global_settings"],
        "kind": "Library",
    }
    targets["mongo/db/standalone_runtime_test"] = {
        "sources": ["mongo/db/standalone_runtime_test.cpp"],
        "dependencies": ["mongo/db/repl/repl_coordinator_standalone",
                         "mongo/unittest/unittest_main"],
        "kind": "CppUnitTest",
    }
    for name in ("mongo/mongodmain", "mongo/db/serveronly"):
        targets[name]["dependencies"] = [
            "mongo/db/s/sharding_runtime_d_embedded"
            if dep == "mongo/db/s/sharding_runtime_d" else dep
            for dep in targets[name]["dependencies"]
        ]
    # Replica-set-only commands must not register on this standalone server.
    targets["mongo/db/commands/mongod"]["sources"] = [
        src for src in targets["mongo/db/commands/mongod"]["sources"]
        if Path(src).name not in {"resize_oplog.cpp", "oplog_note.cpp", "dbcheck.cpp"}
    ]
    targets["mongo/db/commands/mongod"]["dependencies"] = [
        dep for dep in targets["mongo/db/commands/mongod"]["dependencies"]
        if dep != "mongo/db/repl/dbcheck"
    ]
    # Substitute the small factory at the link boundary instead of branching shared code.
    targets["mongo/db/logical_session_cache_factory_mongod"]["sources"] = [
        "mongo/db/logical_session_cache_factory_standalone.cpp",
    ]
    targets["mongo/db/logical_session_cache_factory_mongod"]["dependencies"] = [
        "mongo/db/logical_session_cache_impl", "mongo/db/service_liaison_mongod",
        "mongo/db/sessions_collection_standalone",
    ]

    # The common request path needs CollectionShardingState, not the shard-server runtime.
    # Reuse MongoDB's unsharded metadata implementation without its embedded application.
    entry_point = targets["mongo/db/service_entry_point_common"]
    entry_point["dependencies"] = [
        (
            "mongo/db/s/sharding_runtime_d_embedded"
            if item == "mongo/db/s/sharding_runtime_d"
            else item
        )
        for item in entry_point["dependencies"]
    ]

    # Keep local Pipeline functionality, but omit the separately compiled sharding entry points,
    # distributed planner, and remote cursor implementation. No rejection stubs are needed:
    # pipeline.cpp no longer references the distributed planner.
    pipeline = targets["mongo/db/pipeline/pipeline"]
    pipeline["sources"] = [
        item for item in pipeline["sources"]
        if item
        not in {
            "mongo/db/pipeline/pipeline_sharded.cpp",
            "mongo/db/pipeline/cluster_aggregation_planner.cpp",
            "mongo/db/pipeline/document_source_internal_split_pipeline.cpp",
            "mongo/db/pipeline/document_source_merge_cursors.cpp",
        }
    ]
    # pipeline.h exposes AsyncResultsMergerParams even in the local-only planner. Retain the IDL
    # value type (but not the async-results-merger runtime) so clean builds generate its header and
    # provide any serialization symbols used by the shared pipeline API.
    pipeline["sources"].append("mongo/s/query/async_results_merger_params_gen.cpp")
    pipeline["dependencies"] = [
        item
        for item in pipeline["dependencies"]
        if item != "mongo/s/query/async_results_merger"
    ]

    # dbcommands.cpp consumes the generated profile request type. Full mongod reaches it through
    # the mongod command bundle, while the embedded command bundle omitted that edge.
    targets["mongo/db/commands/standalone"]["dependencies"].append(
        "mongo/db/commands/profile_common"
    )

    # catalog_impl has no Balancer symbol references. This legacy LIBDEPS edge exists to retain
    # static initializers in full mongod; the single-node executable must not retain them.
    catalog_impl = targets["mongo/db/catalog/catalog_impl"]
    catalog_impl["dependencies"] = [
        item for item in catalog_impl["dependencies"] if item != "mongo/db/s/balancer"
    ]

    # Standalone startup does not initialize a sharded logical-time key client.
    keys_manager = targets["mongo/db/keys_collection_manager"]
    keys_manager["dependencies"] = [
        item
        for item in keys_manager["dependencies"]
        if item != "mongo/db/keys_collection_client_sharded"
    ]

    # The SpiderMonkey SConscript appends generated unified translation units with Glob().
    # Keep that explicit platform source set without executing SCons.
    mozjs = targets["third_party/mozjs-45/mozjs"]
    if target_arch == "x86_64":
        mozjs["sources"].append(
            "third_party/mozjs-45/extract/js/src/jit/x86-shared/Disassembler-x86-shared.cpp")
    mozjs_root = source_root or Path(__file__).resolve().parents[1] / "src"
    mozjs_platform = mozjs_root / "third_party/mozjs-45/platform" / target_arch / "linux/build"
    if not mozjs_platform.is_dir():
        raise RuntimeError(f"No pinned SpiderMonkey configuration for {target_arch}/linux")
    mozjs["sources"].extend(
        path.relative_to(mozjs_root).as_posix()
        for path in sorted(mozjs_platform.glob("*.cpp"))
    )

    # TLS is excluded from this single-node profile, so use MongoDB's small vendored TomCrypt
    # implementation for SCRAM hashes rather than compiling an SSL provider.
    targets["mongo/crypto/sha_block_${MONGO_CRYPTO}"] = {
        "sources": ["mongo/crypto/sha_block_tom.cpp"],
        "dependencies": [
            "mongo/base",
            "mongo/crypto/sha1_block",
            "mongo/crypto/sha256_block",
            "third_party/shim_tomcrypt",
        ],
        "kind": "Library",
    }

    # SCons carries interface dependencies on cloned environments. Recreate the subset used by
    # this build explicitly; these are ordinary vendored libraries, not SCons runtime behavior.
    interface_dependencies = {
        "third_party/shim_asio": ["third_party/asio-master/asio"],
        "third_party/shim_boost": [
            "third_party/boost-1.60.0/boost_program_options",
            "third_party/boost-1.60.0/boost_filesystem",
            "third_party/boost-1.60.0/boost_system",
            "third_party/boost-1.60.0/boost_iostreams",
        ],
        "third_party/shim_intel_decimal128": [
            "third_party/IntelRDFPMathLib20U1/intel_decimal128"
        ],
        "third_party/shim_icu": ["third_party/icu4c-57.1/source/icu_i18n"],
        "third_party/shim_pcrecpp": ["third_party/pcre-8.41/pcrecpp"],
        "third_party/shim_mozjs": ["third_party/mozjs-45/mozjs"],
        "third_party/shim_snappy": ["third_party/snappy-1.1.3/snappy"],
        "third_party/shim_stemmer": ["third_party/libstemmer_c/stemmer"],
        "third_party/shim_timelib": ["third_party/timelib-2018.01alpha1/timelib"],
        "third_party/shim_tomcrypt": ["third_party/tomcrypt-1.18.1/tomcrypt"],
        "third_party/shim_yaml": ["third_party/yaml-cpp-0.5.3/yaml"],
        "third_party/shim_zlib": ["third_party/zlib-1.2.11/zlib"],
    }
    for target, dependencies in interface_dependencies.items():
        targets[target]["dependencies"].extend(dependencies)

    selected: Set[str] = set()
    pending = ["mongo/eloqdoc"]
    while pending:
        name = pending.pop()
        if name in selected:
            continue
        if name not in targets:
            raise RuntimeError(f"Minimal EloqDoc graph references unknown target: {name}")
        selected.add(name)
        pending.extend(targets[name]["dependencies"])

    bad = sorted(
        name
        for name in selected
        if any(name.startswith(item) for item in FORBIDDEN_TARGET_PREFIXES)
    )
    if bad:
        raise RuntimeError("Forbidden distributed/alternate-engine targets selected: " + ", ".join(bad))
    return selected


def _dependency_closure(targets: Dict[str, Target], roots: Iterable[str]) -> Set[str] | None:
    selected: Set[str] = set()
    pending = list(roots)
    while pending:
        name = pending.pop()
        if name in selected:
            continue
        if name not in targets:
            return None
        selected.add(name)
        pending.extend(targets[name]["dependencies"])
    return selected


def _normalized_sources(sources: Iterable[str]) -> Set[str]:
    normalized = {SOURCE_REPLACEMENTS.get(source, source) for source in sources}
    normalized.difference_update(
        {
            "mongo/util/exception_filter_win32.cpp",
            "mongo/util/signal_win32.cpp",
            "mongo/util/winutil.cpp",
            "third_party/shim_intel_decimal128.cppshim_intel_decimal128.cpp",
        }
    )
    if "third_party/shim_intel_decimal128.cppshim_intel_decimal128.cpp" in sources:
        normalized.add("third_party/shim_intel_decimal128.cpp")
    return {source for source in normalized if source.endswith((".c", ".cc", ".cpp"))}


def select_minimal_tests(
    targets: Dict[str, Target], production_targets: Set[str]
) -> tuple[List[Dict[str, Any]], Set[str]]:
    """Select tests for the retained single-node modules and their shared test helpers."""
    # Production drops alternative engine registrations, but the MongoD test fixture
    # explicitly starts ephemeralForTest. Restore that edge only in the test graph.
    targets = dict(targets)
    fixture_name = "mongo/db/service_context_d_test_fixture"
    targets[fixture_name] = {
        **targets[fixture_name],
        "dependencies": targets[fixture_name]["dependencies"] + [
            "mongo/db/storage/ephemeral_for_test/storage_ephemeral_for_test"],
    }
    production_sources = _normalized_sources(
        source for name in production_targets for source in targets[name]["sources"]
    )
    production_directories = {
        PurePosixPath(source).parent.as_posix() for source in production_sources
    }
    tests: List[Dict[str, Any]] = []
    support_targets: Set[str] = set()
    # String-named initialization edges cannot pull an otherwise unreferenced object out of a
    # static archive. Retain these providers directly, but only for their original test closure.
    initializer_sources = {
        # These providers run through named initializers, not references from the test main.
        # Retain them only when their original SCons dependency closure includes them.
        "mongo/util/options_parser/options_parser_init.cpp",
        "mongo/util/version_impl.cpp",
        "mongo/util/icu_init.cpp",
        "mongo/db/auth/authorization_manager_global.cpp",
        # The global constructor calls these factories through MONGO_REGISTER_SHIM, not
        # through a linker reference to the implementation classes. Retain both providers
        # for query/service-context tests as well as tests that use the classes directly.
        "mongo/db/auth/authorization_manager_impl.cpp",
        "mongo/db/auth/authorization_session_impl.cpp",
        "mongo/db/catalog/database_holder_impl.cpp",
        "mongo/db/catalog/database_impl.cpp",
        "mongo/db/catalog/collection_impl.cpp",
        "mongo/db/catalog/collection_info_cache_impl.cpp",
        "mongo/db/catalog/index_catalog_entry_impl.cpp",
        "mongo/db/catalog/index_catalog_impl.cpp",
        "mongo/db/catalog/index_create_impl.cpp",
    }
    # Aggregation factories are looked up by stage/accumulator name, not by a C++ symbol.
    # As with SCons' library retention, keep their implementation objects when the original
    # test closure includes them; otherwise authorization/parser tests silently lose stages.
    initializer_sources.update(
        source for source in production_sources
        if source.startswith(("mongo/db/pipeline/document_source_",
                              "mongo/db/pipeline/accumulator_",
                              "mongo/db/pipeline/granularity_rounder_"))
    )

    for name, target in sorted(targets.items()):
        if target["kind"] not in ("CppUnitTest", "CppIntegrationTest"):
            continue
        if name in EXCLUDED_DISTRIBUTED_TEST_TARGETS:
            continue
        if any(name.startswith(prefix) for prefix in EXCLUDED_TEST_TARGET_PREFIXES):
            continue

        main_target = (
            "mongo/unittest/unittest_main"
            if target["kind"] == "CppUnitTest"
            else "mongo/unittest/integration_test_main"
        )
        explicit_dependencies = [
            dependency for dependency in target["dependencies"] if dependency != main_target
        ]
        dependency_closure = _dependency_closure(targets, target["dependencies"])
        related_closure = _dependency_closure(targets, explicit_dependencies) or set()
        if dependency_closure is None:
            continue
        if any(
            dependency.startswith(prefix)
            for dependency in dependency_closure
            for prefix in FORBIDDEN_TARGET_PREFIXES
        ):
            continue
        # A new third-party dependency would require feature-specific flags and headers outside the
        # server profile. Keep tests whose vendor dependencies are already part of the server.
        if any(
            dependency.startswith("third_party/") and dependency not in production_targets
            for dependency in dependency_closure
        ):
            continue

        test_sources = _normalized_sources(target["sources"])
        source_directories = {
            PurePosixPath(source).parent.as_posix() for source in test_sources
        }
        related = bool(related_closure & production_targets) or bool(
            source_directories & production_directories
        )
        if not related:
            continue
        if any(
            source.startswith(prefix)
            for source in test_sources
            for prefix in EXCLUDED_TEST_TARGET_PREFIXES
        ):
            continue

        test_support = dependency_closure - production_targets - {main_target}
        if any(targets[item]["kind"] != "Library" for item in test_support):
            continue
        per_test_support_sources = _normalized_sources(
            source for item in test_support for source in targets[item]["sources"]
        ) - production_sources
        per_test_support_sources.update(
            source for item in dependency_closure for source in targets[item]["sources"]
            if source in initializer_sources
        )
        if name == "mongo/db/pipeline/document_source_facet_test":
            # The facet parser tests use this internal stage to express conflicting
            # host requirements. It is a test fixture, not a server registration.
            per_test_support_sources.add(
                "mongo/db/pipeline/document_source_internal_split_pipeline.cpp")
        if any(
            source.startswith(prefix)
            for source in per_test_support_sources
            for prefix in EXCLUDED_TEST_TARGET_PREFIXES
        ):
            continue
        support_targets.update(test_support)
        tests.append(
            {
                "id": hashlib.sha1(name.encode("utf-8")).hexdigest()[:12],
                "kind": "unit" if target["kind"] == "CppUnitTest" else "integration",
                "name": name,
                "requires_eloq": name.startswith("mongo/db/modules/eloq/")
                or any(item.startswith("mongo/db/modules/eloq/") for item in dependency_closure),
                "sources": sorted(test_sources),
                "support_sources": sorted(per_test_support_sources),
            }
        )

    support_sources = _normalized_sources(
        source for name in support_targets for source in targets[name]["sources"]
    ) - production_sources
    support_sources.update(source for test in tests for source in test["support_sources"])
    return tests, support_sources


def _cmake_list(name: str, values: Iterable[str]) -> str:
    body = "\n".join(f'    "{value}"' for value in sorted(set(values)))
    return f"set({name}\n{body}\n)\n"


def _cmake_scalar(name: str, value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'set({name} "{escaped}")\n'


def generate(source_root: Path, target_arch: str = platform.machine()) -> str:
    targets = load_targets(source_root, target_arch)
    selected = select_minimal_graph(targets, source_root, target_arch)
    sources = _normalized_sources(
        source
        for name in selected
        for source in targets[name]["sources"]
    )
    tests, test_support_sources = select_minimal_tests(targets, selected)
    all_test_sources = test_support_sources | {
        source for test in tests for source in test["sources"]
    }

    missing = sorted(
        source
        for source in sources
        if "_gen.cpp" not in source
        and source
        not in GENERATED_WITHOUT_SOURCE_FILE
        and not (source_root / source).is_file()
    )
    if missing:
        raise RuntimeError("Selected source files do not exist: " + ", ".join(missing))

    missing_test_sources = sorted(
        source
        for source in all_test_sources
        if "_gen.cpp" not in source
        and source not in GENERATED_WITHOUT_SOURCE_FILE
        and not (source_root / source).is_file()
    )
    if missing_test_sources:
        raise RuntimeError(
            "Selected test source files do not exist: " + ", ".join(missing_test_sources)
        )

    mongo_sources = [source for source in sources if source.startswith("mongo/")]
    third_party_sources = [source for source in sources if source.startswith("third_party/")]
    manifest = (
        "# Generated by cmake/generate_eloqdoc_sources.py; do not edit.\n"
        + _cmake_list("ELOQDOC_MONGO_TARGETS", selected)
        + _cmake_list("ELOQDOC_MONGO_SOURCES", mongo_sources)
        + _cmake_list("ELOQDOC_THIRD_PARTY_SOURCES", third_party_sources)
        + _cmake_list("ELOQDOC_MONGO_TEST_SUPPORT_SOURCES", test_support_sources)
        + _cmake_list("ELOQDOC_MONGO_TEST_IDS", (test["id"] for test in tests))
    )
    for test in tests:
        prefix = f'ELOQDOC_MONGO_TEST_{test["id"]}'
        manifest += _cmake_scalar(prefix + "_NAME", test["name"])
        manifest += _cmake_scalar(prefix + "_KIND", test["kind"])
        manifest += _cmake_scalar(
            prefix + "_REQUIRES_ELOQ", "TRUE" if test["requires_eloq"] else "FALSE"
        )
        manifest += _cmake_list(prefix + "_SOURCES", test["sources"])
        manifest += _cmake_list(prefix + "_SUPPORT_SOURCES", test["support_sources"])
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-arch", default=platform.machine())
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    manifest = generate(args.source_root.resolve(), args.target_arch)
    if not args.output.exists() or args.output.read_text(encoding="utf-8") != manifest:
        args.output.write_text(manifest, encoding="utf-8")


if __name__ == "__main__":
    main()
