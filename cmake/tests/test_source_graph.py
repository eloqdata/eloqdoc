#!/usr/bin/env python3
"""Regression checks for the standalone server's feature and dependency boundaries."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from generate_eloqdoc_sources import (
    EXCLUDED_DISTRIBUTED_TEST_TARGETS, load_targets, select_minimal_graph, select_minimal_tests)


class StandaloneGraphTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[2]
        cls.targets = load_targets(cls.root / "src")
        cls.full_pipeline_sources = set(cls.targets["mongo/db/pipeline/pipeline"]["sources"])
        cls.selected = select_minimal_graph(cls.targets)
        cls.sources = {
            source for name in cls.selected for source in cls.targets[name]["sources"]
        }

    def test_normal_server_features_are_retained(self):
        required = {
            "mongo/db/dbmain.cpp", "mongo/db/db.cpp",
            "mongo/db/service_entry_point_mongod.cpp",
            "mongo/db/op_observer_impl.cpp",
            "mongo/db/repl/replication_info.cpp",
            "mongo/db/auth/authorization_manager_impl.cpp",
            "mongo/db/auth/authorization_session_impl.cpp",
            "mongo/db/auth/sasl_commands.cpp", "mongo/db/commands/user_management_commands.cpp",
            "mongo/db/ttl.cpp", "mongo/db/clientcursor.cpp",
            "mongo/db/logical_session_cache_factory_standalone.cpp",
            "mongo/db/periodic_runner_job_abort_expired_transactions.cpp",
            "mongo/scripting/mozjs/engine.cpp", "mongo/db/commands/mr.cpp",
            "mongo/db/commands/shutdown_d.cpp", "mongo/db/commands/fsync.cpp",
            "mongo/db/modules/eloq/src/eloq_init.cpp",
        }
        self.assertFalse(required - self.sources, sorted(required - self.sources))

    def test_no_embedded_application_or_replica_set_implementation(self):
        for source in self.sources:
            self.assertFalse(source.startswith("mongo/embedded/"), source)
            self.assertNotIn("replication_coordinator_impl", source)
            self.assertNotIn("initial_syncer", source)
            self.assertNotIn("/bgsync", source)
        self.assertNotIn("mongo/scripting/scripting_none.cpp", self.sources)
        self.assertNotIn("mongo/s/query/async_results_merger.cpp", self.sources)
        self.assertNotIn("mongo/db/pipeline/document_source_merge_cursors.cpp", self.sources)
        self.assertNotIn("mongo/client/parallel.cpp", self.sources)
        self.assertNotIn("mongo/s/client/shard_connection.cpp", self.sources)
        self.assertNotIn("mongo/db/logical_session_cache_factory_mongod.cpp", self.sources)
        self.assertNotIn("mongo/db/sessions_collection_sharded.cpp", self.sources)
        self.assertNotIn("mongo/db/sessions_collection_rs.cpp", self.sources)

    def test_collations_use_scons_pinned_icu_and_data(self):
        self.assertTrue({
            "third_party/icu4c-57.1/source/icu_common",
            "third_party/icu4c-57.1/source/icu_i18n",
            "third_party/icu4c-57.1/source/icu_data",
        } <= self.selected)
        self.assertIn("mongo/util/icu_init.cpp", self.sources)
        self.assertNotIn("mongo/util/icu_init_stub.cpp", self.sources)

    def test_integration_tests_retain_startup_initializers(self):
        tests, _ = select_minimal_tests(self.targets, self.selected)
        integration = [test for test in tests if test["kind"] == "integration"]
        self.assertEqual(7, len(integration))
        for test in integration:
            with self.subTest(name=test["name"]):
                self.assertIn("mongo/util/options_parser/options_parser_init.cpp",
                              test["support_sources"])
                self.assertIn("mongo/util/version_impl.cpp", test["support_sources"])

    def test_icu_tests_retain_embedded_data_initializer(self):
        tests, _ = select_minimal_tests(self.targets, self.selected)
        by_name = {test["name"]: test for test in tests}
        for name in ("mongo/util/icu_test", "mongo/db/query/collation/collator_factory_icu_test",
                     "mongo/db/query/collation/collator_interface_icu_test"):
            with self.subTest(name=name):
                self.assertIn("mongo/util/icu_init.cpp", by_name[name]["support_sources"])

    def test_sharded_pipeline_is_only_in_full_build(self):
        common = "mongo/db/pipeline/pipeline.cpp"
        sharded = {"mongo/db/pipeline/pipeline_sharded.cpp",
                   "mongo/db/pipeline/cluster_aggregation_planner.cpp"}
        self.assertIn(common, self.full_pipeline_sources)
        self.assertTrue(sharded <= self.full_pipeline_sources)
        self.assertIn(common, self.sources)
        self.assertFalse(sharded & self.sources)

        tests, support = select_minimal_tests(self.targets, self.selected)
        standalone_sources = self.sources | support | {
            source for test in tests for source in test["sources"]}
        self.assertFalse(sharded & standalone_sources)
        self.assertNotIn("mongo/db/pipeline/cluster_aggregation_planner_standalone.cpp",
                         standalone_sources)

    def test_tests_cover_server_but_not_data_substrate(self):
        tests, support = select_minimal_tests(self.targets, self.selected)
        required = {
            "mongo/db/standalone_runtime_test",
            "mongo/db/query/get_executor_test",
            "mongo/db/commands/plan_cache_commands_test",
            "mongo/db/commands/index_filter_commands_test",
            "mongo/db/storage/ephemeral_for_test/storage_ephemeral_for_test_record_store_test",
        }
        self.assertFalse(required - {test["name"] for test in tests})
        for source in support | {src for test in tests for src in test["sources"]}:
            self.assertNotIn("/data_substrate/", source)

    def test_replication_suites_are_excluded(self):
        tests, _ = select_minimal_tests(self.targets, self.selected)
        for test in tests:
            self.assertFalse(test["name"].startswith("mongo/db/repl/"), test["name"])

    def test_distributed_suites_are_excluded_without_dropping_shared_coverage(self):
        tests, _ = select_minimal_tests(self.targets, self.selected)
        names = {test["name"] for test in tests}
        excluded = set(EXCLUDED_DISTRIBUTED_TEST_TARGETS)
        self.assertTrue(excluded <= self.targets.keys())
        self.assertFalse(excluded & names)
        for name in names:
            self.assertFalse(name.startswith(("mongo/db/repl/", "mongo/db/s/", "mongo/s/")), name)
        # Keep local request lifecycle, wire-format, metadata, and catalog coverage.
        self.assertTrue({
            "mongo/db/operation_context_test",
            "mongo/db/standalone_runtime_test",
            "mongo/db/logical_time_test",
            "mongo/db/logical_session_cache_test",
            "mongo/db/catalog/collection_options_test",
            "mongo/db/catalog/uuid_catalog_test",
            "mongo/rpc/client_metadata_test",
            "mongo/rpc/rpc_metadata_test",
            "mongo/client/read_preference_test",
        } <= names)

    def test_auth_tests_retain_name_registered_implementations(self):
        tests, _ = select_minimal_tests(self.targets, self.selected)
        auth = next(test for test in tests
                    if test["name"] == "mongo/db/auth/authorization_session_test")
        self.assertIn("mongo/db/auth/authorization_manager_global.cpp", auth["support_sources"])
        self.assertIn("mongo/db/pipeline/document_source_lookup.cpp", auth["support_sources"])
        self.assertIn("mongo/db/pipeline/document_source_facet.cpp", auth["support_sources"])

    def test_query_tests_retain_authorization_factories(self):
        tests, _ = select_minimal_tests(self.targets, self.selected)
        required = {
            "mongo/db/auth/authorization_manager_global.cpp",
            "mongo/db/auth/authorization_manager_impl.cpp",
            "mongo/db/auth/authorization_session_impl.cpp",
        }
        for name in ("mongo/db/query/get_executor_test",
                     "mongo/db/commands/plan_cache_commands_test",
                     "mongo/db/commands/index_filter_commands_test",
                     "mongo/db/operation_context_test"):
            with self.subTest(name=name):
                test = next(test for test in tests if test["name"] == name)
                self.assertFalse(required - set(test["support_sources"]))

    def test_test_only_engine_and_parser_registrations(self):
        tests, _ = select_minimal_tests(self.targets, self.selected)
        by_name = {test["name"]: test for test in tests}
        engine = "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_init.cpp"
        self.assertNotIn(engine, self.sources)
        self.assertIn(engine, by_name["mongo/db/exec/sort_test"]["support_sources"])
        for name in ("mongo/db/exec/sort_test", "mongo/db/exec/queued_data_stage_test",
                     "mongo/db/concurrency/lock_manager_test"):
            self.assertIn("mongo/db/catalog/database_holder_impl.cpp",
                          by_name[name]["support_sources"])
            self.assertIn("mongo/db/catalog/collection_impl.cpp", by_name[name]["support_sources"])
        self.assertIn("mongo/db/pipeline/document_source_internal_split_pipeline.cpp",
                      by_name["mongo/db/pipeline/document_source_facet_test"]["support_sources"])
        self.assertIn("mongo/db/pipeline/granularity_rounder_powers_of_two.cpp",
                      by_name["mongo/db/pipeline/granularity_rounder_test"]["support_sources"])


if __name__ == "__main__":
    unittest.main()
