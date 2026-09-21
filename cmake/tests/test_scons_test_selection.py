"""Tests for the explicit SCons EloqDoc validation profile (Python 3)."""

import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "select_eloq_unit_tests", Path(__file__).resolve().parents[2] /
    "scripts/buildscripts/select_eloq_unit_tests.py")
selector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(selector)


class EloqUnitTestSelectionTest(unittest.TestCase):
    def test_unsupported_engines_and_embedded_are_skipped(self):
        for target in ("mongo/db/storage/mmap_v1/btree/btree_interface_test",
                       "mongo/db/storage/wiredtiger/storage_wiredtiger_prefixed_record_store_test",
                       "mongo/embedded/mongo_embedded_capi_test"):
            for prefix in ("build/gcc15/", "/tmp/test-build/gcc15/"):
                self.assertIsNotNone(selector.skip_reason(prefix + target))

    def test_obsolete_interfaces_have_narrow_exclusions(self):
        self.assertIsNotNone(selector.skip_reason("build/gcc15/mongo/db/views/views_test"))
        self.assertIsNone(selector.skip_reason("build/gcc15/mongo/db/pipeline/pipeline_test"))

    def test_replication_suites_are_excluded_including_future_targets(self):
        for name in ("replication_process_test", "replication_consistency_markers_impl_test",
                     "drop_pending_collection_reaper_test", "replication_coordinator_impl_test",
                     "initial_syncer_test", "future_test"):
            for prefix in ("", "build/gcc15/", "/tmp/test-build/gcc15/"):
                self.assertIn("replication", selector.skip_reason(prefix + "mongo/db/repl/" + name))

    def test_fixed_tests_and_applicable_catalog_coverage_remain_selected(self):
        for name in ("mongo/db/query/collation/collator_factory_icu_test",
                     "mongo/db/operation_time_tracker_test", "mongo/util/background_job_test",
                     "mongo/s/query/async_results_merger_test",
                     "mongo/db/catalog/collection_test",
                     "mongo/db/storage/kv/kv_collection_catalog_entry_test",
                     "mongo/db/storage/storage_repair_observer_test",
                     "mongo/db/standalone_runtime_test",
                     "mongo/db/free_mon/free_mon_test",
                     "mongo/db/storage/ephemeral_for_test/storage_ephemeral_for_test_record_store_test"):
            self.assertIsNone(selector.skip_reason(name))

    def test_confirmed_legacy_fixtures_have_explicit_reasons(self):
        for target, reason in selector.UNSUPPORTED_RUNTIME_FIXTURES.items():
            self.assertTrue(reason)
            self.assertEqual(reason, selector.skip_reason("/tmp/build/gcc15/" + target))
        self.assertIn("oplog", selector.skip_reason("mongo/db/catalog/database_test"))
        self.assertIn("renameCollection",
                      selector.skip_reason("mongo/db/catalog/create_collection_test"))
        self.assertIn("repair", selector.skip_reason("mongo/db/storage/kv/kv_storage_engine_test"))
        # New tests in these directories must be assessed, not silently skipped.
        self.assertIsNone(selector.skip_reason("mongo/db/catalog/future_test"))
        self.assertIsNone(selector.skip_reason("mongo/db/s/future_test"))

    def test_missing_or_unrecognized_binary_is_not_silently_skipped(self):
        binaries = ["/no-such-build/mongo/db/future_test", "unknown_test"]
        self.assertEqual((binaries, []), selector.select_tests(binaries))


if __name__ == "__main__":
    unittest.main()
