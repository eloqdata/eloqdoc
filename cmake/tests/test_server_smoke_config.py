#!/usr/bin/env python3
"""Check smoke-fixture backend selection without a server or cloud service."""

import configparser
from pathlib import Path
import unittest

from server_smoke_config import substrate_config


class ServerSmokeConfigTest(unittest.TestCase):
    environment = {"S3_ENDPOINT": "http://127.0.0.1:9900/",
                   "S3_ACCESS_KEY": "test-access", "S3_SECRET_KEY": "test-secret"}

    def config(self, data_store="ELOQDSS_ELOQSTORE", log_state="ROCKSDB_CLOUD_S3",
               environment=None, core_number=2):
        config = configparser.ConfigParser(interpolation=None)
        config.read_string(substrate_config(
            Path("/tmp/smoke fixture"), 12345, 12346, data_store, log_state,
            self.environment if environment is None else environment, core_number=core_number))
        return config

    def test_eloqstore_s3_uses_same_bucket_and_credentials_for_data_and_log(self):
        config = self.config()
        local, store = config["local"], config["store"]
        bucket = local["txlog_rocksdb_cloud_bucket_name"]
        self.assertRegex(bucket, r"^eloqdoc-smoke-[0-9a-f]{32}$")
        self.assertTrue(local.getboolean("enable_wal"))
        self.assertEqual("", local["txlog_rocksdb_cloud_bucket_prefix"])
        self.assertEqual("txlog", local["txlog_rocksdb_cloud_object_path"])
        self.assertEqual(f"http://127.0.0.1:9900/{bucket}/txlog",
                         local["txlog_rocksdb_cloud_object_store_service_url"])
        self.assertEqual(local["txlog_rocksdb_cloud_s3_endpoint_url"],
                         store["eloq_store_cloud_endpoint"])
        self.assertEqual(f"{bucket}/eloqstore", store["eloq_store_cloud_store_path"])
        self.assertEqual("aws", store["eloq_store_cloud_provider"])
        self.assertEqual("test-access", store["aws_access_key_id"])
        self.assertEqual(store["aws_access_key_id"], store["eloq_store_cloud_access_key"])
        self.assertEqual("test-secret", store["aws_secret_key"])
        self.assertEqual(store["aws_secret_key"], store["eloq_store_cloud_secret_key"])

    def test_cloud_invocations_are_isolated(self):
        self.assertNotEqual(self.config()["store"]["eloq_store_cloud_store_path"],
                            self.config()["store"]["eloq_store_cloud_store_path"])

    def test_missing_s3_configuration_is_rejected(self):
        for key in self.environment:
            with self.subTest(key=key):
                environment = dict(self.environment, **{key: ""})
                with self.assertRaisesRegex(ValueError, key):
                    self.config(environment=environment)

    def test_local_rocksdb_needs_no_cloud_service(self):
        for data_store in ("ROCKSDB", "ELOQDSS_ROCKSDB"):
            with self.subTest(data_store=data_store):
                config = self.config(data_store, "ROCKSDB", environment={})
                self.assertFalse(config["local"].getboolean("enable_wal"))
                self.assertNotIn("store", config)
                self.assertFalse(any("cloud" in key for key in config["local"]))

    def test_unsupported_backend_does_not_silently_use_local_rocksdb(self):
        for data_store, log_state in (("ELOQDSS_ELOQSTORE", "ROCKSDB"),
                                      ("ELOQDSS_ROCKSDB", "ROCKSDB_CLOUD_S3"),
                                      ("ELOQDSS_ROCKSDB_CLOUD_S3", "ROCKSDB_CLOUD_S3")):
            with self.subTest(data_store=data_store, log_state=log_state):
                with self.assertRaisesRegex(ValueError, "Unsupported smoke-test backend"):
                    self.config(data_store, log_state)

    def test_private_paths_and_ports_are_retained(self):
        config = self.config()
        self.assertEqual("/tmp/smoke fixture/eloq", config["local"]["eloq_data_path"])
        self.assertEqual("/tmp/smoke fixture/log", config["local"]["log_service_data_path"])
        self.assertEqual(2, config["local"].getint("core_number"))
        self.assertEqual(12345, config["local"].getint("tx_port"))
        self.assertEqual(12346, config["local"].getint("hm_port"))
        self.assertEqual("127.0.0.1:12345", config["cluster"]["tx_ip_port_list"])

    def test_active_shutdown_can_use_a_single_worker(self):
        for data_store, log_state in (("ELOQDSS_ROCKSDB", "ROCKSDB"),
                                      ("ELOQDSS_ELOQSTORE", "ROCKSDB_CLOUD_S3")):
            with self.subTest(data_store=data_store):
                self.assertEqual(1, self.config(data_store, log_state, core_number=1)[
                    "local"].getint("core_number"))

    def test_larger_runtime_suites_can_set_the_memory_budget(self):
        config = configparser.ConfigParser()
        config.read_string(substrate_config(Path("/tmp/runtime"), 12345, 12346,
                                           "ELOQDSS_ROCKSDB", "ROCKSDB", {}, memory_limit_mb=4000))
        self.assertEqual(4000, config["local"].getint("node_memory_limit_mb"))


if __name__ == "__main__":
    unittest.main()
