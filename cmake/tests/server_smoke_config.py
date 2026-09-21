"""Data Substrate configuration for the Mongo-facing server smoke fixture."""

import uuid


def substrate_config(root, tx_port, hm_port, data_store, log_state, environment, *, core_number=2):
    cloud = (data_store, log_state) == ("ELOQDSS_ELOQSTORE", "ROCKSDB_CLOUD_S3")
    local = data_store in ("ROCKSDB", "ELOQDSS_ROCKSDB") and log_state == "ROCKSDB"
    if not (cloud or local):
        raise ValueError(f"Unsupported smoke-test backend: {data_store}/{log_state}")

    cloud_local, cloud_store = "", ""
    if cloud:
        required = ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")
        missing = [key for key in required if not environment.get(key)]
        if missing:
            raise ValueError("EloqStore/S3 smoke test requires " + ", ".join(missing))
        endpoint = environment["S3_ENDPOINT"].rstrip("/")
        access_key, secret_key = environment["S3_ACCESS_KEY"], environment["S3_SECRET_KEY"]
        # A fresh bucket isolates each invocation, including retries. CI owns the disposable
        # S3 service; the smoke test never removes buckets from an externally supplied service.
        bucket = "eloqdoc-smoke-" + uuid.uuid4().hex
        cloud_local = (
            "checkpoint_interval=10\nlogserver_snapshot_interval=60\n"
            "txlog_rocksdb_cloud_bucket_prefix=\n"
            f"txlog_rocksdb_cloud_bucket_name={bucket}\n"
            "txlog_rocksdb_cloud_object_path=txlog\n"
            f"txlog_rocksdb_cloud_s3_endpoint_url={endpoint}\n"
            f"txlog_rocksdb_cloud_object_store_service_url={endpoint}/{bucket}/txlog\n"
            "txlog_rocksdb_cloud_sst_file_cache_size=1GB\n")
        cloud_store = (
            "[store]\n"
            f"aws_access_key_id={access_key}\naws_secret_key={secret_key}\n"
            "eloq_store_buffer_pool_size=500MB\neloq_store_local_space_limit=2GB\n"
            "eloq_store_manifest_limit=1048576\neloq_store_pages_per_file_shift=8\n"
            "eloq_store_cloud_provider=aws\n"
            f"eloq_store_cloud_endpoint={endpoint}\n"
            f"eloq_store_cloud_store_path={bucket}/eloqstore\n"
            f"eloq_store_cloud_access_key={access_key}\n"
            f"eloq_store_cloud_secret_key={secret_key}\n"
            "eloq_store_cloud_verify_ssl=false\neloq_store_cloud_request_threads=2\n")

    return (
        f"[local]\ncore_number={core_number}\n"
        f"node_memory_limit_mb={2048 if cloud else 512}\n"
        f"enable_data_store=true\nenable_wal={'true' if cloud else 'false'}\n"
        "enable_mvcc=true\nevent_dispatcher_num=1\nbind_all=false\n"
        f"tx_ip=127.0.0.1\ntx_port={tx_port}\nhm_port={hm_port}\n"
        f"eloq_data_path={root / 'eloq'}\nlog_service_data_path={root / 'log'}\n"
        + cloud_local
        + f"[cluster]\ntx_ip_port_list=127.0.0.1:{tx_port}\n"
        + cloud_store)
