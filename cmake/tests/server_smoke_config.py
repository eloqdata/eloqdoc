"""Data Substrate configuration for the Mongo-facing server smoke fixture, and the ports and process
group a test's server needs."""

import os
import random
import signal
import socket
import time
import uuid
from pathlib import Path

# The ports a node's data substrate opens from its tx port up: the tx port itself, then +1 (the
# cc node RPC), +2 (the log service), +3 (log replay) and +7 (the data store service).
TX_PORT_BLOCK = 8
# The lowest port a test's servers listen on. Their ports come from below the kernel's ephemeral
# range, which client connections draw from: a client socket a previous server left in TIME_WAIT
# keeps its port from a server's bind for a minute, even with SO_REUSEADDR.
NODE_PORT_LOW = 10000
# The ports this process has handed to servers, so that two servers never share one.
_ports_taken = set()


def port_free(port):
    """Whether a server could listen on the port now, as a node's servers do: on every address,
    with SO_REUSEADDR."""
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", port))
        except OSError:
            return False
        return True


def free_node_port(width=1):
    """The first of `width` consecutive ports, all free now and below the ephemeral range."""
    try:
        high = int(Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        high = 32768
    high = max(high, NODE_PORT_LOW + 1000)
    for _ in range(1000):
        port = random.randrange(NODE_PORT_LOW, high - width)
        block = range(port, port + width)
        if not _ports_taken.intersection(block) and all(port_free(p) for p in block):
            _ports_taken.update(block)
            return port
    raise AssertionError(f"no {width} free consecutive ports below {high}")


def reap_process_group(process, ports, timeout=60):
    """Kills what is left of a server's process group and waits until its ports are free.

    The host manager the server forks inherits the server's listening sockets and outlives a
    server that crashed or was killed for a moment, so a server started next on the same ports
    would find them in use. Waits for the ports rather than for the group, whose zombies a
    container's init may never reap.
    """
    if not process:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + timeout
    while not all(port_free(port) for port in ports):
        if time.monotonic() > deadline:
            busy = [port for port in ports if not port_free(port)]
            raise AssertionError(f"the server's ports {busy} stayed in use after it exited")
        time.sleep(0.1)


def substrate_config(root, tx_port, hm_port, data_store, log_state, environment, *, core_number=2,
                     memory_limit_mb=None):
    # EloqStore keeps its data in S3 with either log state; only the cloud log state puts the log
    # service's history there too.
    cloud = data_store == "ELOQDSS_ELOQSTORE" and log_state in ("ROCKSDB", "ROCKSDB_CLOUD_S3")
    cloud_log = cloud and log_state == "ROCKSDB_CLOUD_S3"
    local = data_store in ("ROCKSDB", "ELOQDSS_ROCKSDB") and log_state == "ROCKSDB"
    if not (cloud or local):
        raise ValueError(f"Unsupported smoke-test backend: {data_store}/{log_state}")
    if memory_limit_mb is None:
        memory_limit_mb = 2048 if cloud else 512

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
        cloud_local = "checkpointer_interval=10\nlogserver_snapshot_interval=60\n"
        if cloud_log:
            cloud_local += (
                "txlog_rocksdb_cloud_bucket_prefix=\n"
                f"txlog_rocksdb_cloud_bucket_name={bucket}\n"
                "txlog_rocksdb_cloud_object_path=txlog\n"
                f"txlog_rocksdb_cloud_s3_endpoint_url={endpoint}\n"
                f"txlog_rocksdb_cloud_object_store_service_url={endpoint}/{bucket}/txlog\n"
                "txlog_rocksdb_cloud_sst_file_cache_size=1GB\n")
        cloud_store = (
            "[store]\n"
            # The log service's S3 credentials, which a local log state does not define.
            + (f"aws_access_key_id={access_key}\naws_secret_key={secret_key}\n"
               if cloud_log else "")
            + "eloq_store_buffer_pool_size=500MB\neloq_store_local_space_limit=2GB\n"
            "eloq_store_manifest_limit=1048576\neloq_store_pages_per_file_shift=8\n"
            "eloq_store_cloud_provider=aws\n"
            f"eloq_store_cloud_endpoint={endpoint}\n"
            f"eloq_store_cloud_store_path={bucket}/eloqstore\n"
            f"eloq_store_cloud_access_key={access_key}\n"
            f"eloq_store_cloud_secret_key={secret_key}\n"
            "eloq_store_cloud_verify_ssl=false\neloq_store_cloud_request_threads=2\n")

    return (
        f"[local]\ncore_number={core_number}\n"
        f"node_memory_limit_mb={memory_limit_mb}\n"
        f"enable_data_store=true\nenable_wal={'true' if cloud else 'false'}\n"
        "enable_mvcc=true\nevent_dispatcher_num=1\nbind_all=false\n"
        # A DDL's failed data store step is retried after a second, not the default 30.
        "ddl_kv_retry_interval_seconds=1\n"
        f"tx_ip=127.0.0.1\ntx_port={tx_port}\nhm_port={hm_port}\n"
        f"eloq_data_path={root / 'eloq'}\nlog_service_data_path={root / 'log'}\n"
        + cloud_local
        + f"[cluster]\ntx_ip_port_list=127.0.0.1:{tx_port}\n"
        + cloud_store)
