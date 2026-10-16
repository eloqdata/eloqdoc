"""A multi-node EloqDoc cluster of separate processes on one host, for recovery tests.

Each process is a node of its own node group (one entry of tx_ip_port_list each), with the log
service built in: one log group replicated on every node (txlog_group_replica_num = node count), so
a single node killed leaves the log group a majority. Each node keeps its data under its own
directory, which survives restarts until the cluster is removed.

Usage:
    with Cluster(server, root, nodes=3) as cluster:
        cluster.start()
        cluster.client(0).db.coll.insert_one({...})
        cluster.kill(1); cluster.restart(1)
"""

import json
import os
import re
import shutil
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import PyMongoError

START_TIMEOUT_S = 180
TX_PORT_BASE = 17000
MONGO_PORT_BASE = 28000
# tx_port + 2 is the node's log server, + 3 its log replay server, + 4 its host manager, + 5 its
# node group's raft, + 7 its data store service, + 8 its metrics endpoint (when on).
PORT_STRIDE = 10
METRICS_PORT_OFFSET = 8
TX_PORT_OFFSETS = (0, 2, 3, 4, 5, 7, METRICS_PORT_OFFSET)
PORT_FREE_TIMEOUT_S = 60


def port_free(port):
    """Whether a listener can bind 127.0.0.1:port as the servers bind theirs (SO_REUSEADDR, so a
    socket in TIME-WAIT does not count)."""
    probe = socket.socket()
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        probe.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()
LOG_LEADER = re.compile(r"node lg(\d+):[0-9.]+:(\d+):\d+:\d+ term (\d+) become leader")

SUBSTRATE_CONFIG = """[local]
core_number={cores}
checkpointer_interval={checkpointer_interval}
ddl_kv_retry_interval_seconds=1
node_memory_limit_mb={memory_mb}
enable_data_store=true
enable_wal=true
eloq_data_path={root}/data/eloq
log_service_data_path={root}/data/log_service
enable_mvcc=true
event_dispatcher_num=1
logserver_snapshot_interval=60
bind_all=false
tx_ip=127.0.0.1
tx_port={tx_port}
hm_port={hm_port}
[cluster]
tx_ip_port_list={ip_port_list}
node_group_replica_num={ng_replicas}
txlog_group_replica_num={log_replicas}
[store]
eloq_store_buffer_pool_size=1GB
eloq_store_local_space_limit=50GB
eloq_store_manifest_limit=1048576
eloq_store_pages_per_file_shift=8
"""

METRICS_CONFIG = """[metrics]
enable_metrics=true
metrics_port={metrics_port}
"""

# EloqStore's cloud mode, appended to [store]: the shards live in an S3 bucket shared by every node,
# each node's eloqstore_data being a cache of it.
CLOUD_STORE_CONFIG = """eloq_store_cloud_provider=aws
eloq_store_cloud_endpoint={endpoint}
eloq_store_cloud_store_path={bucket}/{prefix}
eloq_store_cloud_access_key={access_key}
eloq_store_cloud_secret_key={secret_key}
eloq_store_cloud_auto_credentials=false
eloq_store_cloud_verify_ssl=false
eloq_store_cloud_request_threads=2
"""


def read_cloud_settings(path):
    """The S3 settings of a cloud store from a file of key=value lines: endpoint, bucket,
    access_key and secret_key. Keep the file private: it holds the secret key."""
    settings = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            settings[key.strip()] = value.strip()
    missing = {"endpoint", "bucket", "access_key", "secret_key"} - settings.keys()
    if missing:
        raise ValueError(f"{path} lacks {sorted(missing)}")
    return settings


class Node:
    """One EloqDoc process: node i, of node group i."""

    def __init__(self, cluster, index):
        self.cluster = cluster
        self.index = index
        self.root = cluster.root / f"n{index}"
        self.tx_port = cluster.tx_port(index)
        self.port = MONGO_PORT_BASE + cluster.port_offset + PORT_STRIDE * index
        self.metrics_port = self.tx_port + METRICS_PORT_OFFSET if cluster.metrics else None
        self.process = None
        self._client = None
        (self.root / "db").mkdir(parents=True, exist_ok=True)
        self.write_config(cluster.ip_port_list(), cluster.log_replicas)
        (self.root / "eloqdoc.yaml").write_text(json.dumps({
            "storage": {"dbPath": str(self.root / "db")},
            "net": {"port": self.port, "bindIp": "127.0.0.1",
                    "serviceExecutor": "adaptive", "adaptiveThreadNum": 1},
        }), encoding="utf-8")

    def write_config(self, ip_port_list, log_replicas):
        """Writes the node's data substrate config for a cluster of the given nodes."""
        cluster = self.cluster
        config = SUBSTRATE_CONFIG.format(
            cores=cluster.cores, checkpointer_interval=cluster.checkpointer_interval,
            memory_mb=cluster.memory_mb, root=self.root, tx_port=self.tx_port,
            hm_port=self.tx_port + 4, ip_port_list=ip_port_list, log_replicas=log_replicas,
            ng_replicas=cluster.ng_replicas)
        if cluster.cloud:
            # Still in the [store] section the base config ends with.
            config += CLOUD_STORE_CONFIG.format(prefix=cluster.cloud_prefix, **cluster.cloud)
        if self.metrics_port:
            config += METRICS_CONFIG.format(metrics_port=self.metrics_port)
        path = self.root / "data_substrate.cnf"
        path.write_text(config, encoding="utf-8")
        if cluster.cloud:
            path.chmod(0o600)  # it holds the store's secret key

    def bootstrap(self, timeout_s=START_TIMEOUT_S):
        """Runs the node once in data substrate's bootstrap mode ([local] bootstrap=true): alone,
        with the write-ahead log off, it creates its data store shard (shard 0) with the data
        substrate's system tables (Sequences) and runs EloqDoc's whole start-up, which creates
        admin.system.version and its featureCompatibilityVersion document, then exits. Restores
        the cluster's config and drops any raft state the one-node run left."""
        config = self.root / "data_substrate.cnf"
        normal = config.read_text(encoding="utf-8")
        config.write_text(normal.replace("[local]\n", "[local]\nbootstrap=true\n", 1),
                          encoding="utf-8")
        try:
            self.launch()
            self.process.wait(timeout=timeout_s)
            if self.process.returncode != 0:
                raise AssertionError(f"node {self.index}'s bootstrap exited with status "
                                     f"{self.process.returncode}")
        finally:
            config.write_text(normal, encoding="utf-8")
        shutil.rmtree(self.root / "data" / "log_service", ignore_errors=True)
        shutil.rmtree(self.root / "data" / "eloq" / "tx_service", ignore_errors=True)

    def ports(self):
        """Every port the node listens on."""
        return [self.port] + [self.tx_port + offset for offset in TX_PORT_OFFSETS]

    def wait_ports_free(self, timeout_s=PORT_FREE_TIMEOUT_S):
        """Waits until no other process holds one of the node's ports (seen once: a node whose
        mongod port was still held failed to start, exit status 48); names the holder if one
        does past the timeout."""
        deadline = time.monotonic() + timeout_s
        while True:
            busy = [port for port in self.ports() if not port_free(port)]
            if not busy:
                return
            if time.monotonic() > deadline:
                holders = subprocess.run(["ss", "-ltnp"], capture_output=True, text=True).stdout
                held = [line for line in holders.splitlines()
                        if any(f":{port} " in line for port in busy)]
                raise AssertionError(f"node {self.index}'s ports {busy} stayed busy: {held}")
            time.sleep(0.5)

    def launch(self, extra_env=None):
        """Starts the process once its ports are free; returns without waiting for it to
        serve. extra_env adds variables to its environment (ELOQ_DSS_FAULT_INJECT arms data store
        service faults from the start)."""
        self.wait_ports_free()
        log = (self.root / "server.log").open("ab")
        env = dict(os.environ)
        env.update(extra_env or {})
        if self.cluster.lib_dir:
            env["LD_LIBRARY_PATH"] = str(self.cluster.lib_dir)
        cmd = [str(self.cluster.server),
               "--data_substrate_config=" + str(self.root / "data_substrate.cnf"),
               "--config=" + str(self.root / "eloqdoc.yaml"), "--log_dir=" + str(self.root),
               "--setParameter=enableTestCommands=1",
               "--setParameter=diagnosticDataCollectionEnabled=false"]
        if self.cluster.cpus:
            cmd = ["taskset", "-c", self.cluster.cpus[self.index]] + cmd
        self.process = subprocess.Popen(cmd, cwd=self.root, stdout=log, stderr=subprocess.STDOUT,
                                        env=env, start_new_session=True)
        self._client = None

    def client(self, timeout_ms=5000):
        """A client of this node; it reconnects across restarts."""
        if self._client is None:
            self._client = MongoClient("127.0.0.1", self.port, directConnection=True,
                                       serverSelectionTimeoutMS=timeout_ms,
                                       socketTimeoutMS=600000)
        return self._client

    def wait_serving(self, timeout_s=START_TIMEOUT_S):
        """Waits until the node answers a ping: it has finished its start-up, its log replay
        included. Touches no user collection, whose catalog entry a recovered schema op may hold."""
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"node {self.index} exited during start-up "
                                     f"(status {self.process.returncode})")
            try:
                if self.client(1000).admin.command("ping")["ok"] == 1:
                    return
            except PyMongoError as exc:
                last = exc
            time.sleep(0.5)
        raise AssertionError(f"node {self.index} did not come up: {last}")

    def wait_recovered(self, timeout_s=START_TIMEOUT_S):
        """Waits until the node's latest process has replayed its node group from every log group
        ("replay connection: cc node group: <i>, ..., set recovering status to finished" once per
        log group): until then the node group is still failing over, and requests through it can
        fail transiently (seen: a create answered "Requested table schema mismatch")."""
        groups = len(self.cluster.nodes_spec) // self.cluster.log_replicas
        needle = f"replay connection: cc node group: {self.index},"
        deadline = time.monotonic() + timeout_s
        while True:
            finished = {line.split("log group:", 1)[1].split(",", 1)[0].strip()
                        for line in self.current_log_lines(needle)
                        if "set recovering status to finished" in line and "log group:" in line}
            if len(finished) >= groups:
                return
            if self.process.poll() is not None:
                raise AssertionError(f"node {self.index} exited during recovery "
                                     f"(status {self.process.returncode})")
            if time.monotonic() > deadline:
                raise AssertionError(f"node {self.index} never finished recovering its node "
                                     f"group (log groups done: {sorted(finished)} of {groups})")
            time.sleep(0.5)

    def kill(self):
        """kill -9 of the node's process group: the node dies with nothing flushed, and its host
        manager with it."""
        if self.process:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if self.process.poll() is None:
                self.process.wait()
        self._client = None

    def stop(self, timeout_s=120):
        """A clean shutdown through the shutdown command, waited for; then the rest of the node's
        process group (its host manager). SIGTERM is no clean shutdown here: threads created
        before mongod masks its signals (brpc's bvar_sampler) leave it deliverable to a thread
        with no handler, so the process dies at once (exit status -15, nothing logged). It is
        the fallback if the command fails."""
        if self.process and self.process.poll() is None:
            try:
                MongoClient("127.0.0.1", self.port, directConnection=True,
                            serverSelectionTimeoutMS=2000).admin.command("shutdown")
            except PyMongoError:
                pass  # the server closes the connection as it shuts down
            try:
                self.process.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    pass
        if self.process:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if self.process.poll() is None:
                self.process.wait()
        self._client = None

    def log_lines(self, needle):
        """The lines of the node's logs (server.log and the data substrate's) holding needle."""
        lines = []
        for path in self.root.glob("*"):
            if path.is_file() and (path.suffix in (".log", ".INFO", ".WARNING", ".ERROR")
                                   or ".log." in path.name or "INFO" in path.name):
                try:
                    with path.open("r", errors="replace") as f:
                        lines += [line.rstrip() for line in f if needle in line]
                except OSError:
                    pass
        return lines

    def current_log_lines(self, needle):
        """The lines holding needle in the data substrate's log of the node's latest process
        (glog's eloqdb.log.INFO link to it)."""
        try:
            with (self.root / "eloqdb.log.INFO").open("r", errors="replace") as f:
                return [line.rstrip() for line in f if needle in line]
        except OSError:
            return []


class Cluster:
    """N nodes, each its own node group, sharing one replicated log group."""

    def __init__(self, server, root, nodes=3, cores=4, checkpointer_interval=10, memory_mb=16000,
                 lib_dir=None, cpus=None, keep=False, port_offset=0, metrics=False,
                 log_replicas=None, ng_replicas=1, cloud=None):
        self.server = Path(server).resolve()
        self.root = Path(root)
        self.cores = cores
        self.checkpointer_interval = checkpointer_interval
        self.memory_mb = memory_mb
        self.lib_dir = lib_dir
        self.cpus = cpus
        self.keep = keep
        self.port_offset = port_offset  # lets clusters run side by side on one host
        self.metrics = metrics  # each node exports metrics on tx_port + METRICS_PORT_OFFSET
        self.nodes_spec = list(range(nodes))
        # Replicas of each log group: every node by default, one log group; 1 makes a log group
        # of each node.
        self.log_replicas = log_replicas or nodes
        # Raft members of each node group: node i is the only candidate of node group i, and with
        # more than 1 the next nodes join it as voters, which hold no data but count for its
        # majority.
        self.ng_replicas = ng_replicas
        # EloqStore's cloud mode (read_cloud_settings): every node's shards in one bucket, under a
        # prefix of this cluster's own, so that a node can lead any node group.
        self.cloud = cloud
        self.cloud_prefix = f"{self.root.name}-{int(time.time())}" if cloud else None
        if self.root.exists():
            shutil.rmtree(self.root)
        self.root.mkdir(parents=True)
        self.nodes = [Node(self, i) for i in self.nodes_spec]

    def tx_port(self, index):
        return TX_PORT_BASE + self.port_offset + PORT_STRIDE * index

    def ip_port_list(self):
        return ",".join(f"127.0.0.1:{self.tx_port(i)}" for i in self.nodes_spec)

    def start(self):
        """Creates the data store shards, then starts every node at once (their log group needs a
        majority) and waits for all. With a cloud store, node 0's bootstrap writes shard 0 to the
        bucket and the other nodes read it there; without one, as follows.

        A node of a multi-node cluster opens its node group's data store shard only if it exists
        (create_if_missing is false); only a node alone in its cluster, as in bootstrap mode,
        creates one, shard 0. So node 0 bootstraps, which creates shard 0 with the system tables
        and EloqDoc's start-up collections in it, and every other node gets a copy of that shard
        as its own. A node group reads and writes only the partitions of the buckets it owns, so
        each copy serves as a shared store would; the partitions of other buckets in a copy are
        never touched."""
        first = self.nodes[0]
        first.bootstrap()
        if self.cloud:
            # The bootstrap wrote shard 0 to the bucket, where every node reads it.
            for node in self.nodes:
                node.launch()
            self._wait_first_start()
            self.wait_all_recovered()
            return
        dss = first.root / "data" / "eloq" / "eloq_dss"
        rocksdb_shard0 = dss / "rocksdb_data" / "ds_0"
        eloqstore = dss / "eloqstore_data"
        for node in self.nodes[1:]:
            target = node.root / "data" / "eloq" / "eloq_dss"
            if rocksdb_shard0.is_dir():
                # The RocksDB service keeps each shard in a directory named after it.
                (target / "rocksdb_data").mkdir(parents=True, exist_ok=True)
                shutil.copytree(rocksdb_shard0, target / "rocksdb_data" / f"ds_{node.index}")
            elif eloqstore.is_dir():
                # The EloqStore service keeps the node's one shard under eloqstore_data.
                target.mkdir(parents=True, exist_ok=True)
                shutil.copytree(eloqstore, target / "eloqstore_data")
            else:
                raise AssertionError(f"the seed run left no data store shard under {dss}")
        for node in self.nodes:
            node.launch()
        self._wait_first_start()
        self.wait_all_recovered()

    def restart_all(self):
        """A clean stop of every node, then a start of all of them together, waited for as the
        first start is."""
        self.stop()
        for node in self.nodes:
            node.launch()
        self._wait_first_start()
        self.wait_all_recovered()
        self.wait_catalog_reads()

    def _wait_first_start(self, relaunches=10, pause_s=5):
        """Waits for every node's first start-up, launching again, after a pause, a node that
        exits. A node's start-up reads the system collections across the cluster, and a read that
        reaches a data store shard whose node is not serving yet comes back empty: the node finds
        no featureCompatibilityVersion document and stops (fassert 40652). Once every node serves,
        a start-up succeeds."""
        pending = {node.index: relaunches for node in self.nodes}
        relaunch_at = {}
        deadline = time.monotonic() + START_TIMEOUT_S * 3
        while pending:
            if time.monotonic() > deadline:
                raise AssertionError(f"nodes {sorted(pending)} did not come up")
            for index in list(pending):
                node = self.nodes[index]
                if node.process.poll() is not None:
                    if pending[index] == 0:
                        raise AssertionError(f"node {index} kept exiting during start-up "
                                             f"(status {node.process.returncode})")
                    if index not in relaunch_at:
                        relaunch_at[index] = time.monotonic() + pause_s
                    elif time.monotonic() >= relaunch_at[index]:
                        del relaunch_at[index]
                        pending[index] -= 1
                        node.launch()
                    continue
                try:
                    if node.client(1000).admin.command("ping")["ok"] == 1:
                        del pending[index]
                except PyMongoError:
                    pass
            time.sleep(1)

    def client(self, index):
        return self.nodes[index].client()

    def log_leader(self, group=0):
        """The node that last became the leader of a log group, by the raft term in the "become
        leader" lines of each node's latest process (node 0's bootstrap run led a log group of its
        own), or None if no node has led it yet."""
        best = None
        for node in self.nodes:
            for line in node.current_log_lines("become leader"):
                match = LOG_LEADER.search(line)
                if (match and int(match.group(1)) == group
                        and int(match.group(2)) == node.tx_port + 2):
                    term = int(match.group(3))
                    if best is None or term > best[0]:
                        best = (term, node.index)
        return None if best is None else best[1]

    def kill(self, index):
        self.nodes[index].kill()

    def restart(self, index, wait=True):
        """Relaunches a node; with wait, until it serves and has recovered its node group."""
        self.nodes[index].launch()
        if wait:
            self.nodes[index].wait_serving()
            self.nodes[index].wait_recovered()
            self.wait_catalog_reads()

    def wait_all_recovered(self):
        """Waits until every node has recovered its node group."""
        for node in self.nodes:
            node.wait_recovered()

    def wait_catalog_reads(self, timeout_s=60):
        """Waits until every running node can list the databases, a read of the catalog from every
        data store shard. For a few seconds after a node restarts, the others' requests to its data
        store service fail at once ("Not connected ... yet": brpc holds the peer down until its
        health check reconnects), and EloqDoc does not retry the catalog listing, so a
        listCollections or a change stream's open fails with InternalError meanwhile."""
        deadline = time.monotonic() + timeout_s
        for node in self.nodes:
            if node.process is None or node.process.poll() is not None:
                continue
            while True:
                try:
                    node.client().list_database_names()
                    break
                except PyMongoError as exc:
                    if time.monotonic() > deadline:
                        raise AssertionError(f"node {node.index} cannot read the catalog: {exc}")
                    time.sleep(0.5)

    def stop(self):
        """Shuts every node down at once, as a whole-cluster stop does: a node shut down after the
        others have gone waits forever (its log group has lost its majority) until stop() gives
        up on it and kills it."""
        threads = [threading.Thread(target=node.stop) for node in self.nodes]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        if not self.keep and exc_type is None:
            shutil.rmtree(self.root, ignore_errors=True)
        return False
