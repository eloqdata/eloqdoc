#!/usr/bin/env python3
"""EloqDoc change streams through the drivers' watch() (docs/cdc-change-stream.md §10.2; plan C13).

The same three scenarios run through PyMongo, the Node.js driver and the Java sync driver, the
drivers the M1 gate names. PyMongo's are below; the other two are small clients in
change_stream_clients/, run once per server. Each driver gets fresh servers, since two scenarios
need one that has not yet completed a checkpoint round:

  (i)   watch() on a server with no completed checkpoint round returns once the first round
        completes, within cdc_open_wait_ms, with a resume token and no operationTime error;
  (ii)  the cursor is killed before the first event, and a change E commits after the opening
        watermark S and before the driver resumes: PyMongo resumes from S and E arrives exactly
        once (a fresh open would have started past E);
  (iii) with checkpoint rounds skipped by fault injection, watch() fails with
        ChangeStreamStartNotReady (5000) once cdc_open_wait_ms runs out, and succeeds when retried
        after the rounds resume. Needs a server built with WITH_FAULT_INJECT; skipped otherwise.

The server backend comes from server_smoke_config (the CI backends), or from --substrate-config
for any other local build. Requires pymongo>=4.6; the Node.js and Java drivers need node and npm,
and java and javac, and are fetched into --work-dir on first use. Each is pinned to the last
release that still accepts a MongoDB 4.0 server (wire version 7): later ones refuse to connect.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from server_smoke_config import (TX_PORT_BLOCK, free_node_port,  # noqa: E402
                                 reap_process_group, substrate_config)

CHANGE_STREAM_START_NOT_READY = 5000
CLIENTS_DIR = Path(__file__).resolve().parent / "change_stream_clients"
# The last releases that accept a MongoDB 4.0 server: Node.js 6.17 and Java 5.5 require 4.2.
NODE_DRIVER_VERSION = "6.16.0"
JAVA_DRIVER_VERSION = "5.4.0"
JAVA_DRIVER_JARS = ("bson", "mongodb-driver-core", "mongodb-driver-sync")
MAVEN_CENTRAL = "https://repo1.maven.org/maven2/org/mongodb"
# The scenarios each fresh server runs: (iii) must arm its fault before the first checkpoint
# round, so it has a server of its own.
SERVER_GROUPS = (("opening", ["opening", "resume"]), ("not-ready", ["not-ready"]))


def token_timestamp(token):
    """The microsecond timestamp a resume token records: _data = 02 | u64 ts | kind | ..."""
    return int(token["_data"][2:18], 16)


def token_kind(token):
    """0 event, 1 watermark, 2 invalidate."""
    return int(token["_data"][18:20], 16)


class Server:
    """One disposable server, started fresh in its own directory."""

    def __init__(self, args, label):
        self.args = args
        self.root = Path(tempfile.mkdtemp(prefix=f"eloqdoc-cdc-drivers-{label}-"))
        self.port = args.port or free_node_port()
        self.process = None
        # The ports stop() waits for: the mongo port, and the data substrate's when this script
        # chose them (a --substrate-config template fixes its own).
        self.ports = [self.port]

    def start(self):
        (self.root / "db").mkdir(exist_ok=True)
        config = self.root / "data_substrate.cnf"
        if self.args.substrate_config:
            # A template: "{root}" becomes this server's directory, so each server starts empty.
            template = self.args.substrate_config.read_text(encoding="utf-8")
            config.write_text(template.replace("{root}", str(self.root)), encoding="utf-8")
        else:
            tx_port = free_node_port(TX_PORT_BLOCK)
            hm_port = free_node_port()
            self.ports += list(range(tx_port, tx_port + TX_PORT_BLOCK)) + [hm_port]
            config.write_text(substrate_config(self.root, tx_port, hm_port,
                                               self.args.data_store, self.args.log_state,
                                               os.environ), encoding="utf-8")
        server_config = self.root / "eloqdoc.yaml"
        server_config.write_text(json.dumps({
            "storage": {"dbPath": str(self.root / "db")},
            "net": {"port": self.port, "bindIp": "127.0.0.1",
                    "serviceExecutor": "adaptive", "adaptiveThreadNum": 1},
        }), encoding="utf-8")
        log = (self.root / "server.log").open("wb")
        self.process = subprocess.Popen(
            [str(self.args.server.resolve()), "--data_substrate_config=" + str(config),
             "--config=" + str(server_config), "--log_dir=" + str(self.root),
             "--setParameter=enableTestCommands=1",
             "--setParameter=diagnosticDataCollectionEnabled=false"],
            cwd=self.root, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        self.client = MongoClient("127.0.0.1", self.port, directConnection=True,
                                  serverSelectionTimeoutMS=1000)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            assert self.process.poll() is None, "server exited during start-up"
            try:
                if self.client.admin.command("ping")["ok"] == 1:
                    return self
            except PyMongoError:
                time.sleep(0.2)
        raise AssertionError("server did not come up")

    def stop(self, keep_logs):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.process.kill()
        reap_process_group(self.process, self.ports)
        if keep_logs and self.args.diagnostics_dir:
            target = self.args.diagnostics_dir / self.root.name
            target.mkdir(parents=True, exist_ok=True)
            for name in ("server.log",):
                if (self.root / name).exists():
                    shutil.copy(self.root / name, target / name)
        shutil.rmtree(self.root, ignore_errors=True)


def scenario_open_before_first_round(server):
    """(i) The open waits for the first checkpoint round and returns with a resume token."""
    coll = server.client.cdc_drivers.opening
    start = time.monotonic()
    with coll.watch(max_await_time_ms=1000) as stream:
        elapsed = time.monotonic() - start
        token = stream.resume_token
        assert token is not None, "the opening reply carried no postBatchResumeToken"
        assert token_kind(token) == 1, token
        assert stream.try_next() is None
    print(f"PASS python (i) watch() on a fresh server returned a token after {elapsed:.1f} s", flush=True)


def scenario_resume_before_first_event(server):
    """(ii) A killed cursor resumes from the opening watermark; E arrives exactly once."""
    db = server.client.cdc_drivers
    coll = db.resume
    # Inserts only: the insert that creates the collection also delivers its create event.
    with coll.watch([{"$match": {"operationType": "insert"}}], max_await_time_ms=1000) as stream:
        opening = stream.resume_token
        assert opening is not None
        # Kill the server-side cursor: the driver's next getMore gets CursorNotFound, a resumable
        # error, and PyMongo reopens with resumeAfter set to the token it holds.
        db.command("killCursors", coll.name, cursors=[stream._cursor.cursor_id])
        coll.insert_one({"_id": "E"})
        received = []
        deadline = time.monotonic() + 120
        while not received and time.monotonic() < deadline:
            change = stream.try_next()
            if change is not None:
                received.append(change)
        assert [c["documentKey"]["_id"] for c in received] == ["E"], received
        event_ts = token_timestamp(received[0]["_id"])
        # Nothing more at or below E: read on until the watermark passes it.
        while time.monotonic() < deadline:
            change = stream.try_next()
            assert change is None, f"E arrived twice or something else arrived: {change}"
            token = stream.resume_token
            if token_kind(token) == 1 and token_timestamp(token) >= event_ts:
                break
        else:
            raise AssertionError("the watermark never passed E")
    print("PASS python (ii) resumed from the opening watermark; E arrived exactly once", flush=True)


def scenario_start_not_ready(server):
    """(iii) Skipped checkpoint rounds make the open fail with ChangeStreamStartNotReady; a later
    retry succeeds."""
    admin = server.client.admin
    try:
        admin.command({"eloqFaultInject": "checkpointer_skip_ckpt"})
    except OperationFailure as exc:
        if exc.code == 115:  # CommandNotSupported: no fault points in this build
            print("SKIP python (iii) the server was built without WITH_FAULT_INJECT", flush=True)
            return
        raise
    admin.command({"setParameter": 1, "cdc_open_wait_ms": 3000})
    coll = server.client.cdc_drivers.not_ready
    try:
        with coll.watch():
            raise AssertionError("the open succeeded with every checkpoint round skipped")
    except OperationFailure as exc:
        assert exc.code == CHANGE_STREAM_START_NOT_READY, exc.details
    admin.command({"eloqFaultInject": "checkpointer_skip_ckpt", "params": "remove"})
    admin.command({"setParameter": 1, "cdc_open_wait_ms": 120000})
    with coll.watch(max_await_time_ms=1000) as stream:
        assert stream.resume_token is not None
    print("PASS python (iii) ChangeStreamStartNotReady surfaced; the retry succeeded", flush=True)


PYTHON_SCENARIOS = {
    "opening": scenario_open_before_first_round,
    "resume": scenario_resume_before_first_event,
    "not-ready": scenario_start_not_ready,
}


def require_tools(driver, *tools):
    """Fails with a clear message when a driver's toolchain is not on PATH."""
    missing = [tool for tool in tools if shutil.which(tool) is None]
    if missing:
        raise SystemExit(f"the {driver} scenarios need {', '.join(missing)} on PATH; "
                         f"install them or leave {driver} out of --drivers")


def node_runner(work_dir):
    """Installs the pinned Node.js driver once and returns a function running its client."""
    require_tools("node", "node", "npm")
    prefix = work_dir / "node"
    installed = prefix / "node_modules" / "mongodb" / "package.json"
    if not installed.exists() or \
            json.loads(installed.read_text())["version"] != NODE_DRIVER_VERSION:
        prefix.mkdir(parents=True, exist_ok=True)
        subprocess.run(["npm", "install", "--no-audit", "--no-fund", "--prefix", str(prefix),
                        f"mongodb@{NODE_DRIVER_VERSION}"], check=True)
    env = dict(os.environ, NODE_PATH=str(prefix / "node_modules"))

    def run(server, names):
        subprocess.run(["node", str(CLIENTS_DIR / "change_stream_check.js"), str(server.port),
                        *names], check=True, env=env, timeout=900)
    return run


def java_runner(work_dir):
    """Fetches and compiles against the pinned Java driver once; returns a function running it."""
    require_tools("java", "java", "javac")
    root = work_dir / "java"
    jars = []
    for artifact in JAVA_DRIVER_JARS:
        name = f"{artifact}-{JAVA_DRIVER_VERSION}.jar"
        jar = root / "lib" / name
        if not jar.exists():
            jar.parent.mkdir(parents=True, exist_ok=True)
            url = f"{MAVEN_CENTRAL}/{artifact}/{JAVA_DRIVER_VERSION}/{name}"
            with urllib.request.urlopen(url, timeout=120) as response:
                jar.write_bytes(response.read())
        jars.append(str(jar))
    classes = root / "classes"
    classes.mkdir(parents=True, exist_ok=True)
    classpath = os.pathsep.join(jars)
    subprocess.run(["javac", "-d", str(classes), "-cp", classpath,
                    str(CLIENTS_DIR / "ChangeStreamCheck.java")], check=True)
    classpath = os.pathsep.join([classpath, str(classes)])

    def run(server, names):
        subprocess.run(["java", "-cp", classpath, "ChangeStreamCheck", str(server.port), *names],
                       check=True, timeout=900)
    return run


def python_runner(_work_dir):
    """Returns a function running PyMongo's scenarios, defined above."""
    def run(server, names):
        for name in names:
            PYTHON_SCENARIOS[name](server)
    return run


RUNNERS = {"python": python_runner, "node": node_runner, "java": java_runner}


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--data-store", default="ELOQDSS_ROCKSDB")
    parser.add_argument("--log-state", default="ROCKSDB")
    parser.add_argument("--substrate-config", type=Path,
                        help="A data substrate config to use instead of generating one; "
                             "'{root}' in it becomes each server's own directory")
    parser.add_argument("--port", type=int, help="Server port; default a free one")
    parser.add_argument("--diagnostics-dir", type=Path,
                        help="Copy a failed scenario's server log here")
    parser.add_argument("--drivers", default="python,node,java",
                        help="Comma-separated drivers to run: python, node, java (default all)")
    parser.add_argument("--work-dir", type=Path,
                        default=Path(tempfile.gettempdir()) / "eloqdoc-change-stream-clients",
                        help="Where the Node.js and Java drivers are fetched and built, reused "
                             "across runs")
    args = parser.parse_args()
    # Keep the server and its helpers within the user's CPU limit.
    if hasattr(os, "sched_getaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:8])

    drivers = [d for d in args.drivers.split(",") if d]
    unknown = [d for d in drivers if d not in RUNNERS]
    if unknown:
        raise SystemExit(f"unknown drivers: {', '.join(unknown)}")
    # Set every driver up before starting any server, so a missing toolchain fails at once.
    runners = [(driver, RUNNERS[driver](args.work_dir)) for driver in drivers]
    for driver, run in runners:
        for label, names in SERVER_GROUPS:
            server = Server(args, f"{driver}-{label}")
            ok = False
            try:
                server.start()
                run(server, names)
                ok = True
            finally:
                server.stop(keep_logs=not ok)


if __name__ == "__main__":
    main()
