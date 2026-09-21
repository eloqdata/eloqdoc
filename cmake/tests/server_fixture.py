#!/usr/bin/env python3
"""Run a command against a private EloqDoc server; require clean server shutdown.

The command may use {port}, {connection_string}, and {fixture} placeholders.
Unlike resmoke's external-server mode, this wrapper owns and checks the server.
Requires PyMongo in this interpreter, not in the child command's environment.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

from server_smoke_config import substrate_config
from server_smoke_diagnostics import describe_exit, report_failure, wait_for_clean_shutdown


def reserve_ports(port):
    # Substrate also uses ports adjacent to tx_port. Hold all reservations until
    # immediately before launch, and fail rather than connect to someone else's server.
    sockets = []
    try:
        listener = socket.socket()
        sockets.append(listener)
        # Match the server listener: permit reuse after the preceding fixture's
        # connections enter TIME_WAIT, but do not use SO_REUSEPORT/shared listeners.
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        port = listener.getsockname()[1]
        for _ in range(100):
            group = []
            try:
                for offset in range(10):
                    sock = socket.socket()
                    group.append(sock)
                    sock.bind(("127.0.0.1", 0 if offset == 0 else tx_port + offset))
                    if offset == 0:
                        tx_port = sock.getsockname()[1]
                        if tx_port > 65525:
                            raise OSError("not enough adjacent ports")
                sockets.extend(group)
                break
            except OSError:
                for sock in group:
                    sock.close()
        else:
            raise RuntimeError("Could not reserve Data Substrate ports")
        host_manager = socket.socket()
        sockets.append(host_manager)
        host_manager.bind(("127.0.0.1", 0))
        return sockets, port, tx_port, host_manager.getsockname()[1]
    except BaseException:
        for sock in sockets:
            sock.close()
        raise


def child_environment(environment):
    result = dict(environment)
    # The wrapper uses Python 3 PyMongo. Never inject it into Python 2 resmoke
    # or TPCC's independently provisioned Python 3 virtual environment.
    result.pop("PYTHONPATH", None)
    return result


def expand_command(command, port, root):
    replacements = {"{port}": str(port), "{connection_string}": f"127.0.0.1:{port}",
                    "{fixture}": str(root)}
    for old, new in replacements.items():
        command = [argument.replace(old, new) for argument in command]
    return command


def stop_process_group(process):
    # Only kill groups we created. Even when their leader exited, test clients
    # and the forked host manager must not leak into the next CI phase.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)


def run_command(command, root, timeout):
    # Retain complete output and print it live for CI's heartbeat and test summaries.
    # Using a file avoids a blocked pipe if a test or one of its descendants hangs.
    with (root / "tests.log").open("wb") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   env=child_environment(os.environ), start_new_session=True)
        try:
            started = time.monotonic()
            with (root / "tests.log").open(errors="replace") as stream:
                while process.poll() is None:
                    print(stream.read(), end="", flush=True)
                    if time.monotonic() - started > timeout:
                        raise TimeoutError(f"Test command timed out after {timeout}s")
                    time.sleep(0.5)
                print(stream.read(), end="", flush=True)
            if process.returncode:
                raise RuntimeError("Test command failed: " + describe_exit(process.returncode))
        finally:
            stop_process_group(process)


def run_fixture(args):
    from pymongo import MongoClient
    from pymongo.errors import AutoReconnect, PyMongoError

    root = Path(tempfile.mkdtemp(prefix=f"eloqdoc-{args.label}-"))
    print(f"Fixture: {root}", flush=True)
    (root / "db").mkdir()
    reservations, port, tx_port, hm_port = reserve_ports(args.port)
    try:
        (root / "substrate.cnf").write_text(substrate_config(
            root, tx_port, hm_port, args.data_store, args.log_state, os.environ,
            memory_limit_mb=args.memory_limit_mb), encoding="utf-8")
        (root / "server.json").write_text(json.dumps({
            "storage": {"engine": "eloq", "dbPath": str(root / "db")},
            "net": {"port": port, "bindIp": "127.0.0.1", "serviceExecutor": "adaptive",
                    "adaptiveThreadNum": 1},
            "setParameter": {"enableTestCommands": True, "diagnosticDataCollectionEnabled": False,
                             "disableLogicalSessionCacheRefresh": True}}), encoding="utf-8")
    finally:
        for sock in reservations:
            sock.close()
    with (root / "server.log").open("wb") as log:
        server = subprocess.Popen([
            str(args.server.resolve()), "--config=" + str(root / "server.json"),
            "--data_substrate_config=" + str(root / "substrate.cnf"), "--log_dir=" + str(root)],
            cwd=root, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        client = MongoClient("127.0.0.1", port, directConnection=True,
                             serverSelectionTimeoutMS=1000, socketTimeoutMS=10000)
        try:
            deadline = time.monotonic() + 90
            while True:
                if server.poll() is not None:
                    raise RuntimeError("Server exited before readiness: " + describe_exit(server.returncode))
                try:
                    client.admin.command("ping")
                    break
                except PyMongoError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Server readiness timed out")
                    time.sleep(0.5)
            run_command(expand_command(args.command, port, root), root, args.timeout)
            # A successful client/test command must not hide a server crash.
            if server.poll() is not None:
                raise RuntimeError("Server exited during tests: " + describe_exit(server.returncode))
            try:
                client.admin.command("shutdown", force=True)
            except AutoReconnect:
                pass  # Disconnect is expected; the process exit code is authoritative.
            wait_for_clean_shutdown(server, timeout=60)
            print(f"PASS {args.label}: test command and server shutdown", flush=True)
        except BaseException as exc:
            report_failure(root, server.poll(), exc, args.diagnostics_dir)
            raise
        finally:
            client.close()
            stop_process_group(server)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--data-store", required=True)
    parser.add_argument("--log-state", required=True)
    parser.add_argument("--label", choices=("integration", "jstests", "tpcc"), required=True)
    parser.add_argument("--port", type=int, default=27017)
    parser.add_argument("--memory-limit-mb", type=int, default=4000)
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("--diagnostics-dir", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command[:1] == ["--"]:
        args.command.pop(0)
    if not args.command:
        parser.error("a test command after -- is required")
    run_fixture(args)


if __name__ == "__main__":
    main()
