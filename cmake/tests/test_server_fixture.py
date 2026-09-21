#!/usr/bin/env python3
"""Regression checks for CI scope, client isolation, and runtime result propagation."""

import contextlib
import io
from pathlib import Path
import socket
import sys
import tempfile
import types
import unittest
from unittest import mock

import yaml

from server_fixture import child_environment, expand_command, reserve_ports, run_command, run_fixture


class ServerFixtureTest(unittest.TestCase):
    def test_port_reservation_allows_time_wait_but_rejects_a_busy_port(self):
        listener = mock.Mock()
        listener.bind.side_effect = OSError("Address already in use")
        with mock.patch("server_fixture.socket.socket", return_value=listener):
            with self.assertRaisesRegex(OSError, "Address already in use"):
                reserve_ports(27017)
        listener.setsockopt.assert_called_once_with(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.close.assert_called_once()

    def test_client_python_environments_are_isolated(self):
        environment = {"PYTHONPATH": "/python3/pymongo", "PATH": "/shell/bin:/usr/bin",
                       "LD_LIBRARY_PATH": "/third-party/lib"}
        self.assertEqual({key: value for key, value in environment.items() if key != "PYTHONPATH"},
                         child_environment(environment))
        self.assertIn("PYTHONPATH", environment)

    def test_command_arguments_are_substituted_without_shell_expansion(self):
        command = ["ctest", "--connection={connection_string}", "--port={port}",
                   "--report={fixture}/report.json", "{not_a_placeholder}"]
        self.assertEqual(["ctest", "--connection=127.0.0.1:12345", "--port=12345",
                          "--report=/tmp/path with spaces/report.json", "{not_a_placeholder}"],
                         expand_command(command, 12345, Path("/tmp/path with spaces")))

    def test_test_failure_is_not_hidden_by_fixture_cleanup(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "exit code 23"):
                run_command([sys.executable, "-c", "print('failure context'); raise SystemExit(23)"],
                            root, 5)
            self.assertIn("failure context", (root / "tests.log").read_text())

    def test_hung_command_times_out(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(TimeoutError, "timed out"):
                run_command([sys.executable, "-c", "import time; time.sleep(60)"], Path(directory), 1)

    def test_successful_command_is_recorded(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            root = Path(directory)
            run_command([sys.executable, "-c", "print('passed')"], root, 5)
            self.assertEqual("passed\n", (root / "tests.log").read_text())

    def check_server_exit(self, returncode):
        # No PyMongo installation or network access needed for these lifecycle checks.
        client = mock.Mock()
        pymongo = types.ModuleType("pymongo")
        pymongo.MongoClient = mock.Mock(return_value=client)
        errors = types.ModuleType("pymongo.errors")
        errors.PyMongoError = type("PyMongoError", (Exception,), {})
        errors.AutoReconnect = type("AutoReconnect", (errors.PyMongoError,), {})
        server = mock.Mock()
        server.poll.side_effect = [None, None, returncode]
        server.wait.return_value = returncode
        with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as stack:
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            stack.enter_context(mock.patch.dict(sys.modules, {"pymongo": pymongo,
                                                              "pymongo.errors": errors}))
            stack.enter_context(mock.patch("server_fixture.tempfile.mkdtemp", return_value=directory))
            stack.enter_context(mock.patch("server_fixture.reserve_ports",
                                           return_value=([], 27017, 16379, 16389)))
            stack.enter_context(mock.patch("server_fixture.subprocess.Popen", return_value=server))
            stack.enter_context(mock.patch("server_fixture.run_command"))
            cleanup = stack.enter_context(mock.patch("server_fixture.stop_process_group"))
            report = stack.enter_context(mock.patch("server_fixture.report_failure"))
            args = types.SimpleNamespace(label="integration", port=27017, memory_limit_mb=4000,
                                         server=Path("/tmp/eloqdoc"), data_store="ELOQDSS_ROCKSDB",
                                         log_state="ROCKSDB", command=["ctest"], timeout=10,
                                         diagnostics_dir=None)
            if returncode:
                with self.assertRaisesRegex(AssertionError, "server shutdown failed"):
                    run_fixture(args)
                report.assert_called_once()
            else:
                run_fixture(args)
                report.assert_not_called()
            cleanup.assert_called_once_with(server)
            client.close.assert_called_once()
            client.admin.command.assert_any_call("shutdown", force=True)

    def test_passing_tests_do_not_hide_a_server_crash(self):
        self.check_server_exit(-11)

    def test_passing_tests_require_clean_shutdown(self):
        self.check_server_exit(0)

    def test_ci_enables_all_runtime_phases(self):
        source = Path(__file__).resolve().parents[2]
        workflow = yaml.safe_load((source / ".github/workflows/ci.yml").read_text())
        job = workflow["jobs"]["cmake-build-smoke"]
        self.assertNotIn("strategy", job)
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        self.assertIn("-DELOQDOC_REGISTER_INTEGRATION_TESTS=ON", commands)
        for phase in ("integration", "jstests", "tpcc"):
            self.assertIn("cmake_ci_runtime.sh build/cmake-ci " + phase, commands)
        runtime = (source / ".github/scripts/cmake_ci_runtime.sh").read_text()
        self.assertIn("--suites=eloq_basic,eloq_core", runtime)
        self.assertIn("--no-tests=error", runtime)
        self.assertIn('--server "$build_dir/eloqdoc"', runtime)
        shell = (source / ".github/scripts/build_cmake_test_shell.sh").read_text()
        self.assertIn("install-shell", shell)
        self.assertNotIn("install-core", shell)
        self.assertNotIn("install-servers", shell)


if __name__ == "__main__":
    unittest.main()
