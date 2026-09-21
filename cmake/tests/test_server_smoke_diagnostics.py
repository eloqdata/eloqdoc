#!/usr/bin/env python3
"""Check smoke-test failure reporting with real child exits and disposable log files."""

import contextlib
import io
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from server_smoke_diagnostics import describe_exit, report_failure, wait_for_clean_shutdown


class SmokeDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="eloqdoc-smoke-diagnostics-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "fixture"
        self.root.mkdir()
        self.artifacts = Path(self.temp.name) / "artifacts"
        self.output = io.StringIO()

    def report(self, returncode, error):
        with contextlib.redirect_stderr(self.output):
            report_failure(self.root, returncode, error, self.artifacts)
        return self.output.getvalue()

    def test_clean_exit_is_required(self):
        with subprocess.Popen([sys.executable, "-c", "pass"]) as process:
            wait_for_clean_shutdown(process, timeout=5)
        self.assertEqual(0, process.returncode)

    def test_nonzero_exit_is_reported_and_full_logs_are_preserved(self):
        content = "early log context\n" + "later log context\n" * 250 + "shutdown failure\n"
        (self.root / "server.log").write_text(content)
        with subprocess.Popen([sys.executable, "-c", "raise SystemExit(23)"]) as process:
            with self.assertRaisesRegex(AssertionError, "shutdown failed: exit code 23") as failure:
                wait_for_clean_shutdown(process, timeout=5)
        output = self.report(process.returncode, failure.exception)
        self.assertIn("Server status before fixture cleanup: exit code 23", output)
        self.assertIn("shutdown failure", output)
        self.assertNotIn("early log context", output)
        destination = self.artifacts / self.root.name
        self.assertEqual(content, (destination / "server.log").read_text())
        self.assertIn("exit code 23", (destination / "failure.txt").read_text())

    @unittest.skipUnless(sys.platform != "win32", "POSIX signal exit codes")
    def test_signal_exit_has_number_and_name(self):
        # SIGTERM exercises signal reporting without generating a core dump.
        with subprocess.Popen([sys.executable, "-c",
                               "import os, signal; os.kill(os.getpid(), signal.SIGTERM)"]) as process:
            with self.assertRaisesRegex(AssertionError, r"exit code -15 \(SIGTERM\)"):
                wait_for_clean_shutdown(process, timeout=5)
        self.assertEqual(-signal.SIGTERM, process.returncode)
        self.assertEqual("exit code -11 (SIGSEGV)", describe_exit(-signal.SIGSEGV))
        self.assertEqual("exit code -999 (signal 999)", describe_exit(-999))

    def test_timeout_is_not_reported_as_a_crash(self):
        process = mock.Mock()
        process.wait.side_effect = subprocess.TimeoutExpired("server", 30)
        with self.assertRaisesRegex(AssertionError, "shutdown timed out after 30s") as failure:
            wait_for_clean_shutdown(process, timeout=30)
        self.assertIsInstance(failure.exception.__cause__, subprocess.TimeoutExpired)
        output = self.report(None, failure.exception)
        self.assertIn("Server status before fixture cleanup: still running", output)
        self.assertNotIn("SIGTERM", output)
        process.terminate.assert_not_called()
        process.kill.assert_not_called()

    def test_artifacts_exclude_configs_data_core_files_and_symlinks(self):
        included = ("server.log", "replSet.log", "eloqdb.log.FATAL.20260921.123",
                    "host_manager.log.INFO.20260921.456")
        for name in included:
            (self.root / name).write_text("test log")
        for name in ("data_substrate.cnf", "eloqdoc.yaml", "core.123"):
            (self.root / name).write_text("must not upload")
        (self.root / "db").mkdir()
        (self.root / "db" / "nested.log").write_text("data must not upload")
        (self.root / "linked.log").symlink_to(self.root / "data_substrate.cnf")
        self.report(-11, AssertionError("crashed"))
        self.assertEqual(set(included) | {"failure.txt"},
                         {path.name for path in (self.artifacts / self.root.name).iterdir()})
        self.assertNotIn("must not upload", self.output.getvalue())

    def test_huge_line_and_non_utf8_log_do_not_break_diagnostics(self):
        content = b"x" * (128 * 1024) + b"\xfflast line\n"
        (self.root / "server.log").write_bytes(content)
        output = self.report(1, AssertionError("failure"))
        self.assertIn("last line", output)
        self.assertLess(len(output), 66 * 1024)
        self.assertEqual(content, (self.artifacts / self.root.name / "server.log").read_bytes())

    def test_unwritable_artifact_destination_still_prints_logs(self):
        self.artifacts.write_text("a file blocks directory creation")
        (self.root / "server.log").write_text("shutdown stack trace")
        output = self.report(1, AssertionError("original failure"))
        self.assertIn("Could not save failure summary", output)
        self.assertIn("original failure", output)
        self.assertIn("shutdown stack trace", output)

    def test_copy_failure_does_not_mask_original_failure(self):
        (self.root / "server.log").write_text("shutdown stack trace")
        with mock.patch("server_smoke_diagnostics.shutil.copyfile", side_effect=OSError("disk full")):
            output = self.report(1, AssertionError("original failure"))
        self.assertIn("Could not preserve server.log: disk full", output)
        self.assertIn("original failure", output)
        self.assertIn("shutdown stack trace", output)

    def test_missing_logs_do_not_mask_original_failure(self):
        self.root.rmdir()
        output = self.report(1, AssertionError("original failure"))
        self.assertIn("Could not list fixture logs", output)
        self.assertIn("original failure", output)


if __name__ == "__main__":
    unittest.main()
