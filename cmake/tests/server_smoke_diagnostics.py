"""Failure diagnostics for the Mongo-facing smoke test (standard library only)."""

import shutil
import signal
import subprocess
import sys


def describe_exit(returncode):
    if returncode is None:
        return "still running"
    description = f"exit code {returncode}"
    if returncode < 0:
        try:
            description += f" ({signal.Signals(-returncode).name})"
        except ValueError:
            description += f" (signal {-returncode})"
    return description


def wait_for_clean_shutdown(process, timeout=30):
    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(f"server shutdown timed out after {timeout}s") from exc
    if returncode != 0:
        raise AssertionError("server shutdown failed: " + describe_exit(returncode))


def report_failure(root, returncode, error, diagnostics_dir=None):
    """Print bounded log tails and preserve full logs, without masking the original failure."""
    summary = (f"Fixture: {root}\n"
               f"Failure: {type(error).__name__}: {error}\n"
               f"Server status before fixture cleanup: {describe_exit(returncode)}\n")
    print(summary, file=sys.stderr, flush=True)
    destination = None
    if diagnostics_dir is not None:
        try:
            destination = diagnostics_dir / root.name
            destination.mkdir(parents=True, exist_ok=True)
            (destination / "failure.txt").write_text(summary, encoding="utf-8")
        except OSError as exc:
            print(f"Could not save failure summary: {exc}", file=sys.stderr, flush=True)
            destination = None

    try:
        # Include Mongo output, rejected-option logs, and timestamped glog files. Do not
        # upload data directories, core dumps, credential-bearing configs, or symlinks.
        logs = sorted(path for path in root.iterdir()
                      if not path.is_symlink() and path.is_file()
                      and (path.name.endswith(".log") or ".log." in path.name))
    except OSError as exc:
        print(f"Could not list fixture logs: {exc}", file=sys.stderr, flush=True)
        return
    for path in logs:
        try:
            # Limit console output, but keep the complete file in the artifact.
            with path.open("rb") as log:
                log.seek(0, 2)
                log.seek(max(0, log.tell() - 64 * 1024))
                tail = log.read().decode("utf-8", errors="replace").splitlines()[-200:]
            print(f"--- {path.name} (last up to 200 lines / 64 KiB) ---\n" + "\n".join(tail),
                  file=sys.stderr, flush=True)
        except OSError as exc:
            print(f"Could not read {path.name}: {exc}", file=sys.stderr, flush=True)
        if destination is not None:
            try:
                shutil.copyfile(path, destination / path.name)
            except OSError as exc:
                print(f"Could not preserve {path.name}: {exc}", file=sys.stderr, flush=True)
    if destination is not None:
        print(f"Failure diagnostics: {destination}", file=sys.stderr, flush=True)
