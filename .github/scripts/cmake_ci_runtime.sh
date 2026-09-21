#!/usr/bin/env bash
# One disposable server and S3 fixture per test phase. No build work runs here.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
build_dir=$(realpath "${1:?usage: $0 cmake-build-dir integration|jstests|tpcc}")
phase=${2:?usage: $0 cmake-build-dir integration|jstests|tpcc}
cd "$ELOQDOC_BASE_PATH"
runtime_env

# Read the actual configured backend; never silently test a different storage engine.
cache_value() { sed -n "s/^$1:[^=]*=//p" "$build_dir/CMakeCache.txt"; }
data_store=$(cache_value WITH_DATA_STORE)
log_state=$(cache_value WITH_LOG_STATE)
port=27017
case "$phase" in
  integration)
    if [ "$(cache_value ELOQDOC_REGISTER_INTEGRATION_TESTS)" != ON ]; then
      echo "Configure with -DELOQDOC_REGISTER_INTEGRATION_TESTS=ON first" >&2
      exit 1
    fi
    connection_string=$(cache_value ELOQDOC_TEST_CONNECTION_STRING)
    if [[ ! "$connection_string" =~ ^(localhost|127\.0\.0\.1):([0-9]+)$ ]]; then
      echo "The managed integration fixture requires a localhost:port connection string" >&2
      exit 1
    fi
    port=${BASH_REMATCH[2]}
    # These suites share failpoints and global server parameters; keep them serial.
    command=(ctest --test-dir "$build_dir" -L '^integration$' --no-tests=error
             --output-on-failure -j1)
    phase_timeout=${INTEGRATION_TIMEOUT_SECONDS:-4800}
    ;;
  jstests)
    setup_python2
    test_shell=${ELOQDOC_TEST_SHELL:?Set ELOQDOC_TEST_SHELL to eloqdoc-cli}
    test_shell=$(realpath "$test_shell")
    export PATH="$(dirname "$test_shell"):$PATH"
    command=(python2 scripts/buildscripts/resmoke.py --mongo="$test_shell"
             --suites=eloq_basic,eloq_core '--shellPort={port}' --jobs=1
             --continueOnFailure '--dbpathPrefix={fixture}/resmoke-data'
             "--reportFile=$build_dir/jstests-results.json")
    phase_timeout=${JSTEST_TIMEOUT_SECONDS:-7200}
    ;;
  tpcc)
    # Reuse exactly the SCons CI workload (two warehouses, transactional driver).
    command=(bash -c 'source "$1/.github/scripts/common.sh"; run_tpcc "$2"'
             bash "$ELOQDOC_BASE_PATH" "$build_dir/tpcc-client")
    phase_timeout=${TPCC_PHASE_TIMEOUT_SECONDS:-2400}
    ;;
  *) echo "Unknown runtime phase: $phase" >&2; exit 1 ;;
esac

finish_runtime() {
  local status=$?
  if [ "$status" -ne 0 ] && [ -n "${RUSTFS_RUN_DIR:-}" ] && [ -f /tmp/rustfs.log ]; then
    mkdir -p "$build_dir/runtime-diagnostics"
    cp /tmp/rustfs.log "$build_dir/runtime-diagnostics/rustfs-${phase}.log"
  fi
  stop_rustfs
  exit "$status"
}
trap finish_runtime EXIT
if needs_s3 "$data_store" "$log_state"; then
  start_rustfs "$S3_ENDPOINT" "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
fi
"${CMAKE_TEST_PYTHON:-/usr/bin/python3}" cmake/tests/server_fixture.py \
  --server "$build_dir/eloqdoc" --data-store "$data_store" --log-state "$log_state" \
  --label "$phase" --timeout "$phase_timeout" \
  --port "$port" \
  --memory-limit-mb "${NODE_MEMORY_LIMIT_MB:-4000}" \
  --diagnostics-dir "$build_dir/runtime-diagnostics" -- "${command[@]}"
