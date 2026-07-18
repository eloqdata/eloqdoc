#!/usr/bin/env bash
set -Eexo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

MINIO_ENDPOINT=${1:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state [all|build|jstests|tpcc]}
MINIO_ACCESS_KEY=${2:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state [all|build|jstests|tpcc]}
MINIO_SECRET_KEY=${3:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state [all|build|jstests|tpcc]}
DATA_STORE_TYPE=${4:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state [all|build|jstests|tpcc]}
WITH_LOG_STATE=${5:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state [all|build|jstests|tpcc]}
CI_PHASE=${6:-${CI_PHASE:-all}}

BUILD_TYPE=${BUILD_TYPE:?BUILD_TYPE env var not set}
CI_MODE=${CI_MODE:-pr}
ENGINE_ID="$(engine_id "${DATA_STORE_TYPE}" "${WITH_LOG_STATE}")"
export ELOQDOC_INSTALL_PREFIX="${ELOQDOC_BASE_PATH}/install-${BUILD_TYPE}-${ENGINE_ID}"

case "${CI_PHASE}" in
  all|build|jstests|tpcc) ;;
  *) echo "Unsupported CI phase: ${CI_PHASE}" >&2; exit 2 ;;
esac

trap 'rc=$?; failed_command=$BASH_COMMAND; set +x; if [ "$rc" -ne 0 ]; then dump_ci_failure_logs "$rc" "$failed_command"; fi; shutdown_eloqdoc "$ELOQDOC_INSTALL_PREFIX"; stop_minio; exit "$rc"' EXIT

ulimit -n 1000000 || true
ulimit -l || true
ulimit -c unlimited || true
echo '/tmp/core.%e.%p.%t' >/proc/sys/kernel/core_pattern 2>/dev/null || true

git config --global --add safe.directory '*'
cd "${ELOQDOC_BASE_PATH}"
bash scripts/checkout_product_submodules.sh

echo "CI_MODE=${CI_MODE} CI_PHASE=${CI_PHASE} BUILD_TYPE=${BUILD_TYPE} WITH_DATA_STORE=${DATA_STORE_TYPE} WITH_LOG_STATE=${WITH_LOG_STATE}"

if [ "${CI_PHASE}" != "build" ] && needs_minio "${DATA_STORE_TYPE}" "${WITH_LOG_STATE}"; then
  start_minio "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"
fi

RUN_DIR="${ELOQDOC_BASE_PATH}/.github/runtime/${BUILD_TYPE}-${ENGINE_ID}"
BUCKET_ENGINE_ID="${ENGINE_ID//_/-}"
BASE_BUCKET_NAME="eloqdoc-${CI_MODE}-${BUCKET_ENGINE_ID}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
BUCKET_PREFIX="gh-"
CURRENT_BUCKET_NAME=""

configure_runtime() {
  local bucket_suffix="$1"
  CURRENT_BUCKET_NAME="${BASE_BUCKET_NAME}-${bucket_suffix}"
  write_runtime_configs \
    "${DATA_STORE_TYPE}" \
    "${WITH_LOG_STATE}" \
    "${RUN_DIR}" \
    "${ELOQDOC_INSTALL_PREFIX}" \
    "${MINIO_ENDPOINT}" \
    "${MINIO_ACCESS_KEY}" \
    "${MINIO_SECRET_KEY}" \
    "${BASE_BUCKET_NAME}-${bucket_suffix}" \
    "${BUCKET_PREFIX}"
}

reset_runtime_data() {
  rm -rf "${ELOQDOC_INSTALL_PREFIX}/data"
  mkdir -p \
    "${ELOQDOC_INSTALL_PREFIX}/data/mongo" \
    "${ELOQDOC_INSTALL_PREFIX}/data/eloq" \
    "${ELOQDOC_INSTALL_PREFIX}/data/log_service" \
    "${ELOQDOC_INSTALL_PREFIX}/log"
}

run_build_phase() {
  configure_runtime jstests
  build_eloqdoc "${BUILD_TYPE}" "${DATA_STORE_TYPE}" "${WITH_LOG_STATE}" "${ELOQDOC_INSTALL_PREFIX}"
  cleanup_build_outputs
}

run_jstests_phase() {
  configure_runtime jstests
  reset_runtime_data
  launch_eloqdoc "${RUN_DIR}" "${ELOQDOC_INSTALL_PREFIX}" \
    "${CURRENT_BUCKET_NAME}" \
    "${BUCKET_PREFIX}"
  wait_for_eloqdoc "${ELOQDOC_INSTALL_PREFIX}"
  run_jstests "${ELOQDOC_INSTALL_PREFIX}"
  shutdown_eloqdoc "${ELOQDOC_INSTALL_PREFIX}"
}

run_tpcc_phase() {
  configure_runtime tpcc
  reset_runtime_data
  launch_eloqdoc "${RUN_DIR}" "${ELOQDOC_INSTALL_PREFIX}" \
    "${CURRENT_BUCKET_NAME}" \
    "${BUCKET_PREFIX}"
  wait_for_eloqdoc "${ELOQDOC_INSTALL_PREFIX}"
  run_tpcc "${ELOQDOC_INSTALL_PREFIX}"
}

case "${CI_PHASE}" in
  all)
    run_build_phase
    run_jstests_phase
    run_tpcc_phase
    ;;
  build) run_build_phase ;;
  jstests) run_jstests_phase ;;
  tpcc) run_tpcc_phase ;;
esac

echo "CI phase ${CI_PHASE} completed successfully for ${CI_MODE} ${BUILD_TYPE} ${DATA_STORE_TYPE}/${WITH_LOG_STATE}"
