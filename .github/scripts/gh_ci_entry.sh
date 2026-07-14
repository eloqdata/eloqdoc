#!/usr/bin/env bash
set -Eexo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

MINIO_ENDPOINT=${1:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state}
MINIO_ACCESS_KEY=${2:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state}
MINIO_SECRET_KEY=${3:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state}
DATA_STORE_TYPE=${4:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state}
WITH_LOG_STATE=${5:?usage: $0 minio_endpoint minio_access_key minio_secret_key data_store_type log_state}

BUILD_TYPE=${BUILD_TYPE:?BUILD_TYPE env var not set}
CI_MODE=${CI_MODE:-pr}
ENGINE_ID="$(engine_id "${DATA_STORE_TYPE}" "${WITH_LOG_STATE}")"
export ELOQDOC_INSTALL_PREFIX="${ELOQDOC_BASE_PATH}/install-${BUILD_TYPE}-${ENGINE_ID}"

trap 'rc=$?; failed_command=$BASH_COMMAND; set +x; if [ "$rc" -ne 0 ]; then dump_ci_failure_logs "$rc" "$failed_command"; fi; shutdown_eloqdoc "$ELOQDOC_INSTALL_PREFIX"; stop_minio; exit "$rc"' EXIT

ulimit -n 1000000 || true
ulimit -l || true
ulimit -c unlimited || true
echo '/tmp/core.%e.%p.%t' >/proc/sys/kernel/core_pattern 2>/dev/null || true

git config --global --add safe.directory '*'
cd "${ELOQDOC_BASE_PATH}"
bash scripts/checkout_product_submodules.sh

echo "CI_MODE=${CI_MODE} BUILD_TYPE=${BUILD_TYPE} WITH_DATA_STORE=${DATA_STORE_TYPE} WITH_LOG_STATE=${WITH_LOG_STATE}"

if needs_minio "${DATA_STORE_TYPE}" "${WITH_LOG_STATE}"; then
  start_minio "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"
fi

RUN_DIR="${ELOQDOC_BASE_PATH}/.github/runtime/${BUILD_TYPE}-${ENGINE_ID}"
BUCKET_ENGINE_ID="${ENGINE_ID//_/-}"
BUCKET_NAME="eloqdoc-${CI_MODE}-${BUCKET_ENGINE_ID}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
BUCKET_PREFIX="gh-"

write_runtime_configs \
  "${DATA_STORE_TYPE}" \
  "${WITH_LOG_STATE}" \
  "${RUN_DIR}" \
  "${ELOQDOC_INSTALL_PREFIX}" \
  "${MINIO_ENDPOINT}" \
  "${MINIO_ACCESS_KEY}" \
  "${MINIO_SECRET_KEY}" \
  "${BUCKET_NAME}" \
  "${BUCKET_PREFIX}"

build_eloqdoc "${BUILD_TYPE}" "${DATA_STORE_TYPE}" "${WITH_LOG_STATE}" "${ELOQDOC_INSTALL_PREFIX}"

rm -rf "${ELOQDOC_INSTALL_PREFIX}/data"
mkdir -p \
  "${ELOQDOC_INSTALL_PREFIX}/data/mongo" \
  "${ELOQDOC_INSTALL_PREFIX}/data/eloq" \
  "${ELOQDOC_INSTALL_PREFIX}/data/log_service" \
  "${ELOQDOC_INSTALL_PREFIX}/log"
launch_eloqdoc "${RUN_DIR}" "${ELOQDOC_INSTALL_PREFIX}"
wait_for_eloqdoc "${ELOQDOC_INSTALL_PREFIX}"
run_jstests "${ELOQDOC_INSTALL_PREFIX}"

shutdown_eloqdoc "${ELOQDOC_INSTALL_PREFIX}"
rm -rf "${ELOQDOC_INSTALL_PREFIX}/data"
mkdir -p \
  "${ELOQDOC_INSTALL_PREFIX}/data/mongo" \
  "${ELOQDOC_INSTALL_PREFIX}/data/eloq" \
  "${ELOQDOC_INSTALL_PREFIX}/data/log_service" \
  "${ELOQDOC_INSTALL_PREFIX}/log"
launch_eloqdoc "${RUN_DIR}" "${ELOQDOC_INSTALL_PREFIX}"
wait_for_eloqdoc "${ELOQDOC_INSTALL_PREFIX}"
run_tpcc "${ELOQDOC_INSTALL_PREFIX}"

echo "CI completed successfully for ${CI_MODE} ${BUILD_TYPE} ${DATA_STORE_TYPE}/${WITH_LOG_STATE}"
