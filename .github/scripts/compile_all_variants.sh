#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

cd "${ELOQDOC_BASE_PATH}"
git config --global --add safe.directory '*'
bash scripts/checkout_product_submodules.sh

VARIANTS=(
  "rocksdb|ELOQDSS_ROCKSDB|ROCKSDB"
  "rocks_s3|ELOQDSS_ROCKSDB_CLOUD_S3|ROCKSDB_CLOUD_S3"
  "eloqstore_s3|ELOQDSS_ELOQSTORE|ROCKSDB_CLOUD_S3"
)

summary_file="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
{
  echo "## EloqDoc compile matrix"
  echo ""
  echo "| Variant | WITH_DATA_STORE | WITH_LOG_STATE | Result | Duration |"
  echo "|---|---|---|---|---|"
} >> "${summary_file}"

overall_status=0
for entry in "${VARIANTS[@]}"; do
  IFS='|' read -r id data_store log_state <<< "${entry}"
  install_prefix="${ELOQDOC_BASE_PATH}/install-compile-${id}"

  echo "::group::Compile ${id} (${data_store}/${log_state})"
  start=$(date +%s)
  status="pass"
  if ! build_eloqdoc "RelWithDebInfo" "${data_store}" "${log_state}" "${install_prefix}"; then
    status="FAIL"
    overall_status=1
  fi
  end=$(date +%s)
  dur=$(( end - start ))
  echo "::endgroup::"
  printf '| %s | %s | %s | %s | %ds |\n' \
    "${id}" "${data_store}" "${log_state}" "${status}" "${dur}" >> "${summary_file}"

  rm -rf "${install_prefix}" "${ELOQDOC_BASE_PATH}/build" \
    "${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/build"
done

if [ "${overall_status}" -ne 0 ]; then
  echo "One or more variants failed to compile." >&2
fi
exit "${overall_status}"
