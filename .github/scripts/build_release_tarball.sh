#!/usr/bin/env bash
# Build and package one shipped EloqDoc variant, plus its matching LogServer.
#
# Port of concourse/scripts/build_tarball.bash to GitHub Actions. The engine
# build itself is delegated to build_eloqdoc() in common.sh so release and CI
# stay on one code path; this script only adds the release-only steps:
# dss_server, runtime library bundling, rpath fixups, config files, tarballs.
#
# Unlike the Concourse task, the tarballs are left in ${OUTPUT_DIR} for the
# workflow to attach to a GitHub release instead of being pushed to S3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

: "${TAGGED:?TAGGED (version tag) must be set}"
: "${VARIANT_ID:?VARIANT_ID must be set}"
: "${DATA_STORE_TYPE:?DATA_STORE_TYPE must be set}"
: "${WITH_LOG_STATE:?WITH_LOG_STATE must be set}"

BUILD_TYPE="${BUILD_TYPE:-RelWithDebInfo}"
OS_ID="${OS_ID:-ubuntu24}"
OUTPUT_DIR="${OUTPUT_DIR:-${GITHUB_WORKSPACE:-${ELOQDOC_BASE_PATH}}}"

case "$(uname -m)" in
  amd64 | x86_64) ARCH=amd64 ;;
  arm64 | aarch64) ARCH=arm64 ;;
  *) ARCH="$(uname -m)" ;;
esac

DEST_DIR="${HOME}/EloqDoc"
LOG_SRC_DIR="${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/data_substrate/eloq_log_service"
DSS_SRC_DIR="${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service"

mkdir -p "${OUTPUT_DIR}"

# Copy a binary's non-glibc shared-library dependencies next to it, and point
# each copy at its own directory so the tarball is self-contained.
copy_libraries() {
  local executable="$1"
  local path="$2"
  local lib libname

  mkdir -p "${path}"
  ldd "${executable}" | awk 'NF==4{print $(NF-1)}{}' | while read -r lib; do
    [ -e "${lib}" ] || continue
    rsync -aL --ignore-existing "${lib}" "${path}/"
    libname="$(basename "${lib}")"
    if [ -f "${path}/${libname}" ]; then
      patchelf --set-rpath '$ORIGIN' "${path}/${libname}" || true
    fi
  done
}

write_license() {
  cat > "$1" <<'EOF'
License

Copyright (c) 2024 EloqData

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to use,
copy, modify, and distribute the Software, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE, AND NONINFRINGEMENT. IN NO EVENT SHALL ELOQDATA
OR ITS CONTRIBUTORS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT, OR OTHERWISE, ARISING FROM, OUT OF, OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

IMPORTANT: By using this software, you acknowledge that EloqData shall not be
liable for any loss or damage, including but not limited to loss of data, arising
from the use of the software. The responsibility for backing up any data, checking
the software's appropriateness for your needs, and using it within the bounds of
the law lies entirely with you.
EOF
}

# concourse/artifact only ships configs for the three data stores Concourse
# builds. The cloud-GCS data store reuses the cloud-S3 templates.
artifact_config_dir() {
  local base="${ELOQDOC_BASE_PATH}/concourse/artifact"
  if [ -d "${base}/${DATA_STORE_TYPE}" ]; then
    echo "${base}/${DATA_STORE_TYPE}"
  elif [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ] && [ -d "${base}/ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
    echo "${base}/ELOQDSS_ROCKSDB_CLOUD_S3"
  fi
}

echo "::group::Build EloqDoc ${VARIANT_ID} (${DATA_STORE_TYPE}/${WITH_LOG_STATE}, ${ARCH})"
build_eloqdoc "${BUILD_TYPE}" "${DATA_STORE_TYPE}" "${WITH_LOG_STATE}" "${DEST_DIR}"

# The SCons tree alone is ~11 GB and the runner only starts with ~12 GB free.
# Everything needed is already installed into DEST_DIR, so drop the build trees
# before packaging; otherwise tar runs out of disk.
cleanup_build_outputs
echo "::endgroup::"

echo "::group::Package EloqDoc ${VARIANT_ID}"
mkdir -p "${DEST_DIR}/bin" "${DEST_DIR}/lib" "${DEST_DIR}/etc"
write_license "${DEST_DIR}/LICENSE.txt"

# dss_server ships with every ELOQDSS_* data store.
if [[ "${DATA_STORE_TYPE}" == ELOQDSS_* ]]; then
  dss_cmake_args=()
  if [ "${DATA_STORE_TYPE}" = "ELOQDSS_ELOQSTORE" ]; then
    dss_cmake_args+=(-DELOQ_MODULE_ENABLED=ON)
  fi

  rm -rf "${DSS_SRC_DIR}/build"
  cmake -S "${DSS_SRC_DIR}" -B "${DSS_SRC_DIR}/build" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DWITH_DATA_STORE="${DATA_STORE_TYPE}" \
    -DUSE_ONE_ELOQDSS_PARTITION_ENABLED=OFF \
    -DELOQ_THIRD_PARTY_PREFIX="${ELOQ_THIRD_PARTY_PREFIX}" \
    -DELOQ_THIRD_PARTY_REQUIRED="${ELOQ_THIRD_PARTY_REQUIRED}" \
    "${dss_cmake_args[@]}"
  cmake --build "${DSS_SRC_DIR}/build" -j"${BUILD_JOBS}"
  copy_libraries "${DSS_SRC_DIR}/build/dss_server" "${DEST_DIR}/lib"
  cp "${DSS_SRC_DIR}/build/dss_server" "${DEST_DIR}/bin/"
  rm -rf "${DSS_SRC_DIR}/build"
fi

copy_libraries "${DEST_DIR}/bin/eloqdoc-cli" "${DEST_DIR}/lib"
copy_libraries "${DEST_DIR}/bin/eloqdoc" "${DEST_DIR}/lib"
if [ -f "${DEST_DIR}/lib/libstorage_eloq.so" ]; then
  copy_libraries "${DEST_DIR}/lib/libstorage_eloq.so" "${DEST_DIR}/lib"
fi
if [ -f "${DEST_DIR}/bin/host_manager" ]; then
  copy_libraries "${DEST_DIR}/bin/host_manager" "${DEST_DIR}/lib"
fi

for bin in eloqdoc eloqdoc-cli host_manager dss_server; do
  if [ -f "${DEST_DIR}/bin/${bin}" ]; then
    patchelf --set-rpath '$ORIGIN/../lib' "${DEST_DIR}/bin/${bin}"
  fi
done

config_dir="$(artifact_config_dir)"
if [ -n "${config_dir}" ]; then
  cp "${config_dir}"/* "${DEST_DIR}/etc/"
else
  echo "WARNING: no concourse/artifact config directory for ${DATA_STORE_TYPE}; shipping without etc/ templates" >&2
fi

DOC_TARBALL="eloqdoc-${TAGGED}-${VARIANT_ID}-${OS_ID}-${ARCH}.tar.gz"
tar -czf "${OUTPUT_DIR}/${DOC_TARBALL}" -C "${DEST_DIR}" .
echo "packaged ${DOC_TARBALL}"
# Nothing after this point reads DEST_DIR; free it before the LogServer build.
rm -rf "${DEST_DIR}"
log_disk_usage "after packaging EloqDoc"
echo "::endgroup::"

echo "::group::Build and package LogServer ${VARIANT_ID}"
LOG_PKG_DIR="${LOG_SRC_DIR}/LogServer"
rm -rf "${LOG_SRC_DIR}/build" "${LOG_PKG_DIR}"
mkdir -p "${LOG_PKG_DIR}/bin" "${LOG_PKG_DIR}/lib"

cmake -S "${LOG_SRC_DIR}" -B "${LOG_SRC_DIR}/build" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DWITH_LOG_STATE="${WITH_LOG_STATE}" \
  -DWITH_ASAN=OFF \
  -DDISABLE_CODE_LINE_IN_LOG=ON \
  -DELOQ_THIRD_PARTY_PREFIX="${ELOQ_THIRD_PARTY_PREFIX}" \
  -DELOQ_THIRD_PARTY_REQUIRED="${ELOQ_THIRD_PARTY_REQUIRED}"
cmake --build "${LOG_SRC_DIR}/build" --target launch_sv -j"${BUILD_JOBS}"

cp "${LOG_SRC_DIR}/build/launch_sv" "${LOG_PKG_DIR}/bin/"
copy_libraries "${LOG_PKG_DIR}/bin/launch_sv" "${LOG_PKG_DIR}/lib"
patchelf --set-rpath '$ORIGIN/../lib' "${LOG_PKG_DIR}/bin/launch_sv"
write_license "${LOG_PKG_DIR}/LICENSE.txt"

LOG_TARBALL="log-service-${TAGGED}-${VARIANT_ID}-${OS_ID}-${ARCH}.tar.gz"
tar -czf "${OUTPUT_DIR}/${LOG_TARBALL}" -C "${LOG_SRC_DIR}" LogServer
echo "packaged ${LOG_TARBALL}"
echo "::endgroup::"

rm -rf "${LOG_PKG_DIR}" "${LOG_SRC_DIR}/build"
log_disk_usage "after packaging LogServer"

{
  echo "## ${VARIANT_ID} (${ARCH})"
  echo ""
  echo "| Asset | Size |"
  echo "|---|---|"
  printf '| %s | %s |\n' "${DOC_TARBALL}" "$(du -h "${OUTPUT_DIR}/${DOC_TARBALL}" | cut -f1)"
  printf '| %s | %s |\n' "${LOG_TARBALL}" "$(du -h "${OUTPUT_DIR}/${LOG_TARBALL}" | cut -f1)"
} >> "${GITHUB_STEP_SUMMARY:-/dev/stdout}"
