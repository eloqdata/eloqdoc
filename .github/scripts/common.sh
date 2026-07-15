#!/usr/bin/env bash
set -euo pipefail

if [ -n "${GITHUB_WORKSPACE:-}" ]; then
  if [ -z "${ELOQDOC_BASE_PATH:-}" ]; then
    if [ -d "${GITHUB_WORKSPACE}/eloqdoc" ]; then
      export ELOQDOC_BASE_PATH="${GITHUB_WORKSPACE}/eloqdoc"
    else
      export ELOQDOC_BASE_PATH="${GITHUB_WORKSPACE}"
    fi
  fi
  export PY_TPCC_PATH="${PY_TPCC_PATH:-${GITHUB_WORKSPACE}/py_tpcc_src}"
fi

if [ -z "${ELOQDOC_BASE_PATH:-}" ]; then
  export ELOQDOC_BASE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi
export PY_TPCC_PATH="${PY_TPCC_PATH:-${ELOQDOC_BASE_PATH}/../py_tpcc_src}"

if [ -z "${ELOQ_THIRD_PARTY_PREFIX:-}" ]; then
  local_third_party_prefix="${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/data_substrate/third_party/install"
  if [ -d "${local_third_party_prefix}" ]; then
    export ELOQ_THIRD_PARTY_PREFIX="${local_third_party_prefix}"
  else
    export ELOQ_THIRD_PARTY_PREFIX="/opt/eloq/third_party"
  fi
fi
export ELOQ_THIRD_PARTY_REQUIRED="${ELOQ_THIRD_PARTY_REQUIRED:-ON}"
export BUILD_JOBS="${BUILD_JOBS:-$(nproc)}"
[ "${BUILD_JOBS}" -lt 1 ] && BUILD_JOBS=1

runtime_env() {
  export LD_LIBRARY_PATH="${ELOQ_THIRD_PARTY_PREFIX}/lib:${ELOQ_THIRD_PARTY_PREFIX}/lib64:${LD_LIBRARY_PATH:-}"
  export PATH="${ELOQ_THIRD_PARTY_PREFIX}/bin:${PATH}"
}

setup_python2() {
  export PYENV_ROOT="${PYENV_ROOT:-${HOME}/.pyenv}"
  if [ -d "${PYENV_ROOT}" ]; then
    export PATH="${PYENV_ROOT}/bin:${PATH}"
    if command -v pyenv >/dev/null 2>&1; then
      eval "$(pyenv init -)" || true
      pyenv shell 2.7.18 >/dev/null 2>&1 || pyenv global 2.7.18 >/dev/null 2>&1 || true
    fi
  fi

  if ! command -v python2 >/dev/null 2>&1; then
    echo "python2 is required to build EloqDoc. Install Python 2.7 and ensure python2 is on PATH; see docs/how-to-compile.md." >&2
    exit 1
  fi
  python2 --version
}

engine_id() {
  local data_store_type="$1"
  local log_state="$2"

  case "${data_store_type}:${log_state}" in
    ELOQDSS_ROCKSDB:ROCKSDB) echo "rocksdb" ;;
    ELOQDSS_ROCKSDB_CLOUD_S3:ROCKSDB_CLOUD_S3) echo "rocks_s3" ;;
    ELOQDSS_ELOQSTORE:ROCKSDB_CLOUD_S3) echo "eloqstore_s3" ;;
    *)
      echo "Unsupported engine combination: ${data_store_type}/${log_state}" >&2
      return 1
      ;;
  esac
}

needs_minio() {
  local data_store_type="$1"
  local log_state="$2"

  [[ "${log_state}" == "ROCKSDB_CLOUD_S3" || \
     "${data_store_type}" == "ELOQDSS_ROCKSDB_CLOUD_S3" || \
     "${data_store_type}" == "ELOQDSS_ELOQSTORE" ]]
}

write_runtime_configs() {
  local data_store_type="$1"
  local log_state="$2"
  local run_dir="$3"
  local install_prefix="$4"
  local minio_endpoint="$5"
  local minio_access_key="$6"
  local minio_secret_key="$7"
  local bucket_name="$8"
  local bucket_prefix="$9"

  mkdir -p \
    "${run_dir}" \
    "${install_prefix}/log" \
    "${install_prefix}/data/mongo" \
    "${install_prefix}/data/eloq" \
    "${install_prefix}/data/log_service"

  cat > "${run_dir}/eloqdoc.yaml" <<EOF
systemLog:
  verbosity: 0
  destination: file
  path: "${install_prefix}/log/eloqdoc.log"
  component:
    ftdc:
      verbosity: 0
net:
  port: 27017
  bindIp: "0.0.0.0"
  serviceExecutor: "adaptive"
  adaptiveThreadNum: 1
storage:
  dbPath: "${install_prefix}/data/mongo"
  engine: "eloq"
  eloq:
    txService:
      ccProtocol: "OccRead"
setParameter:
  diagnosticDataCollectionEnabled: false
  disableLogicalSessionCacheRefresh: true
  ttlMonitorEnabled: true
EOF

  cat > "${run_dir}/data_substrate.cnf" <<EOF
[local]
core_number=2
checkpoint_interval=10
node_memory_limit_mb=${NODE_MEMORY_LIMIT_MB:-2048}
enable_data_store=true
enable_wal=true
eloq_data_path=${install_prefix}/data/eloq
event_dispatcher_num=1
enable_mvcc=true
logserver_snapshot_interval=60
log_service_data_path=${install_prefix}/data/log_service
EOF

  if [ "${log_state}" = "ROCKSDB_CLOUD_S3" ]; then
    cat >> "${run_dir}/data_substrate.cnf" <<EOF
txlog_rocksdb_cloud_bucket_prefix=${bucket_prefix}
txlog_rocksdb_cloud_bucket_name=${bucket_name}
txlog_rocksdb_cloud_object_path=txlog
txlog_rocksdb_cloud_s3_endpoint_url=${minio_endpoint}
txlog_rocksdb_cloud_sst_file_cache_size=1GB
EOF
  fi

  cat >> "${run_dir}/data_substrate.cnf" <<EOF
[cluster]
tx_ip_port_list=127.0.0.1:16379

[store]
eloq_store_open_files_limit=40960
eloq_store_buffer_pool_size=500MB
eloq_store_pages_per_file_shift=11
EOF

  if [ "${log_state}" = "ROCKSDB_CLOUD_S3" ]; then
    cat >> "${run_dir}/data_substrate.cnf" <<EOF
aws_access_key_id=${minio_access_key}
aws_secret_key=${minio_secret_key}
EOF
  fi

  if [ "${data_store_type}" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
    cat >> "${run_dir}/data_substrate.cnf" <<EOF
rocksdb_cloud_bucket_prefix=${bucket_prefix}
rocksdb_cloud_bucket_name=${bucket_name}
rocksdb_cloud_object_path=dss
rocksdb_cloud_s3_endpoint_url=${minio_endpoint}
EOF
    if [ "${log_state}" != "ROCKSDB_CLOUD_S3" ]; then
      cat >> "${run_dir}/data_substrate.cnf" <<EOF
aws_access_key_id=${minio_access_key}
aws_secret_key=${minio_secret_key}
EOF
    fi
  elif [ "${data_store_type}" = "ELOQDSS_ELOQSTORE" ]; then
    cat >> "${run_dir}/data_substrate.cnf" <<EOF
eloq_store_cloud_provider=aws
eloq_store_cloud_endpoint=${minio_endpoint}
eloq_store_cloud_store_path=${bucket_prefix}${bucket_name}/eloqstore
eloq_store_cloud_access_key=${minio_access_key}
eloq_store_cloud_secret_key=${minio_secret_key}
eloq_store_cloud_verify_ssl=false
eloq_store_cloud_request_threads=2
EOF
  fi
}

build_eloqdoc() {
  local build_type="$1"
  local data_store_type="$2"
  local log_state="$3"
  local install_prefix="${4:-${ELOQDOC_BASE_PATH}/install}"

  runtime_env
  setup_python2

  cd "${ELOQDOC_BASE_PATH}"
  rm -rf src/mongo/db/modules/eloq/build build "${install_prefix}"
  mkdir -p "${install_prefix}"
  export DEST_DIR="${install_prefix}"

  cmake -G Ninja \
    -S "${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq" \
    -B "${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/build" \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
    -DCMAKE_INSTALL_PREFIX="${install_prefix}" \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_BUILD_TYPE="${build_type}" \
    -DEXT_TX_PROC_ENABLED=ON \
    -DSTATISTICS=ON \
    -DELOQ_MODULE_ENABLED=ON \
    -DUSE_ASAN=OFF \
    -DWITH_DATA_STORE="${data_store_type}" \
    -DWITH_LOG_STATE="${log_state}" \
    -DWITH_LOG_SERVICE=ON \
    -DFORK_HM_PROCESS=ON \
    -DELOQ_THIRD_PARTY_PREFIX="${ELOQ_THIRD_PARTY_PREFIX}" \
    -DELOQ_THIRD_PARTY_REQUIRED="${ELOQ_THIRD_PARTY_REQUIRED}"
  cmake --build "${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/build" -j"${BUILD_JOBS}"
  cmake --install "${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/build"

  export WITH_DATA_STORE="${data_store_type}"
  export WITH_LOG_STATE="${log_state}"
  export FORK_HM_PROCESS=1
  export CXX="$(command -v g++)"
  export CC="$(command -v gcc)"

  local dbg_flags=(--dbg=off --opt=on)
  if [ "${build_type}" = "Debug" ]; then
    dbg_flags=(--dbg=on --opt=off)
  fi

  local scons_arch_flags=""
  case "$(uname -m)" in
    aarch64|arm64) scons_arch_flags="-march=armv8-a+crc" ;;
  esac

  local scons_third_party_include="-idirafter ${ELOQ_THIRD_PARTY_PREFIX}/include"
  local scons_cflags="${scons_third_party_include} ${scons_arch_flags} -Wno-nonnull"
  local scons_cxxflags="${scons_third_party_include} ${scons_arch_flags} -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move"
  local scons_libpath="${ELOQ_THIRD_PARTY_PREFIX}/lib ${ELOQ_THIRD_PARTY_PREFIX}/lib64"
  local scons_cache_args=()
  if [ -n "${SCONS_CACHE_DIR:-}" ]; then
    mkdir -p "${SCONS_CACHE_DIR}"
    echo "Using SCons cache at ${SCONS_CACHE_DIR}"
    scons_cache_args=(--cache="${SCONS_CACHE_MODE:-nolinked}" --cache-dir="${SCONS_CACHE_DIR}")
  fi

  env FORK_HM_PROCESS="${FORK_HM_PROCESS}" \
      WITH_DATA_STORE="${WITH_DATA_STORE}" \
      WITH_LOG_STATE="${WITH_LOG_STATE}" \
      ELOQ_THIRD_PARTY_PREFIX="${ELOQ_THIRD_PARTY_PREFIX}" \
    python2 scripts/buildscripts/scons.py \
      "${scons_cache_args[@]}" \
      MONGO_VERSION=4.0.3 \
      VARIANT_DIR="${build_type}" \
      CFLAGS="${scons_cflags}" \
      CXXFLAGS="${scons_cxxflags}" \
      CPPDEFINES="ELOQ_MODULE_ENABLED EXT_TX_PROC_ENABLED" \
      LIBPATH="${scons_libpath}" \
      CXX="${CXX}" \
      CC="${CC}" \
      --build-dir=#build \
      --prefix="${install_prefix}" \
      "${dbg_flags[@]}" \
      --allocator=system \
      --link-model=dynamic \
      --install-mode=hygienic \
      --disable-warnings-as-errors \
      -j"${BUILD_JOBS}" \
      install-core

  if [ -n "${SCONS_CACHE_DIR:-}" ] && [ -d "${SCONS_CACHE_DIR}" ]; then
    python2 scripts/buildscripts/scons_cache_prune.py \
      --cache-dir="${SCONS_CACHE_DIR}" \
      --cache-size="${SCONS_CACHE_SIZE_GB:-2}" \
      --prune-ratio="${SCONS_CACHE_PRUNE_RATIO:-0.8}" || true
  fi
}

cleanup_build_outputs() {
  rm -rf \
    "${ELOQDOC_BASE_PATH}/build" \
    "${ELOQDOC_BASE_PATH}/src/mongo/db/modules/eloq/build"
}

start_minio() {
  local endpoint="$1"
  local access_key="$2"
  local secret_key="$3"

  case "$(uname -m)" in
    x86_64) MINIO_ARCH=amd64 ;;
    aarch64|arm64) MINIO_ARCH=arm64 ;;
    *) echo "Unsupported arch $(uname -m) for MinIO" >&2; return 1 ;;
  esac

  cd "${GITHUB_WORKSPACE:-/tmp}"
  wget -q "https://dl.min.io/server/minio/release/linux-${MINIO_ARCH}/minio"
  chmod +x minio
  mkdir -p /tmp/minio_data
  MINIO_ROOT_USER="${access_key}" MINIO_ROOT_PASSWORD="${secret_key}" \
    ./minio server /tmp/minio_data --address :9900 --console-address :9901 \
    >/tmp/minio.log 2>&1 &
  MINIO_PID=$!
  export MINIO_PID

  for _ in $(seq 1 30); do
    if curl -sf "${endpoint}/minio/health/live" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${MINIO_PID}" 2>/dev/null; then
      cat /tmp/minio.log
      return 1
    fi
    sleep 1
  done

  cat /tmp/minio.log
  return 1
}

stop_minio() {
  if [ -n "${MINIO_PID:-}" ]; then
    kill "${MINIO_PID}" 2>/dev/null || true
    wait "${MINIO_PID}" 2>/dev/null || true
  fi
  rm -rf /tmp/minio_data
  rm -f "${GITHUB_WORKSPACE:-/tmp}/minio"
}

launch_eloqdoc() {
  local run_dir="$1"
  local install_prefix="$2"

  runtime_env
  export LD_LIBRARY_PATH="${install_prefix}/lib:${LD_LIBRARY_PATH:-}"
  export PATH="${install_prefix}/bin:${PATH}"

  mkdir -p \
    "${install_prefix}/log" \
    "${install_prefix}/data/mongo" \
    "${install_prefix}/data/eloq" \
    "${install_prefix}/data/log_service"
  nohup "${install_prefix}/bin/eloqdoc" \
    --config="${run_dir}/eloqdoc.yaml" \
    --data_substrate_config="${run_dir}/data_substrate.cnf" \
    >"${install_prefix}/log/eloqdoc.out" 2>&1 &
  ELOQDOC_PID=$!
  export ELOQDOC_PID
}

wait_for_eloqdoc() {
  local install_prefix="$1"

  for _ in $(seq 1 60); do
    if "${install_prefix}/bin/eloqdoc-cli" --eval "db.runCommand({ping: 1})" \
        >/dev/null 2>&1; then
      return 0
    fi
    if [ -n "${ELOQDOC_PID:-}" ] && ! kill -0 "${ELOQDOC_PID}" 2>/dev/null; then
      tail -n 300 "${install_prefix}/log/eloqdoc.out" || true
      return 1
    fi
    sleep 1
  done

  tail -n 300 "${install_prefix}/log/eloqdoc.out" || true
  return 1
}

shutdown_eloqdoc() {
  local install_prefix="$1"

  "${install_prefix}/bin/eloqdoc-cli" admin --eval "db.shutdownServer()" >/dev/null 2>&1 || true
  if [ -n "${ELOQDOC_PID:-}" ]; then
    wait "${ELOQDOC_PID}" 2>/dev/null || true
  fi
}

run_jstests() {
  local install_prefix="$1"

  cd "${ELOQDOC_BASE_PATH}"
  env LD_LIBRARY_PATH="${install_prefix}/lib:${LD_LIBRARY_PATH:-}" \
      PATH="${install_prefix}/bin:${PATH}" \
    python2 scripts/buildscripts/resmoke.py \
      --mongo="${install_prefix}/bin/eloqdoc-cli" \
      --suites=eloq_basic,eloq_core \
      --shellPort=27017 \
      --continueOnFailure
}

run_tpcc() {
  local install_prefix="$1"

  if [ ! -d "${PY_TPCC_PATH}/pytpcc" ]; then
    echo "py-tpcc checkout not found at ${PY_TPCC_PATH}" >&2
    return 1
  fi

  cd "${PY_TPCC_PATH}/pytpcc"
  pip3 install pymongo==4.13.2
  ln -sfn "${ELOQDOC_BASE_PATH}/concourse/scripts/pytpcc.cfg" mongodb.config
  python3 tpcc.py --config=mongodb.config --reset --no-execute --no-load mongodb
  python3 tpcc.py --config=mongodb.config --no-execute --warehouses 2 --clients 2 mongodb
  python3 tpcc.py --config=mongodb.config --no-load --warehouses 2 --clients 10 \
    --duration "${TPCC_DURATION_SECONDS:-600}" mongodb >./tpcc-run.log 2>&1
  tail -n 1000 ./tpcc-run.log
}

dump_ci_failure_logs() {
  local rc="$1"
  local failed_command="$2"
  local install_prefix="${ELOQDOC_INSTALL_PREFIX:-${ELOQDOC_BASE_PATH}/install}"

  echo "CI failed with rc=${rc}; command=${failed_command}"
  if [ -d "${install_prefix}/log" ]; then
    find "${install_prefix}/log" -maxdepth 1 -type f -print -exec sh -c \
      'echo "===== $1 ====="; tail -n 300 "$1" || true' sh {} \;
  fi
  if [ -f /tmp/minio.log ]; then
    echo "===== /tmp/minio.log ====="
    tail -n 300 /tmp/minio.log || true
  fi
}
