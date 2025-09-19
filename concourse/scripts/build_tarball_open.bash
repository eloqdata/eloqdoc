#!/bin/bash
set -exo pipefail

# Build EloqDoc tarball with OPEN_LOG_SERVICE enabled on Ubuntu 24.04

export WORKSPACE=$PWD
export AWS_PAGER=""
export DEBIAN_FRONTEND=noninteractive

# Minimal tools needed before dependency installer
apt-get update && apt-get install -y sudo openssh-client rsync patchelf

current_user=$(whoami)
sudo chown -R "$current_user" "$PWD"

# Setup SSH for private submodules if provided
if [ -n "${GIT_SSH_KEY:-}" ]; then
  mkdir -p ~/.ssh
  echo "${GIT_SSH_KEY}" > ~/.ssh/id_rsa
  chmod 600 ~/.ssh/id_rsa
  ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null 
fi

# Link workspace inputs to expected layout
cd "$HOME"
ln -sfn "${WORKSPACE}/eloqdoc_src" eloqdoc
cd eloqdoc
ln -sfn "$WORKSPACE/logservice_src" src/mongo/db/modules/eloq/log_service 
pushd src/mongo/db/modules/eloq/tx_service
ln -sfn "$WORKSPACE/raft_host_manager_src" raft_host_manager 
popd
ELOQDOC_SRC=${PWD}

# Determine OS identifier for artifact naming
source /etc/os-release
if [[ "$ID" == "centos" ]] || [[ "$ID" == "rocky" ]]; then
  OS_ID="rhel${VERSION_ID%.*}"
else
  OS_ID="${ID}${VERSION_ID%.*}"
fi

case $(uname -m) in
  amd64|x86_64) ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) ARCH=$(uname -m) ;;
esac

# Install dependencies (Ubuntu 24.04) using repository script
# Provide a temp directory as required by the installer
TMP_DEPS_DIR="$HOME/deps"
mkdir -p "$TMP_DEPS_DIR"
pushd "$ELOQDOC_SRC"
bash scripts/install_dependency_ubuntu2404.sh "$TMP_DEPS_DIR"
popd

# Ensure pyenv available in current shell
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init - bash)" 
pyenv local 2.7.18 || pyenv global 2.7.18 

# Build configuration
: "${BUILD_TYPE:=Debug}"
: "${ASAN:=OFF}"
: "${NCORE:=8}"

# Require DATA_STORE_TYPE to be provided
if [ -z "${DATA_STORE_TYPE:-}" ]; then
  echo "DATA_STORE_TYPE must be provided. Supported: ELOQDSS_ROCKSDB_CLOUD_S3, ELOQDSS_ROCKSDB_CLOUD_GCS, ELOQDSS_ROCKSDB"
  exit 1
fi

if [ "${ASAN}" = "ON" ]; then
  export ASAN_OPTIONS=abort_on_error=1:detect_container_overflow=0:leak_check_at_exit=0
fi

# Destination
DEST_DIR="${HOME}/EloqDoc"
mkdir -p "${DEST_DIR}/{bin,lib,etc}"

# License
cat >"${DEST_DIR}/LICENSE.txt" <<EOF
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

# Sync submodules
cd "$ELOQDOC_SRC"
git submodule sync
git submodule update --init --recursive
if [ -d src/mongo/db/modules/eloq/log_service ]; then
  pushd src/mongo/db/modules/eloq/log_service
  git submodule sync
  git submodule update --init --recursive
  popd
fi

copy_libraries() {
  local executable="$1"
  local path="$2"

  # Report missing dependencies (do not exit)
  local missing
  missing=$(ldd "$executable" | awk '/=> not found/ {print $1}')
  if [ -n "$missing" ]; then
    echo "Missing libraries for $executable:"
    echo "$missing"
  fi

  # Collect only absolute paths
  local libraries
  libraries=$(ldd "$executable" | awk '{for (i=1;i<=NF;i++) if ($i ~ /^\//) print $i}')

  mkdir -p "$path"
  for lib in $libraries; do
    rsync -avL --ignore-existing "$lib" "$path/"
    libname=$(basename "$lib")
    if [ -f "${path}/${libname}" ]; then
      patchelf --set-rpath '$ORIGIN' "${path}/${libname}"
    fi
  done
}

echo "Configuring and building EloqDoc (OPEN_LOG_SERVICE=ON)"
pyenv local 2.7.18 
export OPEN_LOG_SERVICE=1 FORK_HM_PROCESS=0

# Configure and build engine via CMake
# Extra cmake args for log service RocksDB cloud backend selection
CMAKE_EXTRA_ARGS=""
if [ "${DATA_STORE_TYPE:-}" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
  CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DWITH_ROCKSDB_CLOUD=S3"
  export WITH_ROCKSDB_CLOUD=S3
elif [ "${DATA_STORE_TYPE:-}" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ]; then
  CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DWITH_ROCKSDB_CLOUD=GCS"
  export WITH_ROCKSDB_CLOUD=GCS
else
  CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DWITH_ROCKSDB_CLOUD=OFF"
  export WITH_ROCKSDB_CLOUD=0
fi

cmake -G "Unix Makefiles" \
      -S "$ELOQDOC_SRC/src/mongo/db/modules/eloq" \
      -B "$ELOQDOC_SRC/src/mongo/db/modules/eloq/build" \
      -DCMAKE_INSTALL_PREFIX="$DEST_DIR" \
      -DCMAKE_CXX_STANDARD=17 \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
      -DUSE_ASAN=${ASAN} \
      -DWITH_DATA_STORE=${DATA_STORE_TYPE} \
      ${CMAKE_EXTRA_ARGS}
cmake --build "$ELOQDOC_SRC/src/mongo/db/modules/eloq/build" -j${NCORE}
cmake --install "$ELOQDOC_SRC/src/mongo/db/modules/eloq/build"

echo "Building MongoDB via scons (OPEN_LOG_SERVICE=1)"
export WITH_DATA_STORE=${DATA_STORE_TYPE}
SCONS_VARIANT=${BUILD_TYPE}
python2 buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=${SCONS_VARIANT} \
    LIBPATH=/usr/local/lib \
    CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
    --build-dir=#build \
    --prefix=$DEST_DIR \
    $( if [ "${BUILD_TYPE}" = "Debug" ]; then echo --dbg=on --opt=off; elif [ "${BUILD_TYPE}" = "RelWithDebInfo" ]; then echo --dbg=off --opt=on; else echo --dbg=off --opt=on; fi ) \
    $( [ "${BUILD_TYPE}" = "Release" ] && echo --release --lto ) \
    --allocator=system \
    --link-model=dynamic \
    --install-mode=hygienic \
    --disable-warnings-as-errors \
    -j${NCORE} \
    install-core

# Collect runtime libraries
copy_libraries ${DEST_DIR}/bin/mongo ${DEST_DIR}/lib
copy_libraries ${DEST_DIR}/bin/mongod ${DEST_DIR}/lib
if [ -f ${DEST_DIR}/lib/libstorage_eloq.so ]; then
  copy_libraries ${DEST_DIR}/lib/libstorage_eloq.so ${DEST_DIR}/lib
fi
if [ -f ${DEST_DIR}/bin/host_manager ]; then
  copy_libraries ${DEST_DIR}/bin/host_manager ${DEST_DIR}/lib
fi

# Fix rpath for executables
patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/mongo 
patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/mongod 
if [ -f ${DEST_DIR}/bin/host_manager ]; then
  patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/host_manager 
fi

# Preload libmimalloc and libbrpc at launch.
patchelf --remove-needed libmimalloc.so.2 ${DEST_DIR}/bin/mongod
patchelf --remove-needed libbrpc.so ${DEST_DIR}/bin/mongod
patchelf --add-needed libbrpc.so ${DEST_DIR}/bin/mongod
patchelf --add-needed libmimalloc.so.2 ${DEST_DIR}/bin/mongod

# Config files
cp ${ELOQDOC_SRC}/concourse/artifact/${DATA_STORE_TYPE}/* ${DEST_DIR}/etc 


# Cleanup
cd "$ELOQDOC_SRC"
rm -rf src/mongo/db/modules/eloq/build 
rm -rf build 
rm -rf ${DEST_DIR} 

echo "Build completed."


