#!/bin/bash
set -exo pipefail

export WORKSPACE=$PWD
export AWS_PAGER=""

# Prepare SSH for private submodules (if provided)
mkdir -p ~/.ssh
if [ -n "${GIT_SSH_KEY:-}" ]; then
  echo "$GIT_SSH_KEY" > ~/.ssh/id_rsa
  chmod 600 ~/.ssh/id_rsa
  ssh-keyscan github.com >> ~/.ssh/known_hosts || true
fi

# Get current user and ensure proper ownership
current_user=$(whoami)
sudo chown -R $current_user $PWD

# Ensure workspace ownership
sudo chown -R $current_user $HOME/workspace 2>/dev/null || true

cd $HOME
ln -s ${WORKSPACE}/eloqdoc_src eloqdoc
cd eloqdoc
ln -s $WORKSPACE/eloq_logservice_src src/mongo/db/modules/eloq/eloq_log_service
pushd src/mongo/db/modules/eloq/tx_service
ln -s $WORKSPACE/raft_host_manager_src raft_host_manager
popd
ELOQDOC_SRC=${PWD}

# Get OS information from /etc/os-release
source /etc/os-release
if [[ "$ID" == "centos" ]] || [[ "$ID" == "rocky" ]]; then
    OS_ID="rhel${VERSION_ID%.*}"
else
    OS_ID="${ID}${VERSION_ID%.*}"
fi
if [[ "$OS_ID" == rhel* ]]; then
    case "$VERSION_ID" in
    7*)
        sudo yum update -y
        sudo yum install rsync -y
        source /opt/rh/devtoolset-11/enable
        g++ --version
        INSTALL_PSQL="sudo yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm && sudo yum install -y postgresql14"
        ;;
    8*)
        sudo dnf update -y
        sudo dnf install rsync -y
        source scl_source enable gcc-toolset-11
        g++ --version
        INSTALL_PSQL="sudo dnf install -y postgresql"
        ;;
    9*)
        sudo dnf update -y
        sudo dnf install rsync -y
        INSTALL_PSQL="sudo dnf install -y postgresql"
        # detected dubious ownership
        git config --global --add safe.directory ${WORKSPACE}/eloqdoc_src
        git config --global --add safe.directory ${WORKSPACE}/eloq_logservice_src
        git config --global --add safe.directory ${WORKSPACE}/raft_host_manager_src
        ;;
    esac
elif [[ "$OS_ID" == ubuntu* ]]; then
    sudo apt update -y
    sudo apt install rsync -y
    INSTALL_PSQL="DEBIAN_FRONTEND=noninteractive sudo apt install -y postgresql-client"
fi
export LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64:/usr/lib:/usr/lib64:/lib:/lib64:$LD_LIBRARY_PATH

# Kernel version from the running system
KERNEL_VERSION="$(uname -r || true)"
if [ -z "${KERNEL_VERSION}" ]; then
    KERNEL_VERSION="unknown"
fi
echo "Linux kernel version: ${KERNEL_VERSION}"

case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH=$(uname -m) ;;
esac

# Checkout to the latest tag if TAGGED is set, aligning submodules to release branches
if [ -n "${TAGGED}" ]; then
    TAGGED=$(git tag --sort=-v:refname | head -n 1)
    if [ -z "${TAGGED}" ]; then
        echo "No tags found but TAGGED requested"
        exit 1
    fi
    scripts/git-checkout.sh "${TAGGED}" || true
fi

S3_BUCKET="eloq-release"
S3_PREFIX="s3://${S3_BUCKET}/eloqdoc"

# Require DATA_STORE_TYPE to be provided (no KV_TYPE fallback)
if [ -z "${DATA_STORE_TYPE:-}" ]; then
  echo "DATA_STORE_TYPE must be provided. Supported: ELOQDSS_ROCKSDB_CLOUD_S3, ELOQDSS_ROCKSDB_CLOUD_GCS, ELOQDSS_ROCKSDB"
  exit 1
fi

# Validate and normalize DATA_STORE_TYPE and derive DATA_STORE_ID
if [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
  DATA_STORE_ID="rocks_s3"
elif [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ]; then
  DATA_STORE_ID="rocks_gcs"
elif [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB" ]; then
  DATA_STORE_ID="eloqdss_rocksdb"
else
  echo "Unsupported DATA_STORE_TYPE: ${DATA_STORE_TYPE}. Supported: ELOQDSS_ROCKSDB_CLOUD_S3, ELOQDSS_ROCKSDB_CLOUD_GCS, ELOQDSS_ROCKSDB"
  exit 1
fi

if [ "${ASAN:-OFF}" = "ON" ]; then
    export ASAN_OPTIONS=abort_on_error=1:detect_container_overflow=0:leak_check_at_exit=0
fi

# init destination directory
DEST_DIR="${HOME}/EloqDoc"
mkdir -p ${DEST_DIR}/{bin,lib,etc}
export DEST_DIR

# Define and write LICENSE
LICENSE_CONTENT=$(cat <<EOF
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
)
echo "$LICENSE_CONTENT" >"${DEST_DIR}/LICENSE.txt"

# build eloqdoc
cd $ELOQDOC_SRC
git submodule sync
git submodule update --init --recursive

# Ensure nested submodule sync for log service
if [ -d src/mongo/db/modules/eloq/eloq_log_service ]; then
  pushd src/mongo/db/modules/eloq/eloq_log_service
  git submodule sync
  git submodule update --init --recursive
  popd
fi

copy_libraries() {
    local executable="$1"
    local path="$2"
    libraries=$(ldd "$executable" | awk 'NF==4{print $(NF-1)}{}')
    mkdir -p "$path"
    for lib in $libraries; do
        rsync -avL --ignore-existing "$lib" "$path/"
        libname=$(basename "$lib")
        # Align with nightly: ensure each copied library has rpath set to $ORIGIN
        if [ -f "${path}/${libname}" ]; then
          patchelf --set-rpath '$ORIGIN' "${path}/${libname}" || true
        fi
    done
}

echo "building and installing"
pyenv local 2.7.18
export OPEN_LOG_SERVICE=0 FORK_HM_PROCESS=1

# Configure and build engine via CMake
# Align WITH_ROCKSDB_CLOUD env with DATA_STORE_TYPE
if [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
  export WITH_ROCKSDB_CLOUD=S3
elif [ "${DATA_STORE_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ]; then
  export WITH_ROCKSDB_CLOUD=GCS
else
  unset WITH_ROCKSDB_CLOUD
fi

cmake -G "Unix Makefiles" \
      -S $ELOQDOC_SRC/src/mongo/db/modules/eloq \
      -B $ELOQDOC_SRC/src/mongo/db/modules/eloq/build \
      -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
      -DCMAKE_INSTALL_PREFIX=$DEST_DIR \
      -DCMAKE_CXX_STANDARD=17 \
      -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
      -DCOROUTINE_ENABLED=ON \
      -DEXT_TX_PROC_ENABLED=ON \
      -DSTATISTICS=ON \
      -DUSE_ASAN=${ASAN:-OFF} \
      -DWITH_DATA_STORE=${DATA_STORE_TYPE} \
      -DFORK_HM_PROCESS=ON \
      -DOPEN_LOG_SERVICE=OFF \
      -DWITH_ROCKSDB_CLOUD=$WITH_ROCKSDB_CLOUD
cmake --build $ELOQDOC_SRC/src/mongo/db/modules/eloq/build -j6
cmake --install $ELOQDOC_SRC/src/mongo/db/modules/eloq/build

# Construct variables file
if [ "$ID" == "centos" ]; then
  cat > env.vars <<EOF
ENV = {
  'PATH': '$PATH'
}
EOF
fi

# Build and install MongoDB binaries via scons
export WITH_DATA_STORE=${DATA_STORE_TYPE}
export CXX=`which g++`
export CC=`which gcc`

SCONS_VARIANT=${BUILD_TYPE}
env OPEN_LOG_SERVICE=$OPEN_LOG_SERVICE FORK_HM_PROCESS=$FORK_HM_PROCESS WITH_DATA_STORE=$DATA_STORE_TYPE \
python2 buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=${SCONS_VARIANT} \
    LIBPATH=/usr/local/lib \
    CFLAGS="-Wno-nonnull" \
    CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
    CXX=${CXX} \
    CC=${CC} \
    $( [ "$ID" == "centos" ] && echo "--variables-files=env.vars" ) \
    --build-dir=#build \
    --prefix=$DEST_DIR \
    $( if [ "${BUILD_TYPE}" = "Debug" ]; then echo --dbg=on --opt=off; elif [ "${BUILD_TYPE}" = "RelWithDebInfo" ]; then echo --dbg=off --opt=on; else echo --dbg=off --opt=on; fi ) \
    $( [ "${BUILD_TYPE}" = "Release" ] && echo --release --lto ) \
    --allocator=system \
    --link-model=dynamic \
    --install-mode=hygienic \
    --disable-warnings-as-errors \
    -j4 \
    install-core

# Collect runtime libraries for binaries
copy_libraries ${DEST_DIR}/bin/mongo ${DEST_DIR}/lib
copy_libraries ${DEST_DIR}/bin/mongod ${DEST_DIR}/lib
if [ -f ${DEST_DIR}/lib/libstorage_eloq.so ]; then
  copy_libraries ${DEST_DIR}/lib/libstorage_eloq.so ${DEST_DIR}/lib
fi

# Collect host_manager if present
if [ -f ${DEST_DIR}/bin/host_manager ]; then
  copy_libraries ${DEST_DIR}/bin/host_manager ${DEST_DIR}/lib
fi

# Build dss_server and include in tarball
# Map DATA_STORE_TYPE to DSS-compatible values (any ELOQDSS_* builds DSS; others skip)
if [[ "${DATA_STORE_TYPE}" == ELOQDSS_* ]]; then
  DSS_TYPE="${DATA_STORE_TYPE}"
else
  DSS_TYPE=""
fi

if [ -n "${DSS_TYPE}" ]; then
  DSS_SRC_DIR="${ELOQDOC_SRC}/src/mongo/db/modules/eloq/store_handler/eloq_data_store_service"
  cd "${DSS_SRC_DIR}"
  mkdir -p build && cd build
  cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DWITH_DATA_STORE=${DSS_TYPE} -DUSE_ONE_ELOQDSS_PARTITION_ENABLED=OFF
  cmake --build . --config ${BUILD_TYPE} -j${NCORE:-4}
  copy_libraries dss_server ${DEST_DIR}/lib
  mv dss_server ${DEST_DIR}/bin/
  cd "${ELOQDOC_SRC}"
fi

# Fix rpath for executables
patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/mongo
patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/mongod
if [ -f ${DEST_DIR}/bin/host_manager ]; then
  patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/host_manager
fi
if [ -f ${DEST_DIR}/bin/dss_server ]; then
  patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/dss_server
fi

# Preload libmimalloc and libbrpc at launch.
patchelf --remove-needed libmimalloc.so.2 ${DEST_DIR}/bin/mongod
patchelf --remove-needed libbrpc.so ${DEST_DIR}/bin/mongod
patchelf --add-needed libbrpc.so ${DEST_DIR}/bin/mongod
patchelf --add-needed libmimalloc.so.2 ${DEST_DIR}/bin/mongod

# Config files
cp ${ELOQDOC_SRC}/concourse/artifact/${DATA_STORE_TYPE}/* ${DEST_DIR}/etc

cd $HOME
tar -czvf eloqdoc.tar.gz -C $DEST_DIR .

# Tarball naming and upload (align with eloqkv)
if [ -n "${TAGGED}" ]; then
    DOC_TARBALL="eloqdoc-${TAGGED}-${OS_ID}-${ARCH}.tar.gz"
    # optional record
    eval ${INSTALL_PSQL}
    SQL="INSERT INTO tx_release VALUES ('eloqdoc', '${ARCH}', '${OS_ID}', '${DATA_STORE_ID}', $(echo ${TAGGED} | tr '.' ',')) ON CONFLICT DO NOTHING"
    psql postgresql://${PG_CONN}/eloq_release?sslmode=require -c "${SQL}" || true
else
    DOC_TARBALL="eloqdoc-${OUT_NAME}-${OS_ID}-${ARCH}.tar.gz"
fi
aws s3 cp eloqdoc.tar.gz ${S3_PREFIX}/${DATA_STORE_ID}/${DOC_TARBALL}
if [ -n "${CLOUDFRONT_DIST:-}" ]; then
    aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DIST} --paths "/eloqdoc/${DATA_STORE_ID}/${DOC_TARBALL}"
fi

# clean up eloqdoc build artifacts
rm -rf eloqdoc.tar.gz
cd $ELOQDOC_SRC
rm -rf src/mongo/db/modules/eloq/build
rm -rf build
rm -rf ${DEST_DIR}
