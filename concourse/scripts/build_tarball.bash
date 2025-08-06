#!/bin/bash
set -exo pipefail

export WORKSPACE=$PWD
export AWS_PAGER=""

# Get current user and ensure proper ownership
current_user=$(whoami)
sudo chown -R $current_user $PWD

# make coredump dir writable for debugging
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash

ulimit -c unlimited
echo '/var/crash/core.%t.%e.%p' | sudo tee /proc/sys/kernel/core_pattern

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

KERNEL_VERSION="0.0.0"
if [[ "$ID" == "centos" ]] && [[ "$VERSION_ID" == "7" ]]; then
    KERNEL_VERSION="3.10.0"
elif [[ "$ID" == "centos" ]] && [[ "$VERSION_ID" == "8" ]]; then
    KERNEL_VERSION="4.18.0"
elif [[ "$ID" == "rocky" ]] && [[ "$VERSION_ID" == 9.* ]]; then
    KERNEL_VERSION="5.14.0"
elif [[ "$ID" == "ubuntu" ]] ; then
    case "$VERSION_ID" in
    18.*)
        KERNEL_VERSION="4.15.0"
        ;;
    20.*)
        KERNEL_VERSION="5.8.0"
        ;;
    22.*)
        KERNEL_VERSION="5.15.0"
        ;;
    24.*)
        KERNEL_VERSION="6.8.0"
        ;;
    esac
fi
echo "Linux kernel version: ${KERNEL_VERSION}"

case $(uname -m) in
amd64 | x86_64) ARCH=amd64 ;;
arm64 | aarch64) ARCH=arm64 ;;
*) ARCH=$(uname -m) ;;
esac

if [ -n "${TAGGED}" ]; then
    TAGGED=$(git tag --sort=-v:refname | head -n 1)
    if [ -z "${TAGGED}" ]; then
        exit 1
    fi
    git checkout "${TAGGED}"
fi



S3_BUCKET="eloq-release"
S3_PREFIX="s3://${S3_BUCKET}/eloqdoc"
KVS_ID=$(echo ${KV_TYPE} | tr '[:upper:]' '[:lower:]')
if [ "${KV_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
    KVS_ID="rocks_s3"
elif [ "${KV_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ]; then
    KVS_ID="rocks_gcs"
fi

if [ "$ASAN" = "ON" ]; then
    export ASAN_OPTIONS=abort_on_error=1:detect_container_overflow=0:leak_check_at_exit=0
fi

# init destination directory
DEST_DIR="${HOME}/EloqDoc"
mkdir ${DEST_DIR}
mkdir ${DEST_DIR}/bin
mkdir ${DEST_DIR}/lib
mkdir ${DEST_DIR}/conf

# Define the license content for tarball
LICENSE_CONTENT=$(
    cat <<EOF
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

# Write the license content to LICENSE.txt in the destination directory
echo "$LICENSE_CONTENT" >"${DEST_DIR}/LICENSE.txt"

# build eloqdoc
cd $ELOQDOC_SRC
git submodule sync
git submodule update --init --recursive

# Init and sync submodules in required locations
cd src/mongo/db/modules/eloq/eloq_log_service
git submodule sync
git submodule update --init --recursive
cd $ELOQDOC_SRC

copy_libraries() {
    local executable="$1"
    local path="$2"
    libraries=$(ldd "$executable" | awk 'NF==4{print $(NF-1)}{}')
    mkdir -p "$path"
    for lib in $libraries; do
        cp -n "$lib" "$path/"
        libname=$(basename "$lib")
        patchelf --set-rpath '$ORIGIN' "${path}/${libname}"
    done
}

echo "building and installing"
pyenv local 2.7.18
export OPEN_LOG_SERVICE=0 WITH_ROCKSDB_CLOUD=S3 FORK_HM_PROCESS=1

if [ "${BUILD_TYPE:-RelWithDebInfo}" = "Debug" ]; then
    cmake -G "Unix Makefiles" \
          -S $ELOQDOC_SRC/src/mongo/db/modules/eloq \
          -B $ELOQDOC_SRC/src/mongo/db/modules/eloq/build \
          -DCMAKE_INSTALL_PREFIX=$DEST_DIR \
          -DCMAKE_CXX_STANDARD=17 \
          -DCMAKE_CXX_FLAGS_DEBUG_INIT="-march=x86-64 -mtune=generic -Wno-error -fPIC" \
          -DCMAKE_BUILD_TYPE=Debug \
          -DRANGE_PARTITION_ENABLED=ON \
          -DCOROUTINE_ENABLED=ON \
          -DEXT_TX_PROC_ENABLED=ON \
          -DWITH_LOG_SERVICE=ON \
          -DBUILD_WITH_TESTS=ON \
          -DSTATISTICS=ON \
          -DUSE_ASAN=OFF \
          -DWITH_ROCKSDB_CLOUD=S3 \
          -DWITH_DATA_STORE=${KV_TYPE:-ELOQDSS_ROCKSDB_CLOUD_S3} \
          -DFORK_HM_PROCESS=ON \
          -DOPEN_LOG_SERVICE=OFF
    cmake --build $ELOQDOC_SRC/src/mongo/db/modules/eloq/build -j${NCORE:-4}
    cmake --install $ELOQDOC_SRC/src/mongo/db/modules/eloq/build
    python2 buildscripts/scons.py \
        MONGO_VERSION=4.0.3  \
        VARIANT_DIR=Debug \
        LIBPATH=/usr/local/lib \
        CFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull" \
        CXXFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
        --build-dir=#build \
        --prefix=$DEST_DIR \
        --dbg=on \
        --opt=off \
        --allocator=system \
        --link-model=dynamic \
        --install-mode=hygienic \
        --disable-warnings-as-errors \
        -j${NCORE:-4} \
        install-core
elif [ "${BUILD_TYPE:-RelWithDebInfo}" = "RelWithDebInfo" ]; then
    cmake -G "Unix Makefiles" \
          -S $ELOQDOC_SRC/src/mongo/db/modules/eloq \
          -B $ELOQDOC_SRC/src/mongo/db/modules/eloq/build \
          -DCMAKE_INSTALL_PREFIX=$DEST_DIR \
          -DCMAKE_CXX_STANDARD=17 \
          -DCMAKE_CXX_FLAGS_RELWITHDEBINFO_INIT="-march=x86-64 -mtune=generic -Wno-error -fPIC" \
          -DCMAKE_BUILD_TYPE=RelWithDebInfo \
          -DRANGE_PARTITION_ENABLED=ON \
          -DCOROUTINE_ENABLED=ON \
          -DEXT_TX_PROC_ENABLED=ON \
          -DWITH_LOG_SERVICE=ON \
          -DBUILD_WITH_TESTS=ON \
          -DSTATISTICS=ON \
          -DUSE_ASAN=OFF \
          -DWITH_ROCKSDB_CLOUD=S3 \
          -DWITH_DATA_STORE=${KV_TYPE:-ELOQDSS_ROCKSDB_CLOUD_S3} \
          -DFORK_HM_PROCESS=ON \
          -DOPEN_LOG_SERVICE=OFF
    cmake --build $ELOQDOC_SRC/src/mongo/db/modules/eloq/build -j${NCORE:-4}
    cmake --install $ELOQDOC_SRC/src/mongo/db/modules/eloq/build
    python2 buildscripts/scons.py \
        MONGO_VERSION=4.0.3  \
        VARIANT_DIR=RelWithDebInfo \
        LIBPATH=/usr/local/lib \
        CFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull" \
        CXXFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
        --build-dir=#build \
        --prefix=$DEST_DIR \
        --dbg=off \
        --opt=on \
        --release \
        --allocator=system \
        --link-model=dynamic \
        --install-mode=hygienic \
        --disable-warnings-as-errors \
        -j${NCORE:-4} \
        install-core
elif [ "${BUILD_TYPE:-RelWithDebInfo}" = "Release" ]; then
    cmake -G "Unix Makefiles" \
          -S $ELOQDOC_SRC/src/mongo/db/modules/eloq \
          -B $ELOQDOC_SRC/src/mongo/db/modules/eloq/build \
          -DCMAKE_INSTALL_PREFIX=$DEST_DIR \
          -DCMAKE_CXX_STANDARD=17 \
          -DCMAKE_CXX_FLAGS_RELEASE_INIT="-march=x86-64 -mtune=generic -Wno-error -fPIC" \
          -DCMAKE_BUILD_TYPE=Release \
          -DRANGE_PARTITION_ENABLED=ON \
          -DCOROUTINE_ENABLED=ON \
          -DEXT_TX_PROC_ENABLED=ON \
          -DWITH_LOG_SERVICE=ON \
          -DBUILD_WITH_TESTS=ON \
          -DSTATISTICS=ON \
          -DUSE_ASAN=OFF \
          -DWITH_ROCKSDB_CLOUD=S3 \
          -DWITH_DATA_STORE=${KV_TYPE:-ELOQDSS_ROCKSDB_CLOUD_S3} \
          -DFORK_HM_PROCESS=ON \
          -DOPEN_LOG_SERVICE=OFF
    cmake --build $ELOQDOC_SRC/src/mongo/db/modules/eloq/build -j${NCORE:-4}
    cmake --install $ELOQDOC_SRC/src/mongo/db/modules/eloq/build
    python2 buildscripts/scons.py  \
        MONGO_VERSION=4.0.3  \
        VARIANT_DIR=Release \
        LIBPATH=/usr/local/lib \
        CFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull" \
        CXXFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
        --build-dir=#build \
        --prefix=$DEST_DIR \
        --dbg=off \
        --opt=on \
        --release --lto \
        --allocator=system \
        --link-model=dynamic \
        --install-mode=hygienic \
        --disable-warnings-as-errors \
        -j${NCORE:-4} \
        install-core
fi

copy_libraries ${DEST_DIR}/bin/mongo ${DEST_DIR}/lib
copy_libraries ${DEST_DIR}/bin/mongod ${DEST_DIR}/lib
copy_libraries ${DEST_DIR}/lib/libstorage_eloq.so ${DEST_DIR}/lib
patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/mongo
patchelf --set-rpath '$ORIGIN/../lib' ${DEST_DIR}/bin/mongod

mkdir -p ${DEST_DIR}/etc
cp ${ELOQDOC_SRC}/concourse/scripts/mongod.conf ${DEST_DIR}/etc

cd $HOME
tar -czvf eloqdoc.tar.gz -C $DEST_DIR .

BUILD_TYPE_LOWER=$(echo ${BUILD_TYPE:-RelWithDebInfo} | tr '[:upper:]' '[:lower:]')

URI_PREFIX="s3://eloq-release/eloqdoc"
if [ -n "${TAGGED}" ]; then
    DOC_TARBALL="${BUILD_TYPE_LOWER}-${OS_ID}-${ARCH}.tar.gz"
    eval ${INSTALL_PSQL}
    SQL="INSERT INTO doc_release VALUES ('eloqdoc', '${ARCH}', '${OS_ID}', '${KVS_ID}', $(echo ${TAGGED} | tr '.' ',')) ON CONFLICT DO NOTHING"
    psql postgresql://${PG_CONN}/eloq_release?sslmode=require -c "${SQL}" || true
    aws s3 cp eloqdoc.tar.gz ${URI_PREFIX}/release/${KVS_ID}/${DOC_TARBALL}
else
    DOC_TARBALL="${BUILD_TYPE_LOWER}-${OS_ID}-${ARCH}.tar.gz"
    aws s3 cp eloqdoc.tar.gz ${URI_PREFIX}/daily/${KVS_ID}/${DOC_TARBALL}
fi

if [ -n "${CLOUDFRONT_DIST}" ]; then
    if [ -n "${TAGGED}" ]; then
        aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DIST} --paths "/eloqdoc/release/${KVS_ID}/${DOC_TARBALL}"
    else
        aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DIST} --paths "/eloqdoc/daily/${KVS_ID}/${DOC_TARBALL}"
    fi
fi

# clean up eloqdoc build artifacts
rm -rf eloqdoc.tar.gz
cd $ELOQDOC_SRC
rm -rf src/mongo/db/modules/eloq/build
rm -rf ${DEST_DIR}