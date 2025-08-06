#!/bin/bash
set -exo pipefail

export WORKSPACE=$PWD
sudo chown -R eloq $PWD
cd $HOME
ln -s ${WORKSPACE}/eloqdoc_src eloqdoc
ln -s ${WORKSPACE}/raft_host_manager_src eloqdoc/src/mongo/db/modules/eloq/tx_service/raft_host_manager
ln -s ${WORKSPACE}/eloq_logservice_src eloqdoc/src/mongo/db/modules/eloq/eloq_log_service

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
        INSTALL_PSQL="sudo yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm && sudo yum install -y postgresql14"
        INSTALL_MONGO_DEPENDENCY="sudo yum install -y build-essential libssl-dev libcurl4-openssl-dev liblzma-dev python2 python-dev"
        ;;
    8*)
        sudo dnf update -y
        sudo dnf install rsync -y
        source scl_source enable gcc-toolset-11
        INSTALL_PSQL="sudo dnf install -y postgresql"
        INSTALL_MONGO_DEPENDENCY="sudo dnf install -y build-essential libssl-dev libcurl4-openssl-dev liblzma-dev python2 python-dev"
        ;;
    9*)
        sudo dnf update -y
        sudo dnf install rsync -y
        INSTALL_PSQL="sudo dnf install -y postgresql"
        INSTALL_MONGO_DEPENDENCY="sudo dnf install -y build-essential libssl-dev libcurl4-openssl-dev liblzma-dev python2 python-dev"
        # detected dubious ownership
        git config --global --add safe.directory ${WORKSPACE}/eloqdoc_src
        git config --global --add safe.directory ${WORKSPACE}/logservice_src
        git config --global --add safe.directory ${WORKSPACE}/raft_host_manager_src
        ;;
    esac
    g++ --version
elif [[ "$OS_ID" == ubuntu* ]]; then
    sudo apt update -y
    sudo apt install rsync -y
    INSTALL_PSQL="DEBIAN_FRONTEND=noninteractive sudo apt-get install -y postgresql-client"
    INSTALL_MONGO_DEPENDENCY="DEBIAN_FRONTEND=noninteractive sudo apt-get install -y --no-install-recommends build-essential libssl-dev libcurl4-openssl-dev liblzma-dev"
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


cd eloqdoc
MONGO_SRC=$PWD
cd $MONGO_SRC && git submodule update --recursive
ELOQ_ENGINE_SRC=$MONGO_SRC/src/mongo/db/modules/eloq
cd $ELOQ_ENGINE_SRC/eloq_log_service && git pull origin main
cd $ELOQ_ENGINE_SRC/tx_service/raft_host_manager && git pull origin main
cd $MONGO_SRC
copy_libraries() {
    local executable=$1
    local path=$2
    libraries=$(ldd $executable | awk 'NF==4{print $3}{}')
    mkdir -p $path
    for lib in $libraries; do
        cp -n $lib ${path}/
        libname=$(basename $lib)
        patchelf --set-rpath '$ORIGIN' ${path}/${libname}
    done
}

cd $MONGO_SRC && mkdir build

echo "building and installing"
pyenv local 2.7.18
export OPEN_LOG_SERVICE=0 WITH_ROCKSDB_CLOUD=S3 FORK_HM_PROCESS=1
if [ $BUILD_TYPE = "Debug" ]; then
    cmake -G "Unix Makefiles" \
          -S $MONGO_SRC/src/mongo/db/modules/eloq \
          -B $MONGO_SRC/src/mongo/db/modules/eloq/build \
          -DCMAKE_INSTALL_PREFIX=$MONGO_SRC/install \
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
          -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 \
          -DFORK_HM_PROCESS=ON \
          -DOPEN_LOG_SERVICE=OFF
    cmake --build $MONGO_SRC/src/mongo/db/modules/eloq/build -j$NCORE
    cmake --install $MONGO_SRC/src/mongo/db/modules/eloq/build
    python2 buildscripts/scons.py \
        MONGO_VERSION=4.0.3  \
        VARIANT_DIR=Debug \
        LIBPATH=/usr/local/lib \
        CFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull" \
        CXXFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
        --build-dir=#build \
        --prefix=$MONGO_SRC/install \
        --dbg=on \
        --opt=off \
        --allocator=system \
        --link-model=dynamic \
        --install-mode=hygienic \
        --disable-warnings-as-errors \
        -j$NCORE \
        install-core
elif [ $BUILD_TYPE = "RelWithDebInfo" ]; then
    cmake -G "Unix Makefiles" \
          -S $MONGO_SRC/src/mongo/db/modules/eloq \
          -B $MONGO_SRC/src/mongo/db/modules/eloq/build \
          -DCMAKE_INSTALL_PREFIX=$MONGO_SRC/install \
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
          -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3
          -DFORK_HM_PROCESS=ON \
          -DOPEN_LOG_SERVICE=OFF
    cmake --build $MONGO_SRC/src/mongo/db/modules/eloq/build -j$NCORE
    cmake --install $MONGO_SRC/src/mongo/db/modules/eloq/build
    python2 buildscripts/scons.py \
        MONGO_VERSION=4.0.3  \
        VARIANT_DIR=RelWithDebInfo \
        LIBPATH=/usr/local/lib \
        CFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull" \
        CXXFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
        --build-dir=#build \
        --prefix=$MONGO_SRC/install \
        --dbg=off \
        --opt=on \
        --release \
        --allocator=system \
        --link-model=dynamic \
        --install-mode=hygienic \
        --disable-warnings-as-errors \
        -j$NCORE \
        install-core
elif [ $BUILD_TYPE = "Release" ]; then
    cmake -G "Unix Makefiles" \
          -S $MONGO_SRC/src/mongo/db/modules/eloq \
          -B $MONGO_SRC/src/mongo/db/modules/eloq/build \
          -DCMAKE_INSTALL_PREFIX=$MONGO_SRC/install \
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
          -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3
          -DFORK_HM_PROCESS=ON \
          -DOPEN_LOG_SERVICE=OFF
    cmake --build $MONGO_SRC/src/mongo/db/modules/eloq/build -j$NCORE
    cmake --install $MONGO_SRC/src/mongo/db/modules/eloq/build
    python2 buildscripts/scons.py  \
        MONGO_VERSION=4.0.3  \
        VARIANT_DIR=Release \
        LIBPATH=/usr/local/lib \
        CFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull" \
        CXXFLAGS="-march=x86-64 -mtune=generic -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
        --build-dir=#build \
        --prefix=$MONGO_SRC/install \
        --dbg=off \
        --opt=on \
        --release --lto \
        --allocator=system \
        --link-model=dynamic \
        --install-mode=hygienic \
        --disable-warnings-as-errors \
        -j$NCORE \
        install-core
fi

copy_libraries ${MONGO_SRC}/install/bin/mongo ${MONGO_SRC}/install/lib
copy_libraries ${MONGO_SRC}/install/bin/mongod ${MONGO_SRC}/install/lib
copy_libraries ${MONGO_SRC}/install/bin/host_manager ${MONGO_SRC}/install/lib
copy_libraries ${MONGO_SRC}/install/lib/libstorage_eloq.so ${MONGO_SRC}/install/lib
patchelf --set-rpath '$ORIGIN/../lib' ${MONGO_SRC}/install/bin/mongo
patchelf --set-rpath '$ORIGIN/../lib' ${MONGO_SRC}/install/bin/mongod

mkdir -p ${MONGO_SRC}/install/etc
cp ${MONGO_SRC}/concourse/scripts/mongod.conf ${MONGO_SRC}/install/etc

cd $HOME
tar -czf eloqdoc.tar.gz -C $MONGO_SRC/install .

KVS_ID=$(echo ${KV_TYPE} | tr '[:upper:]' '[:lower:]')
if [ "${KV_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_S3" ]; then
    KVS_ID="rocks_s3"
elif [ "${KV_TYPE}" = "ELOQDSS_ROCKSDB_CLOUD_GCS" ]; then
    KVS_ID="rocks_gcs"
fi

BUILD_TYPE_LOWER=$(echo ${BUILD_TYPE} | tr '[:upper:]' '[:lower:]')

URI_PREFIX="s3://eloq-release/eloqdoc"
TX_TARBALL="${BUILD_TYPE_LOWER}-${OS_ID}-${ARCH}.tar.gz"

aws s3 cp eloqdoc.tar.gz ${URI_PREFIX}/daily/${KVS_ID}/${TX_TARBALL}
if [ -n "$CLOUDFRONT_DIST" ]; then
    aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DIST} --paths "/eloqdoc/daily/${KVS_ID}/${TX_TARBALL}"
fi

# clean up eloqdoc tx build artifacts
rm -rf eloqdoc.tar.gz
cd $MONGO_SRC
rm -rf src/mongo/db/modules/eloq/build
rm -rf build
rm -rf install
