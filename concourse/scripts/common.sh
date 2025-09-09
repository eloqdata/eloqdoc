#!/bin/bash
set -exo pipefail

# Set python2 as the default python version
pyenv global 2.7.18

# Set MinIO credentials and endpoint
pip3 install minio
MINIO_ENDPOINT="http://172.17.0.1:9000"
MINIO_ACCESS_KEY="35cxOCh64Ef1Mk5U1bgU"
MINIO_SECRET_KEY="M6oJQWdFCr27TUUS40wS6POQzbKhbFTHG9bRayoC"

# Setup Minio mc Client command
mc alias set minio_server ${MINIO_ENDPOINT} ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}

# Make coredump dir writable.
if [ ! -d "/var/crash" ]; then sudo mkdir -p /var/crash; fi
sudo chmod 777 /var/crash
ulimit -n 1000000
ulimit -c unlimited

# Prepare the build and execution environment
export ASAN_OPTIONS=abort_on_error=1:leak_check_at_exit=0
export PREFIX="/home/eloq/workspace/mongo/install"

# Clears data for the log and storage services from the shared RocksDB Cloud bucket.
# A single bucket with distinct path prefixes is used for both services
# to bypass bucket creation rate limits and global name uniqueness constraints.
cleanup_all() {
      if [ $# -lt 3 ]; then
            echo "Error: bucket_name and bucket_prefix parameters are required"
            echo "Usage: cleanup_all <data_dir> <bucket_name> <bucket_prefix>"
            exit 1
      fi
      local data_dir="$1"
      local bucket_name="$2"
      local bucket_prefix="$3"
      local full_bucket_name="${bucket_prefix}${bucket_name}"
      
      echo "Cleaning buckets: $full_bucket_name"
      mc rb minio_server/${full_bucket_name} --force

      echo "Cleaning data folder: ${data_dir}"
      rm -rf ${data_dir}/*
}

compile_and_install() {
      cmake_version=$(cmake --version 2>&1)
      if [[ $? -eq 0 ]]; then
            echo "cmake version: $cmake_version"
      else
            echo "fail to get cmake version"
      fi

      export ASAN_OPTIONS=abort_on_error=1:leak_check_at_exit=0
      echo "cmake compile and install eloq."
      cmake -G "Ninja" \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
            -S src/mongo/db/modules/eloq \
            -B src/mongo/db/modules/eloq/build \
            -DCMAKE_INSTALL_PREFIX="$PREFIX" \
            -DCMAKE_CXX_STANDARD=17 \
            -DCMAKE_CXX_FLAGS_DEBUG_INIT="-Wno-error -fPIC" \
            -DCMAKE_BUILD_TYPE=Debug \
            -DEXT_TX_PROC_ENABLED=ON \
            -DSTATISTICS=ON \
            -DUSE_ASAN=OFF \
            -DWITH_ROCKSDB_CLOUD=OFF \
            -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3

      cmake --build src/mongo/db/modules/eloq/build
      cmake --install src/mongo/db/modules/eloq/build

      echo "scons compile and install mongo."

      # Detect CPU cores for optimal parallel builds
      # CPU_CORE_SIZE=$(nproc 2>/dev/null || grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 4)
      CPU_CORE_SIZE=4
      OPEN_LOG_SERVICE=ON python2 buildscripts/scons.py MONGO_VERSION=4.0.3 \
            VARIANT_DIR=Debug \
            LIBPATH=/usr/local/lib \
            CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
            --build-dir=#build \
            --prefix="$PREFIX" \
            --dbg=on \
            --opt=off \
            --allocator=system \
            --link-model=dynamic \
            --install-mode=hygienic \
            --disable-warnings-as-errors \
            -j"${CPU_CORE_SIZE}" \
            install-core
}

compile_and_install_ent() {
      cmake_version=$(cmake --version 2>&1)
      if [[ $? -eq 0 ]]; then
            echo "cmake version: $cmake_version"
      else
            echo "fail to get cmake version"
      fi

      export ASAN_OPTIONS=abort_on_error=1:leak_check_at_exit=0
      echo "cmake compile and install eloq."
      cmake -G "Ninja" \
            -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
            -S src/mongo/db/modules/eloq \
            -B src/mongo/db/modules/eloq/build \
            -DCMAKE_INSTALL_PREFIX="$PREFIX" \
            -DCMAKE_CXX_STANDARD=17 \
            -DCMAKE_CXX_FLAGS_DEBUG_INIT="-Wno-error -fPIC" \
            -DCMAKE_BUILD_TYPE=Debug \
            -DEXT_TX_PROC_ENABLED=ON \
            -DSTATISTICS=ON \
            -DUSE_ASAN=OFF \
            -DWITH_ROCKSDB_CLOUD=S3 \
            -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 \
            -DFORK_HM_PROCESS=ON \
            -DOPEN_LOG_SERVICE=OFF

      cmake --build src/mongo/db/modules/eloq/build
      cmake --install src/mongo/db/modules/eloq/build

      echo "scons compile and install mongo."

      # Detect CPU cores for optimal parallel builds
      # CPU_CORE_SIZE=$(nproc 2>/dev/null || grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 4)
      CPU_CORE_SIZE=4
      env OPEN_LOG_SERVICE=0 WITH_ROCKSDB_CLOUD=S3 FORK_HM_PROCESS=1 \
      python2 buildscripts/scons.py MONGO_VERSION=4.0.3 \
            VARIANT_DIR=Debug \
            LIBPATH=/usr/local/lib \
            CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
            --build-dir=#build \
            --prefix="$PREFIX" \
            --dbg=on \
            --opt=off \
            --allocator=system \
            --link-model=dynamic \
            --install-mode=hygienic \
            --disable-warnings-as-errors \
            -j"${CPU_CORE_SIZE}" \
            install-core
}

launch_mongod() {
      if [ $# -lt 2 ]; then
            echo "Error: bucket_name and bucket_prefix parameters are required"
            echo "Usage: launch_mongod <bucket_name> <bucket_prefix>"
            exit 1
      fi
      local bucket_name="$1"
      local bucket_prefix="$2"
      echo "launch mongod with bucket name: $bucket_name, bucket prefix: $bucket_prefix"
      export LD_PRELOAD=/usr/local/lib/libmimalloc.so
      mkdir -p "$PREFIX/log" "$PREFIX/data"
      sed -i "s|rocksdbCloudEndpointUrl: \"http://[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+:[0-9]\+\"|rocksdbCloudEndpointUrl: \"${MINIO_ENDPOINT}\"|g" /home/eloq/workspace/mongo/concourse/scripts/store_rocksdb_cloud.yaml
      sed -i "s|txlogRocksDBCloudEndpointUrl: \"http://[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+:[0-9]\+\"|txlogRocksDBCloudEndpointUrl: \"${MINIO_ENDPOINT}\"|g" /home/eloq/workspace/mongo/concourse/scripts/store_rocksdb_cloud.yaml
      nohup $PREFIX/bin/mongod \
            --config ./concourse/scripts/store_rocksdb_cloud.yaml \
            --eloqRocksdbCloudBucketName="$bucket_name" \
            --eloqRocksdbCloudBucketPrefix="$bucket_prefix" \
            --eloqTxlogRocksDBCloudBucketName="$bucket_name" \
            --eloqTxlogRocksDBCloudBucketPrefix="$bucket_prefix" \
            &>$PREFIX/log/mongod.out &
}

launch_mongod_fast() {
      if [ $# -lt 2 ]; then
            echo "Error: bucket_name and bucket_prefix parameters are required"
            echo "Usage: launch_mongod_fast <bucket_name> <bucket_prefix>"
            exit 1
      fi
      local bucket_name="$1"
      local bucket_prefix="$2"
      echo "launch mongod fast with bucket name: $bucket_name, bucket prefix: $bucket_prefix"
      export LD_PRELOAD=/usr/local/lib/libmimalloc.so
      mkdir -p "$PREFIX/log" "$PREFIX/data"
      sed -i "s|rocksdbCloudEndpointUrl: \"http://[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+:[0-9]\+\"|rocksdbCloudEndpointUrl: \"${MINIO_ENDPOINT}\"|g" /home/eloq/workspace/mongo/concourse/scripts/store_rocksdb_cloud.yaml
      sed -i "s|txlogRocksDBCloudEndpointUrl: \"http://[0-9]\+\.[0-9]\+\.[0-9]\+\.[0-9]\+:[0-9]\+\"|txlogRocksDBCloudEndpointUrl: \"${MINIO_ENDPOINT}\"|g" /home/eloq/workspace/mongo/concourse/scripts/store_rocksdb_cloud.yaml
      nohup $PREFIX/bin/mongod \
            --config ./concourse/scripts/store_rocksdb_cloud.yaml \
            --eloqSkipRedoLog=1 \
            --eloqRocksdbCloudBucketName="$bucket_name" \
            --eloqRocksdbCloudBucketPrefix="$bucket_prefix" \
            --eloqTxlogRocksDBCloudBucketName="$bucket_name" \
            --eloqTxlogRocksDBCloudBucketPrefix="$bucket_prefix" \
            &>$PREFIX/log/mongod.out &
}

shutdown_mongod() {
      $PREFIX/bin/mongo admin --eval "db.shutdownServer()"
}

try_connect() {
      set +e
      mongo_ready=0
      for ((i = 1; i < 30; i++)); do
            $PREFIX/bin/mongo --eval "db.runCommand({ping: 1})" &>/dev/null
            if [ $? -eq 0 ]; then
                  echo "MongoDB is up and running!"
                  mongo_ready=1
                  break
            else
                  echo "MongoDB is not ready. Retrying in 1 second..."
                  sleep 1
            fi
      done
      set -e

      if [ $mongo_ready -eq 0 ]; then
            echo "Failed to connect to MongoDB after 30 seconds."
            tail -n200 $PREFIX/log/mongod.out
            exit 1
      fi
}

run_jstests() {
      echo "run jstests"
      env PATH=$PREFIX/bin:$PATH \
      python2 buildscripts/resmoke.py --mongo=$PREFIX/bin/mongo --suites=eloq_basic,eloq_core --shellPort=27017 --continueOnFailure
}

run_tpcc() {
      pushd /home/$current_user/workspace/py-tpcc/pytpcc
      echo "install pymongo"
      pip3 install pymongo==4.13.2
      echo "pytpcc reset"
      ln -s /home/$current_user/workspace/mongo/concourse/scripts/pytpcc.cfg mongodb.config
      python3 tpcc.py --config=mongodb.config --reset --no-execute --no-load mongodb
      echo "pytpcc load"
      python3 tpcc.py --config=mongodb.config --no-execute --warehouses 2 --clients 2 mongodb 
      echo "pytpcc run"
      python3 tpcc.py --config=mongodb.config --no-load --warehouses 2 --clients 10 --duration 600 mongodb &> ./tpcc-run.log
      tail -n1000 ./tpcc-run.log
      popd
}
