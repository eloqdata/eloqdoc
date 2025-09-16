# How To Run EloqDoc-RocksDBCloud

EloqDoc-RocksDBCloud use RocksDB-Cloud to store data. By using AWS S3 as backend storage, user can quickly start an EloqDoc instance.

Of course, you can download compiled package from our official website, and start from [2](#2. Prepare a S3 bucket or launch a S3 compatible storage server "Prepare a S3 bucket or launch a S3 compatible storage server"). It is easy to compile EloqDoc from scratch.

```bash
git clone --recurse-submodules https://github.com/eloqdata/eloqdoc
```

## 0. Install dependencies

Assume you are using ubuntu24.04 as your development evironment. Then execute `install_dependency-ubuntu2404.sh` to install dependencies

```bash
bash scritps/install_dependency-ubuntu2404.sh
```

If you are using other Linux distribution, follow `install_dependency-ubuntu2404.sh` to install dependencies manually.

## 1. Compile

There are two components to compile: EloqDoc and core libs. EloqDoc itself is compiled with `scons`, and core libs are compiled with `cmake`.

### 1.1 Define an install path

```bash
export INSTALL_PREFIX=/your/install/path/absolute
```

### 1.2 Compile core libs

```bash
cmake -S src/mongo/db/modules/eloq \
      -B src/mongo/db/modules/eloq/build \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX \
      -DWITH_ROCKSDB_CLOUD=S3 \
      -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3
cmake --build src/mongo/db/modules/eloq/build -j8
cmake --install src/mongo/db/modules/eloq/build
```

### 1.3 Compile EloqDoc-RocksDBCloud

The `scons` build tool depends on python2. Switch to python2 environment to run `scons` before continue. `install_dependency-ubuntu2404.sh` should have installed a python-2.7.18 and requirements.

```bash
pyenv global 2.7.18
```

Compile EloqDoc.

```bash
env WITH_ROCKSDB_CLOUD=S3 WITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3 \
python2 buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=RelWithDebInfo \
    LIBPATH=/usr/local/lib \
    CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
    --build-dir=#build \
    --prefix=$INSTALL_PREFIX \
    --disable-warnings-as-errors \
    -j8 \
    install-core
```

All executable files shall install to `$INSTALL_PREFIX/bin`, and all libs shall install to `$INSTALL_PREFIX/lib`.

## 2. Prepare a S3 bucket or launch a S3 compatible storage server

Of course you need to specify a S3 bucket to EloqDoc-RocksDBCloud to store data. If you don't have one, you could use a S3 compatible storage server, for example `minio`.

```bash
mkdir minio-service && cd minio-service
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
nohup ./minio server ./data &> minio.out &
```

## 3. Prepare deploy directory and configuration file

Assume you want to deploy EloqDoc at `$HOME/eloqdoc-cloud` and your home path is `/home/eloq`.

```bash
mkdir -p eloqdoc-cloud/etc eloqdoc-cloud/db eloqdoc-cloud/logs
```

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/mongod.conf` to `eloqdoc-cloud/etc`, and use your favorite editor to modify it.

* Set `systemLog.path` to `/home/eloq/eloqdoc-cloud/logs/mongod.log`.
* Set `storage.dbPath` to `/home/eloq/eloqdoc-cloud/db`.
* Adjust `storage.eloq.adaptiveThreadNum`, `storage.eloq.reservedThreadNum`, and `storage.eloq.txService.nodeMemoryLimitMB` based on your machine to achieve best performance.
* Set  `storage.eloq.storage.awsAccessKeyId`, `storage.eloq.storage.awsSecretKey`, `storage.eloq.storage.rocksdbCloud.*`and `storage.eloq.txService.txlogRocksDBCloud.*` based on your S3 resource.

## 4. Launch EloqDoc-RocksDBCloud

If you want to run EloqDoc-RocksDBCloud compiled by yourself.

```bash
export LD_PRELOAD=/usr/local/lib/libmimalloc.so:/usr/lib/libbrpc.so
export PATH=$INSTALL_PREFIX/bin:$PATH
nohup mongod --config eloqdoc-cloud/etc/mongod.conf &> eloqdoc-cloud/logs/mongod.out &
```

If you want to run our official package, and suppose that you have uncompress the package to `$HOME/opt/eloqdoc` .

```bash
export LD_PRELOAD=$HOME/opt/eloqdoc/lib/libmimalloc.so.2:$HOME/opt/eloqdoc/lib/libbrpc.so
export PATH=$HOME/opt/eloqdoc/bin:$PATH
nohup mongod --config eloqdoc-cloud/etc/mongod.conf &> eloqdoc-cloud/logs/mongod.out &
```

## 5. Connect to EloqDoc-RocksDBCloud

```bash
mongo --eval "db.t1.save({k: 1}); db.t1.find();"
```
