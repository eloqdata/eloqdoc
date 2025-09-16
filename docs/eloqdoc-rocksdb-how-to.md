# How To Run EloqDoc-RocksDB

EloqDoc-RocksDB use RocksDB to store data. Run EloqDoc with RocksDB is the easiest way to experience with EloqDoc.

Of course, you can download compiled package from our official website, and start from [2](#2. Prepare deploy directory and configuration file "Prepare deploy directory and configuration file"). It is easy to compile EloqDoc from scratch.

```bash
git clone --recurse-submodules https://github.com/eloqdata/eloqdoc
```

## 0. Install dependencies

Assume you are using ubuntu24.04 as your development evironment. Then execute `install_dependency-ubuntu2404.sh` to install dependencies.

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
      -DWITH_DATA_STORE=ELOQDSS_ROCKSDB
cmake --build src/mongo/db/modules/eloq/build -j8
cmake --install src/mongo/db/modules/eloq/build
```

### 1.3 Compile EloqDoc-RocksDB

The `scons` build tool depends on python2.7. Switch to python2.7 environment to run `scons` before continue. `install_dependency-ubuntu2404.sh` should have installed a python-2.7.18 and requirements.

```bash
pyenv global 2.7.18
```

Compile EloqDoc.

```bash
env WITH_DATA_STORE=ELOQDSS_ROCKSDB \
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

All executable files shall installed to `$INSTALL_PREFIX/bin`, and all libs shall installed to `$INSTALL_PREFIX/lib`.

## 2. Prepare deploy directory and configuration file

Assume you want to deploy EloqDoc to `$HOME/eloqdoc-rocksdb` and your home path is `/home/eloq`.

```bash
mkdir -p eloqdoc-rocksdb/etc eloqdoc-rocksdb/db eloqdoc-rocksdb/logs
```

Copy `concourse/artifact/ELOQDSS_ROCKSDB/mongod.conf` to `eloqdoc-rocksdb/etc`, and use your favorite editor to modify it.

* Set `systemLog.path` to `/home/eloq/eloqdoc-rocksdb/logs/mongod.log`.
* Set `storage.dbPath` to `/home/eloq/eloqdoc-rocksdb/db`.
* Adjust `storage.eloq.adaptiveThreadNum`, `storage.eloq.reservedThreadNum`, and `storage.eloq.txService.nodeMemoryLimitMB` based on your machine to achieve best performance.

## 3. Launch EloqDoc-RocksDB

If you want to run EloqDoc-RocksDB compiled by yourself.

```bash
export LD_PRELOAD=/usr/local/lib/libmimalloc.so:/usr/lib/libbrpc.so
export PATH=$INSTALL_PREFIX/bin:$PATH
nohup mongod --config eloqdoc-rocksdb/etc/mongod.conf &> eloqdoc-rocksdb/logs/mongod.out &
```

If you want to run our official package, and suppose that you have uncompressed the package to `$HOME/opt/eloqdoc` .

```bash
export LD_PRELOAD=$HOME/opt/eloqdoc/lib/libmimalloc.so.2:$HOME/opt/eloqdoc/lib/libbrpc.so
export PATH=$HOME/opt/eloqdoc/bin:$PATH
nohup mongod --config eloqdoc-rocksdb/etc/mongod.conf &> eloqdoc-rocksdb/logs/mongod.out &
```

## 4. Connect to EloqDoc-RocksDB

```bash
mongo --eval "db.t1.save({k: 1}); db.t1.find();"
```
