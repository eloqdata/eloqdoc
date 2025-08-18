# EloqDoc

A MongoDB API compatible , high-performance, elastic, distributed document database.

[![GitHub Stars](https://img.shields.io/github/stars/eloqdata/eloqdoc?style=social)](https://github.com/eloqdata/eloqdoc/stargazers)

---

## Overview

EloqDoc is a high-performance, elastic, distributed transactional document database with MongoDB API compability. Built on top of [Data Substrate](https://www.eloqdata.com/blog/2024/08/11/data-substrate), it leverages a decoupled storage and compute architecture to deliver fast scaling, ACID transaction support, and efficient resource utilization.

EloqDoc eliminates the need for sharding components like `mongos` in MongoDB, offering a simpler, more powerful distributed database experience. It’s ideal for workloads requiring rapid scaling, high write throughput, and flexible resource management.

EloqDoc is a fork of MongoDB 4.0.3 that replaces the WiredTiger storage engine with the Eloq storage engine. It is distributed under the GNU Affero General Public License (AGPL).

Explore [EloqDoc](https://www.eloqdata.com/product/eloqdoc) website for more details.

👉 **Use Cases**: web applications, ducument store, content management systems — anywhere you need MongoDB API compatibility **but** demand distributed performance and elasticity.

---

## Key Features

### ⚙️ MongoDB API Compatibility

Seamlessly integrates with MongoDB clients, drivers, and tools, enabling you to use existing MongoDB workflows with a distributed backend.

### 🌐 Distributed Architecture

Supports **multiple writers** and **fast distributed transactions**, ensuring high concurrency and fault tolerance across a cluster without sharding complexity.

### 🔄 Elastic Scalability

- Scales compute and memory **100x faster** than traditional databases by avoiding data movement on disk.
- Scales storage independently, conserving CPU resources for compute-intensive tasks.
- Scales redo logs independently to optimize write throughput.

### 🔥 High-Performance Transactions

Delivers **ACID transaction support** with especially fast distributed transactions, making it suitable for mission-critical applications.

### 🔒 Simplified Distributed Design

Operates as a distributed database without requiring a sharding coordinator (e.g., `mongos`), reducing operational complexity and overhead.

---

## Architecture Highlights

- **Fast Scaling**: Compute and memory scale independently without disk data movement, enabling rapid elasticity for dynamic workloads.
- **Storage Flexibility**: Storage scales separately from compute, optimizing resource allocation and reducing waste.
- **Write Optimization**: Independent redo log scaling boosts write throughput, ideal for high-velocity data ingestion.
- **No Sharding Overhead**: Distributes data natively across the cluster, eliminating the need for additional sharding components.

---

## Run with Tarball

Download the EloqDoc tarball from the [EloqData website](https://www.eloqdata.com/download/eloqdoc).

Follow the [instruction guide](https://www.eloqdata.com/eloqdoc/install-from-binary) to set up and run EloqDoc on your local machine.

---

## Build from Source

Follow these steps to build and run EloqDoc from source.

### 1. Install Dependencies:

It is recommended to use our Docker image with pre-installed dependencies for a quick build and run of EloqDoc.

```bash
docker pull eloqdata/eloq-dev-ci-ubuntu2404:latest
docker run -it --name eloqdoc eloqdata/eloq-dev-ci-ubuntu2404
git clone https://github.com/eloqdata/eloqdoc.git
cd eloqdoc
```
Alternatively, you can also pull the source code in an existing Linux environment (currently, ubuntu2404 is preferred), and manually run the script to install dependencies on your local machine. Notice that this might take a while.

```bash
git clone https://github.com/eloqdata/eloqdoc.git
cd eloqdoc
bash scripts/install_dependency_ubuntu2404.sh
```

### 2. Initialize Submodules

Fetch the code and initialize submodules:

```bash
git clone https://github.com/eloqdata/eloqdoc.git
git submodule update --init --recursive
```

### 3. Build EloqDoc

First, Specify an install path.

```bash
export INSTALL_PREFIX=/your/install/path/absolute
```

Then, compile Eloq dependencies.

```bash
cmake -S src/mongo/db/modules/eloq \
      -B src/mongo/db/modules/eloq/build \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX
cmake --build src/mongo/db/modules/eloq/build -j8
cmake --install src/mongo/db/modules/eloq/build
```

Finally, compile EloqDoc.

```bash
pyenv global 2.7.18
python buildscripts/scons.py MONGO_VERSION=4.0.3 \
    VARIANT_DIR=RelWithDebInfo \
    LIBPATH="/usr/local/lib" \
    CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
    --build-dir=#build \
    --prefix=$INSTALL_PREFIX \
    --disable-warnings-as-errors \
    -j8 \
    install-core
```

### 4. Set Up Storage Backend

EloqDoc use s3 as storage backends. For testing, just deploy a s3 emulator.

```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server ./data
```

### 5. Config EloqDoc

Create a configuration file `mongod.conf` according to `config/example.conf`. Modify `/home/mono` with your home path.
The configuration file specifies `$HOME/eloqdoc-cloud` as deploy directory.

```bash
mkdir ~/eloqdoc-cloud && cd ~/eloqdoc-cloud
mkdir etc db logs
mv ~/mongod.conf etc/

```

### 6. Start EloqDoc Node

```bash
export LD_PRELOAD=/usr/local/lib/libmimalloc.so:/usr/lib/libbrpc.so
export PATH=$INSTALL_PREFIX/bin:$PATH
mongod --config etc/mongod.conf
```


### 7. Connect to EloqDoc

```bash
mongo --eval "db.t1.save({k: 1}); db.t1.find();"
```

---

**Star This Repo ⭐** to Support Our Journey — Every Star Helps Us Reach More Developers!
