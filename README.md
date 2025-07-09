# EloqDoc  
A MongoDB-compatible, high-performance, elastic, distributed document database.

[![GitHub Stars](https://img.shields.io/github/stars/eloqdata/eloqdoc?style=social)](https://github.com/eloqdata/eloqdoc/stargazers)
---

## Overview
EloqDoc is a high-performance, elastic, distributed transactional document database with MongoDB compability. Built on top of [Data Substrate](https://www.eloqdata.com/blog/2024/08/11/data-substrate), it leverages a decoupled storage and compute architecture to deliver fast scaling, ACID transaction support, and efficient resource utilization.

EloqDoc eliminates the need for sharding components like `mongos` in MongoDB, offering a simpler, more powerful distributed database experience. It’s ideal for workloads requiring rapid scaling, high write throughput, and flexible resource management.

EloqDoc is a fork of MongoDB 4.0.3 that replaces the WiredTiger storage engine with the Eloq storage engine. It is distributed under the GNU Affero General Public License (AGPL).

Explore [EloqDoc](https://www.eloqdata.com/product/eloqdoc) website for more details.

👉 **Use Cases**: web applications, ducument store, content management systems — anywhere you need MongoDB compatibility **but** demand distributed performance and elasticity.

---

## Key Features

### ⚙️ MongoDB Compatibility
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
docker pull eloqdata/eloqdoc-build-ubuntu2404:latest
```

Or, you can manually run the following script to install dependencies on your local machine.

```bash
bash scripts/install_dependency_ubuntu2404.sh
```

### 2. Initialize Submodules
Fetch the code and initialize submodules:

```bash
git clone https://github.com/eloqdata/eloqdoc.git
git submodule update --init --recursive
```

### 3. Build EloqDoc
Configure and compile with optimized settings:

```bash
# Config RelWithDebInfo build for Eloq dependencies
cmake -S ./src/mongo/db/modules/eloq/ -B ./src/mongo/db/modules/eloq/release_build -DCMAKE_BUILD_TYPE=RelWithDebInfo
# Or Debug build
# cmake -S ./src/mongo/db/modules/eloq/ -B ./src/mongo/db/modules/eloq/debug_build -DCMAKE_BUILD_TYPE=Debug -DUSE_ASAN=ON

# Build Eloq dependencies
cmake --build src/mongo/db/modules/eloq/release_build/ -j6

# Build Mongo server
./scripts/build_mongo.sh RelWithDebInfo

# Build Mongo client
./scripts/build_mongo.sh Client
```

### 4. Set Up Storage Backend
EloqDoc supports multiple storage backends (e.g., Cassandra). Example setup with Cassandra:

```bash
wget https://archive.apache.org/dist/cassandra/4.1.8/apache-cassandra-4.1.8-bin.tar.gz
tar -zxvf apache-cassandra-4.1.8-bin.tar.gz
./apache-cassandra-4.1.8/bin/cassandra -f
# Verify Cassandra is running:
./apache-cassandra-4.1.8/bin/cqlsh localhost -u cassandra -p cassandra
```

### 5. (Optional) Config EloqDoc
Edit `config/example.conf` with example settings:
```
systemLog:
  verbosity: 2
  destination: file
  path: "~/eloqdoc_test/log.json"
  component:
    ftdc:
      verbosity: 0
net:
  port: 27017
  serviceExecutor: "adaptive"
  enableCoroutine: true
  reservedThreadNum: 2
  adaptiveThreadNum: 1
storage:
  dbPath: "~/eloqdoc_test"
  engine: "eloq"
  eloq:
    txService:
      coreNum: 2
      checkpointerIntervalSec: 10
      nodeMemoryLimitMB: 8192
      realtimeSampling: true
      collectActiveTxTsIntervalSec: 2
      checkpointerDelaySec: 5
    storage:
      keyspaceName: "mongo_test"
      cassHosts: 127.0.0.1
setParameter:
  diagnosticDataCollectionEnabled: false
  disableLogicalSessionCacheRefresh: true
  ttlMonitorEnabled: false
```

### 6. Start EloqDoc Node

```bash
mkdir ~/eloqdoc_test
scripts/run_with_config.sh RelWithDebInfo
```
Or
```bash
mkdir ~/eloqdoc_test
export LD_PRELOAD=/usr/local/lib/libmimalloc.so:/lib/libbrpc.so
./mongod --config config/example.conf
```

### 7. Connect to EloqDoc
```bash
./mongo --eval "db.t1.save({k: 1}); db.t1.find();"
```

---

**Star This Repo ⭐** to Support Our Journey — Every Star Helps Us Reach More Developers!
