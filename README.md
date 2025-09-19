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

## Quick Start

### Try EloqDoc-RocksDB Using Official Package

**Step-1**, download the official package for EloqDoc-RocksDB.

```bash
wget -c https://download.eloqdata.com/eloqdoc/eloqdss_rocksdb/eloqdoc-debug-ubuntu22-amd64.tar.gz
```

All released package can be found at [download](https://www.eloqdata.com/download) page.

**Step-2**, uncompress the package to your `$HOME`.

```bash
mkdir $HOME/eloqdoc-rocksdb && tar -xf eloqdoc-debug-ubuntu22-amd64.tar.gz -C $HOME/eloqdoc-rocksdb
```

After uncompress the package, you should see three directories: `bin`, `lib`, and `etc`.
`bin` contains all executable files, `lib` contains all dependencies, and `etc` contains an example configuration file `mongod.conf`. Switch to `eloqdoc-rocksdb` to verify it.

```bash
cd $HOME/eloqdoc-rocksdb && ls
```

**Step-3**, create a data directory and a log directory. Simply place them under `$HOME/eloqdoc-rocksdb`.

```bash
mkdir db logs
```

**Step-4**, modify  `etc/mongod.conf`. Assume your `$HOME` is `/home/eloq`, then

* Set `systemLog.path` to `/home/eloq/eloqdoc-rocksdb/logs/mongod.log`.
* Set `storage.dbPath` to `/home/eloq/eloqdoc-rocksdb/db`.

**Step-5**, start the server with:

```bash
./bin/mongod --config ./etc/mongod.conf
```

**Step-6**, open another terminal and run mongo client.

```bash
./bin/mongo --eval "db.t1.save({k: 1}); db.t1.find();"
```

It should output

```bash
MongoDB shell version v4.0.3
connecting to: mongodb://127.0.0.1:27017
Implicit session: session { "id" : UUID("288393c1-aff6-4a84-ad46-dee6691b361d") }
MongoDB server version: 4.0.3
{ "_id" : ObjectId("68493ec41cc981ea926ec094"), "k" : 1 }
```

### Try EloqDoc-RocksDBCloud Using Official Package

**Step-1**, download the official package for EloqDoc-RocksDBCloud.

```bash
wget -c https://download.eloqdata.com/eloqdoc/rocks_s3/eloqdoc-debug-ubuntu22-amd64.tar.gz
```

All released package can be found at [download](https://www.eloqdata.com/download) page.

**Step-2**, uncompress the package to your `$HOME`.

```bash
mkdir eloqdoc-rocksdbcloud && tar -xf eloqdoc-debug-ubuntu22-amd64.tar.gz -C eloqdoc-rocksdbcloud
```

After uncompress the package, you should see three directories: `bin`, `lib`, and `etc`.
`bin` contains all executable files, `lib` contains all dependencies, and `etc` contains an example configuration file `mongod.conf`. Switch to `eloqdoc-rocksdbcloud` to verify it.

**Step-3**, create a data directory and a log directory. Simply place them under `$HOME/eloqdoc-rocksdbcloud`.

```bash
mkdir db logs
```

**Step-4**, start a S3 emulator, takes `minio` as an exmaple.

```bash
cd $HOME
mkdir minio-service && cd minio-service
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server ./data
```

By default, `minio` listens on `http://127.0.0.1:9000`, whose default credentials is `minioadmin:minioadmin`,.

**Step-5**, go back to `$HOME/eloqdoc-rocksdbcloud` and modify `etc/mongod.conf`. Assume your `$HOME` is `/home/eloq`.

```bash
cd $HOME/eloqdoc-rocksdbcloud
```

* Set `systemLog.path` to `/home/eloq/eloqdoc-rocksdbcloud/logs/mongod.log`.
* Set `storage.dbPath` to `/home/eloq/eloqdoc-rocksdbcloud/db`.
* `etc/mongod.conf` has configured minio as its cloud storage, and needs no modification.

**Step-6**, start the server with:

```bash
./bin/mongod --config ./etc/mongod.conf
```

**Step-7**, open another terminal and run mongo client.

```bash
./bin/mongo --eval "db.t1.save({k: 1}); db.t1.find();"
```

It should output

```bash
MongoDB shell version v4.0.3
connecting to: mongodb://127.0.0.1:27017
Implicit session: session { "id" : UUID("288393c1-aff6-4a84-ad46-dee6691b361d") }
MongoDB server version: 4.0.3
{ "_id" : ObjectId("68493ec41cc981ea926ec094"), "k" : 1 }
```

---

## Advanced Topics

* Follow [compile tutorial](docs/how-to-compile.md) to learn how to compile EloqDoc-RocksDB and EloqDocRocksDBCloud from scratch.
* Follow [deploy cluster](docs/how-to-deploy-cluster.md) to learn how to deploy an EloqDoc-RocksDBCloud cluster.
* Follow [configuration description](docs/configuration-description.md) to learn major configuration parameters.

---

**Star This Repo ⭐** to Support Our Journey — Every Star Helps Us Reach More Developers!
