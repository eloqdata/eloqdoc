# How To Deploy An EloqDoc-RocksDBCloud-Cluster

Assume you have read [quick start](../README.md) and know how to run an EloqDoc-RocksDB/EloqDoc-RocksDBCloud. As an advanced user, you want to study cluster deploying. Deploying an EloqDoc-RocksDBCloud cluster is more involved, but if you follow the instructions below, you should be able to deploy it successfully.

In this document, you will launch three `EloqDoc-RockDBCloud` servers, one `dss_server `, and one `minio` server on a single machine.

## 0. Download EloqDoc-Cloud

Run the following commands in your home directory (`$HOME`):

```bash
wget -c https://download.eloqdata.com/eloqdoc/rocks_s3/eloqdoc-v0.2.1-ubuntu24-amd64.tar.gz
mkdir -p $HOME/eloqdoc && tar -zxf eloqdoc-v0.2.1-ubuntu24-amd64.tar.gz -C $HOME/eloqdoc
export PATH=$HOME/eloqdoc/bin:$PATH
```

## 1. Prepare an S3 bucket or launch an S3-compatible storage server

Specify an S3 bucket to store data. If you do not have one, you can use an S3-compatible storage server, for example MinIO.

```bash
mkdir -p $HOME/minio-service && cd $HOME/minio-service
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
nohup ./minio server ./data &> minio.out &
```

## 2. Launch dss_server

EloqDoc-Cluster consists of multiple compute nodes and a single storage node called `dss_server`.

```bash
mkdir -p $HOME/eloqdoc-dss/data $HOME/eloqdoc-dss/etc $HOME/eloqdoc-dss/logs
```

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/eloqdss.conf` to `$HOME/eloqdoc-dss/etc/eloqdss.conf`, and edit it as needed.

* Set `local.data_path` to the absolute data directory path, for example `/home/eloq/eloqdoc-dss/data`.
* Set `aws_access_key_id`, `aws_secret_key`, and `rocksdb_cloud_bucket_.*` according to your S3 resources.

Launch `dss_server`:

```bash
nohup dss_server -config=$HOME/eloqdoc-dss/etc/eloqdss.conf &> $HOME/eloqdoc-dss/dss_server.out &
```

Read [dss_server](https://github.com/eloqdata/store_handler/blob/main/eloq_data_store_service/README.md) to learn more about it.

## 3. Deploy EloqDoc compute nodes

Deploy three EloqDoc compute nodes:

```bash
mkdir -p $HOME/eloqdoc-cloud-a/db $HOME/eloqdoc-cloud-a/etc $HOME/eloqdoc-cloud-a/logs
mkdir -p $HOME/eloqdoc-cloud-b/db $HOME/eloqdoc-cloud-b/etc $HOME/eloqdoc-cloud-b/logs
mkdir -p $HOME/eloqdoc-cloud-c/db $HOME/eloqdoc-cloud-c/etc $HOME/eloqdoc-cloud-c/logs
```

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/mongod_cluster_a.conf` to `$HOME/eloqdoc-cloud-a/etc/mongod.conf`.

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/mongod_cluster_b.conf` to `$HOME/eloqdoc-cloud-b/etc/mongod.conf`.

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/mongod_cluster_c.conf` to `$HOME/eloqdoc-cloud-c/etc/mongod.conf`.

Edit data path, log path, and S3 configuration in each file according to your environment.

## 4. Bootstrap

```bash
mongod --eloqBootstrap 1 --config $HOME/eloqdoc-cloud-a/etc/mongod.conf
```

## 5. Launch EloqDoc compute nodes

```bash
nohup mongod --pidfilepath $HOME/eloqdoc-cloud-a/db/mongod.pid --config $HOME/eloqdoc-cloud-a/etc/mongod.conf &> $HOME/eloqdoc-cloud-a/logs/mongod.out &
nohup mongod --pidfilepath $HOME/eloqdoc-cloud-b/db/mongod.pid --config $HOME/eloqdoc-cloud-b/etc/mongod.conf &> $HOME/eloqdoc-cloud-b/logs/mongod.out &
nohup mongod --pidfilepath $HOME/eloqdoc-cloud-c/db/mongod.pid --config $HOME/eloqdoc-cloud-c/etc/mongod.conf &> $HOME/eloqdoc-cloud-c/logs/mongod.out &
```

## 6. Configure an L4 proxy

Provide a unified entry point for Mongo clients. Any L4 proxy such as Linux LVS, AWS NLB, or HAProxy is acceptable. Using HAProxy as an example, configure it as follows:

```bash
# frontend: Listen for client connections
frontend mongo_front
    bind *:27017
    mode tcp
    default_backend mongo_back

# backend: EloqDoc backend servers
backend mongo_back
    mode tcp
    option tcp-check
    balance roundrobin
    server mongo1 127.0.0.1:17000 maxconn 10240 check
    server mongo2 127.0.0.1:17001 maxconn 10240 check
    server mongo3 127.0.0.1:17002 maxconn 10240 check
```

## 7. Connect to EloqDoc Cluster

```bash
mongo --eval "db.t1.save({k: 1}); db.t1.find();"
```
