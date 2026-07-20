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

Copy `.github/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/eloqdss.conf` to `$HOME/eloqdoc-dss/etc/eloqdss.conf`, and edit it as needed.

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

Each node needs two files: `eloqdoc.conf` for the MongoDB-facing server and
`data_substrate.cnf` for the cluster and transaction-log settings.

Copy `.github/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/eloqdoc_cluster_a.conf` to `$HOME/eloqdoc-cloud-a/etc/eloqdoc.conf`, and `data_substrate_cluster_a.cnf` from the same directory to `$HOME/eloqdoc-cloud-a/etc/data_substrate.cnf`.

Copy `.github/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/eloqdoc_cluster_b.conf` to `$HOME/eloqdoc-cloud-b/etc/eloqdoc.conf`, and `data_substrate_cluster_b.cnf` to `$HOME/eloqdoc-cloud-b/etc/data_substrate.cnf`.

Copy `.github/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/eloqdoc_cluster_c.conf` to `$HOME/eloqdoc-cloud-c/etc/eloqdoc.conf`, and `data_substrate_cluster_c.cnf` to `$HOME/eloqdoc-cloud-c/etc/data_substrate.cnf`.

Edit data path, log path, and S3 configuration in each file according to your environment. The three `data_substrate_cluster_*.cnf` files differ only in `tx_port` (9200/9210/9220); they all list the same three nodes in `tx_ip_port_list`, so that section must stay identical across nodes.

In each `data_substrate.cnf`, set `aws_access_key_id` and `aws_secret_key` under `[local]` to the credentials for the bucket named by `txlog_rocksdb_cloud_bucket_*`. The compute nodes write transaction logs to S3 themselves, so they need credentials even though the data store is reached through `dss_server`.

## 4. Bootstrap

```bash
eloqdoc --eloqBootstrap 1 --config $HOME/eloqdoc-cloud-a/etc/eloqdoc.conf --data_substrate_config $HOME/eloqdoc-cloud-a/etc/data_substrate.cnf
```

## 5. Launch EloqDoc compute nodes

```bash
nohup eloqdoc --pidfilepath $HOME/eloqdoc-cloud-a/db/eloqdoc.pid --config $HOME/eloqdoc-cloud-a/etc/eloqdoc.conf --data_substrate_config $HOME/eloqdoc-cloud-a/etc/data_substrate.cnf &> $HOME/eloqdoc-cloud-a/logs/eloqdoc.out &
nohup eloqdoc --pidfilepath $HOME/eloqdoc-cloud-b/db/eloqdoc.pid --config $HOME/eloqdoc-cloud-b/etc/eloqdoc.conf --data_substrate_config $HOME/eloqdoc-cloud-b/etc/data_substrate.cnf &> $HOME/eloqdoc-cloud-b/logs/eloqdoc.out &
nohup eloqdoc --pidfilepath $HOME/eloqdoc-cloud-c/db/eloqdoc.pid --config $HOME/eloqdoc-cloud-c/etc/eloqdoc.conf --data_substrate_config $HOME/eloqdoc-cloud-c/etc/data_substrate.cnf &> $HOME/eloqdoc-cloud-c/logs/eloqdoc.out &
```

## 6. Configure an L4 proxy

Provide a unified entry point for EloqDoc clients. Any L4 proxy such as Linux LVS, AWS NLB, or HAProxy is acceptable. Using HAProxy as an example, configure it as follows:

```bash
# frontend: Listen for client connections
frontend eloqdoc_front
    bind *:27017
    mode tcp
    default_backend eloqdoc_back

# backend: EloqDoc backend servers
backend eloqdoc_back
    mode tcp
    option tcp-check
    balance roundrobin
    server eloqdoc1 127.0.0.1:17000 maxconn 10240 check
    server eloqdoc2 127.0.0.1:17001 maxconn 10240 check
    server eloqdoc3 127.0.0.1:17002 maxconn 10240 check
```

## 7. Connect to EloqDoc Cluster

```bash
eloqdoc-cli --eval "db.t1.save({k: 1}); db.t1.find();"
```
