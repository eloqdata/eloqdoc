# How To Deploy EloqDoc-Cluster

Assume you have read [EloqDoc-RocksDBCloud](eloqdoc-rocksdb-cloud-how-to.md), and knew how to deploy a EloqDoc-RocksDBCloud. As an advanced user, you want to study EloqDoc Cluster. Deploy a Eloqdoc Cluster is a bit complicated, but follow below instructions, you are believed to deploy successfully.

In this document, we will launch three EloqDoc server, one dss_server, and one minio server on one machine.

## 0. Download EloqDoc-Cloud

```bash
wget -c https://download.eloqdata.com/eloqdoc/rocks_s3/eloqdoc-v0.2.1-ubuntu24-amd64.tar.gz
mkdir eloqdoc && tar -zxf eloqdoc-v0.2.1-ubuntu24-amd64.tar.gz -C eloqdoc
export PATH=$HOME/eloqdoc/bin:$PATH
```

## 1. Prepare a S3 bucket or launch a S3 compatible storage server

You need to specify a S3 bucket to store data. If you don't have one, you could use a S3 compatible storage server, for example `minio`.

```
mkdir minio-service && cd minio-service
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
nohup ./minio server ./data &> minio.out &
```

## 2. Launch dss_server

There are multiple compute nodes and one storage node in EloqDoc-Cluster. The storage node is called `dss_server`.

```bash
mkdir -p eloqdoc-dss/data eloqdoc-dss/etc eloqdoc-dss/logs
```

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/eloqdss.conf` to `eloqdoc-dss/etc`, and use your favorite editor to modify it.

* Set `local.data_path` to `/home/eloq/eloqdoc-dss/data`. Assume that your home path is `/home/eloq`.
* Set `aws_access_key_id`, `aws_secret_key`, and `rocksdb_cloud_bucket_.*` based on your S3 resource.

Launch dss_server:

```bash
nohup dss_server -config=eloqdoc-dss/etc/eloqdss.cnf &> eloqdoc-dss/dss_server.out &
```

## 3. Deploy EloqDoc compute nodes

We will deploy three EloqDoc compute nodes.

```bash
mkdir -p eloqdoc-cloud-a/db eloqdoc-cloud-a/etc eloqdoc-cloud-a/logs
mkdir -p eloqdoc-cloud-b/db eloqdoc-cloud-b/etc eloqdoc-cloud-b/logs
mkdir -p eloqdoc-cloud-c/db eloqdoc-cloud-c/etc eloqdoc-cloud-c/logs
```

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/mongod_cluster_a.conf` to `eloqdoc-cloud-a/etc/mongod.conf`.

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/mongod_cluster_b.conf` to `eloqdoc-cloud-b/etc/mongod.conf`.

Copy `concourse/artifact/ELOQDSS_ROCKSDB_CLOUD_S3/mongod_cluster_c.conf` to `eloqdoc-cloud-c/etc/mongod.conf`.

Edit data path, log path, and S3 configurations based on your environment.

## 4. Bootstrap

```bash
mongo --eloqBootstrap 1 --config eloqdoc-cloud-a/etc/mongod.conf
```

## 5. Launch EloqDoc compute nodes

```bash
nohup mongo --pidfilepath eloqdoc-cloud-a/db/mongod.pid --config eloqdoc-cloud-a/etc/mongod.conf &> eloqdoc-cloud-a/logs/mongod.out &
nohup mongo --pidfilepath eloqdoc-cloud-b/db/mongod.pid --config eloqdoc-cloud-b/etc/mongod.conf &> eloqdoc-cloud-b/logs/mongod.out &
nohup mongo --pidfilepath eloqdoc-cloud-c/db/mongod.pid --config eloqdoc-cloud-c/etc/mongod.conf &> eloqdoc-cloud-c/logs/mongod.out &
```

## 6. Configure a L4 Proxy

We want to provide a unified entry point for mongo clients. All L4 proxy: `Linux LVS`, `AWS NLB` or `haproxy` are OK. Takes `haproxy` as an example, you might configure it as follows:

```bash
# frontend：Listen client connection
frontend mongo_front
    bind *:27017
    mode tcp
    default_backend mongo_back

# backend：EloqDoc backend servers
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
