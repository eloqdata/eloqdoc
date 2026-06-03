# EloqDoc + TiKV CentOS 7 Docker 构建与本机试用指南

本文说明如何在 `pingkai-master` 分支上用固定 Linux 构建基线编译 EloqDoc + TiKV 后端，并在宿主机用 `tiup playground` 试跑。

核心目标：release binary 不随开发机系统库漂移。编译统一在 CentOS 7 / glibc 2.17 基线的 Docker builder 中完成；产物目录会打包非 glibc 运行时库并重写 rpath。

边界：Docker 只隔离 EloqDoc 编译依赖；TiKV/PD 仍用宿主机 `tiup playground` 启动。

## 1. 拉取代码和子模块

```bash
git clone https://git.pingcap.net/pingkai/eloqdoc
cd eloqdoc
git checkout pingkai-master
git submodule update --init --recursive
```

当前 TiKV 支持依赖本仓库的 `tikv-client-c` 子模块：

- 路径：`src/mongo/db/modules/eloq/tikv-client-c`
- 来源：<https://git.pingcap.net/pingkai/client-c>
- 分支：`eloqdoc/pr239-240-241-local`

## 2. 构建 CentOS 7 builder 镜像

```bash
docker build -f docker/tikv-build.Dockerfile -t eloqdoc-tikv-build:centos7 .
```

镜像基线：

- base image：`quay.io/pypa/manylinux2014_x86_64:latest`，对应 CentOS 7 / glibc 2.17 ABI 基线。
- toolchain：devtoolset-10 `gcc/g++`。
- C++ 依赖：brpc、braft、mimalloc、gRPC、protobuf、Poco、RocksDB、Boost 等都在 builder 内安装，宿主机不需要安装这些编译依赖。

## 3. 在 Docker 中编译、打包并检查 ABI

下面命令会把源码挂载到容器内，主要写入：

- `src/mongo/db/modules/eloq/build-centos7`
- `build-centos7`
- `install/tikv-centos7`

命令会先运行 `docker/tikv-prepare-submodules.sh`，把本仓库保存的 TiKV 构建补丁应用到 `data_substrate` 子模块工作区；补丁是幂等的。

```bash
mkdir -p logs

docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD":/work/eloqdoc -w /work/eloqdoc \
  eloqdoc-tikv-build:centos7 bash -lc '
set -euxo pipefail
export REPO=/work/eloqdoc
export INSTALL_PREFIX=$REPO/install/tikv-centos7
export ELOQ_CMAKE_BUILD=$REPO/src/mongo/db/modules/eloq/build-centos7
export TIKV_CLIENT_C_ROOT=$REPO/src/mongo/db/modules/eloq/tikv-client-c
export TIKV_CLIENT_C_BUILD_DIR=$ELOQ_CMAKE_BUILD/tikv-client-c
export DEST_DIR=$INSTALL_PREFIX
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:$TIKV_CLIENT_C_BUILD_DIR/src:$TIKV_CLIENT_C_BUILD_DIR/third_party/kvproto/cpp:/usr/local/lib64:/usr/local/lib:/opt/rh/devtoolset-10/root/usr/lib64:${LD_LIBRARY_PATH:-}"

./docker/tikv-prepare-submodules.sh

cmake -S src/mongo/db/modules/eloq \
      -B "$ELOQ_CMAKE_BUILD" \
      -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_CXX_STANDARD=17 \
      -DBUILD_SHARED_LIBS=ON \
      -DWITH_DATA_STORE=ELOQDSS_TIKV \
      -DWITH_LOG_STATE=ROCKSDB \
      -DTIKV_CLIENT_C_ROOT="$TIKV_CLIENT_C_ROOT"

cmake --build "$ELOQ_CMAKE_BUILD" -j"$(nproc)"
cmake --install "$ELOQ_CMAKE_BUILD"

env WITH_DATA_STORE=ELOQDSS_TIKV \
    WITH_LOG_STATE=ROCKSDB \
    ELOQ_CMAKE_BUILD="$ELOQ_CMAKE_BUILD" \
    TIKV_CLIENT_C_ROOT="$TIKV_CLIENT_C_ROOT" \
    TIKV_CLIENT_C_BUILD_DIR="$TIKV_CLIENT_C_BUILD_DIR" \
    DEST_DIR="$INSTALL_PREFIX" \
python scripts/buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=RelWithDebInfo \
    CC=/opt/rh/devtoolset-10/root/usr/bin/gcc \
    CXX=/opt/rh/devtoolset-10/root/usr/bin/g++ \
    CXXFLAGS="-include gflags/gflags.h -include unistd.h -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move -Wno-deprecated-declarations" \
    --build-dir=#build-centos7 \
    --prefix="$INSTALL_PREFIX" \
    --link-model=dynamic \
    --install-mode=hygienic \
    --disable-warnings-as-errors \
    -j"$(nproc)" \
    install-core

./docker/tikv-copy-runtime-libs.sh \
    "$INSTALL_PREFIX/lib" \
    "$TIKV_CLIENT_C_BUILD_DIR/src" \
    "$TIKV_CLIENT_C_BUILD_DIR/third_party/kvproto/cpp" \
    "$ELOQ_CMAKE_BUILD/tx_service-abseil"

./docker/tikv-check-abi.sh "$INSTALL_PREFIX" 2.17
' 2>&1 | tee logs/tikv-centos7-build.log
```

`docker/tikv-prepare-submodules.sh` 会在子模块工作区应用以下构建补丁：

- 让 `data_substrate` 和 `tikv-client-c` 共用同一份 tx_service Abseil，避免重复定义 `absl::*` CMake target。
- 兼容当前 `log_service` 头文件里使用 gflags 类型但未显式包含 gflags 头的问题。

`docker/tikv-copy-runtime-libs.sh` 会：

- 复制 `eloqdoc` 运行需要的非 glibc 动态库到 `install/tikv-centos7/lib/`。
- 打包兼容的 `libstdc++.so*` / `libgcc_s.so*`，因此允许产物出现 `GLIBCXX_*` 依赖。
- 不打包 glibc 主运行时库，如 `libc.so*`、`libpthread.so*`、`libdl.so*`、`ld-linux*`。
- 将 `bin/` 下可执行文件 rpath 设置为 `$ORIGIN/../lib`，将 `lib/` 下动态库 rpath 设置为 `$ORIGIN`，避免运行时指向 builder 内的绝对路径。

`docker/tikv-check-abi.sh` 的验收标准：

- `ldd install/tikv-centos7/bin/eloqdoc` 和 `eloqdoc-cli` 没有 `not found`。
- 所有 ELF 文件最高 `GLIBC_*` 不超过 `GLIBC_2.17`。
- 如存在 `GLIBCXX_*`，必须同时打包 `libstdc++.so*`。
- 除 glibc 系列库外，运行时依赖都应从 `install/tikv-centos7/lib/` 解析。

## 4. 编译产物位置

主要 release 产物在 `install/tikv-centos7/`：

- 主程序：`install/tikv-centos7/bin/eloqdoc`
- CLI：`install/tikv-centos7/bin/eloqdoc-cli`
- 已打包运行时库：`install/tikv-centos7/lib/`
- CMake 安装库：`install/tikv-centos7/lib/libdata_substrate.so`、`libtxservice.so`、`liblogservice.so`、`libkv_client.so`、`libkvproto.so`

中间构建产物：

- CMake build dir：`src/mongo/db/modules/eloq/build-centos7/`
- SCons build dir：`build-centos7/`

拷贝到新机器试跑时，至少带上整个 `install/tikv-centos7/` 目录，并在启动前设置：

```bash
export INSTALL_PREFIX=/path/to/install/tikv-centos7
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:${LD_LIBRARY_PATH:-}"
```

## 5. 用 tiup playground 启动 TiKV

另开一个宿主机终端启动 TiKV/PD：

```bash
tiup playground v8.5.5 --mode tikv-slim --tag eloqdoc-tikv-centos7 --without-monitor --host 127.0.0.1
```

默认 PD endpoint 是 `127.0.0.1:2379`。如果命令输出了不同地址，后续配置里的 `tikv_pd_endpoints` 要同步修改。

## 6. 准备本机运行配置

回到 EloqDoc 仓库目录：

```bash
export REPO=$PWD
export INSTALL_PREFIX=$REPO/install/tikv-centos7
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:${LD_LIBRARY_PATH:-}"

export RUN_ROOT=$REPO/run/tikv-centos7-trial
mkdir -p "$RUN_ROOT"/{db,logs,etc,data}
```

创建 EloqDoc 配置：

```bash
cat > "$RUN_ROOT/etc/eloqdoc.conf" <<EOF_CONF
systemLog:
  verbosity: 0
  destination: file
  path: "$RUN_ROOT/logs/eloqdoc.log"
  component:
    ftdc:
      verbosity: 0
net:
  bindIp: 127.0.0.1
  port: 27017
  serviceExecutor: "adaptive"
  adaptiveThreadNum: 2
storage:
  dbPath: "$RUN_ROOT/db"
  engine: "eloq"
  eloq:
    enableMVCC: true
    txService:
      ccProtocol: "OccRead"
setParameter:
  diagnosticDataCollectionEnabled: false
  disableLogicalSessionCacheRefresh: true
  ttlMonitorEnabled: true
EOF_CONF
```

创建 Data Substrate 配置。第一次启动需要 `bootstrap=true`：

```bash
cat > "$RUN_ROOT/etc/data_substrate.cnf" <<EOF_CONF
[local]
core_number=2
node_memory_limit_mb=4000
enable_data_store=true
enable_wal=true
eloq_data_path=$RUN_ROOT/data
event_dispatcher_num=1
enable_mvcc=true
bootstrap=true
tx_ip=127.0.0.1
tx_port=16379

[cluster]
tx_ip_port_list=127.0.0.1:16379
node_group_replica_num=1

[store]
tikv_pd_endpoints=127.0.0.1:2379
tikv_key_prefix=eloqdoc-centos7-local/
tikv_request_timeout_seconds=5
tikv_scan_batch_size=256
tikv_archive_cleanup_retention_seconds=0
EOF_CONF
```

注意：

- `storage.eloq.enableMVCC` 必须与 `[local] enable_mvcc` 一致。
- `tikv_key_prefix` 建议每个本地试用环境使用独立值，避免与同一 TiKV 集群里的其他数据混用。
- `tikv_archive_cleanup_retention_seconds=0` 表示不启用保守 retention fallback；archive/tombstone cleanup 只能依赖 Eloq 安全水位。

## 7. Bootstrap 并正常启动 EloqDoc

执行一次 bootstrap。成功后进程会输出 `Bootstrap for Eloqdoc success. Exiting...` 并退出：

```bash
"$INSTALL_PREFIX/bin/eloqdoc" \
  --config="$RUN_ROOT/etc/eloqdoc.conf" \
  --data_substrate_config="$RUN_ROOT/etc/data_substrate.cnf"
```

把配置切到正常启动模式：

```bash
sed -i 's/^bootstrap=true$/bootstrap=false/' "$RUN_ROOT/etc/data_substrate.cnf"
```

启动 EloqDoc，并保持该终端运行：

```bash
"$INSTALL_PREFIX/bin/eloqdoc" \
  --config="$RUN_ROOT/etc/eloqdoc.conf" \
  --data_substrate_config="$RUN_ROOT/etc/data_substrate.cnf"
```

正常启动后，宿主机应能看到 `27017` 和 `16379` 端口：

```bash
ss -ltnp | grep -E ':(2379|20160|27017|16379)\b'
```

## 8. 连接并试用

另开一个宿主机终端，先设置与第 6 节相同的环境变量，然后执行：

```bash
"$INSTALL_PREFIX/bin/eloqdoc-cli" --quiet --eval '
var d = db.getSiblingDB("tikv_trial_db");
d.tikv_trial.drop();
d.tikv_trial.insert({k: 1, v: "tikv"});
print("after_insert=" + tojson(d.tikv_trial.find({k: 1}).toArray()));
d.tikv_trial.update({k: 1}, {$set: {v: "updated"}});
print("after_update=" + tojson(d.tikv_trial.find({k: 1}).toArray()));
print("create_index=" + tojson(d.tikv_trial.createIndex({k: 1})));
print("indexes=" + tojson(d.tikv_trial.getIndexes()));
d.tikv_trial.remove({k: 1});
print("after_remove=" + tojson(d.tikv_trial.find({k: 1}).toArray()));
d.tikv_trial.insert({k: 2, v: "persist"});
print("before_restart=" + tojson(d.tikv_trial.find({k: 2}).toArray()));
'
```

如需验证重启持久化：停止 EloqDoc，保持 TiKV playground 运行，再按第 7 节正常启动 EloqDoc，然后查询：

```bash
"$INSTALL_PREFIX/bin/eloqdoc-cli" --quiet --eval '
var d = db.getSiblingDB("tikv_trial_db");
print("after_restart=" + tojson(d.tikv_trial.find({k: 2}).toArray()));
print("count_after_restart=" + d.tikv_trial.count({k: 2}));
'
```

`count_after_restart=1` 表示 TiKV 中的数据在 EloqDoc 重启后仍可读。

## 9. 多节点 EloqDoc + TiKV/PD 集群部署示例

第 5 节的 `tiup playground` 只适合单机试用，不具备 TiKV/PD 的高可用能力。生产或准生产验证建议至少部署 3 个 PD、3 个 TiKV 和 3 个 EloqDoc 计算节点。

本节给出一个三节点示例。实际部署时请替换 IP、目录、CPU/内存和端口。

### 9.1 HA 边界和配置原则

- TiKV/PD 必须是多副本拓扑。单节点 TiKV/PD 即使接入多个 EloqDoc 节点，也只能验证计算层连接，不能提供存储层 HA。
- EloqDoc 计算层建议 3 节点起步，并保持各节点 `[cluster] tx_ip_port_list` 完全一致，`node_group_replica_num=3`。
- 同一个 EloqDoc 集群内，所有节点必须使用相同的 `tikv_pd_endpoints` 和 `tikv_key_prefix`。不同环境或不同集群必须使用不同的 `tikv_key_prefix`，避免共享 TiKV 集群时数据混用。
- `bootstrap=true` 只能在一个 EloqDoc 节点上执行一次。成功后必须改回 `bootstrap=false`，再启动全部 EloqDoc 节点。
- TiKV/PD 的高可用由 TiKV/PD 自身 Raft 副本保证；EloqDoc 客户端入口建议通过 HAProxy、LVS、NLB 等四层代理暴露。

### 9.2 示例拓扑

三台机器分别同时部署 PD、TiKV 和 EloqDoc：

| 节点 | IP | EloqDoc 客户端端口 | EloqDoc tx_service | PD client | TiKV |
| --- | --- | --- | --- | --- | --- |
| node-a | `10.0.0.11` | `27017` | `16379` | `2379` | `20160` |
| node-b | `10.0.0.12` | `27017` | `16379` | `2379` | `20160` |
| node-c | `10.0.0.13` | `27017` | `16379` | `2379` | `20160` |

如果只是在同一台机器模拟多 EloqDoc 进程，端口和目录必须全部错开，例如客户端端口用 `17000/17001/17002`，tx_service 端口用 `9200/9210/9220`。

### 9.3 部署 3 PD + 3 TiKV

推荐用 TiUP Cluster 部署 TiKV/PD。下面是一个最小 TiKV 拓扑示例：

```yaml
# topology.yaml
global:
  user: "tidb"
  ssh_port: 22
  deploy_dir: "/data/deploy"
  data_dir: "/data/tidb-data"

pd_servers:
  - host: 10.0.0.11
    client_port: 2379
    peer_port: 2380
  - host: 10.0.0.12
    client_port: 2379
    peer_port: 2380
  - host: 10.0.0.13
    client_port: 2379
    peer_port: 2380

tikv_servers:
  - host: 10.0.0.11
    port: 20160
    status_port: 20180
  - host: 10.0.0.12
    port: 20160
    status_port: 20180
  - host: 10.0.0.13
    port: 20160
    status_port: 20180
```

部署和启动：

```bash
tiup cluster check topology.yaml --user root
tiup cluster deploy eloqdoc-tikv v8.5.5 topology.yaml --user root
tiup cluster start eloqdoc-tikv
tiup cluster display eloqdoc-tikv
```

如果部署机不能直接用 `root` SSH，也可以使用已具备 sudo 权限的部署用户。最终只要能拿到 3 个 PD endpoint 即可，例如：

```text
10.0.0.11:2379,10.0.0.12:2379,10.0.0.13:2379
```

### 9.4 分发 EloqDoc release 产物

第 4 节生成的 release 产物是整个 `install/tikv-centos7/` 目录。将它分发到每台 EloqDoc 节点：

```bash
rsync -a install/tikv-centos7/ 10.0.0.11:/opt/eloqdoc/tikv-centos7/
rsync -a install/tikv-centos7/ 10.0.0.12:/opt/eloqdoc/tikv-centos7/
rsync -a install/tikv-centos7/ 10.0.0.13:/opt/eloqdoc/tikv-centos7/
```

每台 EloqDoc 节点启动前设置：

```bash
export INSTALL_PREFIX=/opt/eloqdoc/tikv-centos7
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:${LD_LIBRARY_PATH:-}"
mkdir -p /data/eloqdoc/{db,logs,etc,data}
```

### 9.5 EloqDoc 节点配置

每台 EloqDoc 节点需要两个配置文件：

- `/data/eloqdoc/etc/eloqdoc.conf`：Mongo wire protocol 入口、日志、dbPath 等本机配置。
- `/data/eloqdoc/etc/data_substrate.cnf`：tx_service 集群、TiKV PD endpoint、TiKV key prefix 等配置。

#### node-a：`/data/eloqdoc/etc/eloqdoc.conf`

```yaml
systemLog:
  verbosity: 0
  destination: file
  path: "/data/eloqdoc/logs/eloqdoc.log"
  component:
    ftdc:
      verbosity: 0
net:
  bindIpAll: true
  port: 27017
  serviceExecutor: "adaptive"
  adaptiveThreadNum: 2
storage:
  dbPath: "/data/eloqdoc/db"
  engine: "eloq"
  eloq:
    enableMVCC: true
    txService:
      ccProtocol: "OccRead"
setParameter:
  diagnosticDataCollectionEnabled: false
  disableLogicalSessionCacheRefresh: true
  ttlMonitorEnabled: true
```

node-b 和 node-c 如果分别在独立机器上部署，可以使用相同的本机路径和端口；如果在单机模拟，则必须把 `systemLog.path`、`storage.dbPath` 和 `net.port` 改成互不冲突的值。

#### node-a：`/data/eloqdoc/etc/data_substrate.cnf`

```ini
[local]
core_number=8
node_memory_limit_mb=32768
enable_data_store=true
enable_wal=true
eloq_data_path=/data/eloqdoc/data
event_dispatcher_num=4
enable_mvcc=true
bootstrap=false
tx_ip=10.0.0.11
tx_port=16379

[cluster]
tx_ip_port_list=10.0.0.11:16379,10.0.0.12:16379,10.0.0.13:16379
node_group_replica_num=3

[store]
tikv_pd_endpoints=10.0.0.11:2379,10.0.0.12:2379,10.0.0.13:2379
tikv_key_prefix=eloqdoc-prod/
tikv_request_timeout_seconds=5
tikv_scan_batch_size=256
tikv_archive_cleanup_retention_seconds=0
```

node-b 和 node-c 的 `data_substrate.cnf` 只需要改本机差异项：

```ini
# node-b
[local]
eloq_data_path=/data/eloqdoc/data
bootstrap=false
tx_ip=10.0.0.12
tx_port=16379

# node-c
[local]
eloq_data_path=/data/eloqdoc/data
bootstrap=false
tx_ip=10.0.0.13
tx_port=16379
```

其余 `[cluster]` 和 `[store]` 内容保持完全一致。单机模拟时还要分别修改 `tx_port` 和 `eloq_data_path`，例如 `9200/9210/9220` 和 `/data/eloqdoc-a/data`、`/data/eloqdoc-b/data`、`/data/eloqdoc-c/data`。

### 9.6 Bootstrap 和启动顺序

先确认 TiKV/PD 已正常启动：

```bash
tiup cluster display eloqdoc-tikv
```

然后只在一个 EloqDoc 节点执行一次 bootstrap。以 node-a 为例，临时把 node-a 的 `/data/eloqdoc/etc/data_substrate.cnf` 改成：

```ini
bootstrap=true
```

执行：

```bash
"$INSTALL_PREFIX/bin/eloqdoc" \
  --config=/data/eloqdoc/etc/eloqdoc.conf \
  --data_substrate_config=/data/eloqdoc/etc/data_substrate.cnf
```

看到 `Bootstrap for Eloqdoc success. Exiting...` 后，立即改回：

```ini
bootstrap=false
```

再在三台 EloqDoc 节点上分别启动正常服务：

```bash
nohup "$INSTALL_PREFIX/bin/eloqdoc" \
  --config=/data/eloqdoc/etc/eloqdoc.conf \
  --data_substrate_config=/data/eloqdoc/etc/data_substrate.cnf \
  > /data/eloqdoc/logs/eloqdoc.out 2>&1 &
```

启动后检查端口：

```bash
ss -ltnp | grep -E ':(27017|16379)\b'
```

### 9.7 统一客户端入口

客户端可以直连任一 EloqDoc 节点，也可以通过四层代理获得统一入口。HAProxy TCP 示例：

```haproxy
frontend eloqdoc_front
    bind *:27017
    mode tcp
    default_backend eloqdoc_back

backend eloqdoc_back
    mode tcp
    option tcp-check
    balance roundrobin
    server eloqdoc-a 10.0.0.11:27017 maxconn 10240 check
    server eloqdoc-b 10.0.0.12:27017 maxconn 10240 check
    server eloqdoc-c 10.0.0.13:27017 maxconn 10240 check
```

客户端连接 HAProxy 时使用代理地址和 `27017`；如果直连节点，则使用对应节点 IP 和端口。如果 HAProxy 与某个 EloqDoc 节点部署在同一台机器，HAProxy 的监听端口不能与本机 EloqDoc 的 `net.port` 冲突。

### 9.8 基础验证和故障演练

连接任一节点或 HAProxy 入口执行基本读写：

```bash
"$INSTALL_PREFIX/bin/eloqdoc-cli" --host 10.0.0.11 --port 27017 --quiet --eval '
var d = db.getSiblingDB("tikv_ha_db");
d.t.drop();
d.t.insert({k: 1, v: "ha"});
print("find=" + tojson(d.t.find({k: 1}).toArray()));
print("count=" + d.t.count({k: 1}));
'
```

可做的最小 HA 验证：

1. 停止一个 EloqDoc 进程，确认 HAProxy 仍能把新连接转发到其他 EloqDoc 节点。
2. 停止一个 TiKV 或 PD 节点，确认 `tiup cluster display eloqdoc-tikv` 中剩余节点健康，EloqDoc 基础读写仍可继续。
3. 恢复被停止的节点，再次确认集群健康。

不要把这个验证理解为可以承受任意多节点故障。实际容灾能力取决于 TiKV/PD 副本数、region 分布、机器/机架故障域，以及 EloqDoc 计算节点和四层代理的部署方式。
