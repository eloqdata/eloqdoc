# EloqDoc + TiKV Docker 构建与本机试用指南

本文说明如何在 `pingkai-master` 分支上 **只用 Docker 编译 EloqDoc**，再在宿主机上用 `tiup playground` 启动 TiKV/PD，并运行 Docker 编译出的 EloqDoc 产物完成本地试用。

边界说明：Docker 只用于隔离 EloqDoc 编译依赖；TiKV 和 PD 不在 Docker 中编译，也不使用 TiKV/PD Docker 镜像。

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

## 2. 构建 Docker 编译镜像

```bash
docker build -f docker/tikv-build.Dockerfile -t eloqdoc-tikv-build:ubuntu24 .
```

镜像里包含 CMake、Python 2.7、SCons 依赖，以及 EloqDoc TiKV 后端需要的 brpc、braft、mimalloc、gRPC、protobuf、Poco、RocksDB 等 C++ 依赖。宿主机不需要安装这些编译依赖。

## 3. 在 Docker 中编译 EloqDoc

下面命令会把源码挂载到容器内，只在仓库目录下写入 `src/mongo/db/modules/eloq/build`、`build` 和 `install/tikv-docker`。

```bash
mkdir -p logs

docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -v "$PWD":/work/eloqdoc -w /work/eloqdoc \
  eloqdoc-tikv-build:ubuntu24 bash -lc '
set -euxo pipefail
export REPO=/work/eloqdoc
export INSTALL_PREFIX=$REPO/install/tikv-docker
export ELOQ_CMAKE_BUILD=$REPO/src/mongo/db/modules/eloq/build
export TIKV_CLIENT_C_ROOT=$REPO/src/mongo/db/modules/eloq/tikv-client-c
export TIKV_CLIENT_C_BUILD_DIR=$ELOQ_CMAKE_BUILD/tikv-client-c

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

export DEST_DIR="$INSTALL_PREFIX"
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:$TIKV_CLIENT_C_BUILD_DIR/src:$TIKV_CLIENT_C_BUILD_DIR/third_party/kvproto/cpp:/usr/local/lib:${LD_LIBRARY_PATH:-}"

env WITH_DATA_STORE=ELOQDSS_TIKV \
    WITH_LOG_STATE=ROCKSDB \
    TIKV_CLIENT_C_ROOT="$TIKV_CLIENT_C_ROOT" \
    TIKV_CLIENT_C_BUILD_DIR="$TIKV_CLIENT_C_BUILD_DIR" \
    DEST_DIR="$INSTALL_PREFIX" \
python scripts/buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=RelWithDebInfo \
    CXXFLAGS="-include gflags/gflags.h -include unistd.h -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move -Wno-deprecated-declarations" \
    --build-dir=#build \
    --prefix="$INSTALL_PREFIX" \
    --link-model=dynamic \
    --install-mode=hygienic \
    --disable-warnings-as-errors \
    -j"$(nproc)" \
    install-core

./docker/tikv-copy-runtime-libs.sh "$INSTALL_PREFIX/lib"
'
```

确认宿主机可解析运行时动态库：

```bash
export REPO=$PWD
export INSTALL_PREFIX=$REPO/install/tikv-docker
export ELOQ_CMAKE_BUILD=$REPO/src/mongo/db/modules/eloq/build
export TIKV_CLIENT_C_BUILD_DIR=$ELOQ_CMAKE_BUILD/tikv-client-c
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:$TIKV_CLIENT_C_BUILD_DIR/src:$TIKV_CLIENT_C_BUILD_DIR/third_party/kvproto/cpp:${LD_LIBRARY_PATH:-}"

ldd "$INSTALL_PREFIX/bin/eloqdoc" | grep 'not found' || true
ldd "$INSTALL_PREFIX/bin/eloqdoc-cli" | grep 'not found' || true
```

两条 `ldd` 命令没有 `not found` 输出即可。

### 编译产物位置

- EloqDoc 主程序：`install/tikv-docker/bin/eloqdoc`
- EloqDoc CLI：`install/tikv-docker/bin/eloqdoc-cli`
- EloqDoc 安装库和从 Docker 镜像复制出的运行时库：`install/tikv-docker/lib/`
- data_substrate/tx_service/log_service 等 CMake 安装库：`install/tikv-docker/lib/libdata_substrate.so`、`libtxservice.so`、`liblogservice.so`
- `tikv-client-c` C API 库：`src/mongo/db/modules/eloq/build/tikv-client-c/src/libkv_client.so`
- `kvproto` C++ 库：`src/mongo/db/modules/eloq/build/tikv-client-c/third_party/kvproto/cpp/libkvproto.so`

## 4. 用 tiup playground 启动 TiKV

另开一个宿主机终端启动 TiKV/PD：

```bash
tiup playground v8.5.5 --mode tikv-slim --tag eloqdoc-tikv-trial --without-monitor --host 127.0.0.1
```

默认 PD endpoint 是 `127.0.0.1:2379`。如果命令输出了不同地址，后续配置里的 `tikv_pd_endpoints` 要同步修改。

## 5. 准备本机运行配置

回到 EloqDoc 仓库目录：

```bash
export REPO=$PWD
export INSTALL_PREFIX=$REPO/install/tikv-docker
export ELOQ_CMAKE_BUILD=$REPO/src/mongo/db/modules/eloq/build
export TIKV_CLIENT_C_BUILD_DIR=$ELOQ_CMAKE_BUILD/tikv-client-c
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:$TIKV_CLIENT_C_BUILD_DIR/src:$TIKV_CLIENT_C_BUILD_DIR/third_party/kvproto/cpp:${LD_LIBRARY_PATH:-}"

export RUN_ROOT=$REPO/run/tikv-docker-trial
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
tikv_key_prefix=eloqdoc-local/
tikv_request_timeout_seconds=5
tikv_scan_batch_size=256
tikv_archive_cleanup_retention_seconds=0
EOF_CONF
```

注意：

- `storage.eloq.enableMVCC` 必须与 `[local] enable_mvcc` 一致。
- `tikv_key_prefix` 建议每个本地试用环境使用独立值，避免与同一 TiKV 集群里的其他数据混用。
- `tikv_archive_cleanup_retention_seconds=0` 表示不启用保守 retention fallback；archive/tombstone cleanup 只能依赖 Eloq 安全水位。

## 6. Bootstrap 并正常启动 EloqDoc

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

## 7. 连接并试用

另开一个宿主机终端，先设置与第 5 节相同的环境变量，然后执行：

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

如需验证重启持久化：停止 EloqDoc，保持 TiKV playground 运行，再按第 6 节正常启动 EloqDoc，然后查询：

```bash
"$INSTALL_PREFIX/bin/eloqdoc-cli" --quiet --eval '
var d = db.getSiblingDB("tikv_trial_db");
print("after_restart=" + tojson(d.tikv_trial.find({k: 2}).toArray()));
print("count_after_restart=" + d.tikv_trial.count({k: 2}));
'
```

`count_after_restart=1` 表示 TiKV 中的数据在 EloqDoc 重启后仍可读。

## 8. 清理

- 停止 EloqDoc：在 EloqDoc 终端按 `Ctrl-C`。
- 停止 TiKV playground：在 `tiup playground` 终端按 `Ctrl-C`。
- 仓库内可删除产物：`build/`、`install/tikv-docker/`、`src/mongo/db/modules/eloq/build/`、`run/tikv-docker-trial/`、`logs/`。
- TiUP 的 playground 数据在 `~/.tiup/data/<tag>/`；需要时按实际 tag 清理。

## 9. 本文实测结果

本机实测使用：

- Docker 镜像：`eloqdoc-tikv-build:ubuntu24`
- TiKV/PD：`tiup playground v8.5.5 --mode tikv-slim --host 127.0.0.1`
- EloqDoc 产物：`install/tikv-docker/bin/eloqdoc` 和 `install/tikv-docker/bin/eloqdoc-cli`

已通过：

- Docker 内 CMake `ELOQDSS_TIKV` build/install。
- Docker 内 SCons `install-core`。
- 宿主机 `ldd` 检查 `eloqdoc` / `eloqdoc-cli` 无 `not found`。
- 宿主机启动 `tiup playground` 后，运行 Docker 编译出的 EloqDoc 完成 bootstrap 和正常启动。
- `eloqdoc-cli` 完成 insert/find/update/createIndex/remove。
- 重启 EloqDoc 后再次查询，`count_after_restart=1`。
