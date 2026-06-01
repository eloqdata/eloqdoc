# EloqDoc + TiKV 正式构建与本地试用指南

本文说明如何在 `pingkai-master` 分支上构建完整的 EloqDoc TiKV 版本，
并把本地 EloqDoc 服务接到本地 TiKV 集群上做基本试用。

当前 TiKV 支持依赖本仓库的 `tikv-client-c` 子模块：

- 子模块路径：`src/mongo/db/modules/eloq/tikv-client-c`
- 子模块来源：<https://git.pingcap.net/pingkai/client-c>
- 使用分支：`eloqdoc/pr239-240-241-local`

## 1. 拉取代码和子模块

```bash
git clone https://git.pingcap.net/pingkai/eloqdoc
cd eloqdoc
git checkout pingkai-master
git submodule update --init --recursive
```

确认 `tikv-client-c` 子模块已经指向 pingkai 版本：

```bash
git config -f .gitmodules --get submodule.src/mongo/db/modules/eloq/tikv-client-c.url
git submodule status src/mongo/db/modules/eloq/tikv-client-c
```

## 2. 安装构建依赖

Ubuntu 24.04 可先复用仓库里的依赖脚本：

```bash
bash scripts/install_dependency_ubuntu2404.sh
pyenv global 2.7.18
```

如果使用其他 Linux 发行版，按该脚本里的包列表手动安装对应依赖。
TiKV 后端额外需要 `gRPC`、`protobuf`、`Poco`、`gflags`、`glog`、`brpc`
等 C++ 依赖。

## 3. 正式构建 EloqDoc TiKV 版本

下面的构建流程与 `docs/how-to-compile.md` 一致：先用 CMake 构建 core
libraries，再用 SCons 构建并安装 EloqDoc；不同点是显式选择
`ELOQDSS_TIKV`，并把 SCons 链接到同一次 CMake 构建出的
`tikv-client-c` 产物。

```bash
export REPO=$PWD
export INSTALL_PREFIX=$REPO/install/tikv
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
```

然后构建 EloqDoc 主二进制：

```bash
env WITH_DATA_STORE=ELOQDSS_TIKV \
    WITH_LOG_STATE=ROCKSDB \
    TIKV_CLIENT_C_ROOT="$TIKV_CLIENT_C_ROOT" \
    TIKV_CLIENT_C_BUILD_DIR="$TIKV_CLIENT_C_BUILD_DIR" \
python scripts/buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=RelWithDebInfo \
    CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
    --build-dir=#build \
    --prefix="$INSTALL_PREFIX" \
    --link-model=dynamic \
    --install-mode=hygienic \
    --disable-warnings-as-errors \
    -j"$(nproc)" \
    install-core
```

构建完成后，`eloqdoc` 和 `eloqdoc-cli` 位于 `$INSTALL_PREFIX/bin`。

## 4. 启动本地 TiKV

在单独终端启动一个本地 TiKV playground：

```bash
tiup playground nightly --mode tikv-slim --without-monitor
```

默认 PD endpoint 是 `127.0.0.1:2379`。如果 playground 输出了不同的 PD
地址，请同步修改下面 Data Substrate 配置里的 `tikv_pd_endpoints`。

## 5. 准备 EloqDoc + TiKV 配置

本地单节点试用不需要额外启动独立 `dss_server`：`ELOQDSS_TIKV` 会在
EloqDoc 进程内启动本地 DataStoreService，并通过 `tikv-client-c` 访问
TiKV。

```bash
export RUN_ROOT=$REPO/run/tikv-local
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
  bindIpAll: true
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

创建 Data Substrate 配置。第一次启动用于 bootstrap，因此先保留
`bootstrap=true`：

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

关键配置说明：

- `storage.eloq.enableMVCC` 必须与 `[local] enable_mvcc` 一致。
- `[store] tikv_pd_endpoints` 是 TiKV PD 地址列表，多个地址用逗号分隔。
- `[store] tikv_key_prefix` 建议每个本地试用环境使用独立前缀，避免与同一
  TiKV 集群里的其他数据混用。
- `tikv_archive_cleanup_retention_seconds=0` 表示不启用保守 retention
  fallback；archive/tombstone cleanup 只能依赖 Eloq 安全水位。

## 6. Bootstrap 并启动 EloqDoc

先设置运行时库路径：

```bash
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:$TIKV_CLIENT_C_BUILD_DIR/src:$TIKV_CLIENT_C_BUILD_DIR/third_party/kvproto/cpp:${LD_LIBRARY_PATH:-}"
```

执行一次 bootstrap。成功后进程会输出 bootstrap 成功并退出：

```bash
"$INSTALL_PREFIX/bin/eloqdoc" \
  --config="$RUN_ROOT/etc/eloqdoc.conf" \
  --data_substrate_config="$RUN_ROOT/etc/data_substrate.cnf"
```

然后把 Data Substrate 配置切回正常启动模式：

```bash
sed -i 's/^bootstrap=true$/bootstrap=false/' "$RUN_ROOT/etc/data_substrate.cnf"
```

启动 EloqDoc 服务：

```bash
"$INSTALL_PREFIX/bin/eloqdoc" \
  --config="$RUN_ROOT/etc/eloqdoc.conf" \
  --data_substrate_config="$RUN_ROOT/etc/data_substrate.cnf"
```

## 7. 连接并试用

另开一个终端执行 Mongo API 兼容的基本读写：

```bash
export INSTALL_PREFIX=/path/to/eloqdoc/install/tikv
export ELOQ_CMAKE_BUILD=/path/to/eloqdoc/src/mongo/db/modules/eloq/build
export TIKV_CLIENT_C_BUILD_DIR=$ELOQ_CMAKE_BUILD/tikv-client-c
export LD_LIBRARY_PATH="$INSTALL_PREFIX/lib:$TIKV_CLIENT_C_BUILD_DIR/src:$TIKV_CLIENT_C_BUILD_DIR/third_party/kvproto/cpp:${LD_LIBRARY_PATH:-}"
"$INSTALL_PREFIX/bin/eloqdoc-cli" --eval 'db.tikv_trial.save({k: 1, v: "tikv"}); db.tikv_trial.find({k: 1});'
```

可以继续尝试更新、索引和删除：

```bash
"$INSTALL_PREFIX/bin/eloqdoc-cli" --eval 'db.tikv_trial.update({k: 1}, {$set: {v: "updated"}}); db.tikv_trial.find({k: 1});'
"$INSTALL_PREFIX/bin/eloqdoc-cli" --eval 'db.tikv_trial.createIndex({k: 1}); db.tikv_trial.getIndexes();'
"$INSTALL_PREFIX/bin/eloqdoc-cli" --eval 'db.tikv_trial.remove({k: 1}); db.tikv_trial.find({k: 1});'
```

如需验证重启持久化，停止并重新执行第 6 节的正常启动命令后，再用
`eloqdoc-cli` 查询同一集合。

## 8. 可选：运行 TiKV 后端冒烟测试

正式 build/试用不依赖这一节；它只是用于开发者快速确认 TiKV backend 的
DataStore 行为是否仍然通过回归测试。

```bash
cmake --build "$ELOQ_CMAKE_BUILD" --target tikv_backend_smoke_test -j"$(nproc)"

TIKV_PD_ENDPOINTS=127.0.0.1:2379 \
  "$ELOQ_CMAKE_BUILD/data_substrate/store_handler/eloq_data_store_service/tikv_backend_smoke_test"
```

当前冒烟集合覆盖基础 put/read/delete/range/drop、正反向 scan、snapshot
read/scan、restart persistence、cleanup safety、事务冲突、失败批写清理、
backup fail-closed，以及 read-path region-error metrics。

## 9. 常见问题

- CMake 仍指向旧的 client-c：删除 `$ELOQ_CMAKE_BUILD` 后重新 configure，
  或显式设置 `-DTIKV_CLIENT_C_ROOT=...`。
- SCons 找不到 `kv_client`/`kvproto`：确认 `TIKV_CLIENT_C_BUILD_DIR` 指向
  CMake 构建目录下的 `tikv-client-c`，其中应包含 `src/libkv_client.*` 和
  `third_party/kvproto/cpp/libkvproto.*`。
- 启动时报 PD 连接失败或 `DB_NOT_OPEN`：确认 `tiup playground` 仍在运行，
  并且 `[store] tikv_pd_endpoints` 与实际 PD endpoint 一致。
- 启动时报 MVCC 配置不一致：同步修改 `storage.eloq.enableMVCC` 和
  `[local] enable_mvcc`，二者必须同时为 `true` 或同时为 `false`。
- 重复试用想清空数据：优先换一个新的 `tikv_key_prefix`；如需复用相同前缀，
  需要确认旧 TiKV key 已被清理，避免读到前一次试用的数据。
