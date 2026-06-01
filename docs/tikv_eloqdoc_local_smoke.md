# EloqDoc + TiKV Local Smoke Test

This note describes the current local check for the EloqDoc TiKV backend on the
`pingkai-master` branch.  It is intentionally small: it verifies the TiKV
DataStore integration that EloqDoc uses, not a full Mongo wire-protocol e2e.

## What is already available

`pingkai-master` contains a ready-to-run TiKV backend smoke test:

- test binary: `tikv_backend_smoke_test`
- source: `src/mongo/db/modules/eloq/data_substrate/store_handler/eloq_data_store_service/tests/tikv_backend_smoke_test.cpp`
- coverage: basic put/read/delete/range/drop, forward/reverse scan, snapshot
  read/scan, restart persistence, cleanup safety, transaction conflict,
  partial-write cleanup, backup fail-closed, and read-path region-error metrics.

## Checkout

```bash
git clone https://git.pingcap.net/pingkai/eloqdoc
cd eloqdoc
git checkout pingkai-master
git submodule update --init --recursive \
  src/mongo/db/modules/eloq/data_substrate \
  src/mongo/db/modules/eloq/tikv-client-c
```

The `tikv-client-c` submodule should point at
`https://git.pingcap.net/pingkai/client-c` on the
`eloqdoc/pr239-240-241-local` branch.

## Build the smoke test

```bash
DS=src/mongo/db/modules/eloq/data_substrate
TIKV_CLIENT_C_ROOT=$PWD/src/mongo/db/modules/eloq/tikv-client-c
BUILD_DIR=/tmp/eloqdoc-tikv-smoke-build

cmake -S "$DS" -B "$BUILD_DIR" \
  -DWITH_DATA_STORE=ELOQDSS_TIKV \
  -DTIKV_CLIENT_C_ROOT="$TIKV_CLIENT_C_ROOT"

cmake --build "$BUILD_DIR" --target tikv_backend_smoke_test -j"$(nproc)"
```

If CMake still points at an old local client-c checkout, remove `BUILD_DIR` and
configure again.

## Start local TiKV

Run this in a separate terminal:

```bash
tiup playground nightly --mode tikv-slim --without-monitor
```

The smoke test expects PD at `127.0.0.1:2379` by default.  To use another PD
endpoint, set `TIKV_PD_ENDPOINTS` when running the test.

## Run

```bash
TIKV_PD_ENDPOINTS=127.0.0.1:2379 \
  /tmp/eloqdoc-tikv-smoke-build/store_handler/eloq_data_store_service/tikv_backend_smoke_test
```

Expected result: all `tikv_backend_smoke_test` cases pass.  The current suite is
12 tests.

For a quick focused check of the new region-error path:

```bash
TIKV_PD_ENDPOINTS=127.0.0.1:2379 \
  /tmp/eloqdoc-tikv-smoke-build/store_handler/eloq_data_store_service/tikv_backend_smoke_test \
  --gtest_filter=TikvBackendSmokeTest.ReadPathRegionErrorInjectionRecordsReadBackoffMetrics
```

## Common failures

- `WITH_DATA_STORE=ELOQDSS_TIKV requires TIKV_CLIENT_C_ROOT`: initialize the
  `tikv-client-c` submodule or pass `-DTIKV_CLIENT_C_ROOT=...` explicitly.
- `DB_NOT_OPEN` or PD connection errors: make sure `tiup playground` is still
  running and `TIKV_PD_ENDPOINTS` matches its PD endpoint.
- Missing `gflags`, `grpc`, `protobuf`, `Poco`, or `gtest`: install the local C++
  build dependencies, then recreate the build directory.

## Scope

This is the repeatable local test that exists today.  A full EloqDoc server +
Mongo client e2e against TiKV is not yet documented as a separate one-command
smoke; add it as a follow-up if API-level coverage is needed.
