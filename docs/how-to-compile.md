# Build EloqDoc

EloqDoc supports two build systems in the same source tree:

- The top-level CMake/Ninja build is the recommended server-only build. It builds the
  MongoDB-compatible single-node server, the Eloq storage-engine integration, and the in-tree Data
  Substrate in one graph. It uses Python 3 and does not invoke SCons.
- The historical MongoDB 4.0 SCons build remains supported for existing development and packaging
  workflows. It uses Python 2.7 and builds the Eloq core libraries separately with CMake.

Use separate build directories and installation prefixes if both builds are used from one checkout.

The top-level CMake server uses the existing EloqDoc `dbmain.cpp` / `db.cpp` startup and
`ServiceEntryPointMongod` request path. It retains authentication and authorization, TTL and
background indexes, background maintenance, session cleanup, diagnostics, and the MozJS
JavaScript engine. It does not use the embedded application runtime.

Both builds use the vendored ICU 57.1 implementation and the same embedded collation data.
CMake does not select the host's ICU: its version is recorded in index collation metadata,
so switching to a newer system version would change compatibility with SCons-created indexes.
Existing Eloq storage-engine restrictions still apply; for example, `mapReduce` remains explicitly
unsupported even though server-side JavaScript such as `$where` is available.
The test-only `restartCatalog` command is also rejected for Eloq in both builds: its no-op MongoDB
locker cannot guarantee that other workers stop using catalog entries during a live reload.

The build excludes `mongos`, MongoDB replica-set coordination and initial sync, WiredTiger,
the MMAPv1 and Mobile engine registrations, and distributed MongoDB aggregation. A standalone
implementation supplies the shared replication interface without starting replica-set workers;
Eloq Data Substrate continues to provide its own distributed transactions and durability.
Sharding-only pipeline methods live in `pipeline_sharded.cpp`, which SCons builds alongside the
original distributed aggregation planner. CMake omits both files; the shared `pipeline.cpp`
handles local aggregation without distributed-planner rejection stubs.
Some `repl/`, `s/`, and MMAPv1 sources remain because MongoDB's
shared command/catalog code uses their wire types, interfaces, and legacy B-tree helpers; the
distributed runtimes and alternate storage backends themselves are excluded and audited during
configuration.

## 1. Get the source and dependencies

```bash
git clone --recurse-submodules https://github.com/eloqdata/eloqdoc
cd eloqdoc
bash scripts/install_dependency_ubuntu2404.sh /tmp/eloqdoc-deps
```

The dependency script supports Ubuntu 24.04. It installs CMake, Ninja, Python 3, ICU, Python 2.7
with the historical SCons requirements, and the Data Substrate dependency prefix. Set
`ELOQ_INSTALL_LEGACY_SCONS=0` when preparing a CMake-only environment. On another Linux
distribution, install equivalent packages and build the dependencies described by
`src/mongo/db/modules/eloq/data_substrate/scripts/third_party/install-ubuntu2404.sh`.

The CMake IDL compiler requires PyYAML in the Python 3 interpreter selected by CMake.
For Ubuntu's `/usr/bin/python3`, install it with `sudo apt-get install python3-yaml`
(the dependency script already does this). For a virtual environment, install `PyYAML`
there and pass `-DPython3_EXECUTABLE=/path/to/venv/bin/python` to CMake. Python 2's
SCons packages and the cached C++ dependency prefix do not provide this Python 3 module.

By default the Data Substrate dependencies are installed at:

```text
src/mongo/db/modules/eloq/data_substrate/third_party/install
```

Set `ELOQ_THIRD_PARTY_PREFIX` before running the installer and either build system to use a different
absolute prefix.

Use a private dependency prefix, not `/usr`: the SCons flags and CI helper add
`-isystem <prefix>/include`. Adding `/usr/include` this way can break GCC's
`#include_next` lookup in the C++ standard-library headers.

## 2. Build with top-level CMake and Ninja

From the repository root:

```bash
export INSTALL_PREFIX=/absolute/path/to/install
export ELOQ_THIRD_PARTY_PREFIX="$PWD/src/mongo/db/modules/eloq/data_substrate/third_party/install"

cmake -S . -B build/cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DBUILD_SHARED_LIBS=ON \
    -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
    -DELOQ_THIRD_PARTY_PREFIX="$ELOQ_THIRD_PARTY_PREFIX" \
    -DWITH_DATA_STORE=ELOQDSS_ROCKSDB \
    -DWITH_LOG_STATE=ROCKSDB

cmake --build build/cmake -j8
cmake --install build/cmake
```

To select GCC 15, add `-DCMAKE_C_COMPILER=gcc-15 -DCMAKE_CXX_COMPILER=g++-15` to
the configure command and use a separate build directory such as `build/cmake-gcc15`.
Do not change compilers inside an existing build directory; keep the GCC 13, GCC 15,
and SCons build trees separate. Selecting GCC 15 keeps the MongoDB sources in C++17 mode.

The server is installed as `$INSTALL_PREFIX/bin/eloqdoc`. Eloq is the default and only registered
storage engine; explicitly selecting another engine is rejected. MongoDB replica-set, shard-server,
and config-server startup options are rejected. Normal server options, including `--auth`,
`--config`, and `--fork`, use the existing server parser and startup handling.

The current Linux profile builds MongoDB without client-facing TLS, matching the historical SCons
configuration documented below. Data Substrate's separate TLS dependencies are unaffected.

IDL generation uses per-output depfiles, including transitive imports, so editing one IDL file
regenerates only its dependent outputs. The historical SCons IDL compiler is unchanged.

Common data-store choices are:

| `WITH_DATA_STORE` | Matching `WITH_LOG_STATE` |
|---|---|
| `ROCKSDB` or `ELOQDSS_ROCKSDB` | `ROCKSDB` |
| `ELOQDSS_ROCKSDB_CLOUD_S3` | `ROCKSDB_CLOUD_S3` |
| `ELOQDSS_ROCKSDB_CLOUD_GCS` | `ROCKSDB_CLOUD_GCS` |
| `ELOQDSS_ELOQSTORE` | `ROCKSDB`, `ROCKSDB_CLOUD_S3`, or `ROCKSDB_CLOUD_GCS` |

To make an AddressSanitizer build, add `-DELOQDOC_ENABLE_ASAN=ON`.

## 3. Build with SCons

The SCons path first builds and installs the Eloq core libraries, then runs MongoDB's historical
build. Use an installation prefix separate from the top-level CMake build:

```bash
export SCONS_INSTALL_PREFIX=/absolute/path/to/scons-install
export ELOQ_THIRD_PARTY_PREFIX="$PWD/src/mongo/db/modules/eloq/data_substrate/third_party/install"

cmake -S src/mongo/db/modules/eloq \
    -B build/eloq-scons-deps \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX="$SCONS_INSTALL_PREFIX" \
    -DELOQ_THIRD_PARTY_PREFIX="$ELOQ_THIRD_PARTY_PREFIX" \
    -DWITH_DATA_STORE=ELOQDSS_ROCKSDB \
    -DWITH_LOG_STATE=ROCKSDB
cmake --build build/eloq-scons-deps -j8
cmake --install build/eloq-scons-deps

pyenv global 2.7.18
env WITH_DATA_STORE=ELOQDSS_ROCKSDB \
    WITH_LOG_STATE=ROCKSDB \
    ELOQ_THIRD_PARTY_PREFIX="$ELOQ_THIRD_PARTY_PREFIX" \
python2 scripts/buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=RelWithDebInfo \
    CXXFLAGS="-isystem $ELOQ_THIRD_PARTY_PREFIX/include -Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
    CPPDEFINES="ELOQ_MODULE_ENABLED EXT_TX_PROC_ENABLED" \
    LIBPATH="$ELOQ_THIRD_PARTY_PREFIX/lib $ELOQ_THIRD_PARTY_PREFIX/lib64" \
    --build-dir=#build/scons \
    --prefix="$SCONS_INSTALL_PREFIX" \
    --allocator=system \
    --link-model=dynamic \
    --install-mode=hygienic \
    --disable-warnings-as-errors \
    -j8 \
    install-core
```

This path retains the existing SCons target definitions and does not define the CMake-only
`ELOQDOC_STANDALONE` compile definition.

### SCons native-test validation profile

The historical `all`/`install-tests` aliases include unsupported engines, embedded executables,
and upstream tests with obsolete fork interfaces. For EloqDoc validation, filter the generated
unit-test manifest explicitly (Python 3 is used only for this selector; SCons/resmoke use Python 2):

```bash
python3 scripts/buildscripts/select_eloq_unit_tests.py \
    --manifest build/scons/unittests.txt \
    --output build/scons/eloq-unittests.txt \
    --report build/scons/eloq-unittest-selection.json \
    --suite build/scons/eloq-unittests.yml
```

Build the binary paths listed in `eloq-unittests.txt` as explicit SCons targets, using the same
compiler/options/build directory as the server command above, instead of `all` or `install-tests`.
For example, in Bash, `mapfile -t test_targets < build/scons/eloq-unittests.txt` supplies the
targets as `"${test_targets[@]}"`. Then run:

```bash
python2 scripts/buildscripts/resmoke.py \
    --suites=build/scons/eloq-unittests.yml --jobs=4 --continueOnFailure
```

The selection report records each skipped target and its reason. MongoDB replication tests
(`mongo/db/repl/`), MMAPv1, WiredTiger, embedded tests, and individually listed obsolete
interfaces/fixtures are excluded; legacy `dbtest` is not part of this profile. Missing retained
binaries are **not** automatically skipped. Keep the build/install
library directories on `LD_LIBRARY_PATH` for dynamic SCons tests, including test-helper libraries
that are not installed by `install-core`. Limit build/test parallelism to eight; on memory-limited
WSL hosts, use two compiler jobs and one linker job (`-j2 --jlink=1`). CPU affinity (`taskset -c 0-7`
on a host with these CPUs available) also bounds internal worker threads.

The `ephemeralForTest` fixture implements the fork's namespace-keyed catalog metadata,
enumeration, collection lookup/version, and cursor ownership interfaces. Its index cursors also
return the documents required by Eloq's fast `_id` lookup. This is a test-only in-memory model,
not a test of distributed storage. Native regression tests cover catalog commit/rollback,
duplicate namespaces, feature documents, and index lookup with a prefetched document.

All suites under `mongo/db/repl/` are skipped, including replication-process, consistency-marker,
drop-pending reaper, replica-set coordinator, and initial-sync tests. This matches CMake's existing
replication-test exclusion; it changes validation selection, not the historical SCons targets or
the shared interfaces used by the standalone server.

Outside that directory, confirmed fixtures requiring MongoDB's replication oplog, local-shard
locking, capped conversion, or removed local-catalog rename/ident-repair APIs are excluded by exact
target in `scripts/buildscripts/select_eloq_unit_tests.py`. These are unsupported/obsolete fixtures,
not WiredTiger failures and not repaired tests. There is no blanket catalog or sharding-directory
exclusion: collection metadata, KV collection catalog, repair observer, and free-monitoring tests
remain selected. New or unexplained failures outside the documented exclusions remain failures.
Do not remove production `MONGO_UNREACHABLE` guards or return empty mock catalogs to make obsolete
fixtures pass.

The nesting-depth integration test runs with either engine: it expects Eloq update failures at
command level (command-level transaction rollback), and upstream-engine failures in `writeErrors`.
Both branches check `Overflow` and verify that the rejected update leaves the document unchanged.

## 4. Build and run CMake tests

Enable the tests when configuring the same build directory:

```bash
cmake -S . -B build/cmake -G Ninja \
    -DELOQDOC_BUILD_TESTS=ON

cmake --build build/cmake --target eloqdoc-tests -j8
ctest --test-dir build/cmake --output-on-failure -j8
```

The generated test graph contains the C++ unit and integration-test declarations belonging to the
retained single-node MongoDB modules, plus the Eloq storage-integration tests declared by the Eloq
Mongo module. Tests for `mongos`, replication runtimes, and excluded storage engines are omitted.
The CMake-only exclusions also cover distributed-only suites outside those directories:
replica-set clients/monitors, cluster-time signing keys/proofs, remote operation-time tracking,
and config-server/replication RPC metadata. Shared wire-format and local catalog/session tests
remain enabled. `operation_context_test` covers tracker reuse during context recycling (reuse
when exclusively owned; preserve the old tracker and replace it when shared), without enabling
the distributed-execution suites. These exclusions do not change SCons target definitions or
its validation selector.
The in-memory `ephemeralForTest` engine is linked only into unit-test fixtures that need a real
MongoDB storage engine and locker; it is not registered in the CMake server.
Data Substrate and data-store unit tests are also deliberately disabled: those components keep and
run their tests in their own repositories.

MongoDB integration tests require an already-running EloqDoc fixture, so their binaries are built
but their server-dependent cases are not registered with CTest by default. Every integration
binary has an always-registered `startup-options` check which runs `--help` without a server;
this catches missing option-parser initializers even in normal CI. To register the full cases,
configure with:

```bash
-DELOQDOC_REGISTER_INTEGRATION_TESTS=ON \
-DELOQDOC_TEST_CONNECTION_STRING=localhost:27017
```

Then run `ctest --test-dir build/cmake --label-regex integration --output-on-failure` after starting
the fixture.

The existing `eloq_basic` and `eloq_core` JavaScript suites can also run against the CMake
server using an EloqDoc shell. Put the shell directory on `PATH` as well as passing resmoke's
`--mongo` option, because some tests spawn `eloqdoc-cli` by name. `set_param1.js` always runs
its generic parameter checks and runs its oplog-fetcher checks only when the server advertises
replication commands. This preserves those assertions for SCons without restoring replication
to the CMake server or excluding the whole test.

The server smoke test uses the build's selected storage/log backend. It supports local
`ELOQDSS_ROCKSDB` (or `ROCKSDB`) with `ROCKSDB` logging and WAL disabled, or
`ELOQDSS_ELOQSTORE` with `ROCKSDB_CLOUD_S3` logging and WAL enabled. Both start an
authenticated server with temporary local storage and at most two Substrate worker cores.
EloqDoc's catalog requires a data-store handler even for a disposable fixture. The test checks
CRUD, ICU 57.1 collation metadata and lookup, local aggregation, JavaScript,
background/TTL indexes, sessions and transaction commit/abort,
adaptive ingress reactors and coroutine request scheduling, unsupported-topology rejection,
maintenance-mode command dispatch, getMore term validation, fsync, diagnostics, and clean shutdown
while a long-running command is active. It enables test-only commands to
check view-catalog invalidation and safe rejection of online catalog restart; compact, touch, and
repairDatabase must still report Eloq's existing storage-engine limitations. Install the test-only
client and run:

```bash
python3 -m pip install --target build/cmake/python 'pymongo==4.8.0'
PYTHONPATH="$PWD/build/cmake/python" \
    cmake --build build/cmake --target eloqdoc-server-smoke -j8
```

For EloqStore/S3, first start a disposable S3-compatible service and export `S3_ENDPOINT`,
`S3_ACCESS_KEY`, and `S3_SECRET_KEY`. The fixture uses a new bucket on each run and retains
it for inspection; stop/remove your disposable service afterward. CI starts and stops its own
RustFS instance. The CMake CI job tests only amd64/RelWithDebInfo/EloqStore-S3; the SCons CI
jobs cover amd64/RelWithDebInfo with RocksDB-S3 and EloqStore-S3.

The fixture is restricted to at most eight CPUs and retains its logs and data in the temporary
directory printed at startup. It tests Mongo-facing behavior, not Data Substrate/data-store
persistence or recovery. Use `--option=value` for server options with values, as in the existing
SCons launch scripts: the gflags pass can reorder separate argument values.

On a runtime failure, the smoke test reports the server's exit code (and signal name when
applicable) before cleanup, prints bounded log tails, and copies full fixture logs plus a failure
summary to `build/cmake/smoke-diagnostics/`. CI uploads these diagnostics and the RustFS log
as a seven-day artifact. Data directories and configuration files are not uploaded. A shutdown
timeout remains distinct from a nonzero exit, and clean shutdown still requires exit code zero.

The smoke target runs twice to verify TTL shutdown ordering: once with the worker in a
600-second sleep (which shutdown must wake promptly), and once with an active pass paused
before storage access. Both require the TTL worker to finish before storage teardown, while
also interrupting a long-running client command. The active-pass run uses a single Substrate
worker so a blocking wait on the shutdown coroutine would strand TTL's requests. It uses the test-only
`hangTTLMonitorBeforeStorageAccess` failpoint; it is inactive during normal operation.

## 5. Inspect the selected MongoDB graph

The source list is derived statically from MongoDB's fine-grained `SConscript` library metadata;
SCons itself is never imported or executed. This keeps the CMake graph synchronized with the
existing module declarations while allowing explicit single-node substitutions.

```bash
cmake --build build/cmake --target eloqdoc-source-manifest
```

For source/compiler validation on a machine without the Data Substrate dependency prefix, configure
with `-DELOQDOC_BUILD_DATA_SUBSTRATE=OFF`. Generated-source and individual Mongo object targets can
then be built, but the final executable is deliberately not linkable in that mode.
