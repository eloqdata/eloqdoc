# How To Compile EloqDoc-RocksDB

```bash
git clone --recurse-submodules https://github.com/eloqdata/eloqdoc
```

## 0. Install dependencies

Assuming Ubuntu 24.04 is your development environment, run the following script to install dependencies.

```bash
bash scripts/install_dependency_ubuntu2404.sh
```

If you are using another Linux distribution, follow the steps in `install_dependency_ubuntu2404.sh` to install dependencies manually.

EloqDoc still uses the MongoDB 4.0 `scons` build, which requires Python 2.7 and its build-time packages. Ubuntu 24.04 does not provide Python 2.7 through the default apt repositories, so the dependency script installs Python 2.7.18 with `pyenv`. If you use a custom image or install dependencies manually, make sure `python2` and `pip2` are available on `PATH` before building.

```bash
python2 --version
pip2 --version
```

## 1. Compile EloqDoc-RocksDB

There are two components to compile: EloqDoc and core libraries. EloqDoc is compiled with `scons`, and the core libraries are compiled with `cmake`.

### 1.1 Define an installation path

```bash
export INSTALL_PREFIX=/absolute/path/to/install
```

The `INSTALL_PREFIX` must be an absolute path. Binaries and libraries will be installed under this directory.

### 1.2 Compile core libraries

Run the following commands from the repository root:

```bash
cmake -S src/mongo/db/modules/eloq \
      -B src/mongo/db/modules/eloq/build \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX \
      -DWITH_DATA_STORE=ELOQDSS_ROCKSDB
cmake --build src/mongo/db/modules/eloq/build -j8
cmake --install src/mongo/db/modules/eloq/build
```

### 1.3 Compile EloqDoc-RocksDB

The `scons` build tool depends on Python 2.7. Switch to a Python 2.7 environment before running `scons`. The `install_dependency_ubuntu2404.sh` script installs Python 2.7.18 and the required packages.

```bash
pyenv global 2.7.18
```

Compile EloqDoc from the repository root.

```bash
env WITH_DATA_STORE=ELOQDSS_ROCKSDB \
python scripts/buildscripts/scons.py \
    MONGO_VERSION=4.0.3 \
    VARIANT_DIR=RelWithDebInfo \
    CXXFLAGS="-Wno-nonnull -Wno-class-memaccess -Wno-interference-size -Wno-redundant-move" \
    --build-dir=#build \
    --prefix=$INSTALL_PREFIX \
    --disable-warnings-as-errors \
    -j8 \
    install-core
```

All executable files will be installed to `$INSTALL_PREFIX/bin`, and all libraries will be installed to `$INSTALL_PREFIX/lib`.
