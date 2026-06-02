FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYENV_ROOT=/opt/pyenv
ENV PATH=/opt/pyenv/shims:/opt/pyenv/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl wget git sudo vim gdb pkg-config file procps \
    build-essential gcc g++ make cmake ninja-build ccache bison m4 \
    openjdk-11-jdk openssh-client \
    libssl-dev libcurl4-openssl-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev \
    libsnappy-dev libreadline-dev libsqlite3-dev libffi-dev liblzma-dev \
    libncurses5-dev libncursesw5-dev xz-utils tk-dev llvm \
    libgflags-dev libgoogle-glog-dev libleveldb-dev libboost-dev libboost-context-dev \
    libprotobuf-dev libprotoc-dev protobuf-compiler libgrpc++-dev protobuf-compiler-grpc \
    libc-ares-dev \
    libpoco-dev librocksdb-dev liburing-dev libjsoncpp-dev \
    autoconf automake libtool unzip tar && \
    rm -rf /var/lib/apt/lists/*

# Build dependencies that are not available as suitable Ubuntu packages.
RUN set -eux; \
    mkdir -p /opt/src; \
    git clone https://github.com/efficient/cuckoofilter.git /opt/src/cuckoofilter; \
    git -C /opt/src/cuckoofilter checkout 917583d6abef692dfa8e14453bd77d6e0b61eef3; \
    mkdir -p /usr/local/include/cuckoofilter; \
    cp /opt/src/cuckoofilter/src/*.h /usr/local/include/cuckoofilter/

RUN set -eux; \
    mkdir -p /opt/src; \
    git clone --depth 1 --branch eloq-v2.1.2 https://github.com/eloqdata/mimalloc.git /opt/src/mimalloc; \
    cmake -S /opt/src/mimalloc -B /opt/src/mimalloc/bld -DCMAKE_BUILD_TYPE=Release -DMI_BUILD_TESTS=OFF; \
    cmake --build /opt/src/mimalloc/bld -j"$(nproc)"; \
    cmake --install /opt/src/mimalloc/bld; \
    ldconfig

# Ubuntu 24.04 ships glog 0.6 headers without the public
# FLAGS_log_file_header declaration used by data_substrate's logging helper.
# Install glog 0.7.0 under /usr so both the compiler and linker prefer it over
# the distro libglog package while keeping apt dependency resolution simple.
RUN set -eux; \
    mkdir -p /opt/src; \
    git clone --depth 1 --branch v0.7.0 https://github.com/google/glog.git /opt/src/glog; \
    cmake -S /opt/src/glog -B /opt/src/glog/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr \
      -DBUILD_SHARED_LIBS=ON \
      -DWITH_GFLAGS=ON \
      -DWITH_GTEST=OFF; \
    cmake --build /opt/src/glog/build -j"$(nproc)"; \
    cmake --install /opt/src/glog/build; \
    for h in /usr/include/glog/logging.h /usr/include/glog/flags.h /usr/include/glog/log_severity.h /usr/include/glog/raw_logging.h /usr/include/glog/vlog_is_on.h; do \
      sed -i '1i#ifndef GLOG_USE_GLOG_EXPORT\n#define GLOG_USE_GLOG_EXPORT\n#endif' "$h"; \
    done; \
    ldconfig

RUN set -eux; \
    mkdir -p /opt/src; \
    git clone --depth 1 https://github.com/eloqdata/brpc.git /opt/src/brpc; \
    cmake -S /opt/src/brpc -B /opt/src/brpc/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DWITH_GLOG=ON \
      -DIO_URING_ENABLED=OFF \
      -DBUILD_SHARED_LIBS=ON; \
    cmake --build /opt/src/brpc/build -j"$(nproc)"; \
    cp -r /opt/src/brpc/build/output/include/* /usr/local/include/; \
    cp /opt/src/brpc/build/output/lib/* /usr/local/lib/; \
    ldconfig

RUN set -eux; \
    mkdir -p /opt/src; \
    git clone --depth 1 https://github.com/eloqdata/braft.git /opt/src/braft; \
    sed -i 's/libbrpc.a//g' /opt/src/braft/CMakeLists.txt; \
    cmake -S /opt/src/braft -B /opt/src/braft/bld \
      -DCMAKE_BUILD_TYPE=Release \
      -DBRPC_WITH_GLOG=ON \
      -DBUILD_SHARED_LIBS=ON; \
    cmake --build /opt/src/braft/bld -j"$(nproc)"; \
    cp -r /opt/src/braft/bld/output/include/* /usr/local/include/; \
    cp /opt/src/braft/bld/output/lib/* /usr/local/lib/; \
    ldconfig

RUN set -eux; \
    mkdir -p /opt/src; \
    git clone --depth 1 --branch v1.1.0 https://github.com/jupp0r/prometheus-cpp.git /opt/src/prometheus-cpp; \
    git -C /opt/src/prometheus-cpp submodule update --init --recursive; \
    cmake -S /opt/src/prometheus-cpp -B /opt/src/prometheus-cpp/_build \
      -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_SHARED_LIBS=ON \
      -DENABLE_TESTING=OFF; \
    cmake --build /opt/src/prometheus-cpp/_build -j"$(nproc)"; \
    cmake --install /opt/src/prometheus-cpp/_build; \
    ldconfig

# Python 2.7 is required by the Mongo/SCons build used by EloqDoc.
RUN set -eux; \
    git clone --depth 1 https://github.com/pyenv/pyenv.git "$PYENV_ROOT"; \
    pyenv install 2.7.18; \
    pyenv global 2.7.18; \
    python --version; \
    pip install --upgrade 'pip<21' 'setuptools<45' wheel; \
    pip install \
      'Cheetah3==3.0.0' \
      'Jinja2==2.10' \
      'MarkupSafe<2' \
      'PyYAML==3.11' \
      'mock==2.0.0' \
      'pymongo>=3.0,<4' \
      'requests>=2.16.1,<3' \
      'subprocess32>=3.2.7' \
      'typing==3.6.1' \
      'unittest-xml-reporting==2.1.0'

WORKDIR /work/eloqdoc
CMD ["bash"]
