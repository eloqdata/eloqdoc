# Release builder for EloqDoc + TiKV.
#
# manylinux2014 is CentOS 7 based, so every binary built in this image is
# constrained by glibc 2.17. Keep this image as the release ABI baseline; do not
# replace it with a newer developer distro image for release artifacts.
FROM quay.io/pypa/manylinux2014_x86_64:latest

ENV PATH=/opt/rh/devtoolset-10/root/usr/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
ENV LD_LIBRARY_PATH=/opt/rh/devtoolset-10/root/usr/lib64:/usr/local/lib64:/usr/local/lib
ENV PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig
# The manylinux image ships a current CMake. This keeps old third-party
# CMakeLists.txt files buildable without downgrading CMake for newer deps.
ENV CMAKE_POLICY_VERSION_MINIMUM=3.5
ENV MAKEFLAGS=-j8

RUN yum install -y \
      autoconf automake bison bzip2-devel ca-certificates curl-devel file git \
      java-11-openjdk-devel jsoncpp-devel leveldb-devel libcurl-devel \
      libtool libzstd-devel lz4-devel m4 ncurses-devel ninja-build \
      openssl-devel patch pkgconfig procps-ng readline-devel snappy-devel \
      sqlite-devel tar unzip wget xz-devel zlib-devel boost-devel \
    && yum clean all

# Python 2.7 is required by the Mongo/SCons build used by EloqDoc.
RUN set -eux; \
    curl -fsSL https://bootstrap.pypa.io/pip/2.7/get-pip.py -o /tmp/get-pip.py; \
    python /tmp/get-pip.py 'pip<21' 'setuptools<45' wheel; \
    rm -f /tmp/get-pip.py; \
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

RUN set -eux; \
    mkdir -p /opt/src; \
    git clone https://github.com/efficient/cuckoofilter.git /opt/src/cuckoofilter; \
    git -C /opt/src/cuckoofilter checkout 917583d6abef692dfa8e14453bd77d6e0b61eef3; \
    mkdir -p /usr/local/include/cuckoofilter; \
    cp /opt/src/cuckoofilter/src/*.h /usr/local/include/cuckoofilter/

RUN set -eux; \
    git clone --depth 1 --branch v2.2.2 https://github.com/gflags/gflags.git /opt/src/gflags; \
    cmake -S /opt/src/gflags -B /opt/src/gflags/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DBUILD_SHARED_LIBS=ON \
      -DBUILD_STATIC_LIBS=OFF \
      -DBUILD_gflags_LIB=ON \
      -DBUILD_gflags_nothreads_LIB=OFF; \
    cmake --build /opt/src/gflags/build; \
    cmake --install /opt/src/gflags/build; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 --branch v0.7.0 https://github.com/google/glog.git /opt/src/glog; \
    cmake -S /opt/src/glog -B /opt/src/glog/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DBUILD_SHARED_LIBS=ON \
      -DWITH_GFLAGS=ON \
      -DWITH_GTEST=OFF; \
    cmake --build /opt/src/glog/build; \
    cmake --install /opt/src/glog/build; \
    for h in /usr/local/include/glog/logging.h /usr/local/include/glog/flags.h /usr/local/include/glog/log_severity.h /usr/local/include/glog/raw_logging.h /usr/local/include/glog/vlog_is_on.h; do \
      sed -i '1i#ifndef GLOG_USE_GLOG_EXPORT\n#define GLOG_USE_GLOG_EXPORT\n#endif' "$h"; \
    done; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 --branch eloq-v2.1.2 https://github.com/eloqdata/mimalloc.git /opt/src/mimalloc; \
    cmake -S /opt/src/mimalloc -B /opt/src/mimalloc/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DMI_BUILD_TESTS=OFF; \
    cmake --build /opt/src/mimalloc/build; \
    cmake --install /opt/src/mimalloc/build; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 --branch v3.21.12 https://github.com/protocolbuffers/protobuf.git /opt/src/protobuf; \
    cmake -S /opt/src/protobuf -B /opt/src/protobuf/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -Dprotobuf_BUILD_TESTS=OFF \
      -Dprotobuf_BUILD_SHARED_LIBS=ON \
      -Dprotobuf_ABSL_PROVIDER=module; \
    cmake --build /opt/src/protobuf/build; \
    cmake --install /opt/src/protobuf/build; \
    ldconfig; \
    protoc --version

RUN set -eux; \
    git clone --depth 1 --branch v1.51.1 --recurse-submodules --shallow-submodules https://github.com/grpc/grpc.git /opt/src/grpc; \
    cmake -S /opt/src/grpc -B /opt/src/grpc/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DBUILD_SHARED_LIBS=ON \
      -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DgRPC_BUILD_CSHARP_EXT=OFF \
      -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=ON \
      -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
      -DgRPC_ABSL_PROVIDER=module \
      -DgRPC_CARES_PROVIDER=module \
      -DgRPC_RE2_PROVIDER=module \
      -DgRPC_SSL_PROVIDER=package \
      -DgRPC_ZLIB_PROVIDER=package \
      -DgRPC_PROTOBUF_PROVIDER=package; \
    cmake --build /opt/src/grpc/build; \
    cmake --install /opt/src/grpc/build; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 --branch poco-1.12.5-release https://github.com/pocoproject/poco.git /opt/src/poco; \
    cmake -S /opt/src/poco -B /opt/src/poco/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DBUILD_SHARED_LIBS=ON \
      -DENABLE_TESTS=OFF \
      -DENABLE_SAMPLES=OFF; \
    cmake --build /opt/src/poco/build; \
    cmake --install /opt/src/poco/build; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 --branch v8.9.1 https://github.com/facebook/rocksdb.git /opt/src/rocksdb; \
    make -C /opt/src/rocksdb shared_lib PORTABLE=1 DEBUG_LEVEL=0 USE_RTTI=1; \
    make -C /opt/src/rocksdb install-shared INSTALL_PATH=/usr/local; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 https://github.com/eloqdata/brpc.git /opt/src/brpc; \
    cmake -S /opt/src/brpc -B /opt/src/brpc/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DWITH_GLOG=ON \
      -DIO_URING_ENABLED=OFF \
      -DBUILD_SHARED_LIBS=ON; \
    cmake --build /opt/src/brpc/build; \
    cp -r /opt/src/brpc/build/output/include/* /usr/local/include/; \
    cp /opt/src/brpc/build/output/lib/* /usr/local/lib/; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 https://github.com/eloqdata/braft.git /opt/src/braft; \
    sed -i 's/libbrpc.a//g' /opt/src/braft/CMakeLists.txt; \
    cmake -S /opt/src/braft -B /opt/src/braft/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DBRPC_WITH_GLOG=ON \
      -DBUILD_SHARED_LIBS=ON; \
    cmake --build /opt/src/braft/build; \
    cp -r /opt/src/braft/build/output/include/* /usr/local/include/; \
    cp /opt/src/braft/build/output/lib/* /usr/local/lib/; \
    ldconfig

RUN set -eux; \
    git clone --depth 1 --branch v1.1.0 https://github.com/jupp0r/prometheus-cpp.git /opt/src/prometheus-cpp; \
    git -C /opt/src/prometheus-cpp submodule update --init --recursive; \
    cmake -S /opt/src/prometheus-cpp -B /opt/src/prometheus-cpp/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DBUILD_SHARED_LIBS=ON \
      -DENABLE_TESTING=OFF; \
    cmake --build /opt/src/prometheus-cpp/build; \
    cmake --install /opt/src/prometheus-cpp/build; \
    ldconfig

# CentOS 7 ships Boost 1.53, while tx_service requires Boost >= 1.70 and
# links boost_context. Install only the libraries EloqDoc needs.
RUN set -eux; \
    curl -fsSL https://archives.boost.io/release/1.82.0/source/boost_1_82_0.tar.gz -o /tmp/boost_1_82_0.tar.gz; \
    tar -xzf /tmp/boost_1_82_0.tar.gz -C /opt/src; \
    cd /opt/src/boost_1_82_0; \
    ./bootstrap.sh --prefix=/usr/local --with-libraries=context,random,thread,system,chrono,date_time,atomic; \
    ./b2 -j"$(nproc)" cxxstd=17 link=shared runtime-link=shared threading=multi variant=release install; \
    rm -f /tmp/boost_1_82_0.tar.gz; \
    ldconfig

WORKDIR /work/eloqdoc
CMD ["bash"]
