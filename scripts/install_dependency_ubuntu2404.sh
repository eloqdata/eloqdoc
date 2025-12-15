#!/bin/bash
#
# Script to install dependencies for EloqDoc
# Usage: ./install_eloq_dependancy.sh <TEMP_DIR> [--skip_eloq_common]
#   TEMP_DIR: Required unless --skip_eloq_common is specified
#   --skip_eloq_common: Optional. Skip common dependencies and only install Python

set -eo pipefail

# Store original directory where script was executed from
SCRIPT_START_DIR=$(pwd)

# ------------- Logging functions -------------
# Define color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" >&2
}

# ------------- Command line argument parsing -------------
SKIP_ELOQ_COMMON=false
TEMP_DIR=""

# First check if --skip_eloq_common is provided as an argument
for arg in "$@"; do
    if [ "$arg" == "--skip_eloq_common" ]; then
        SKIP_ELOQ_COMMON=true
        break
    fi
done

# Process arguments differently based on whether we're skipping common dependencies
if [ "$SKIP_ELOQ_COMMON" = true ]; then
    # When skipping common dependencies, TEMP_DIR is optional
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip_eloq_common)
                log_info "Skipping common dependencies, will only install Python related dependencies"
                shift
                ;;
            *)
                # If another argument is provided, treat it as TEMP_DIR
                if [ -z "$TEMP_DIR" ]; then
                    TEMP_DIR=$1
                else
                    log_warning "Unknown option: $1"
                fi
                shift
                ;;
        esac
    done
    # No need to check if TEMP_DIR was provided when using --skip_eloq_common
else
    # Original behavior: TEMP_DIR is required
    if [ $# -lt 1 ]; then
        log_error "TEMP_DIR is required when not using --skip_eloq_common"
        echo "Usage: $0 <TEMP_DIR> [--skip_eloq_common]"
        exit 1
    fi

    # Set TEMP_DIR
    TEMP_DIR=$1
    shift

    # Process other arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip_eloq_common)
                SKIP_ELOQ_COMMON=true
                log_info "Skipping common dependencies, will only install Python related dependencies"
                shift
                ;;
            *)
                log_warning "Unknown option: $1"
                shift
                ;;
        esac
    done
fi

# Convert TEMP_DIR to absolute path if it's provided
if [ -n "$TEMP_DIR" ]; then
    # Convert to absolute path to avoid issues with relative paths
    TEMP_DIR=$(realpath "$TEMP_DIR")
    log_info "Using directory for downloads and installations: ${TEMP_DIR}"
    
    # Create directory if it doesn't exist
    if [ ! -d "$TEMP_DIR" ]; then
        log_info "Creating directory: ${TEMP_DIR}"
        mkdir -p "$TEMP_DIR"
    else
        log_info "Using existing directory: ${TEMP_DIR}"
    fi
elif [ "$SKIP_ELOQ_COMMON" = false ]; then
    # If TEMP_DIR is not provided and we're not skipping common dependencies, that's an error
    log_error "TEMP_DIR is required when not using --skip_eloq_common"
    echo "Usage: $0 <TEMP_DIR> [--skip_eloq_common]"
    exit 1
else
    log_warning "No TEMP_DIR provided. Python will be installed system-wide."
fi

# ------------- Detect CPU count for optimal parallel compilation -------------
# Get number of CPU cores
CPU_COUNT=$(nproc)
# For better performance, reserve 1 core for system if we have more than 1 core
if [ "$CPU_COUNT" -gt 1 ]; then
    COMPILE_JOBS=$((CPU_COUNT - 1))
else
    COMPILE_JOBS=1
fi
log_info "Detected $CPU_COUNT CPU cores, will use $COMPILE_JOBS for parallel compilation"

export DEBIAN_FRONTEND=noninteractive

if [ "$SKIP_ELOQ_COMMON" = false ]; then
    # ------------- System packages installation -------------
    log_info "Installing required system packages"
    sudo apt update
    # Consolidated all apt-get installations into a single command
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        jq sudo vim wget curl gdb libcurl4-openssl-dev build-essential \
        libncurses5-dev libncursesw5-dev gnutls-dev bison zlib1g-dev ccache \
        cmake ninja-build libuv1-dev git g++ make openjdk-11-jdk openssh-client \
        libssl-dev libgflags-dev libleveldb-dev libsnappy-dev openssl lcov \
        libbz2-dev liblz4-dev libzstd-dev libboost-context-dev ca-certificates \
        libc-ares-dev libc-ares2 m4 pkg-config tar libreadline-dev libsqlite3-dev \
        llvm xz-utils tk-dev libffi-dev liblzma-dev

    # ------------- Mimalloc installation -------------
    log_info "Installing mimalloc"
    cd $TEMP_DIR
    if [ ! -d "mimalloc" ]; then
        git clone https://github.com/eloqdata/mimalloc.git mimalloc
    fi
    cd mimalloc && \
    git checkout eloq-v2.1.2 && \
    mkdir -p bld && cd bld && \
    cmake .. && \
    make -j$COMPILE_JOBS && \
    sudo make install

    # ------------- Abseil installation -------------
    log_info "Installing Abseil"
    cd $TEMP_DIR
    if [ ! -d "abseil-cpp" ]; then
        mkdir -p abseil-cpp
    fi
    cd abseil-cpp && \
    # Download and extract Abseil source code, then modify options.h to disable certain features
    curl -fsSL https://github.com/abseil/abseil-cpp/archive/20230802.0.tar.gz | tar -xzf - --strip-components=1 && \
    sed -i 's/^#define ABSL_OPTION_USE_\(.*\) 2/#define ABSL_OPTION_USE_\1 0/' "absl/base/options.h" && \
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DABSL_BUILD_TESTING=OFF \
        -DBUILD_SHARED_LIBS=yes \
        -S . -B cmake-out && \
    cmake --build cmake-out -- -j $(nproc) && \
    sudo cmake --build cmake-out --target install -- -j $(nproc) && \
    sudo ldconfig 

    # ------------- Protobuf installation -------------
    # Compile protobuf from source code. Protobuf version needs to be compatible
    # with both brpc and grpc. It cannot be too high or too low.
    log_info "Installing protobuf"
    cd $TEMP_DIR
    if [ ! -d "protobuf" ]; then
        mkdir -p protobuf
    fi
    cd protobuf
    curl -fsSL https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=yes \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_ABSL_PROVIDER=package \
    -S . -B cmake-out && \
    cmake --build cmake-out -- -j $COMPILE_JOBS && \
    sudo cmake --build cmake-out --target install -- -j $COMPILE_JOBS && \
    sudo ldconfig

    # ------------- RE2 installation -------------
    log_info "Installing RE2"
    cd "$TEMP_DIR"
    if [ ! -d "re2" ]; then
        mkdir -p re2
    fi
    cd re2 && \
    curl -fsSL https://github.com/google/re2/archive/2023-08-01.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_SHARED_LIBS=ON \
          -DRE2_BUILD_TESTING=OFF \
          -S . -B cmake-out && \
    cmake --build cmake-out -- -j "$COMPILE_JOBS" && \
    sudo cmake --build cmake-out --target install -- -j "$COMPILE_JOBS" && \
    sudo ldconfig

    # ------------- crc32c installation -------------
    log_info "Installing crc32c"
    cd "$TEMP_DIR"
    if [ ! -d "crc32c" ]; then
        mkdir -p crc32c
    fi
    cd crc32c && \
    curl -fsSL https://github.com/google/crc32c/archive/1.1.2.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_SHARED_LIBS=YES \
          -DCRC32C_BUILD_TESTS=OFF \
          -DCRC32C_BUILD_BENCHMARKS=OFF \
          -DCRC32C_USE_GLOG=OFF \
          -S . -B cmake-out && \
    cmake --build cmake-out -- -j "$COMPILE_JOBS" && \
    sudo cmake --build cmake-out --target install -- -j "$COMPILE_JOBS" && \
    sudo ldconfig

    # ------------- nlohmann_json installation -------------
    log_info "Installing nlohmann_json"
    cd "$TEMP_DIR"
    if [ ! -d "nlohmann_json" ]; then
        mkdir -p nlohmann_json
    fi
    cd nlohmann_json && \
    curl -fsSL https://github.com/nlohmann/json/archive/v3.11.2.tar.gz | \
    tar -xzf - --strip-components=1 && \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_SHARED_LIBS=YES \
          -DBUILD_TESTING=OFF \
          -DJSON_BuildTests=OFF \
          -S . -B cmake-out && \
    sudo cmake --build cmake-out --target install -- -j "$COMPILE_JOBS" && \
    sudo ldconfig

    # ------------- grpc installation -------------
    log_info "Installing grpc"
    cd $TEMP_DIR
    if [ ! -d "grpc" ]; then
        mkdir -p grpc
    fi
    cd grpc
    curl -fsSL https://codeload.github.com/grpc/grpc/tar.gz/refs/tags/v1.51.1 | \
    tar -xzf - --strip-components=1 && \
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=yes \
        -DgRPC_INSTALL=ON \
        -DgRPC_BUILD_TESTS=OFF \
        -DgRPC_ABSL_PROVIDER=package \
        -DgRPC_CARES_PROVIDER=package \
        -DgRPC_PROTOBUF_PROVIDER=package \
        -DgRPC_RE2_PROVIDER=package \
        -DgRPC_SSL_PROVIDER=package \
        -DgRPC_ZLIB_PROVIDER=package \
        -S . -B cmake-out && \
    cmake --build cmake-out -- -j $COMPILE_JOBS && \
    sudo cmake --build cmake-out --target install -- -j $COMPILE_JOBS && \
    sudo ldconfig

    # ------------- Glog installation -------------
    log_info "Installing glog"
    cd $TEMP_DIR
    if [ ! -d "glog" ]; then
        git clone https://github.com/eloqdata/glog.git glog
    fi
    cd glog && \
    cmake -S . -B build -G "Unix Makefiles" && \
    cmake --build build -j$COMPILE_JOBS && \
    sudo cmake --build build --target install 

    # ------------- Liburing installation -------------
    log_info "Installing liburing"
    cd $TEMP_DIR
    if [ ! -d "liburing" ]; then
        git clone https://github.com/axboe/liburing.git liburing
    fi
    cd liburing && \
    git checkout tags/liburing-2.6 && \
    ./configure --cc=gcc --cxx=g++ && \
    make -j$COMPILE_JOBS && \
    sudo make install

    # ------------- BRPC installation -------------
    # Build brpc after protobuf and glog
    log_info "Installing brpc"
    cd $TEMP_DIR
    if [ ! -d "brpc" ]; then
        git clone https://github.com/eloqdata/brpc.git brpc
    fi
    cd brpc && \
    mkdir -p build && cd build && \
    cmake .. \
    -DWITH_GLOG=ON \
    -DBUILD_SHARED_LIBS=ON && \
    cmake --build . -j$COMPILE_JOBS && \
    sudo cp -r ./output/include/* /usr/include/ && \
    sudo cp ./output/lib/* /usr/lib/

    # ------------- BRAFT installation -------------
    log_info "Installing braft"
    cd $TEMP_DIR
    if [ ! -d "braft" ]; then
        git clone https://github.com/eloqdata/braft.git braft
    fi
    cd braft && \
    sed -i 's/libbrpc.a//g' CMakeLists.txt && \
    mkdir -p bld && cd bld && \
    cmake .. -DBRPC_WITH_GLOG=ON && \
    cmake --build . -j$COMPILE_JOBS && \
    sudo cp -r ./output/include/* /usr/include/ && \
    sudo cp ./output/lib/* /usr/lib/

    # ------------- Cuckoo filter installation -------------
    log_info "Installing cuckoo filter"
    cd $TEMP_DIR
    if [ ! -d "cuckoofilter" ]; then
        git clone https://github.com/eloqdata/cuckoofilter.git cuckoofilter
    fi
    cd cuckoofilter && \
    sudo make install

    # ------------- Google Cloud CPP installation -------------
    cd $TEMP_DIR
    if [ ! -d "google-cloud-cpp" ]; then
        mkdir google-cloud-cpp && cd google-cloud-cpp && \
        curl -fsSL https://codeload.github.com/googleapis/google-cloud-cpp/tar.gz/refs/tags/v2.24.0 | tar -xzf - --strip-components=1
        cd ../
    fi
    cd google-cloud-cpp && cmake -S . -B cmake-out \
        -DCMAKE_INSTALL_PREFIX="/usr/local" \
        -DBUILD_SHARED_LIBS=ON \
        -DBUILD_TESTING=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF \
        -DGOOGLE_CLOUD_CPP_ENABLE=bigtable,storage
    cmake --build cmake-out -- -j $(nproc)
    sudo cmake --build cmake-out --target install
    cd ../ && rm -rf google-cloud-cpp

    # ------------- AWS SDK installation -------------
    log_info "Installing AWS SDK"
    cd $TEMP_DIR
    if [ ! -d "aws" ]; then
        git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git aws
    fi
    cd aws && \
    git checkout tags/1.11.521 && \
    git submodule update --recursive && \
    mkdir -p bld && cd bld && \
    cmake .. \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX=./output/ \
    -DENABLE_TESTING=OFF \
    -DBUILD_SHARED_LIBS=ON \
    -DFORCE_SHARED_CRT=OFF \
    -DBUILD_ONLY="dynamodb;sqs;s3;kinesis;kafka;transfer" && \
    cmake --build . --config RelWithDebInfo -j$COMPILE_JOBS && \
    cmake --install . --config RelWithDebInfo && \
    sudo cp -r ./output/include/* /usr/include/ && \
    sudo cp -r ./output/lib/* /usr/lib/ 

    # ------------- RocksDB installation -------------
    log_info "Installing RocksDB"
    cd $TEMP_DIR
    if [ ! -d "rocksdb" ]; then
        git clone https://github.com/facebook/rocksdb.git rocksdb
    fi
    cd rocksdb && \
    git checkout tags/v9.1.0 && \
    USE_RTTI=1 PORTABLE=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1 make -j$COMPILE_JOBS shared_lib && \
    sudo make install-shared && \
    cd ../ && \
    sudo ldconfig

    # ------------- RocksDB-Cloud installation -------------
    log_info "Installing rocksdb-cloud"
    cd $WORKSPACE
    git clone https://github.com/eloqdata/rocksdb-cloud.git && \
    cd rocksdb-cloud && \
    git checkout main && \
    LIBNAME=librocksdb-cloud-aws USE_RTTI=1 USE_AWS=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1 make shared_lib -j8 && \
    LIBNAME=librocksdb-cloud-aws PREFIX=`pwd`/output make install-shared && \
    sudo mkdir -p /usr/local/include/rocksdb_cloud_header && \
    sudo cp -r ./output/include/* /usr/local/include/rocksdb_cloud_header && \
    sudo cp -r ./output/lib/* /usr/local/lib && \
    make clean && rm -rf `pwd`/output && \
    LIBNAME=librocksdb-cloud-gcp USE_RTTI=1 USE_GCP=1 ROCKSDB_DISABLE_TCMALLOC=1 ROCKSDB_DISABLE_JEMALLOC=1 make shared_lib -j8 && \
    LIBNAME=librocksdb-cloud-gcp PREFIX=`pwd`/output make install-shared && \
    sudo cp -r ./output/lib/* /usr/local/lib && \
    cd ../ && \
    sudo ldconfig

    # ------------- Prometheus CPP installation -------------
    log_info "Installing prometheus-cpp"
    cd $TEMP_DIR
    if [ ! -d "prometheus-cpp" ]; then
        git clone https://github.com/jupp0r/prometheus-cpp.git prometheus-cpp
    fi
    cd prometheus-cpp && \
    git checkout tags/v1.1.0 && \
    git submodule init && git submodule update && \
    mkdir -p _build && cd _build && \
    cmake .. -DBUILD_SHARED_LIBS=ON && \
    cmake --build . -j$COMPILE_JOBS && \
    sudo cmake --install .
fi

# ------------- Python 2.7.18 installation with pyenv -------------
# Python 2.7.18 is required for building Eloqdoc
log_info "Installing Python 2.7.18 with pyenv (required for Eloqdoc build process)"

# Install system dependencies to build python
sudo apt-get update && sudo apt-get install -y build-essential zlib1g-dev libbz2-dev liblzma-dev libreadline-dev libsqlite3-dev libffi-dev wget unzip git curl

# Install pyenv in user's home directory
curl -fsSL https://pyenv.run | bash

# Initialize pyenv only for this non-interactive shell session
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)" || true
eval "$(pyenv virtualenv-init -)" || true

# Install Python 2.7.18
pyenv install 2.7.18
pyenv global 2.7.18

# Return to the original directory to find the requirements.txt file
cd "$SCRIPT_START_DIR"
log_info "Installing Python dependencies from: $SCRIPT_START_DIR/scripts/buildscripts/requirements.txt"

# Install Python dependencies
pip2 install -r scripts/buildscripts/requirements.txt

log_info "EloqDoc dependency installation completed successfully"
