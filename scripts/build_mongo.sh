#!/bin/bash
#
# Script to build MongoDB with EloqDoc integration
# Usage: ./build_mongo.sh <build-type>
#   build-type: Debug, Release, RelWithDebInfo, Compiledb, or Client

# Exit on error, undefined variable, or pipeline failure
set -euo pipefail

# ------------- Color definitions -------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# ------------- Logging functions -------------
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" >&2
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# ------------- Error handling -------------
cleanup_on_error() {
    log_error "Build failed. See above for errors."
    exit 1
}

# Set up trap for unexpected errors
trap cleanup_on_error ERR

# ------------- Help message -------------
show_usage() {
    echo -e "${BOLD}MongoDB with EloqDoc Builder${NC}"
    echo "Usage: $0 <build-type>"
    echo
    echo "Build Types:"
    echo "  Debug          - Debug build"
    echo "  DebugASAN      - Debug build with ASAN enabled"
    echo "  Release        - Optimized release build with LTO"
    echo "  RelWithDebInfo - Release build with debug symbols"
    echo "  Compiledb      - Generate compilation database"
    echo "  Client         - Build MongoDB client only"
    echo
    echo "Example: $0 Debug"
}

# ------------- Common build settings -------------
# Detect CPU cores for optimal parallel builds
CPU_CORE_SIZE=$(nproc 2>/dev/null || grep -c ^processor /proc/cpuinfo 2>/dev/null || echo 4)

# Default to using llvm-symbolizer-10 if not set
export LLVM_SYMBOLIZER=${LLVM_SYMBOLIZER:-"llvm-symbolizer-10"}

# Check if LLVM symbolizer exists
if ! command -v ${LLVM_SYMBOLIZER} &> /dev/null; then
    log_warning "LLVM symbolizer ${LLVM_SYMBOLIZER} not found. Fallback to default."
    unset LLVM_SYMBOLIZER
fi

# Active Python2
log_info "Active Python 2.7.18"
pyenv local 2.7.18

# Base build command with common options
COMMON_CMD="python2 buildscripts/scons.py \
LIBPATH=/usr/local/lib \
MONGO_VERSION=4.0.3 \
--build-dir=#build \
--allocator=system \
--link-model=dynamic \
--disable-warnings-as-errors \
-j${CPU_CORE_SIZE}"

# Add LLVM symbolizer if available
if [ -n "${LLVM_SYMBOLIZER:-}" ]; then
    COMMON_CMD="${COMMON_CMD} --llvm-symbolizer=${LLVM_SYMBOLIZER}"
fi

# ------------- Build type configurations -------------
build_debug() {
    log_step "Building Debug configuration"
    rm -f ./src/mongo/db/modules/eloq/build
    ln -s debug_build ./src/mongo/db/modules/eloq/build
    ${COMMON_CMD} VARIANT_DIR=Debug --dbg=on --opt=off mongod
}

build_debug_asan() {
    log_step "Building Debug configuration with ASAN"
    rm -f ./src/mongo/db/modules/eloq/build
    ln -s debug_build ./src/mongo/db/modules/eloq/build
    ${COMMON_CMD} VARIANT_DIR=DebugAsan --dbg=on --opt=off --sanitize=leak,address mongod
}

build_release() {
    log_step "Building Release configuration with LTO"
    rm -f ./src/mongo/db/modules/eloq/build
    ln -s release_build ./src/mongo/db/modules/eloq/build
    ${COMMON_CMD} VARIANT_DIR=Release --release --lto --dbg=off --opt=on mongod
}

build_relwithdebinfo() {
    log_step "Building RelWithDebInfo configuration"
    rm -f ./src/mongo/db/modules/eloq/build
    ln -s release_build ./src/mongo/db/modules/eloq/build
    ${COMMON_CMD} VARIANT_DIR=RelWithDebInfo --dbg=on --opt=on mongod
}

build_compiledb() {
    log_step "Generating compilation database"
    rm -f ./src/mongo/db/modules/eloq/build
    ln -s debug_build ./src/mongo/db/modules/eloq/build
    ${COMMON_CMD} VARIANT_DIR=Debug --dbg=on --opt=off compiledb
}

build_client() {
    log_step "Building MongoDB client"
    rm -f ./src/mongo/db/modules/eloq/build
    ln -s release_build ./src/mongo/db/modules/eloq/build
    ${COMMON_CMD} VARIANT_DIR=RelWithDebInfo --dbg=on --opt=off mongo
}

# ------------- Validate input parameters -------------
if [ $# -ne 1 ]; then
    log_error "Exactly one build type must be specified"
    show_usage
    exit 1
fi

# ------------- Check directory structure -------------
if [ ! -d "buildscripts" ] || [ ! -d "src/mongo" ]; then
    log_error "This script must be run from the MongoDB root directory"
    log_error "Current directory: $(pwd)"
    exit 1
fi

# ------------- Build based on parameter -------------
# Convert input to lowercase for case-insensitive comparison
BUILD_TYPE_LOWER=$(echo "$1" | tr '[:upper:]' '[:lower:]')

# Start time tracking
start_time=$(date +%s)
log_info "Build started at $(date)"

# Choose build type
case "${BUILD_TYPE_LOWER}" in
    "debug")
        build_debug
        ;;
    "debugasan")
        build_debug_asan
        ;;
    "release")
        build_release
        ;;
    "relwithdebinfo")
        build_relwithdebinfo
        ;;
    "compiledb")
        build_compiledb
        ;;
    "client")
        build_client
        ;;
    "-h"|"--help"|"help")
        show_usage
        exit 0
        ;;
    *)
        log_error "Invalid build type: $1"
        show_usage
        exit 1
        ;;
esac

# Calculate build time
end_time=$(date +%s)
build_time=$((end_time - start_time))
minutes=$((build_time / 60))
seconds=$((build_time % 60))

# Output success message
log_success "Build completed successfully in ${minutes}m ${seconds}s"
log_info "Build type: $1"
log_info "CPU cores used: ${CPU_CORE_SIZE}"
