#!/bin/bash
#
# Script to run MongoDB with an EloqDoc configuration file
# Usage: ./run_with_config.sh <build-type> --config <config_path> [additional mongod params]

# Exit on error, undefined variable, or pipeline failure
set -euo pipefail

# ------------- Color definitions -------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
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

# ------------- Library Path Setup -------------
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}${LD_LIBRARY_PATH:+:}:src/mongo/db/modules/eloq/build:/usr/local/lib"
# LD_PRELOAD will be set after determining the build type

# ------------- Help message -------------
show_usage() {
    echo "Usage: $0 <build-type> --config <path> [additional mongod params]"
    echo "  build-type: Debug, DebugASAN, Release, or RelWithDebInfo"
    echo
    echo "Required options:"
    echo "  --config <path>   Specify the configuration file path"
    echo
    echo "Optional options:"
    echo "  --eloqBootstrap <true|false>  Enable Eloq bootstrap mode (default: false)"
    echo "  --heapProfile <true|false>  Enable TCMalloc heap profiling (RelWithDebInfo only, default: false)"
    echo
    echo "Example: $0 DebugASAN --config ./config/example.conf --eloqBootstrap true"
    echo "Example: $0 RelWithDebInfo --config ./config/example.conf --heapProfile true"
}

# ------------- Validate input parameters -------------
if [ $# -lt 1 ]; then
    log_error "Build type must be specified"
    show_usage
    exit 1
fi

# ------------- Check if we're in the right directory -------------
if [ ! -d "src/mongo/db/modules/eloq" ]; then
    log_error "This script must be run from the EloqDoc root directory"
    log_error "Current directory: $(pwd)"
    log_error "Please change to the EloqDoc root directory and try again"
    exit 1
fi

# ------------- Parse build type (case insensitive) -------------
BUILD_TYPE_LOWER=$(echo "$1" | tr '[:upper:]' '[:lower:]')
shift  # Remove the build type from the arguments

case "${BUILD_TYPE_LOWER}" in
    "debug")
        BUILD_TYPE="Debug"
        log_info "Debug build (no ASAN). Script's LD_PRELOAD will include libbrpc.so."
        # libbrpc.so must be preloaded because it hooks pthread functions
        # and needs to be loaded before pthread
        export LD_PRELOAD="/lib/libbrpc.so"
        ;;
    "debugasan")
        BUILD_TYPE="Debug" # Mongod executable is still in Debug directory
        # For DebugASAN build, locate and include libasan.so in LD_PRELOAD
        LIBASAN_PATH=$(ldconfig -p | grep libasan | head -n1 | tr ' ' '\n' | grep '/' || true)
        if [ -z "${LIBASAN_PATH}" ]; then
            log_warning "libasan.so not found in ldconfig cache. Script's LD_PRELOAD for DebugASAN will only include libbrpc.so"
            # libbrpc.so must be preloaded
            export LD_PRELOAD="/lib/libbrpc.so"
        else
            log_info "Found libasan at: ${LIBASAN_PATH}. Script's LD_PRELOAD for DebugASAN will include it and libbrpc.so."
            # libbrpc.so must be preloaded
            export LD_PRELOAD="${LIBASAN_PATH}:/lib/libbrpc.so"
        fi
        ;;
    "release")
        BUILD_TYPE="Release"
        # libbrpc.so must be preloaded because it hooks pthread functions
        # and needs to be loaded before pthread
        export LD_PRELOAD="/usr/local/lib/libmimalloc.so:/lib/libbrpc.so"
        ;;
    "relwithdebinfo")
        BUILD_TYPE="RelWithDebInfo"
        # libbrpc.so must be preloaded because it hooks pthread functions
        # and needs to be loaded before pthread
        export LD_PRELOAD="/usr/local/lib/libmimalloc.so:/lib/libbrpc.so"
        ;;
    *)
        log_error "Invalid build type: ${BUILD_TYPE_LOWER}"
        show_usage
        exit 1
        ;;
esac

# ------------- Parse additional options -------------
CONFIG_PATH=""
MONGOD_ARGS=()
ELOQ_BOOTSTRAP_VALUE="" # Default to empty, meaning option not set or false
HEAP_PROFILE_FLAG="false" # Added: Default for heap profiling

while [ $# -gt 0 ]; do
    case "$1" in
        --config)
            if [ $# -lt 2 ]; then
                log_error "Missing value for --config parameter"
                show_usage
                exit 1
            fi
            CONFIG_PATH="$2"
            shift 2
            ;;
        --eloqBootstrap)
            if [ $# -lt 2 ]; then
                log_error "Missing value for --eloqBootstrap parameter"
                show_usage
                exit 1
            fi
            ELOQ_BOOTSTRAP_VALUE="$2"
            if [[ "${ELOQ_BOOTSTRAP_VALUE}" != "true" && "${ELOQ_BOOTSTRAP_VALUE}" != "false" ]]; then
                log_error "Invalid value for --eloqBootstrap: ${ELOQ_BOOTSTRAP_VALUE}. Must be 'true' or 'false'."
                show_usage
                exit 1
            fi
            shift 2
            ;;
        --heapProfile)
            if [ $# -lt 2 ]; then
                log_error "Missing value for --heapProfile parameter"
                show_usage
                exit 1
            fi
            HEAP_PROFILE_FLAG="$2"
            if [[ "${HEAP_PROFILE_FLAG}" != "true" && "${HEAP_PROFILE_FLAG}" != "false" ]]; then
                log_error "Invalid value for --heapProfile: ${HEAP_PROFILE_FLAG}. Must be 'true' or 'false'."
                show_usage
                exit 1
            fi
            shift 2
            ;;
        *)
            MONGOD_ARGS+=("$1")
            shift
            ;;
    esac
done

# Check if config path was provided
if [ -z "${CONFIG_PATH}" ]; then
    log_error "Configuration file path must be specified with --config"
    show_usage
    exit 1
fi

# ------------- Validate executable and config file existence -------------
MONGOD_PATH="./build/${BUILD_TYPE}/mongo/mongod"
if [ ! -f "${MONGOD_PATH}" ]; then
    log_error "MongoDB executable not found at ${MONGOD_PATH}"
    log_error "Please build MongoDB with the ${BUILD_TYPE} configuration first"
    exit 1
fi

if [ ! -f "${CONFIG_PATH}" ]; then
    log_error "Configuration file not found at ${CONFIG_PATH}"
    exit 1
fi

# ------------- Run MongoDB with the configuration -------------
log_info "Starting MongoDB (${BUILD_TYPE}) with configuration: ${CONFIG_PATH}"
if [ -n "${ELOQ_BOOTSTRAP_VALUE}" ]; then
    log_info "Eloq Bootstrap mode: ${ELOQ_BOOTSTRAP_VALUE}"
    MONGOD_ARGS+=(--eloqBootstrap "${ELOQ_BOOTSTRAP_VALUE}")
else
    log_info "Eloq Bootstrap mode: false (default)"
    # Optionally, explicitly pass false if the flag is always expected by mongod
    # MONGOD_ARGS+=(--eloqBootstrap "false")
fi
log_info "Additional parameters: ${MONGOD_ARGS[*]}"
log_info "Press Ctrl+C to stop the server"

# Determine LD_PRELOAD and ENV_VARS specifically for mongod execution
MONGOD_EXEC_LD_PRELOAD=""
MONGOD_HEAP_PROFILE_ENV_VARS=""

if [ "${BUILD_TYPE_LOWER}" = "relwithdebinfo" ] && [ "${HEAP_PROFILE_FLAG}" = "true" ]; then
    log_info "TCMalloc heap profiling is ENABLED for RelWithDebInfo build."
    MONGOD_EXEC_LD_PRELOAD="/usr/local/lib/libtcmalloc.so"
    MONGOD_HEAP_PROFILE_ENV_VARS="HEAPPROFILE=./eloqdoc.heap HEAP_PROFILE_SAMPLING_RATE='262144'"
else
    # Default LD_PRELOAD behavior if not using tcmalloc heap profiling
    if [ "${HEAP_PROFILE_FLAG}" = "true" ]; then # And not RelWithDebInfo
        log_warning "Heap profiling with tcmalloc was requested, but build type is ${BUILD_TYPE} (not RelWithDebInfo). TCMalloc will NOT be used."
    fi

    case "${BUILD_TYPE_LOWER}" in
        "debug")
            # For mongod (Debug, no ASAN), LD_PRELOAD will only include libbrpc.so.
            log_info "For mongod (Debug, no ASAN), LD_PRELOAD will only include libbrpc.so."
            MONGOD_EXEC_LD_PRELOAD="/lib/libbrpc.so"
            ;;
        "debugasan")
            _LIBASAN_PATH_FOR_MONGOD=$(ldconfig -p | grep libasan | head -n1 | tr ' ' '\n' | grep '/' || true)
            if [ -z "${_LIBASAN_PATH_FOR_MONGOD}" ]; then
                # Warning about missing libasan already given when setting script's LD_PRELOAD (if applicable)
                log_warning "libasan.so not found. For mongod (DebugASAN), LD_PRELOAD will only include libbrpc.so."
                MONGOD_EXEC_LD_PRELOAD="/lib/libbrpc.so"
            else
                log_info "For mongod (DebugASAN), LD_PRELOAD will include libasan and libbrpc.so."
                MONGOD_EXEC_LD_PRELOAD="${_LIBASAN_PATH_FOR_MONGOD}:/lib/libbrpc.so"
            fi
            ;;
        "release" | "relwithdebinfo") # relwithdebinfo here means HEAP_PROFILE_FLAG is false
            MONGOD_EXEC_LD_PRELOAD="/usr/local/lib/libmimalloc.so:/lib/libbrpc.so"
            ;;
    esac
    if [ "${BUILD_TYPE_LOWER}" != "relwithdebinfo" ] || [ "${HEAP_PROFILE_FLAG}" = "false" ]; then
      log_info "Using default preloads for ${BUILD_TYPE}: ${MONGOD_EXEC_LD_PRELOAD}"
    fi
fi

# Clear LD_PRELOAD for the current script environment before launching mongod with its own specific LD_PRELOAD
export LD_PRELOAD=""
# export HEAP_PROFILE_PATH="./eloqdoc.heap" # Old tcmalloc var, not used by current HEAPPROFILE
# HEAPCHECK=normal # Another tcmalloc var, can be added to MONGOD_HEAP_PROFILE_ENV_VARS if needed

log_info "Final LD_PRELOAD for mongod process: ${MONGOD_EXEC_LD_PRELOAD}"
if [ -n "${MONGOD_HEAP_PROFILE_ENV_VARS}" ]; then
    log_info "Heap profile ENV_VARS for mongod process: ${MONGOD_HEAP_PROFILE_ENV_VARS}"
    # Execute MongoDB with tcmalloc heap profiling
    env ${MONGOD_HEAP_PROFILE_ENV_VARS} LD_PRELOAD="${MONGOD_EXEC_LD_PRELOAD}" \
        ${MONGOD_PATH} --config ${CONFIG_PATH} "${MONGOD_ARGS[@]}"
else
    # Execute MongoDB without tcmalloc heap profiling (uses ASan or mimalloc/brpc as determined)
    env LD_PRELOAD="${MONGOD_EXEC_LD_PRELOAD}" \
        ${MONGOD_PATH} --config ${CONFIG_PATH} "${MONGOD_ARGS[@]}"
fi
