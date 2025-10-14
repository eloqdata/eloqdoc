cmake_minimum_required(VERSION 3.16)
project(eloq)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
# --- CMake Policies ---
if(POLICY CMP0074) # Allow find_package to use <PackageName>_ROOT variables
    cmake_policy(SET CMP0074 NEW)
endif()
if(POLICY CMP0054) # Only interpret if() arguments as variables or keywords when unquoted
    cmake_policy(SET CMP0054 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-parentheses -Wno-error -W -fPIC -fno-omit-frame-pointer -fdiagnostics-color=always")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fdiagnostics-color=always")

# --- Transaction Service Options ---
option(EXT_TX_PROC_ENABLED "Allows external threads to move forward the tx service." ON)
option(FORK_HM_PROCESS "Whether to fork a separate Host Manager process for Raft." OFF)
option(STATISTICS "Whether to enable table statistics collection in the tx service." ON)
option(ELOQ_MODULE_ENABLED "Enable EloqModule" OFF)
if (ELOQ_MODULE_ENABLED)
  add_definitions(-DELOQ_MODULE_ENABLED)
endif()

# --- ELOQ Metrics Options ---
option(ENABLE_BENCHMARK "Enable Google Benchmark for eloq_metrics." OFF)
option(WITH_GLOG "Enable glog for eloq_metrics module (distinct from BRPC_WITH_GLOG)." OFF) # Note: This is specific to eloq_metrics
option(ENABLE_ELOQ_METRICS_APP "Enable building the ELOQ metrics standalone application." OFF)

# --- Common/Shared Options ---
option(BRPC_WITH_GLOG "Build/link BRPC with glog support (affects tx_service, log_service)." ON)
option(SMALL_RANGE "Optimize for small range sizes (affects tx_service)." OFF)
option(USE_ASAN "Enable AddressSanitizer (ASan) for debugging." OFF)
option(ABSL_PROPAGATE_CXX_STD "ABSL PROPAGATE CXX STD" ON)

option(OPEN_LOG_SERVICE "Compile with open log service." ON)
message("OPEN_LOG_SERVICE: " ${OPEN_LOG_SERVICE})
if (OPEN_LOG_SERVICE)
   add_compile_definitions(OPEN_LOG_SERVICE=1)
endif()

# --- Data Store Configuration (Set externally, e.g., -DWITH_DATA_STORE=ELOQDSS_ROCKSDB_CLOUD_S3) ---
# WITH_DATA_STORE: Specifies the backend data store.
# Supported values: DYNAMODB, BIGTABLE, ELOQDSS_ROCKSDB_CLOUD_S3, ELOQDSS_ROCKSDB_CLOUD_GCS, ELOQDSS_ROCKSDB, etc.
# This variable is not an option() but a cache variable that drives major build paths.
# Its handling is primarily in build_data_store.cmake and find_dependencies.cmake for SDKs.
set(WITH_DATA_STORE "ELOQDSS_ROCKSDB_CLOUD_S3" CACHE STRING "The KV data store for EloqDoc")
set_property(CACHE WITH_DATA_STORE PROPERTY STRINGS "ELOQDSS_ROCKSDB_CLOUD_S3" "ELOQDSS_ROCKSDB_CLOUD_GCS" "ELOQDSS_ROCKSDB" "ELOQDSS_ELOQSTORE")
if(DEFINED WITH_DATA_STORE)
    message(STATUS "WITH_DATA_STORE is set to: ${WITH_DATA_STORE}")
else()
    message(STATUS "WITH_DATA_STORE is not set. Defaulting or error will occur in build_data_store.cmake.")
endif()

# --- Apply relevant global definitions based on options ---
if(EXT_TX_PROC_ENABLED)
    add_definitions(-DEXT_TX_PROC_ENABLED)
    message(STATUS "Global: EXT_TX_PROC_ENABLED definition added.")
endif()

if(FORK_HM_PROCESS)
    add_definitions(-DFORK_HM_PROCESS) # This definition is used by tx_service
    message(STATUS "Global: FORK_HM_PROCESS definition added.")
endif()

#  STATISTICS, SMALL_RANGE definitions are handled in build_tx_service.cmake

if(USE_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
    message(STATUS "Global: ASan flags enabled for CXX, EXE_LINKER, SHARED_LINKER.")
endif()

# --- Global Include Directories ---
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/cc)
# log_service and tx_service both has fault_inject.h
# include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/fault)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/proto)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/remote)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/sequences)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/include/store)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/abseil-cpp)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/tx_service/tx-log-protos) # For shared protos
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/eloq_metrics/include)
if (OPEN_LOG_SERVICE)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/log_service/include)
else()
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/eloq_log_service/include)
endif()

# --- Include build system modules ---
add_subdirectory(tx_service/abseil-cpp)
include(cmake/find_dependencies.cmake) # Finds common and conditional dependencies
include(cmake/compile_protos.cmake)   # Helper function for proto compilation
include(cmake/build_eloq_metrics.cmake)
include(cmake/build_tx_service.cmake)
if (OPEN_LOG_SERVICE)
    include(cmake/build_log_service.cmake)
else()
    include(cmake/build_eloq_log_service.cmake)
endif()
include(cmake/build_data_store.cmake)



# --- Installation Rules ---
# install tx_service lib and includes files
install(TARGETS txservice_shared
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS logservice_shared
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS datastore_shared
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS eloq_metrics_shared
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

if(FORK_HM_PROCESS) # Only install host_manager if it's built
    install(TARGETS host_manager
            RUNTIME DESTINATION bin)
endif()
