# This file finds common and conditionally required dependencies for ELOQ modules.
# It should be included after options are defined in the main CMakeLists.txt.

# --- Mimalloc (Commonly used by tx_service, log_service) ---
find_package(MIMALLOC REQUIRED)
# mimalloc-2.1/mimalloc.h is installed in /usr/local/include/mimalloc-2.1/mimalloc.h, but the build system expects it in /usr/local/include/mimalloc.h in centos7,8 and rocky9.
## Validate any provided/include-dir and clear it if it doesn't actually contain headers
set(_MI_HEADER_OK OFF)
if(DEFINED MIMALLOC_INCLUDE_DIR AND MIMALLOC_INCLUDE_DIR)
    if(EXISTS "${MIMALLOC_INCLUDE_DIR}/mimalloc.h" OR EXISTS "${MIMALLOC_INCLUDE_DIR}/mimalloc-2.1/mimalloc.h")
        set(_MI_HEADER_OK ON)
    endif()
endif()
## Some environments incorrectly set MIMALLOC_INCLUDE_DIR to the CMake package directory
## (e.g., /usr/local/lib64/cmake/mimalloc-2.1) which does not contain headers.
## Detect and discard such values to trigger the fallback include discovery.
if(DEFINED MIMALLOC_INCLUDE_DIR AND MIMALLOC_INCLUDE_DIR MATCHES "/cmake/")
    message(STATUS "Dependencies: Discarding Mimalloc include directory from CMake config: ${MIMALLOC_INCLUDE_DIR}")
    set(_MI_HEADER_OK OFF)
endif()
if(NOT _MI_HEADER_OK)
    unset(MIMALLOC_INCLUDE_DIR CACHE)
    unset(MIMALLOC_INCLUDE_DIR)
endif()
## Fallback: determine include root strictly for Mimalloc 2.1 when MIMALLOC_INCLUDE_DIR is not provided
if(NOT DEFINED MIMALLOC_INCLUDE_DIR OR NOT MIMALLOC_INCLUDE_DIR)
    set(_MIMALLOC_SEARCH_ROOTS /usr/include /usr/local/include)
    unset(MIMALLOC_INCLUDE_DIR)
    foreach(_root ${_MIMALLOC_SEARCH_ROOTS})
        if(EXISTS "${_root}/mimalloc-2.1/mimalloc.h")
            set(MIMALLOC_INCLUDE_DIR "${_root}")
        endif()
    endforeach()

    if(MIMALLOC_INCLUDE_DIR)
        message(STATUS "Dependencies: Fallback determined Mimalloc 2.1 include root: ${MIMALLOC_INCLUDE_DIR}")
    else()
        message(FATAL_ERROR "Dependencies: Could not determine Mimalloc 2.1 include directory (looked for mimalloc-2.1/mimalloc.h under /usr/include and /usr/local/include). Please install mimalloc 2.1 headers.")
    endif()
endif()

message(STATUS "Dependencies: Found Mimalloc. Library: ${MIMALLOC_LIBRARY}, Include directory: ${MIMALLOC_INCLUDE_DIR}")
# Add Mimalloc include directory to the global include paths (guard if resolved)
if(MIMALLOC_INCLUDE_DIR)
    include_directories(${MIMALLOC_INCLUDE_DIR})
endif()
# Some environments install headers under a versioned subdir (e.g., mimalloc-2.X/mimalloc.h).
# Add any matching versioned include directories so both <mimalloc.h> and <mimalloc-2.X/mimalloc.h> resolve.
file(GLOB _MIMALLOC_VERSIONED_DIRS "${MIMALLOC_INCLUDE_DIR}/mimalloc-*")
if(_MIMALLOC_VERSIONED_DIRS)
    foreach(_mi_dir ${_MIMALLOC_VERSIONED_DIRS})
        if(IS_DIRECTORY "${_mi_dir}")
            include_directories("${_mi_dir}")
            message(STATUS "Dependencies: Added versioned Mimalloc include directory: ${_mi_dir}")
        endif()
    endforeach()
endif()

# --- GFLAGS (Commonly used by tx_service, log_service) ---
# Find GFLAGS include path
find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h DOC "GFLAGS include directory")
# Find GFLAGS library
find_library(GFLAGS_LIBRARY NAMES gflags libgflags DOC "GFLAGS library")
if(NOT GFLAGS_INCLUDE_PATH)
    message(FATAL_ERROR "Dependencies: Failed to find GFLAGS include path (gflags/gflags.h).")
endif()
if(NOT GFLAGS_LIBRARY)
    message(FATAL_ERROR "Dependencies: Failed to find GFLAGS library (gflags or libgflags).")
endif()
message(STATUS "Dependencies: Found GFLAGS. Library: ${GFLAGS_LIBRARY}, Include directory: ${GFLAGS_INCLUDE_PATH}")
# Add GFLAGS include directory to the global include paths
include_directories(${GFLAGS_INCLUDE_PATH})
# Determine GFLAGS namespace to handle potential variations in gflags versions/packaging.
# Some versions use 'namespace gflags {}', others use 'namespace GFLAGS_NAMESPACE {}'
# where GFLAGS_NAMESPACE is a macro.
execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    # If the namespace is GFLAGS_NAMESPACE, it's a macro; extract its actual value.
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else()
    # If a direct namespace (e.g., "gflags") is found, or if the above GFLAGS_NAMESPACE extraction fails
    # to change GFLAGS_NS, we might need to override the namespace if our code expects a specific one.
    # This definition allows C++ code to adapt if gflags uses a different namespace than expected (e.g. "google").
    add_compile_definitions(OVERRIDE_GFLAGS_NAMESPACE)
endif()

# --- LevelDB (Commonly used by tx_service, log_service) ---
# Find LevelDB include path
find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h DOC "LevelDB include directory")
# Find LevelDB library
find_library(LEVELDB_LIB NAMES leveldb DOC "LevelDB library")
if(NOT LEVELDB_INCLUDE_PATH)
    message(FATAL_ERROR "Dependencies: Failed to find LevelDB include path (leveldb/db.h).")
endif()
if(NOT LEVELDB_LIB)
    message(FATAL_ERROR "Dependencies: Failed to find LevelDB library (leveldb).")
endif()
message(STATUS "Dependencies: Found LevelDB. Library: ${LEVELDB_LIB}, Include directory: ${LEVELDB_INCLUDE_PATH}")
# Add LevelDB include directory to the global include paths
include_directories(${LEVELDB_INCLUDE_PATH})
message(STATUS "Dependencies: Added LevelDB include directory globally: ${LEVELDB_INCLUDE_PATH}")

# --- brpc (Commonly used by tx_service, log_service) ---
# Find brpc include path
find_path(BRPC_INCLUDE_PATH NAMES brpc/stream.h DOC "brpc include directory")
# Find brpc library
find_library(BRPC_LIB NAMES brpc DOC "brpc library")
if(NOT BRPC_INCLUDE_PATH)
    message(FATAL_ERROR "Dependencies: Failed to find brpc include path (brpc/stream.h).")
endif()
if(NOT BRPC_LIB)
    message(FATAL_ERROR "Dependencies: Failed to find brpc library (brpc).")
endif()
message(STATUS "Dependencies: Found brpc. Library: ${BRPC_LIB}, Include directory: ${BRPC_INCLUDE_PATH}")
# Add brpc include directory to the global include paths
include_directories(${BRPC_INCLUDE_PATH})

# --- braft (Commonly used by tx_service, log_service) ---
# Find braft include path
find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h DOC "braft include directory")
# Find braft library
find_library(BRAFT_LIB NAMES braft DOC "braft library")
if(NOT BRAFT_INCLUDE_PATH)
    message(FATAL_ERROR "Dependencies: Failed to find braft include path (braft/raft.h).")
endif()
if(NOT BRAFT_LIB)
    message(FATAL_ERROR "Dependencies: Failed to find braft library (braft).")
endif()
message(STATUS "Dependencies: Found braft. Library: ${BRAFT_LIB}, Include directory: ${BRAFT_INCLUDE_PATH}")
# Add braft include directory to the global include paths
include_directories(${BRAFT_INCLUDE_PATH})

# --- Glog (Commonly used by tx_service, log_service) ---
# Find Glog include path
find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h DOC "Glog include directory")
# Find Glog library (version >=0.6.0 is required)
find_library(GLOG_LIB NAMES glog VERSION ">=0.6.0" REQUIRED DOC "Glog library") # REQUIRED handles library not found error
if(NOT GLOG_INCLUDE_PATH)
    message(FATAL_ERROR "Dependencies: Failed to find Glog include path (glog/logging.h).")
endif()
message(STATUS "Dependencies: Found Glog. Library: ${GLOG_LIB}, Include directory: ${GLOG_INCLUDE_PATH}")
# Add Glog include directory to the global include paths
include_directories(${GLOG_INCLUDE_PATH})

# --- RocksDB and Cloud SDKs (Conditional) ---
# These dependencies are required if USE_ROCKSDB_LOG_STATE is ON
# or if WITH_DATA_STORE involves RocksDB (e.g., ELOQDSS_ROCKSDB_CLOUD_S3 for data_store).

# Flag to track if RocksDB (base or cloud) is found and configured
set(ROCKSDB_FOUND OFF)
# Flag to track if RocksDB Cloud SDK (S3 or GCS) is found and configured
set(ROCKSDB_CLOUD_SDK_FOUND OFF)

# Determine if any RocksDB related feature is enabled, thus requiring RocksDB
set(NEED_ROCKSDB OFF)
if(USE_ROCKSDB_LOG_STATE)
    set(NEED_ROCKSDB ON)
    message(STATUS "Dependencies: RocksDB needed due to USE_ROCKSDB_LOG_STATE.")
endif()
# Check if data store configuration requires RocksDB
if(DEFINED WITH_DATA_STORE AND (WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS" OR WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB"))
    set(NEED_ROCKSDB ON)
    message(STATUS "Dependencies: RocksDB needed due to WITH_DATA_STORE setting: ${WITH_DATA_STORE}.")
endif()
# Add other conditions for NEED_ROCKSDB here if new modules or features start using it.


if(NEED_ROCKSDB)
    message(STATUS "Dependencies: RocksDB is required by the current configuration. Proceeding with RocksDB setup.")
    # Initialize lists for collecting RocksDB related include directories and libraries
    set(ROCKSDB_GLOBAL_INCLUDE_DIRS "")
    set(ROCKSDB_GLOBAL_LIBRARIES "")

    # Check if specific cloud storage integration is needed for RocksDB
    set(NEED_ROCKSDB_CLOUD_S3 OFF)
    set(NEED_ROCKSDB_CLOUD_GCS OFF)

    # Check for S3 cloud requirement from log_service configuration
    if(USE_ROCKSDB_LOG_STATE AND WITH_ROCKSDB_CLOUD STREQUAL "S3")
        set(NEED_ROCKSDB_CLOUD_S3 ON)
        # Check for GCS cloud requirement from log_service configuration
    elseif(USE_ROCKSDB_LOG_STATE AND WITH_ROCKSDB_CLOUD STREQUAL "GCS")
        set(NEED_ROCKSDB_CLOUD_GCS ON)
    endif()

    # Check for cloud requirements from data_store configuration
    if(DEFINED WITH_DATA_STORE)
        if(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_S3")
            set(NEED_ROCKSDB_CLOUD_S3 ON)
        elseif(WITH_DATA_STORE STREQUAL "ELOQDSS_ROCKSDB_CLOUD_GCS") # Assuming this option might exist for data_store
            set(NEED_ROCKSDB_CLOUD_GCS ON)
        endif()
    endif()

    if(NEED_ROCKSDB_CLOUD_S3)
        message(STATUS "Dependencies: RocksDB with S3 Cloud support is required.")
        # Find AWS SDK Core components
        find_path(AWS_CORE_INCLUDE_PATH aws/core/Aws.h DOC "AWS SDK Core include directory")
        find_library(AWS_CORE_LIB aws-cpp-sdk-core DOC "AWS SDK Core library")
        # Find AWS SDK S3 components
        find_path(AWS_S3_INCLUDE_PATH aws/s3/S3Client.h DOC "AWS SDK S3 include directory")
        find_library(AWS_S3_LIB aws-cpp-sdk-s3 DOC "AWS SDK S3 library")
        # Find AWS SDK Kinesis components (verify if always needed for S3 RocksDB or specific use cases)
        find_path(AWS_KINESIS_INCLUDE_PATH aws/kinesis/KinesisClient.h DOC "AWS SDK Kinesis include directory")
        find_library(AWS_KINESIS_LIB aws-cpp-sdk-kinesis DOC "AWS SDK Kinesis library")

        if(NOT (AWS_CORE_INCLUDE_PATH AND AWS_CORE_LIB AND AWS_S3_INCLUDE_PATH AND AWS_S3_LIB AND AWS_KINESIS_INCLUDE_PATH AND AWS_KINESIS_LIB))
            message(FATAL_ERROR "Dependencies: Failed to find all required AWS SDK components (core, s3, kinesis) for RocksDB S3 support. Please check SDK installation and paths.")
        endif()
        message(STATUS "Dependencies: Found AWS SDK for S3 RocksDB. CoreLib: ${AWS_CORE_LIB} (Inc: ${AWS_CORE_INCLUDE_PATH}), S3Lib: ${AWS_S3_LIB} (Inc: ${AWS_S3_INCLUDE_PATH}), KinesisLib: ${AWS_KINESIS_LIB} (Inc: ${AWS_KINESIS_INCLUDE_PATH})")
        # Append AWS SDK include directories to RocksDB global includes
        list(APPEND ROCKSDB_GLOBAL_INCLUDE_DIRS ${AWS_CORE_INCLUDE_PATH} ${AWS_S3_INCLUDE_PATH} ${AWS_KINESIS_INCLUDE_PATH})
        # Append AWS SDK libraries to RocksDB global libraries
        list(APPEND ROCKSDB_GLOBAL_LIBRARIES ${AWS_CORE_LIB} ${AWS_S3_LIB} ${AWS_KINESIS_LIB})

        # Find RocksDB AWS cloud library
        find_library(ROCKSDB_CLOUD_LIB NAMES rocksdb-cloud-aws DOC "RocksDB AWS Cloud library")
        if(NOT ROCKSDB_CLOUD_LIB)
            message(FATAL_ERROR "Dependencies: Failed to find RocksDB AWS cloud library (rocksdb-cloud-aws).")
        endif()
        message(STATUS "Dependencies: Found RocksDB Cloud AWS Library: ${ROCKSDB_CLOUD_LIB}")
        # Append RocksDB AWS cloud library to global libraries
        list(APPEND ROCKSDB_GLOBAL_LIBRARIES ${ROCKSDB_CLOUD_LIB})
        # Add compile definition to enable AWS S3 specific code paths
        add_compile_definitions(USE_AWS)
        message(STATUS "Dependencies: Added compile definition USE_AWS for S3 support.")
        # Module-specific definitions like ROCKSDB_CLOUD_FS_TYPE=1 or WITH_ROCKSDB_CLOUD=1 should be handled in module CMake files.
        set(ROCKSDB_CLOUD_SDK_FOUND ON)
    elseif(NEED_ROCKSDB_CLOUD_GCS)
        message(STATUS "Dependencies: RocksDB with GCS Cloud support is required.")
        # Find Google Cloud Storage SDK components
        find_path(GCP_CS_INCLUDE_PATH google/cloud/storage/client.h DOC "Google Cloud Storage client include directory")
        find_library(GCP_COMMON_LIB google_cloud_cpp_common DOC "Google Cloud Platform Common library")
        find_library(GCP_CS_LIB google_cloud_cpp_storage DOC "Google Cloud Platform Storage library")

        if(NOT (GCP_CS_INCLUDE_PATH AND GCP_COMMON_LIB AND GCP_CS_LIB))
            message(FATAL_ERROR "Dependencies: Failed to find all required Google Cloud SDK components (storage client include, common library, storage library) for RocksDB GCS support. Please check SDK installation and paths.")
        endif()
        message(STATUS "Dependencies: Found Google Cloud SDK for GCS RocksDB. CommonLib: ${GCP_COMMON_LIB}, StorageLib: ${GCP_CS_LIB} (Inc: ${GCP_CS_INCLUDE_PATH})")
        # Append GCP SDK include directories to RocksDB global includes
        list(APPEND ROCKSDB_GLOBAL_INCLUDE_DIRS ${GCP_CS_INCLUDE_PATH})
        # Append GCP SDK libraries to RocksDB global libraries
        list(APPEND ROCKSDB_GLOBAL_LIBRARIES ${GCP_COMMON_LIB} ${GCP_CS_LIB})

        # Find RocksDB GCP cloud library
        find_library(ROCKSDB_CLOUD_LIB NAMES rocksdb-cloud-gcp DOC "RocksDB GCP Cloud library")
        if(NOT ROCKSDB_CLOUD_LIB)
            message(FATAL_ERROR "Dependencies: Failed to find RocksDB GCP cloud library (rocksdb-cloud-gcp).")
        endif()
        message(STATUS "Dependencies: Found RocksDB Cloud GCP Library: ${ROCKSDB_CLOUD_LIB}")
        # Append RocksDB GCP cloud library to global libraries
        list(APPEND ROCKSDB_GLOBAL_LIBRARIES ${ROCKSDB_CLOUD_LIB})
        # Add compile definition to enable GCP GCS specific code paths
        add_compile_definitions(USE_GCP)
        message(STATUS "Dependencies: Added compile definition USE_GCP for GCS support.")
        # Module-specific definitions like ROCKSDB_CLOUD_FS_TYPE=2 or WITH_ROCKSDB_CLOUD=2 should be handled in module CMake files.
        set(ROCKSDB_CLOUD_SDK_FOUND ON)
    else()
        message(STATUS "Dependencies: RocksDB is requied")
	find_library(ROCKSDB_LIB NAMES rocksdb)

	if(NOT ROCKSDB_LIB)
	    message(FATAL_ERROR "Dependencies: Failed to find all required lib path for RocksDB.")
	endif()
	message(STATUS "Dependenies: RocksDB lib path ${ROCKSDB_LIB}")
	list(APPEND ROCKSDB_GLOBAL_LIBRARIES ${ROCKSDB_LIB} )

	set(ROCKSDB_CLOUD_SDK_FOUND OFF)
    endif() # End of NEED_ROCKSDB_CLOUD_S3 / NEED_ROCKSDB_CLOUD_GCS

    # Find base RocksDB library and include paths, adjusting search paths if cloud SDK is used
    if(ROCKSDB_CLOUD_SDK_FOUND)
        # Cloud versions might bundle RocksDB headers in specific subdirectories like 'rocksdb_cloud_header' or 'rocksdb'
        find_path(ROCKSDB_BASE_INCLUDE_PATH NAMES rocksdb/db.h PATH_SUFFIXES "rocksdb_cloud_header" "rocksdb" DOC "RocksDB base include directory (when cloud SDK is used)")
    else()
        # Standard RocksDB installation path
        find_path(ROCKSDB_BASE_INCLUDE_PATH NAMES rocksdb/db.h DOC "RocksDB base include directory (standard)")
    endif()

    if(NOT ROCKSDB_BASE_INCLUDE_PATH)
        message(FATAL_ERROR "Dependencies: Failed to find RocksDB base include path (rocksdb/db.h). Ensure RocksDB is installed correctly and accessible.")
    endif()
    message(STATUS "Dependencies: Found RocksDB base include path: ${ROCKSDB_BASE_INCLUDE_PATH}")
    # Append RocksDB base include directory to the global list for RocksDB-related components
    list(APPEND ROCKSDB_GLOBAL_INCLUDE_DIRS ${ROCKSDB_BASE_INCLUDE_PATH})
    # Also add RocksDB base include directory globally for immediate availability if needed by other find modules or general project includes.
    # ROCKSDB_GLOBAL_INCLUDE_DIRS will be processed later for more targeted global inclusion.
    include_directories(${ROCKSDB_BASE_INCLUDE_PATH})
    message(STATUS "Dependencies: Added RocksDB base include directory globally for immediate use: ${ROCKSDB_BASE_INCLUDE_PATH}")

    if(ROCKSDB_CLOUD_SDK_FOUND)
        # If using a cloud SDK, ROCKSDB_CLOUD_LIB (e.g., rocksdb-cloud-aws/gcp) was already found and added to ROCKSDB_GLOBAL_LIBRARIES.
        # This cloud library might statically link RocksDB or require separate linking of a base RocksDB library.
        # The current assumption is ROCKSDB_CLOUD_LIB is sufficient or correctly pulls in RocksDB.
        # If a separate plain 'rocksdb' link is needed with cloud builds, logic to find_library(ROCKSDB_BASE_LIB NAMES rocksdb) and append it
        # to ROCKSDB_GLOBAL_LIBRARIES would be added here or handled by the cloud library's own CMake config.
        message(STATUS "Dependencies: Using RocksDB via Cloud SDK. Base RocksDB include: ${ROCKSDB_BASE_INCLUDE_PATH}. Cloud library variable: ${ROCKSDB_CLOUD_LIB}")
    else()
        # Standard RocksDB library (not using cloud extensions found in this script)
        find_library(ROCKSDB_BASE_LIB NAMES rocksdb DOC "RocksDB base library (non-cloud)")
        if(NOT ROCKSDB_BASE_LIB)
            message(FATAL_ERROR "Dependencies: Failed to find RocksDB base library (librocksdb) for non-cloud usage.")
        endif()
        # Append standard RocksDB library to global list for RocksDB-related components
        list(APPEND ROCKSDB_GLOBAL_LIBRARIES ${ROCKSDB_BASE_LIB})
        message(STATUS "Dependencies: Found local RocksDB. Library: ${ROCKSDB_BASE_LIB}, Include: ${ROCKSDB_BASE_INCLUDE_PATH}")
    endif() # End of ROCKSDB_CLOUD_SDK_FOUND check for library

    # Make all collected ROCKSDB_GLOBAL_INCLUDE_DIRS available globally.
    # This ensures modules can find RocksDB headers and any cloud SDK headers.
    if(ROCKSDB_GLOBAL_INCLUDE_DIRS)
        # Remove duplicates before adding globally
        list(REMOVE_DUPLICATES ROCKSDB_GLOBAL_INCLUDE_DIRS)
        include_directories(${ROCKSDB_GLOBAL_INCLUDE_DIRS})
        message(STATUS "Dependencies: Effective RocksDB global include directories added: ${ROCKSDB_GLOBAL_INCLUDE_DIRS}")
    else()
        message(WARNING "Dependencies: No RocksDB global include directories were collected. This is unexpected if RocksDB is needed.")
    endif()

    # Message about effective RocksDB libraries for linking (for informational purposes).
    # Modules will link against these libraries via target_link_libraries using this variable.
    if(ROCKSDB_GLOBAL_LIBRARIES)
        # Remove duplicates before reporting
        list(REMOVE_DUPLICATES ROCKSDB_GLOBAL_LIBRARIES)
        message(STATUS "Dependencies: Effective RocksDB global libraries for linking: ${ROCKSDB_GLOBAL_LIBRARIES}")
    else()
        # This case might occur if RocksDB is header-only (unlikely for full lib) or if cloud SDKs manage everything and no base lib is explicitly added,
        # and no cloud library was found/added. This usually indicates a problem if RocksDB is truly needed.
        message(WARNING "Dependencies: No specific RocksDB global libraries were identified for linking. This might be an issue if RocksDB linkage is required.")
    endif()
    set(ROCKSDB_FOUND ON) # Mark RocksDB as successfully found and configured for dependent modules
else()
    message(STATUS "Dependencies: RocksDB is not required by the current configuration. Skipping RocksDB setup.")
endif() # End of NEED_ROCKSDB

include(FetchContent)

# Import yaml-cpp library used by host manager
FetchContent_Declare(
    yaml-cpp
    GIT_REPOSITORY https://github.com/jbeder/yaml-cpp.git
    GIT_TAG yaml-cpp-0.7.0 # Can be a tag (yaml-cpp-x.x.x), a commit hash, or a branch name (master)
)
FetchContent_MakeAvailable(yaml-cpp)
