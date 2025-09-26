set(LOG_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/eloq_log_service)
set(TX_LOG_PROTOS_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/tx-log-protos) # Shared protos

set(LOG_LIB "") # Initialize list for libraries specific to log_service targets

set(LOG_SERVICE_ROCKSDB_INCLUDE_DIRS "")
set(LOG_SERVICE_ROCKSDB_LIBRARIES "")

set(WITH_LOG_STATE "ROCKSDB" CACHE STRING "The log state implementation")
set_property(CACHE WITH_LOG_STATE PROPERTY STRINGS "MEMORY" "ROCKSDB" "ROCKSDB_CLOUD_S3" "ROCKSDB_CLOUD_GCS")
message(NOTICE "WITH_LOG_STATE: ${WITH_LOG_STATE}")

# Add compile flags for LOG STATE TYPE
if(WITH_LOG_STATE STREQUAL "MEMORY")
  add_compile_definitions(LOG_STATE_TYPE_MEM)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB")
  add_compile_definitions(LOG_STATE_TYPE_RKDB)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_S3")
  add_compile_definitions(LOG_STATE_TYPE_RKDB_S3)
elseif(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_GCS")
  add_compile_definitions(LOG_STATE_TYPE_RKDB_GCS)
else()
  message(FATAL_ERROR "Unknown WITH_LOG_STATE: ${WITH_LOG_STATE}")
endif()

# RocksDB and Cloud SDK finding, conditional for Log Service
if(WITH_LOG_STATE MATCHES "ROCKSDB|ROCKSDB_CLOUD_S3|ROCKSDB_CLOUD_GCS") # WITH_LOG_STATE is a global option
  message(STATUS "LogService: WITH_LOG_STATE is ON. Finding RocksDB...")

  if (WITH_LOG_STATE MATCHES "ROCKSDB_CLOUD_S3|ROCKSDB_CLOUD_GCS")
     if(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_S3")
       message(STATUS "LogService: ROCKSDB_CLOUD is S3. Finding AWS SDK and RocksDB S3 support...")
       if(NOT AWS_CORE_INCLUDE_PATH)
         find_path(AWS_CORE_INCLUDE_PATH aws/core/Aws.h)
         find_path(AWS_S3_INCLUDE_PATH aws/s3/S3Client.h)
         find_path(AWS_KINESIS_INCLUDE_PATH aws/kinesis/KinesisClient.h) # As previously included
       endif()
       if(NOT AWS_CORE_LIB)
         find_library(AWS_CORE_LIB aws-cpp-sdk-core)
         find_library(AWS_S3_LIB aws-cpp-sdk-s3)
         find_library(AWS_KINESIS_LIB aws-cpp-sdk-kinesis) # As previously included
       endif()
       if(NOT ROCKSDB_CLOUD_AWS_LIB)
         find_library(ROCKSDB_CLOUD_AWS_LIB NAMES rocksdb-cloud-aws)
       endif()

       if(NOT (AWS_CORE_INCLUDE_PATH AND AWS_S3_INCLUDE_PATH AND AWS_KINESIS_INCLUDE_PATH AND AWS_CORE_LIB AND AWS_S3_LIB AND AWS_KINESIS_LIB AND ROCKSDB_CLOUD_AWS_LIB))
         message(FATAL_ERROR "LogService: Failed to find all required AWS SDK components (core, s3, kinesis) or rocksdb-cloud-aws library for RocksDB S3 support.")
       else()
         message(STATUS "LogService: Found AWS SDK for S3. Core: ${AWS_CORE_LIB}, S3: ${AWS_S3_LIB}, Kinesis: ${AWS_KINESIS_LIB}. RocksDB Cloud AWS Lib: ${ROCKSDB_CLOUD_AWS_LIB}")
         list(APPEND LOG_SERVICE_ROCKSDB_INCLUDE_DIRS ${AWS_CORE_INCLUDE_PATH} ${AWS_S3_INCLUDE_PATH} ${AWS_KINESIS_INCLUDE_PATH})
         list(APPEND LOG_SERVICE_ROCKSDB_LIBRARIES ${AWS_CORE_LIB} ${AWS_S3_LIB} ${AWS_KINESIS_LIB} ${ROCKSDB_CLOUD_AWS_LIB})
         include_directories(${AWS_CORE_INCLUDE_PATH} ${AWS_S3_INCLUDE_PATH} ${AWS_KINESIS_INCLUDE_PATH})
         add_compile_definitions(USE_AWS)
         message(STATUS "LogService: Added compile definition USE_AWS.")
       endif()
     elseif(WITH_LOG_STATE STREQUAL "ROCKSDB_CLOUD_GCS")
       message(STATUS "LogService: WITH_LOG_STATE is GCS. Finding GCP SDK and RocksDB GCS support...")
       if(NOT GCP_CS_INCLUDE_PATH)
         find_path(GCP_CS_INCLUDE_PATH google/cloud/storage/client.h)
       endif()
       if(NOT GCP_COMMON_LIB)
         find_library(GCP_COMMON_LIB google_cloud_cpp_common)
         find_library(GCP_CS_LIB google_cloud_cpp_storage)
       endif()
       if(NOT ROCKSDB_CLOUD_GCP_LIB)
         find_library(ROCKSDB_CLOUD_GCP_LIB NAMES rocksdb-cloud-gcp)
       endif()

       if(NOT (GCP_CS_INCLUDE_PATH AND GCP_COMMON_LIB AND GCP_CS_LIB AND ROCKSDB_CLOUD_GCP_LIB))
         message(FATAL_ERROR "LogService: Failed to find all required GCP SDK components or rocksdb-cloud-gcp library for RocksDB GCS support.")
       else()
         message(STATUS "LogService: Found GCP SDK for GCS. Common: ${GCP_COMMON_LIB}, Storage: ${GCP_CS_LIB}. RocksDB Cloud GCP Lib: ${ROCKSDB_CLOUD_GCP_LIB}")
         list(APPEND LOG_SERVICE_ROCKSDB_INCLUDE_DIRS ${GCP_CS_INCLUDE_PATH})
         list(APPEND LOG_SERVICE_ROCKSDB_LIBRARIES ${GCP_COMMON_LIB} ${GCP_CS_LIB} ${ROCKSDB_CLOUD_GCP_LIB})
         include_directories(${GCP_CS_INCLUDE_PATH})
         add_compile_definitions(USE_GCP)
         message(STATUS "LogService: Added compile definition USE_GCP.")
       endif()
     endif()
  else()
     # Find local RocksDB
     if(NOT ROCKSDB_BASE_INCLUDE_PATH) # Check if already found by another module (e.g. data_store)
       find_path(ROCKSDB_BASE_INCLUDE_PATH NAMES rocksdb/db.h)
     endif()

     if(NOT ROCKSDB_BASE_LIB)
       find_library(ROCKSDB_BASE_LIB NAMES rocksdb)
     endif()

     if(NOT ROCKSDB_BASE_INCLUDE_PATH OR NOT ROCKSDB_BASE_LIB)
       message(FATAL_ERROR "LogService: Failed to find base RocksDB include path or library.")
     else()
       message(STATUS "LogService: Found base RocksDB. Include: ${ROCKSDB_BASE_INCLUDE_PATH}, Lib: ${ROCKSDB_BASE_LIB}")
       list(APPEND LOG_SERVICE_ROCKSDB_INCLUDE_DIRS ${ROCKSDB_BASE_INCLUDE_PATH})
       list(APPEND LOG_SERVICE_ROCKSDB_LIBRARIES ${ROCKSDB_BASE_LIB})
       include_directories(${ROCKSDB_BASE_INCLUDE_PATH}) # Add to this module's includes
     endif()
  endif()

  list(APPEND LOG_LIB ${LOG_SERVICE_ROCKSDB_LIBRARIES})
else()
  message(STATUS "LogService: WITH_LOG_STATE=MEMORY. Skipping RocksDB discovery.")
endif()

set(LOG_INCLUDE_DIR_MODULE # Module specific include directories
  ${LOG_SOURCE_DIR}/include
  ${TX_LOG_PROTOS_SOURCE_DIR} # For shared log.proto
  # Add other necessary include paths from find_dependencies if not globally included:
  ${GFLAGS_INCLUDE_PATH} # GFLAGS from find_dependencies
  ${LEVELDB_INCLUDE_PATH} # LEVELDB from find_dependencies
  ${LOG_SERVICE_ROCKSDB_INCLUDE_DIRS} # Conditionally added RocksDB includes
  # BRPC/BRAFT includes are typically target-specific
)
# BRPC/BRAFT dependencies
find_path(BRPC_INCLUDE_PATH_LOG NAMES brpc/stream.h)
find_library(BRPC_LIB_LOG NAMES brpc)
find_path(BRAFT_INCLUDE_PATH_LOG NAMES braft/raft.h)
find_library(BRAFT_LIB_LOG NAMES braft)

if(NOT (BRPC_INCLUDE_PATH_LOG AND BRPC_LIB_LOG AND BRAFT_INCLUDE_PATH_LOG AND BRAFT_LIB_LOG))
  message(FATAL_ERROR "Failed to find bRPC or bRaft for log_service.")
endif()
message(STATUS "LogService: Found bRPC: ${BRPC_LIB_LOG} (Inc: ${BRPC_INCLUDE_PATH_LOG}), bRaft: ${BRAFT_LIB_LOG} (Inc: ${BRAFT_INCLUDE_PATH_LOG})")
list(APPEND LOG_INCLUDE_DIR_MODULE ${BRPC_INCLUDE_PATH_LOG} ${BRAFT_INCLUDE_PATH_LOG})


list(APPEND LOG_LIB
  ${CMAKE_THREAD_LIBS_INIT}
  ${GFLAGS_LIBRARY} # From find_dependencies
  ${PROTOBUF_LIBRARY} # From compile_protos (find_package Protobuf)
  ${LEVELDB_LIB} # From find_dependencies
  ${BRAFT_LIB} # Local find for log_service
  ${BRPC_LIB_LOG} # Local find for log_service
  dl
  z
)


# Compile protos for the log service (e.g., log.proto)
compile_protos_in_directory(${TX_LOG_PROTOS_SOURCE_DIR})
set(LOG_COMPILED_PROTO_FILES_FOR_LOG ${COMPILED_PROTO_CC_FILES})
message(STATUS "LogService: Compiled Log protos: ${LOG_COMPILED_PROTO_FILES_FOR_LOG}")

message(STATUS "LOG_SERVICE: TX_LOG_PROTOS_SOURCE_DIR: ${TX_LOG_PROTOS_SOURCE_DIR}, Effective LOG_INCLUDE_DIR_MODULE: ${LOG_INCLUDE_DIR_MODULE}")
message(STATUS "LOG_SERVICE: Effective LOG_LIB: ${LOG_LIB}")

set(_LOG_SERVICE_SOURCES
  ${LOG_SOURCE_DIR}/src/log_instance.cpp
  ${LOG_SOURCE_DIR}/src/log_server.cpp
  ${LOG_SOURCE_DIR}/src/log_state_rocksdb_impl.cpp
  ${LOG_SOURCE_DIR}/src/log_state_rocksdb_cloud_impl.cpp
  ${LOG_SOURCE_DIR}/src/log_state_memory_impl.cpp
  ${LOG_SOURCE_DIR}/src/fault_inject.cpp
  ${LOG_SOURCE_DIR}/src/INIReader.cpp
  ${LOG_SOURCE_DIR}/src/ini.c
)
if(LOG_COMPILED_PROTO_FILES_FOR_LOG)
  list(APPEND _LOG_SERVICE_SOURCES ${LOG_COMPILED_PROTO_FILES_FOR_LOG})
  message(STATUS "LogService: Appended LOG_COMPILED_PROTO_FILES_FOR_LOG to _LOG_SERVICE_SOURCES.")
endif()

add_library(LOG_SERVICE_OBJ OBJECT ${_LOG_SERVICE_SOURCES})
# /usr/bin/ld: CMakeFiles/LOG_SERVICE_OBJ.dir/eloq_log_service/src/ini.c.o: relocation R_X86_64_32 against `.rodata.str1.1' can not be used when making a shared object; recompile with -fPIC
set_property(TARGET LOG_SERVICE_OBJ PROPERTY POSITION_INDEPENDENT_CODE ON)
target_include_directories(LOG_SERVICE_OBJ PUBLIC
  ${LOG_INCLUDE_DIR}
)


add_library(logservice_static STATIC
  $<TARGET_OBJECTS:LOG_SERVICE_OBJ>
)
target_link_libraries(logservice_static PUBLIC
  ${LOG_LIB}
  ${PROTOBUF_LIBRARIES}
)
set_target_properties(logservice_static PROPERTIES OUTPUT_NAME logservice)


add_library(logservice_shared SHARED
  $<TARGET_OBJECTS:LOG_SERVICE_OBJ>
)
target_link_libraries(logservice_shared PUBLIC
  ${LOG_LIB}
  ${PROTOBUF_LIBRARIES}
)
set_target_properties(logservice_shared PROPERTIES OUTPUT_NAME logservice)
set_target_properties(logservice_shared PROPERTIES INSTALL_RPATH "$ORIGIN")
