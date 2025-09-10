set(TX_SERVICE_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tx_service)
set(METRICS_SERVICE_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/eloq_metrics)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DFAULT_INJECTOR")

set(ABSEIL
    absl::btree
    absl::flat_hash_map
    absl::span
)

# Apply compile definitions based on global options
if(SMALL_RANGE) 
    add_compile_definitions(SMALL_RANGE)
    message(STATUS "TxService: Added compile definition SMALL_RANGE.")
endif()

if(STATISTICS)
    add_definitions(-DSTATISTICS)
    message(STATUS "TxService: Added compile definition -DSTATISTICS.")
endif()

# LINK_LIB is specific to this module's targets
set(LOCAL_LINK_LIB "")

# Define paths for proto directories
set(TX_SERVICE_PROTO_DIR_PATH ${TX_SERVICE_SOURCE_DIR}/include/proto)
set(LOG_PROTO_DIR_PATH ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/tx-log-protos)

# Call the centralized proto compilation function
compile_protos_in_directory(${TX_SERVICE_PROTO_DIR_PATH})
set(TX_COMPILED_PROTO_FILES ${COMPILED_PROTO_CC_FILES}) 
message(STATUS "TxService: Compiled TX service protos: ${TX_COMPILED_PROTO_FILES}")

compile_protos_in_directory(${LOG_PROTO_DIR_PATH})
set(LOG_COMPILED_PROTO_FILES ${COMPILED_PROTO_CC_FILES}) 
message(STATUS "TxService: Compiled Log protos for TX: ${LOG_COMPILED_PROTO_FILES}")

message(STATUS "TX_SERVICE_SOURCE_DIR: ${TX_SERVICE_SOURCE_DIR}") # Changed from message(${TX_SERVICE_SOURCE_DIR}) for clarity
set(INCLUDE_DIR # These are include directories specific to or heavily used by TX_SERVICE_OBJ
    ${TX_SERVICE_SOURCE_DIR}/include
    ${TX_SERVICE_SOURCE_DIR}/include/cc
    ${TX_SERVICE_SOURCE_DIR}/include/remote
    ${TX_SERVICE_SOURCE_DIR}/include/fault
    ${LOG_PROTO_DIR_PATH} 
    ${METRICS_SERVICE_SOURCE_DIR}/include # Dependency on eloq_metrics
    ${Protobuf_INCLUDE_DIRS} 
    ${TX_SERVICE_PROTO_DIR_PATH}
)

if(CMAKE_COMPILER_IS_GNUCC)
    list(APPEND INCLUDE_DIR ${BRPC_INCLUDE_PATH})
    if(BRPC_WITH_GLOG)
      list(APPEND INCLUDE_DIR ${GLOG_INCLUDE_PATH}) # GLOG_INCLUDE_PATH from local find_path
    endif()
    list(APPEND INCLUDE_DIR ${GFLAGS_INCLUDE_PATH}) # GFLAGS_INCLUDE_PATH from find_dependencies
    # LEVELDB_INCLUDE_PATH is already added globally by find_dependencies
endif()

list(APPEND LOCAL_LINK_LIB ${PROTOBUF_LIBRARY}) # PROTOBUF_LIBRARY from find_package(Protobuf)
message(STATUS "TxService: Added Protobuf library to LOCAL_LINK_LIB: ${PROTOBUF_LIBRARY}")
list(APPEND LOCAL_LINK_LIB
    mimalloc
    absl::btree
    absl::flat_hash_map
    ${GFLAGS_LIBRARY} # GFLAGS_LIBRARY from find_package(GFLAGS)
    ${LEVELDB_LIB}    # LEVELDB_LIB from find_dependencies
)

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin") # Darwin specific linking
    if(BRPC_LIB) # BRPC_LIB from local find_path
        list(APPEND LOCAL_LINK_LIB ${BRPC_LIB})
    else()
        message(WARNING "BRPC_LIB not defined for Darwin build of tx_service, check find_path for brpc.")
    endif()
endif()
# For non-Darwin GNUCC, BRPC_LIB is added to HOST_MANAGER_LINK_LIB if FORK_HM_PROCESS is ON.
# It should also be added for txservice_static/shared if tx_service itself uses brpc directly.
# Assuming tx_service uses brpc, let's add it if found (GNUCC block handles Darwin differently)
if(CMAKE_COMPILER_IS_GNUCC AND NOT (${CMAKE_SYSTEM_NAME} MATCHES "Darwin"))
    if(BRPC_LIB)
         list(APPEND LOCAL_LINK_LIB ${BRPC_LIB})
    endif()
endif()

message(STATUS "TxService: Final INCLUDE_DIR for TX_SERVICE_OBJ: ${INCLUDE_DIR}")
message(STATUS "TxService: Final LOCAL_LINK_LIB for txservice targets: ${LOCAL_LINK_LIB}")


set(TxService_SOURCES
    ${TX_SERVICE_SOURCE_DIR}/src/tx_key.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_execution.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_operation.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/checkpointer.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_trace.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_start_ts_collector.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_worker_pool.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sharder.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/standby.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/catalog_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/reader_writer_cntl.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/range_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/range_bucket_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_entry.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_map.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_shard.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_handler_result.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_shards.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/non_blocking_lock.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_req_misc.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/range_slice.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/eloq_string_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/eloq_basic_catalog_factory.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_request.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_node_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_receiver.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_sender.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/log_replay_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/cc_node.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/fault_inject.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/dead_lock_check.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_index_operation.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sk_generator.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/data_sync_task.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/store/snapshot_manager.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sequences/sequences.cpp
    ${TX_SERVICE_SOURCE_DIR}/tx-log-protos/log_agent.cpp # Uses log.pb.h
    # log.pb.cc is not added here; it's used by log_service and host_manager
    ${METRICS_SERVICE_SOURCE_DIR}/src/metrics.cc
)
# Add compiled proto files to sources
if(TX_COMPILED_PROTO_FILES)
    list(APPEND TxService_SOURCES ${TX_COMPILED_PROTO_FILES})
    message(STATUS "TxService: Appended TX_COMPILED_PROTO_FILES to TxService_SOURCES.")
endif()

add_library(TX_SERVICE_OBJ OBJECT ${TxService_SOURCES})
target_include_directories(TX_SERVICE_OBJ PUBLIC ${INCLUDE_DIR}) # Use PUBLIC if headers are part of the interface
# target_compile_features(TX_SERVICE_OBJ PUBLIC cxx_std_17) # Already set globally

get_target_property(TX_SERVICE_OBJ_INCLUDES TX_SERVICE_OBJ INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "TX_SERVICE_OBJ INTERFACE_INCLUDE_DIRECTORIES:")
foreach(item ${TX_SERVICE_OBJ_INCLUDES})
    message(STATUS "  - ${item}")
endforeach()

add_library(txservice_static STATIC $<TARGET_OBJECTS:TX_SERVICE_OBJ>)
target_link_libraries(txservice_static PUBLIC ${LOCAL_LINK_LIB} logservice_static eloq_metrics_static ${ABSEIL})
set_target_properties(txservice_static PROPERTIES OUTPUT_NAME txservice)
get_target_property(TXSERVICE_STATIC_LINK_LIBS txservice_static INTERFACE_LINK_LIBRARIES)
message(STATUS "txservice_static INTERFACE_LINK_LIBRARIES:")
foreach(item ${TXSERVICE_STATIC_LINK_LIBS})
    message(STATUS "  - ${item}")
endforeach()
get_target_property(TXSERVICE_STATIC_INCLUDES txservice_static INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "txservice_static INTERFACE_INCLUDE_DIRECTORIES:")
foreach(item ${TXSERVICE_STATIC_INCLUDES})
    message(STATUS "  - ${item}")
endforeach()

add_library(txservice_shared SHARED $<TARGET_OBJECTS:TX_SERVICE_OBJ>)
target_link_libraries(txservice_shared PUBLIC ${LOCAL_LINK_LIB} logservice_shared eloq_metrics_shared ${ABSEIL})
set_target_properties(txservice_shared PROPERTIES OUTPUT_NAME txservice)
set_target_properties(txservice_shared PROPERTIES INSTALL_RPATH "$ORIGIN")
get_target_property(TXSERVICE_SHARED_LINK_LIBS txservice_shared INTERFACE_LINK_LIBRARIES)
message(STATUS "txservice_shared INTERFACE_LINK_LIBRARIES:")
foreach(item ${TXSERVICE_SHARED_LINK_LIBS})
    message(STATUS "  - ${item}")
endforeach()
get_target_property(TXSERVICE_SHARED_INCLUDES txservice_shared INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "txservice_shared INTERFACE_INCLUDE_DIRECTORIES:")
foreach(item ${TXSERVICE_SHARED_INCLUDES})
    message(STATUS "  - ${item}")
endforeach()

message("LINK_LIB (variable):")
foreach(item ${LINK_LIB})
    message(STATUS "  - ${item}")
endforeach()


if(FORK_HM_PROCESS) # FORK_HM_PROCESS is a global option
    set(HOST_MANAGER_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tx_service/raft_host_manager)
    set(HOST_MANAGER_INCLUDE_DIR
        ${HOST_MANAGER_SOURCE_DIR}/include
        ${LOG_PROTO_DIR_PATH} 
        ${OPENSSL_INCLUDE_DIR} # Assuming OPENSSL_INCLUDE_DIR is found if needed (e.g. by bRPC/bRaft)
        ${TX_SERVICE_PROTO_DIR_PATH}
    )
    set(HOST_MANAGER_LINK_LIB "") # Initialize

    if(CMAKE_COMPILER_IS_GNUCC)
        list(APPEND HOST_MANAGER_INCLUDE_DIR ${BRPC_INCLUDE_PATH}) # From local find
        # list(APPEND HOST_MANAGER_INCLUDE_DIR ${BRAFT_INCLUDE_PATH}) # From local find (needs to be added) # Corrected: find braft specifically for HM
        find_path(BRAFT_INCLUDE_PATH_HM NAMES braft/raft.h) # Ensure it's found for HM
        find_library(BRAFT_LIB_HM NAMES braft)
        if((NOT BRAFT_INCLUDE_PATH_HM) OR (NOT BRAFT_LIB_HM))
            message(FATAL_ERROR "Fail to find braft for host_manager")
        endif()
        list(APPEND HOST_MANAGER_INCLUDE_DIR ${BRAFT_INCLUDE_PATH_HM})
        message(STATUS "HostManager: Found bRaft: ${BRAFT_LIB_HM} (Include: ${BRAFT_INCLUDE_PATH_HM})")

        if(BRPC_WITH_GLOG)
            list(APPEND HOST_MANAGER_INCLUDE_DIR ${GLOG_INCLUDE_PATH}) # From local find
        endif()
        list(APPEND HOST_MANAGER_INCLUDE_DIR ${GFLAGS_INCLUDE_PATH}) # Global
    endif()

    list(APPEND HOST_MANAGER_LINK_LIB ${PROTOBUF_LIBRARIES}) # Global
    message(STATUS "HostManager: Added Protobuf libraries to HOST_MANAGER_LINK_LIB: ${PROTOBUF_LIBRARIES}")

    if(CMAKE_COMPILER_IS_GNUCC)
        list(APPEND HOST_MANAGER_LINK_LIB
            ${GFLAGS_LIBRARY}  
            ${LEVELDB_LIB}    
            ${BRAFT_LIB}       
            ${BRPC_LIB}        
            ${OPENSSL_LIB} 
        )
        message(STATUS "HostManager: Appended GFLAGS, LevelDB, bRaft, bRPC to HOST_MANAGER_LINK_LIB.")
        if(BRPC_WITH_GLOG) # GLOG_LIB from local find
            list(APPEND HOST_MANAGER_LINK_LIB ${GLOG_LIB})
            message(STATUS "HostManager: Appended glog to HOST_MANAGER_LINK_LIB.")
        endif()
    endif()
    # Note: OPENSSL dependency is often handled by bRPC/bRaft themselves. If direct linking is needed:
    # find_package(OpenSSL)
    # list(APPEND HOST_MANAGER_LINK_LIB ${OpenSSL_LIBRARIES})


    set(RaftHM_SOURCES
        ${HOST_MANAGER_SOURCE_DIR}/src/main.cpp
        ${HOST_MANAGER_SOURCE_DIR}/src/raft_host_manager_service.cpp
        ${HOST_MANAGER_SOURCE_DIR}/src/raft_host_manager.cpp
        ${HOST_MANAGER_SOURCE_DIR}/src/ini.c
        ${HOST_MANAGER_SOURCE_DIR}/src/INIReader.cpp
        ${LOG_PROTO_DIR_PATH}/log_agent.cpp
    )
    if(LOG_COMPILED_PROTO_FILES) 
        list(APPEND RaftHM_SOURCES ${LOG_COMPILED_PROTO_FILES})
        message(STATUS "HostManager: Appended LOG_COMPILED_PROTO_FILES to RaftHM_SOURCES.")
    endif()
    if(TX_COMPILED_PROTO_FILES) 
        list(APPEND RaftHM_SOURCES ${TX_COMPILED_PROTO_FILES})
        message(STATUS "HostManager: Appended TX_COMPILED_PROTO_FILES to RaftHM_SOURCES.")
    endif()

    add_executable(host_manager ${RaftHM_SOURCES})
    message(STATUS "HostManager: Sources for host_manager: ${RaftHM_SOURCES}")
    target_include_directories(host_manager PUBLIC ${HOST_MANAGER_INCLUDE_DIR})
    message(STATUS "HostManager: Include directories for host_manager: ${HOST_MANAGER_INCLUDE_DIR}")
    target_link_libraries(host_manager ${HOST_MANAGER_LINK_LIB} yaml-cpp::yaml-cpp)
    message(STATUS "HostManager: Link libraries for host_manager: ${HOST_MANAGER_LINK_LIB} yaml-cpp::yaml-cpp")
    set_target_properties(host_manager PROPERTIES INSTALL_RPATH "$ORIGIN/../lib")
    # ... (message logging for host_manager)
endif()
