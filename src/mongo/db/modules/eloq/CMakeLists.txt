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
option(ELOQ_MODULE_ENABLED "Enable EloqModule" ON)
if (ELOQ_MODULE_ENABLED)
  add_definitions(-DELOQ_MODULE_ENABLED)
endif()

add_definitions(-DELOQ_MODULE_ELOQDOC)

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

if(USE_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fsanitize=address")
    message(STATUS "Global: ASan flags enabled for CXX, EXE_LINKER, SHARED_LINKER.")
endif()

# --- Include build system modules ---
add_subdirectory(data_substrate)
