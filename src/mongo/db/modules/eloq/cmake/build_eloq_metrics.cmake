# Set the root directory for ELOQ Metrics
set(ELOQ_METRICS_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/eloq_metrics)
# Set the source directory for ELOQ Metrics
set(ELOQ_METRICS_SRC_DIR "${ELOQ_METRICS_ROOT_DIR}/src")
# Set the include directory for ELOQ Metrics
set(ELOQ_METRICS_INCLUDE_DIR "${ELOQ_METRICS_ROOT_DIR}/include")
message(STATUS "EloqMetrics: Root directory set to: ${ELOQ_METRICS_ROOT_DIR}")
message(STATUS "EloqMetrics: Include directory set to: ${ELOQ_METRICS_INCLUDE_DIR}")

# Find the prometheus-cpp package, which is required for this module
find_package(prometheus-cpp CONFIG REQUIRED)
message(STATUS "EloqMetrics: Found prometheus-cpp. Libraries: ${prometheus-cpp_LIBRARIES}, Include directories: ${prometheus-cpp_INCLUDE_DIRS}")

# Define the list of source files for the ELOQ_METRICS target
set(ELOQ_METRICS_TARGET_SOURCE_LIST
    ${ELOQ_METRICS_INCLUDE_DIR}/metrics.h
    ${ELOQ_METRICS_INCLUDE_DIR}/meter.h
    ${ELOQ_METRICS_INCLUDE_DIR}/metrics_collector.h
    ${ELOQ_METRICS_INCLUDE_DIR}/prometheus_collector.h
    ${ELOQ_METRICS_INCLUDE_DIR}/metrics_manager.h
    ${ELOQ_METRICS_SRC_DIR}/metrics.cc
    ${ELOQ_METRICS_SRC_DIR}/prometheus_collector.cc
    ${ELOQ_METRICS_SRC_DIR}/metrics_manager.cc
)
message(STATUS "EloqMetrics: Source files list: ${ELOQ_METRICS_TARGET_SOURCE_LIST}")
# Create an object library from the source files
add_library(ELOQ_METRICS_OBJ OBJECT ${ELOQ_METRICS_TARGET_SOURCE_LIST})

# Create a static library from the object files
add_library(eloq_metrics_static STATIC $<TARGET_OBJECTS:ELOQ_METRICS_OBJ>)
target_link_libraries(eloq_metrics_static PUBLIC prometheus-cpp::pull)
target_include_directories(eloq_metrics_static PUBLIC ${ELOQ_METRICS_INCLUDE_DIR})

# Create a shared library from the object files
add_library(eloq_metrics_shared SHARED $<TARGET_OBJECTS:ELOQ_METRICS_OBJ>)
target_link_libraries(eloq_metrics_shared PUBLIC prometheus-cpp::pull)
target_include_directories(eloq_metrics_shared PUBLIC ${ELOQ_METRICS_INCLUDE_DIR})

# Apply module-specific definitions, such as enabling GLOG if WITH_GLOG is set
if(WITH_GLOG)
    message(STATUS "EloqMetrics: WITH_GLOG is ON. Applying GLOG configurations.")
    # Add compile definition for WITH_GLOG
    add_compile_definitions(WITH_GLOG=1)
    target_link_libraries(eloq_metrics_static PUBLIC ${GLOG_LIB})
    target_link_libraries(eloq_metrics_shared PUBLIC ${GLOG_LIB})
    target_include_directories(eloq_metrics_static PUBLIC ${GLOG_INCLUDE_PATH})
    target_include_directories(eloq_metrics_shared PUBLIC ${GLOG_INCLUDE_PATH})
    message(STATUS "EloqMetrics: Added GLOG include directories and libraries to eloq_metrics targets.")
endif()

# Set the output name for the static library
set_target_properties(eloq_metrics_static PROPERTIES OUTPUT_NAME eloq_metrics)
# Set the output name for the shared library
set_target_properties(eloq_metrics_shared PROPERTIES OUTPUT_NAME eloq_metrics)
set_target_properties(eloq_metrics_shared PROPERTIES INSTALL_RPATH "$ORIGIN")
