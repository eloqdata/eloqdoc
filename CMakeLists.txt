cmake_minimum_required(VERSION 3.24)

project(EloqDoc VERSION 4.0.3 LANGUAGES C CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

if(NOT CMAKE_GENERATOR MATCHES "Ninja")
    message(FATAL_ERROR "EloqDoc's minimal build supports Ninja generators only. Use: cmake -G Ninja ...")
endif()
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "The minimal EloqDoc build currently supports Linux only")
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ELOQDOC_BUILD_DATA_SUBSTRATE "Build and link the in-tree Data Substrate" ON)
option(ELOQDOC_BUILD_TESTS
    "Build tests for the retained MongoDB runtime and Eloq Mongo module" OFF)
option(ELOQDOC_REGISTER_INTEGRATION_TESTS
    "Register tests which require a running EloqDoc server with CTest" OFF)
option(ELOQDOC_ENABLE_ASAN "Build EloqDoc and Data Substrate with AddressSanitizer" OFF)
set(ELOQDOC_VERSION "4.0.3" CACHE STRING "MongoDB compatibility version reported by EloqDoc")
set(ELOQDOC_TEST_CONNECTION_STRING "localhost:27017" CACHE STRING
    "MongoDB connection string used by C++ integration tests")
set(WITH_DATA_STORE "ELOQDSS_ELOQSTORE" CACHE STRING "Data Substrate storage backend")
set_property(CACHE WITH_DATA_STORE PROPERTY STRINGS
    ROCKSDB ELOQDSS_ROCKSDB ELOQDSS_ROCKSDB_CLOUD_S3
    ELOQDSS_ROCKSDB_CLOUD_GCS ELOQDSS_ELOQSTORE)

find_package(Python3 3.8 REQUIRED COMPONENTS Interpreter)
find_package(Threads REQUIRED)

# PyYAML is needed by the IDL compiler in the selected Python 3 environment, even
# when the separately maintained C++ dependencies were restored from a cache.
execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c "import yaml"
    RESULT_VARIABLE _eloqdoc_pyyaml_status
    ERROR_VARIABLE _eloqdoc_pyyaml_error
    OUTPUT_QUIET)
if(NOT _eloqdoc_pyyaml_status EQUAL 0)
    message(FATAL_ERROR
        "${Python3_EXECUTABLE} cannot import PyYAML (yaml), required for MongoDB IDL generation.\n"
        "On Ubuntu with /usr/bin/python3, install python3-yaml. For a virtual environment, "
        "install PyYAML into that environment and select it with -DPython3_EXECUTABLE=/path/to/python.\n"
        "Installing PyYAML for the SCons Python 2 interpreter does not satisfy this dependency.\n"
        "${_eloqdoc_pyyaml_error}")
endif()

file(GLOB_RECURSE _eloqdoc_sconscript_files CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*SConscript")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/generate_eloqdoc_sources.py"
    ${_eloqdoc_sconscript_files})

set(_source_manifest "${CMAKE_CURRENT_BINARY_DIR}/eloqdoc-sources.cmake")
execute_process(
    COMMAND "${Python3_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/generate_eloqdoc_sources.py"
        --source-root "${CMAKE_CURRENT_SOURCE_DIR}/src"
        --target-arch "${CMAKE_SYSTEM_PROCESSOR}"
        --output "${_source_manifest}"
    RESULT_VARIABLE _source_manifest_status
    ERROR_VARIABLE _source_manifest_error)
if(NOT _source_manifest_status EQUAL 0)
    message(FATAL_ERROR "Failed to select the minimal EloqDoc source graph:\n${_source_manifest_error}")
endif()
include("${_source_manifest}")

list(LENGTH ELOQDOC_MONGO_TARGETS _mongo_target_count)
list(LENGTH ELOQDOC_MONGO_SOURCES _mongo_source_count)
list(LENGTH ELOQDOC_THIRD_PARTY_SOURCES _third_party_source_count)
list(LENGTH ELOQDOC_MONGO_TEST_IDS _mongo_test_count)
message(STATUS
    "Minimal EloqDoc graph: ${_mongo_target_count} Mongo modules, "
    "${_mongo_source_count} Mongo/Eloq sources, ${_third_party_source_count} vendored sources")
if(ELOQDOC_BUILD_TESTS)
    message(STATUS "Single-node MongoDB test graph: ${_mongo_test_count} executables")
endif()

# Generate the two configure-time headers which SCons used to synthesize.
set(mongo_config_altivec_vec_vbpermq_output_index "// #undef MONGO_CONFIG_ALTIVEC_VEC_VBPERMQ_OUTPUT_INDEX")
set(mongo_config_byte_order "#define MONGO_CONFIG_BYTE_ORDER 1234")
set(mongo_config_debug_build "// #undef MONGO_CONFIG_DEBUG_BUILD")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(mongo_config_debug_build "#define MONGO_CONFIG_DEBUG_BUILD 1")
endif()
set(mongo_config_has_ssl_set_ecdh_auto "// #undef MONGO_CONFIG_HAS_SSL_SET_ECDH_AUTO")
set(mongo_config_have_execinfo_backtrace "#define MONGO_CONFIG_HAVE_EXECINFO_BACKTRACE 1")
set(mongo_config_have_fips_mode_set "// #undef MONGO_CONFIG_HAVE_FIPS_MODE_SET")
set(mongo_config_have_header_unistd_h "#define MONGO_CONFIG_HAVE_HEADER_UNISTD_H 1")
set(mongo_config_have_memset_s "// #undef MONGO_CONFIG_HAVE_MEMSET_S")
set(mongo_config_have_posix_monotonic_clock "#define MONGO_CONFIG_HAVE_POSIX_MONOTONIC_CLOCK 1")
set(mongo_config_have_pthread_setname_np "#define MONGO_CONFIG_HAVE_PTHREAD_SETNAME_NP 1")
set(mongo_config_have_std_make_unique "#define MONGO_CONFIG_HAVE_STD_MAKE_UNIQUE 1")
set(mongo_config_have_strnlen "#define MONGO_CONFIG_HAVE_STRNLEN 1")
set(mongo_config_max_extended_alignment "#define MONGO_CONFIG_MAX_EXTENDED_ALIGNMENT 64")
set(mongo_config_optimized_build "// #undef MONGO_CONFIG_OPTIMIZED_BUILD")
if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(mongo_config_optimized_build "#define MONGO_CONFIG_OPTIMIZED_BUILD 1")
endif()
set(mongo_config_ssl "// #undef MONGO_CONFIG_SSL")
set(mongo_config_ssl_has_asn1_any_definitions "// #undef MONGO_CONFIG_HAVE_ASN1_ANY_DEFINITIONS")
set(mongo_config_ssl_provider "// #undef MONGO_CONFIG_SSL_PROVIDER")
set(mongo_config_wiredtiger_enabled "// #undef MONGO_CONFIG_WIREDTIGER_ENABLED")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/generated/mongo/util")
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/mongo/config.h" @ONLY)

string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)(.*)$" _version_match "${ELOQDOC_VERSION}")
if(NOT _version_match)
    message(FATAL_ERROR "ELOQDOC_VERSION must look like major.minor.patch[-extra]")
endif()
set(mongo_version "${ELOQDOC_VERSION}")
set(mongo_version_major "${CMAKE_MATCH_1}")
set(mongo_version_minor "${CMAKE_MATCH_2}")
set(mongo_version_patch "${CMAKE_MATCH_3}")
set(mongo_version_extra 0)
set(mongo_version_extra_str "${CMAKE_MATCH_4}")
execute_process(
    COMMAND git rev-parse HEAD
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    OUTPUT_VARIABLE mongo_git_hash
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT mongo_git_hash)
    set(mongo_git_hash "unknown")
endif()
set(buildinfo_js_engine "mozjs")
set(buildinfo_allocator "system")
set(buildinfo_modules "{ \"eloq\" }")
set(buildinfo_environment_data "{}")
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/util/version_constants.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/mongo/util/version_constants.h" @ONLY)

include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/MongoGeneratedSources.cmake")

set(_eloqdoc_server_main "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/dbmain.cpp")
list(REMOVE_ITEM ELOQDOC_COMPILE_SOURCES "${_eloqdoc_server_main}")
set(_eloqdoc_mongo_core_sources)
set(_eloqdoc_eloq_storage_sources)
foreach(_source IN LISTS ELOQDOC_COMPILE_SOURCES)
    if(_source MATCHES "/mongo/db/modules/eloq/")
        list(APPEND _eloqdoc_eloq_storage_sources "${_source}")
    else()
        list(APPEND _eloqdoc_mongo_core_sources "${_source}")
    endif()
endforeach()

set(_third_party_absolute_sources)
foreach(_source IN LISTS ELOQDOC_THIRD_PARTY_SOURCES)
    list(APPEND _third_party_absolute_sources "${CMAKE_CURRENT_SOURCE_DIR}/src/${_source}")
endforeach()

add_library(eloqdoc-mongo-third-party OBJECT ${_third_party_absolute_sources})
add_library(eloqdoc-mongo-runtime OBJECT ${_eloqdoc_mongo_core_sources})
add_library(eloqdoc-mongo-eloq-storage OBJECT ${_eloqdoc_eloq_storage_sources})
target_include_directories(eloqdoc-mongo-eloq-storage PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/modules/eloq")

set(_mongo_include_directories
    "${CMAKE_CURRENT_BINARY_DIR}/generated"
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/asio-master/asio/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/boost-1.60.0"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/libstemmer_c/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/libstemmer_c/libstemmer"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/pcre-8.41"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/pcre-8.41/build_posix"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/s2"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/snappy-1.1.3"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/snappy-1.1.3/build_linux"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/timelib-2018.01alpha1"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/tomcrypt-1.18.1/src/headers"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/yaml-cpp-0.5.3/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/zlib-1.2.11")

# Match src/third_party/icu4c-57.1/source/SConscript for the implementation and
# every consumer of its headers. Collation versions are persisted in index metadata.
set(_icu_root "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/icu4c-57.1/source")
set(_icu_definitions
    UCONFIG_NO_BREAK_ITERATION=1 UCONFIG_NO_FORMATTING=1
    UCONFIG_NO_TRANSLITERATION=1 UCONFIG_NO_REGULAR_EXPRESSIONS=1
    U_CHARSET_IS_UTF8=1 U_DISABLE_RENAMING=1
    U_STATIC_IMPLEMENTATION=1 U_USING_ICU_NAMESPACE=0)

# Match the pinned SpiderMonkey configuration used by SCons. These settings apply only to
# JavaScript implementation files, leaving MongoDB's own headers and compiler options intact.
set(_mozjs_root "${CMAKE_CURRENT_SOURCE_DIR}/src/third_party/mozjs-45")
list(APPEND _mongo_include_directories
    "${_mozjs_root}/include"
    "${_mozjs_root}/platform/${CMAKE_SYSTEM_PROCESSOR}/linux/include")
foreach(_source IN LISTS _third_party_absolute_sources ELOQDOC_COMPILE_SOURCES)
    if(_source MATCHES "/third_party/mozjs-45/")
        set_property(SOURCE "${_source}" APPEND PROPERTY INCLUDE_DIRECTORIES
            "${_mozjs_root}/extract/js/src" "${_mozjs_root}/extract/mfbt"
            "${_mozjs_root}/extract/intl/icu/source/common" "${_mozjs_root}/mongo_sources"
            "${_mozjs_root}/platform/${CMAKE_SYSTEM_PROCESSOR}/linux/build")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS
            IMPL_MFBT JS_USE_CUSTOM_ALLOCATOR STATIC_JS_API=1 U_NO_DEFAULT_INCLUDE_UTF_HEADERS=1)
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_OPTIONS
            -include js-confdefs.h -Wno-invalid-offsetof)
    elseif(_source MATCHES "/mongo/scripting/mozjs/")
        set_property(SOURCE "${_source}" APPEND PROPERTY INCLUDE_DIRECTORIES
            "${_mozjs_root}/mongo_sources")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS
            JS_USE_CUSTOM_ALLOCATOR STATIC_JS_API=1)
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_OPTIONS
            -include js-config.h -include js/RequiredDefines.h -Wno-invalid-offsetof)
    endif()
endforeach()

function(eloqdoc_configure_mongo_target target)
    target_include_directories(${target} PRIVATE ${_mongo_include_directories})
    # ICU's source include directories also contain private names such as util.h.
    # Search them after project/substrate headers, but before host system headers.
    target_include_directories(${target} SYSTEM PRIVATE
        "${_icu_root}/common" "${_icu_root}/i18n")
    target_compile_definitions(${target} PRIVATE
        _GNU_SOURCE
        _XOPEN_SOURCE=700
        BOOST_SYSTEM_NO_DEPRECATED
        BOOST_MATH_NO_LONG_DOUBLE_MATH_FUNCTIONS
        ELOQDOC_STANDALONE
        ELOQ_MODULE_ENABLED
        ELOQ_MODULE_ELOQDOC
        EXT_TX_PROC_ENABLED
        ${_icu_definitions}
        LTC_NO_PROTOTYPES
        PCRE_STATIC)
    target_compile_options(${target} PRIVATE
        $<$<COMPILE_LANGUAGE:C>:-fno-strict-aliasing>
        $<$<COMPILE_LANGUAGE:CXX>:-fno-strict-aliasing>
        $<$<COMPILE_LANGUAGE:CXX>:-fno-omit-frame-pointer>
        $<$<COMPILE_LANGUAGE:CXX>:-Wno-deprecated-declarations>
        $<$<COMPILE_LANGUAGE:CXX>:-Wno-nonnull>
        $<$<COMPILE_LANGUAGE:CXX>:-Wno-class-memaccess>
        $<$<COMPILE_LANGUAGE:CXX>:-Wno-interference-size>
        $<$<COMPILE_LANGUAGE:CXX>:-Wno-redundant-move>)
endfunction()

foreach(_target IN ITEMS
    eloqdoc-mongo-third-party
    eloqdoc-mongo-runtime
    eloqdoc-mongo-eloq-storage)
    eloqdoc_configure_mongo_target(${_target})
endforeach()

set(_intel_decimal_definitions
    DECIMAL_CALL_BY_REFERENCE=0
    DECIMAL_GLOBAL_ROUNDING=0
    DECIMAL_GLOBAL_EXCEPTION_FLAGS=0
    UNCHANGED_BINARY_STATUS_FLAGS=0
    USE_COMPILER_F128_TYPE=0
    USE_COMPILER_F80_TYPE=0
    USE_NATIVE_QUAD_TYPE=0
    LINUX=1 linux=1 gcc=1)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|aarch64|ppc64le)$")
    list(APPEND _intel_decimal_definitions efi2=1 EFI2=1)
endif()
foreach(_source IN LISTS _third_party_absolute_sources)
    if(_source MATCHES "/IntelRDFPMathLib20U1/")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS
            ${_intel_decimal_definitions})
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_OPTIONS -w)
    elseif(_source MATCHES "/pcre-8\\.41/")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H)
    elseif(_source MATCHES "/snappy-1\\.1\\.3/")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS HAVE_CONFIG_H)
    elseif(_source MATCHES "/timelib-2018\\.01alpha1/")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS
            HAVE_GETTIMEOFDAY HAVE_STRING_H HAVE_DIRENT_H HAVE_SYS_TIME_H HAVE_UNISTD_H)
    elseif(_source MATCHES "/zlib-1\\.2\\.11/")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS
            HAVE_STDARG_H HAVE_UNISTD_H)
    elseif(_source MATCHES "/s2/")
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS DEBUG_MODE=false)
    elseif(_source MATCHES "/icu4c-57\\.1/source/")
        if(_source MATCHES "/common/")
            set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS
                U_COMMON_IMPLEMENTATION=1)
        elseif(_source MATCHES "/i18n/")
            set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_DEFINITIONS
                U_I18N_IMPLEMENTATION=1)
        endif()
        set_property(SOURCE "${_source}" APPEND PROPERTY COMPILE_OPTIONS -Wno-sign-compare)
    endif()
endforeach()

add_dependencies(eloqdoc-mongo-runtime eloqdoc-generated-sources)
add_dependencies(eloqdoc-mongo-third-party eloqdoc-generated-sources)
add_dependencies(eloqdoc-mongo-eloq-storage eloqdoc-generated-sources)

if(ELOQDOC_BUILD_DATA_SUBSTRATE)
    set(ELOQ_MODULE_ENABLED ON CACHE BOOL "Register Data Substrate on brpc workers" FORCE)
    set(EXT_TX_PROC_ENABLED ON CACHE BOOL "Drive TxProcessors from external workers" FORCE)
    set(USE_ASAN "${ELOQDOC_ENABLE_ASAN}" CACHE BOOL "Data Substrate ASan" FORCE)
    set(WITH_TESTS OFF CACHE BOOL "Data Substrate tests are maintained independently" FORCE)
    set(ENABLE_TESTING OFF CACHE BOOL "Eloq metrics tests are maintained independently" FORCE)
    set(WITH_UNIT_TESTS OFF CACHE BOOL "Data-store tests are maintained independently" FORCE)
    set(WITH_EXAMPLE OFF CACHE BOOL "EloqStore examples are not part of EloqDoc" FORCE)
    set(WITH_DB_STRESS OFF CACHE BOOL "EloqStore stress tools are not part of EloqDoc" FORCE)
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/modules/eloq" eloq)
    # The existing substrate build uses directory-scoped definitions rather than exporting
    # all header/ABI switches. Share its actual settings with Mongo consumers without changing
    # the independently maintained substrate build or duplicating its backend option mapping.
    add_library(eloqdoc-data-substrate INTERFACE)
    target_link_libraries(eloqdoc-data-substrate INTERFACE data_substrate)
    # Some prebuilt dependencies (notably Abseil) do not carry their own RUNPATH. Use
    # transitive RPATH lookup so launching from the build tree needs no LD_LIBRARY_PATH.
    target_link_options(eloqdoc-data-substrate INTERFACE "LINKER:--disable-new-dtags")
    target_compile_definitions(eloqdoc-data-substrate INTERFACE
        $<TARGET_PROPERTY:data_substrate,COMPILE_DEFINITIONS>
        $<$<CONFIG:Debug>:FAULT_INJECTOR>)
    set(_data_substrate_target eloqdoc-data-substrate)
else()
    add_library(eloqdoc-data-substrate-placeholder INTERFACE)
    target_include_directories(eloqdoc-data-substrate-placeholder INTERFACE
        "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/modules/eloq/data_substrate"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/modules/eloq/data_substrate/core/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/modules/eloq/data_substrate/tx_service/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/modules/eloq/data_substrate/eloq_metrics/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/mongo/db/modules/eloq/data_substrate/store_handler")
    set(_data_substrate_target eloqdoc-data-substrate-placeholder)
    message(WARNING
        "ELOQDOC_BUILD_DATA_SUBSTRATE=OFF is for source-graph/compiler validation only; "
        "the resulting executable cannot link")
endif()

# MongoDB's Eloq integration includes Data Substrate headers directly. Propagate the selected
# target's public include paths and compile definitions while compiling the Mongo object library,
# not just while linking the final executable.
target_link_libraries(eloqdoc-mongo-runtime PRIVATE ${_data_substrate_target})
target_link_libraries(eloqdoc-mongo-eloq-storage PRIVATE ${_data_substrate_target})

add_executable(eloqdoc
    "${_eloqdoc_server_main}"
    $<TARGET_OBJECTS:eloqdoc-mongo-third-party>
    $<TARGET_OBJECTS:eloqdoc-mongo-runtime>
    $<TARGET_OBJECTS:eloqdoc-mongo-eloq-storage>)
if(TARGET host_manager)
    # Substrate's default startup resolves this helper next to /proc/self/exe. Keep build-tree
    # launches working as well as installs, where both executables already go into bin/.
    set_target_properties(host_manager PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    target_link_options(host_manager PRIVATE "LINKER:--disable-new-dtags")
    add_dependencies(eloqdoc host_manager)
endif()
eloqdoc_configure_mongo_target(eloqdoc)
set_target_properties(eloqdoc PROPERTIES
    INSTALL_RPATH "$ORIGIN/../lib"
    INSTALL_RPATH_USE_LINK_PATH TRUE)
target_link_libraries(eloqdoc PRIVATE
    ${_data_substrate_target}
    Threads::Threads
    ${CMAKE_DL_LIBS}
    m resolv rt)
target_link_options(eloqdoc PRIVATE -rdynamic -Wl,-z,now -Wl,-z,relro -Wl,-z,noexecstack)

if(ELOQDOC_ENABLE_ASAN)
    foreach(_target IN ITEMS
        eloqdoc-mongo-third-party
        eloqdoc-mongo-runtime
        eloqdoc-mongo-eloq-storage
        eloqdoc)
        target_compile_options(${_target} PRIVATE -fsanitize=address)
        target_link_options(${_target} PRIVATE -fsanitize=address)
    endforeach()
endif()

if(ELOQDOC_BUILD_TESTS)
    include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/MongoTests.cmake")
endif()

install(TARGETS eloqdoc RUNTIME DESTINATION bin)

add_custom_target(eloqdoc-source-manifest
    COMMAND "${CMAKE_COMMAND}" -E echo
        "${_mongo_target_count} selected modules, ${_mongo_source_count} Mongo/Eloq sources, ${_third_party_source_count} vendored sources"
    DEPENDS "${_source_manifest}")
