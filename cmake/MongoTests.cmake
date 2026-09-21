include_guard(GLOBAL)

enable_testing()
add_test(NAME cmake/standalone-source-graph
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/test_source_graph.py")
set_tests_properties(cmake/standalone-source-graph PROPERTIES LABELS "cmake;unit")
add_test(NAME cmake/idl-dependencies
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/test_idl_dependencies.py")
set_tests_properties(cmake/idl-dependencies PROPERTIES LABELS "cmake;unit" TIMEOUT 60)
add_test(NAME cmake/scons-test-selection
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/test_scons_test_selection.py")
set_tests_properties(cmake/scons-test-selection PROPERTIES LABELS "cmake;unit" TIMEOUT 60)
add_test(NAME cmake/icu-data-generator
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/test_icu_generator.py")
set_tests_properties(cmake/icu-data-generator PROPERTIES LABELS "cmake;unit" TIMEOUT 60)
add_test(NAME cmake/server-smoke-config
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/test_server_smoke_config.py")
set_tests_properties(cmake/server-smoke-config PROPERTIES LABELS "cmake;unit" TIMEOUT 60)
add_test(NAME cmake/server-smoke-diagnostics
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/test_server_smoke_diagnostics.py")
set_tests_properties(cmake/server-smoke-diagnostics PROPERTIES LABELS "cmake;unit" TIMEOUT 60)
add_test(NAME cmake/server-fixture
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/test_server_fixture.py")
set_tests_properties(cmake/server-fixture PROPERTIES LABELS "cmake;unit" TIMEOUT 30)

# Opt-in wire-protocol regression fixture; PyMongo is an explicit test-only dependency.
add_custom_target(eloqdoc-server-smoke
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/server_smoke.py"
        --server "$<TARGET_FILE:eloqdoc>"
        --data-store "${WITH_DATA_STORE}" --log-state "${WITH_LOG_STATE}"
        --diagnostics-dir "${CMAKE_CURRENT_BINARY_DIR}/smoke-diagnostics"
        --ttl-shutdown-mode sleeping
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/tests/server_smoke.py"
        --server "$<TARGET_FILE:eloqdoc>"
        --data-store "${WITH_DATA_STORE}" --log-state "${WITH_LOG_STATE}"
        --diagnostics-dir "${CMAKE_CURRENT_BINARY_DIR}/smoke-diagnostics"
        --ttl-shutdown-mode active
    DEPENDS eloqdoc
    USES_TERMINAL)

# Keep the production objects in archives for tests. Unlike the server executable, a unit test
# should pull only the MongoDB objects it references; this avoids running unrelated global
# initializers and keeps individual test binaries reasonably small.
add_library(eloqdoc-mongo-runtime-test-archive STATIC EXCLUDE_FROM_ALL
    $<TARGET_OBJECTS:eloqdoc-mongo-runtime>)
add_library(eloqdoc-mongo-third-party-test-archive STATIC EXCLUDE_FROM_ALL
    $<TARGET_OBJECTS:eloqdoc-mongo-third-party>)
add_library(eloqdoc-mongo-eloq-test-archive STATIC EXCLUDE_FROM_ALL
    $<TARGET_OBJECTS:eloqdoc-mongo-eloq-storage>)

# Compile each shared test helper once, but add only the helpers in a test's original SCons
# dependency closure to that executable. This preserves deliberate mock-vs-production choices.
set(_eloqdoc_mongo_test_support_object_targets)
foreach(_relative_source IN LISTS ELOQDOC_MONGO_TEST_SUPPORT_SOURCES)
    string(SHA1 _source_id "${_relative_source}")
    string(SUBSTRING "${_source_id}" 0 12 _source_id)
    set(_support_target "eloqdoc-mongo-test-support-${_source_id}")
    eloqdoc_resolve_mongo_sources(_support_source "${_relative_source}")
    add_library(${_support_target} OBJECT EXCLUDE_FROM_ALL "${_support_source}")
    eloqdoc_configure_mongo_target(${_support_target})
    add_dependencies(${_support_target} eloqdoc-generated-sources)
    target_link_libraries(${_support_target} PRIVATE ${_data_substrate_target})
    if(ELOQDOC_ENABLE_ASAN)
        target_compile_options(${_support_target} PRIVATE -fsanitize=address)
    endif()
    list(APPEND _eloqdoc_mongo_test_support_object_targets ${_support_target})
endforeach()
add_custom_target(eloqdoc-mongo-test-support
    DEPENDS ${_eloqdoc_mongo_test_support_object_targets})

# These objects implement string-named initializer groups used by every MongoDB test. They must be
# present directly in each executable (rather than discovered from a static archive), and object
# libraries let the complete test suite compile each common source only once.
add_library(eloqdoc-mongo-test-initializers OBJECT EXCLUDE_FROM_ALL
    "${PROJECT_SOURCE_DIR}/src/mongo/base/init.cpp"
    "${PROJECT_SOURCE_DIR}/src/mongo/base/shim.cpp"
    "${PROJECT_SOURCE_DIR}/src/mongo/base/validate_locale.cpp"
    "${PROJECT_SOURCE_DIR}/src/mongo/util/options_parser/startup_option_init.cpp")
add_library(eloqdoc-mongo-unit-test-main OBJECT EXCLUDE_FROM_ALL
    "${PROJECT_SOURCE_DIR}/src/mongo/unittest/unittest_main.cpp")
add_library(eloqdoc-mongo-integration-test-main OBJECT EXCLUDE_FROM_ALL
    "${PROJECT_SOURCE_DIR}/src/mongo/unittest/integration_test_main.cpp")
foreach(_support_target IN ITEMS
    eloqdoc-mongo-test-initializers
    eloqdoc-mongo-unit-test-main
    eloqdoc-mongo-integration-test-main)
    eloqdoc_configure_mongo_target(${_support_target})
    add_dependencies(${_support_target} eloqdoc-generated-sources)
    target_link_libraries(${_support_target} PRIVATE ${_data_substrate_target})
    if(ELOQDOC_ENABLE_ASAN)
        target_compile_options(${_support_target} PRIVATE -fsanitize=address)
    endif()
endforeach()

set(_eloqdoc_mongo_test_targets)
set(_eloqdoc_mongo_test_object_targets)
set(_eloqdoc_mongo_test_object_targets_without_eloq)
set(_eloqdoc_mongo_unit_test_count 0)
set(_eloqdoc_mongo_integration_test_count 0)

foreach(_test_id IN LISTS ELOQDOC_MONGO_TEST_IDS)
    set(_name "${ELOQDOC_MONGO_TEST_${_test_id}_NAME}")
    set(_kind "${ELOQDOC_MONGO_TEST_${_test_id}_KIND}")
    set(_requires_eloq "${ELOQDOC_MONGO_TEST_${_test_id}_REQUIRES_ELOQ}")
    set(_relative_sources ${ELOQDOC_MONGO_TEST_${_test_id}_SOURCES})
    set(_relative_support_sources ${ELOQDOC_MONGO_TEST_${_test_id}_SUPPORT_SOURCES})
    eloqdoc_resolve_mongo_sources(_test_sources ${_relative_sources})
    set(_support_objects)
    foreach(_relative_support_source IN LISTS _relative_support_sources)
        string(SHA1 _support_source_id "${_relative_support_source}")
        string(SUBSTRING "${_support_source_id}" 0 12 _support_source_id)
        list(APPEND _support_objects
            $<TARGET_OBJECTS:eloqdoc-mongo-test-support-${_support_source_id}>)
    endforeach()

    string(REPLACE "/" "-" _target_suffix "${_name}")
    string(REPLACE "_" "-" _target_suffix "${_target_suffix}")
    set(_target "eloqdoc-test-${_target_suffix}")

    if(_kind STREQUAL "unit")
        set(_main_objects
            $<TARGET_OBJECTS:eloqdoc-mongo-unit-test-main>)
        math(EXPR _eloqdoc_mongo_unit_test_count "${_eloqdoc_mongo_unit_test_count} + 1")
    else()
        set(_main_objects
            $<TARGET_OBJECTS:eloqdoc-mongo-integration-test-main>)
        math(EXPR _eloqdoc_mongo_integration_test_count
            "${_eloqdoc_mongo_integration_test_count} + 1")
    endif()
    set(_object_target "${_target}-objects")
    add_library(${_object_target} OBJECT EXCLUDE_FROM_ALL ${_test_sources})
    eloqdoc_configure_mongo_target(${_object_target})
    add_dependencies(${_object_target} eloqdoc-generated-sources)
    target_link_libraries(${_object_target} PRIVATE ${_data_substrate_target})
    if(ELOQDOC_ENABLE_ASAN)
        target_compile_options(${_object_target} PRIVATE -fsanitize=address)
    endif()

    add_executable(${_target} EXCLUDE_FROM_ALL
        $<TARGET_OBJECTS:${_object_target}>
        ${_support_objects}
        ${_main_objects}
        $<TARGET_OBJECTS:eloqdoc-mongo-test-initializers>)
    set_target_properties(${_target} PROPERTIES
        OUTPUT_NAME "${_target_suffix}"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/tests/mongo")
    eloqdoc_configure_mongo_target(${_target})
    add_dependencies(${_target} eloqdoc-generated-sources)
    set(_test_archives
        eloqdoc-mongo-runtime-test-archive
        eloqdoc-mongo-third-party-test-archive)
    if(_requires_eloq)
        list(APPEND _test_archives eloqdoc-mongo-eloq-test-archive)
    endif()
    string(JOIN "," _test_archive_group ${_test_archives})
    target_link_libraries(${_target} PRIVATE
        "$<LINK_GROUP:RESCAN,${_test_archive_group}>"
        ${_data_substrate_target}
        Threads::Threads
        ${CMAKE_DL_LIBS}
        m resolv rt)
    target_link_options(${_target} PRIVATE
        -rdynamic -Wl,-z,now -Wl,-z,relro -Wl,-z,noexecstack)
    if(ELOQDOC_ENABLE_ASAN)
        target_compile_options(${_target} PRIVATE -fsanitize=address)
        target_link_options(${_target} PRIVATE -fsanitize=address)
    endif()

    if(_kind STREQUAL "unit")
        add_test(NAME "${_name}" COMMAND ${_target})
        set_tests_properties("${_name}" PROPERTIES LABELS "mongo;unit" TIMEOUT 300
            WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
        if(_requires_eloq)
            set_property(TEST "${_name}" APPEND PROPERTY LABELS "eloq")
        endif()
    else()
        # Exercise integration-only initializer retention even without a running server.
        add_test(NAME "${_name}/startup-options" COMMAND ${_target} --help)
        set_tests_properties("${_name}/startup-options" PROPERTIES
            LABELS "mongo;integration-startup" TIMEOUT 30
            PASS_REGULAR_EXPRESSION "--connectionString")
        if(ELOQDOC_REGISTER_INTEGRATION_TESTS)
            add_test(NAME "${_name}" COMMAND ${_target}
                "--connectionString=${ELOQDOC_TEST_CONNECTION_STRING}")
            set_tests_properties("${_name}" PROPERTIES
                LABELS "mongo;integration" TIMEOUT 600
                RESOURCE_LOCK eloqdoc-integration-server)
        endif()
    endif()

    list(APPEND _eloqdoc_mongo_test_targets ${_target})
    list(APPEND _eloqdoc_mongo_test_object_targets ${_object_target})
    if(NOT _requires_eloq)
        list(APPEND _eloqdoc_mongo_test_object_targets_without_eloq ${_object_target})
    endif()
endforeach()

add_custom_target(eloqdoc-mongo-test-objects
    DEPENDS ${_eloqdoc_mongo_test_object_targets})
add_custom_target(eloqdoc-mongo-test-objects-without-eloq
    DEPENDS ${_eloqdoc_mongo_test_object_targets_without_eloq})
add_custom_target(eloqdoc-mongo-tests DEPENDS ${_eloqdoc_mongo_test_targets})
add_custom_target(eloqdoc-tests DEPENDS ${_eloqdoc_mongo_test_targets})

message(STATUS
    "MongoDB tests: ${_eloqdoc_mongo_unit_test_count} unit, "
    "${_eloqdoc_mongo_integration_test_count} integration")
if(NOT ELOQDOC_REGISTER_INTEGRATION_TESTS AND _eloqdoc_mongo_integration_test_count GREATER 0)
    message(STATUS
        "MongoDB integration binaries will be built but not registered with CTest; "
        "set ELOQDOC_REGISTER_INTEGRATION_TESTS=ON when a server fixture is available")
endif()
