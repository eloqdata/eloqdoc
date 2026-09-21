include_guard(GLOBAL)

set(ELOQDOC_GENERATED_ROOT "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY
    "${ELOQDOC_GENERATED_ROOT}/mongo/base"
    "${ELOQDOC_GENERATED_ROOT}/mongo/db/auth"
    "${ELOQDOC_GENERATED_ROOT}/mongo/db/fts/unicode"
    "${ELOQDOC_GENERATED_ROOT}/mongo/shell")

option(ELOQDOC_CODEGEN_AT_CONFIGURE
    "Generate any missing generated sources during configure, so a freshly \
configured tree is immediately parseable by clangd and other tooling" ON)

# Declare a generated source. This always emits the usual build-time rule, so
# editing an .idl/.err/.tpl input still regenerates on the next build. It also
# records the recipe so the configure-time bootstrap at the bottom of this file
# can produce outputs that do not exist yet.
function(_eloqdoc_codegen)
    cmake_parse_arguments(ARG "" "MKDIR;DEPFILE;COMMENT" "OUTPUT;COMMAND;DEPENDS" ${ARGN})

    set(_command_args)
    if(ARG_MKDIR)
        list(APPEND _command_args COMMAND "${CMAKE_COMMAND}" -E make_directory "${ARG_MKDIR}")
    endif()
    list(APPEND _command_args COMMAND ${ARG_COMMAND})

    set(_optional_args)
    if(ARG_DEPFILE)
        list(APPEND _optional_args DEPFILE "${ARG_DEPFILE}")
    endif()
    if(ARG_COMMENT)
        list(APPEND _optional_args COMMENT "${ARG_COMMENT}")
    endif()

    add_custom_command(
        OUTPUT ${ARG_OUTPUT}
        ${_command_args}
        DEPENDS ${ARG_DEPENDS}
        ${_optional_args}
        VERBATIM)

    get_property(_index GLOBAL PROPERTY ELOQDOC_CODEGEN_COUNT)
    if(NOT _index)
        set(_index 0)
    endif()
    set_property(GLOBAL PROPERTY ELOQDOC_CODEGEN_${_index}_OUTPUT "${ARG_OUTPUT}")
    set_property(GLOBAL PROPERTY ELOQDOC_CODEGEN_${_index}_COMMAND "${ARG_COMMAND}")
    set_property(GLOBAL PROPERTY ELOQDOC_CODEGEN_${_index}_MKDIR "${ARG_MKDIR}")
    set_property(GLOBAL PROPERTY ELOQDOC_CODEGEN_${_index}_COMMENT "${ARG_COMMENT}")
    math(EXPR _index "${_index} + 1")
    set_property(GLOBAL PROPERTY ELOQDOC_CODEGEN_COUNT ${_index})
endfunction()

file(GLOB_RECURSE _eloqdoc_idl_compiler_sources CONFIGURE_DEPENDS
    "${PROJECT_SOURCE_DIR}/scripts/buildscripts/idl/*.py")

set(ELOQDOC_GENERATED_OUTPUTS)
set(_eloqdoc_all_source_requests ${ELOQDOC_MONGO_SOURCES})
if(ELOQDOC_BUILD_TESTS)
    list(APPEND _eloqdoc_all_source_requests ${ELOQDOC_MONGO_TEST_SUPPORT_SOURCES})
    foreach(_test_id IN LISTS ELOQDOC_MONGO_TEST_IDS)
        list(APPEND _eloqdoc_all_source_requests
            ${ELOQDOC_MONGO_TEST_${_test_id}_SOURCES})
    endforeach()
endif()
list(REMOVE_DUPLICATES _eloqdoc_all_source_requests)

# Embed exactly the same collation data as SCons; do not rely on host ICU data files.
set(_icu_init_cpp "${ELOQDOC_GENERATED_ROOT}/mongo/util/icu_init.cpp")
if(CMAKE_C_BYTE_ORDER STREQUAL "BIG_ENDIAN")
    set(_icu_data_name icudt57b.dat)
else()
    set(_icu_data_name icudt57l.dat)
endif()
set(_icu_data "${PROJECT_SOURCE_DIR}/src/third_party/icu4c-57.1/source/mongo_sources/${_icu_data_name}")
_eloqdoc_codegen(
    OUTPUT "${_icu_init_cpp}"
    MKDIR "${ELOQDOC_GENERATED_ROOT}/mongo/util"
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/generate_icu_init.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/util/generate_icu_init_cpp.py"
        -i "${_icu_data}" -o "${_icu_init_cpp}"
    DEPENDS
        "${PROJECT_SOURCE_DIR}/cmake/generate_icu_init.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/util/generate_icu_init_cpp.py"
        "${_icu_data}"
    COMMENT "Embedding pinned ICU 57.1 collation data")
list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_icu_init_cpp}")

foreach(_source IN LISTS _eloqdoc_all_source_requests)
    if(_source MATCHES "_gen\\.cpp$")
        string(REGEX REPLACE "_gen\\.cpp$" ".idl" _idl_relative "${_source}")
        set(_idl_input "${PROJECT_SOURCE_DIR}/src/${_idl_relative}")
        if(NOT EXISTS "${_idl_input}")
            message(FATAL_ERROR "No IDL input found for generated source ${_source}")
        endif()
        set(_generated_cpp "${ELOQDOC_GENERATED_ROOT}/${_source}")
        string(REGEX REPLACE "\\.cpp$" ".h" _generated_h "${_generated_cpp}")
        get_filename_component(_generated_dir "${_generated_cpp}" DIRECTORY)
        _eloqdoc_codegen(
            OUTPUT "${_generated_cpp}" "${_generated_h}"
            MKDIR "${_generated_dir}"
            COMMAND "${Python3_EXECUTABLE}"
                "${PROJECT_SOURCE_DIR}/cmake/generate_idl.py"
                --include "${PROJECT_SOURCE_DIR}/src"
                --base_dir "${ELOQDOC_GENERATED_ROOT}"
                --target_arch "${CMAKE_SYSTEM_PROCESSOR}"
                --header "${_generated_h}"
                --output "${_generated_cpp}"
                --depfile "${_generated_cpp}.d"
                "${_idl_input}"
            DEPENDS
                "${PROJECT_SOURCE_DIR}/cmake/generate_idl.py"
                "${_idl_input}"
                ${_eloqdoc_idl_compiler_sources}
            DEPFILE "${_generated_cpp}.d"
            COMMENT "Generating MongoDB IDL ${_idl_relative}")
        list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_generated_cpp}" "${_generated_h}")
    endif()
endforeach()

set(_error_codes_h "${ELOQDOC_GENERATED_ROOT}/mongo/base/error_codes.h")
set(_error_codes_cpp "${ELOQDOC_GENERATED_ROOT}/mongo/base/error_codes.cpp")
_eloqdoc_codegen(
    OUTPUT "${_error_codes_h}" "${_error_codes_cpp}"
    COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/generate_error_codes.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.err"
        "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.tpl.h=${_error_codes_h}"
        "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.tpl.cpp=${_error_codes_cpp}"
    DEPENDS
        "${PROJECT_SOURCE_DIR}/cmake/generate_error_codes.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.err"
        "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.tpl.h"
        "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.tpl.cpp"
    COMMENT "Generating MongoDB error codes")
list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_error_codes_h}" "${_error_codes_cpp}")

set(_action_type_h "${ELOQDOC_GENERATED_ROOT}/mongo/db/auth/action_type.h")
set(_action_type_cpp "${ELOQDOC_GENERATED_ROOT}/mongo/db/auth/action_type.cpp")
_eloqdoc_codegen(
    OUTPUT "${_action_type_h}" "${_action_type_cpp}"
    COMMAND "${Python3_EXECUTABLE}"
        "${PROJECT_SOURCE_DIR}/cmake/generate_action_types.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/db/auth/generate_action_types.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/db/auth/action_types.txt"
        "${_action_type_h}" "${_action_type_cpp}"
    DEPENDS
        "${PROJECT_SOURCE_DIR}/cmake/generate_action_types.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/db/auth/generate_action_types.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/db/auth/action_types.txt"
    COMMENT "Generating MongoDB authorization action types")
list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_action_type_h}" "${_action_type_cpp}")

set(_stop_words_h "${ELOQDOC_GENERATED_ROOT}/mongo/db/fts/stop_words_list.h")
set(_stop_words_cpp "${ELOQDOC_GENERATED_ROOT}/mongo/db/fts/stop_words_list.cpp")
file(GLOB _stop_word_inputs CONFIGURE_DEPENDS
    "${PROJECT_SOURCE_DIR}/src/mongo/db/fts/stop_words_*.txt")
_eloqdoc_codegen(
    OUTPUT "${_stop_words_h}" "${_stop_words_cpp}"
    COMMAND "${Python3_EXECUTABLE}"
        "${PROJECT_SOURCE_DIR}/cmake/run_legacy_text_generator.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/db/fts/generate_stop_words.py"
        ${_stop_word_inputs} "${_stop_words_h}" "${_stop_words_cpp}"
    DEPENDS
        "${PROJECT_SOURCE_DIR}/cmake/run_legacy_text_generator.py"
        "${PROJECT_SOURCE_DIR}/src/mongo/db/fts/generate_stop_words.py"
        ${_stop_word_inputs}
    COMMENT "Generating MongoDB text-search stop words")
list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_stop_words_h}" "${_stop_words_cpp}")

set(_unicode_dir "${ELOQDOC_GENERATED_ROOT}/mongo/db/fts/unicode")
foreach(_kind IN ITEMS casefold delimiter_list diacritic_list)
    set(_unicode_output "${_unicode_dir}/codepoints_${_kind}.cpp")
    if(_kind STREQUAL "casefold")
        set(_unicode_script "gen_casefold_map.py")
        set(_unicode_input "${PROJECT_SOURCE_DIR}/src/third_party/unicode-8.0.0/CaseFolding.txt")
    elseif(_kind STREQUAL "delimiter_list")
        set(_unicode_script "gen_delimiter_list.py")
        set(_unicode_input "${PROJECT_SOURCE_DIR}/src/third_party/unicode-8.0.0/PropList.txt")
    else()
        set(_unicode_script "gen_diacritic_list.py")
        set(_unicode_input "${PROJECT_SOURCE_DIR}/src/third_party/unicode-8.0.0/PropList.txt")
    endif()
    _eloqdoc_codegen(
        OUTPUT "${_unicode_output}"
        COMMAND "${Python3_EXECUTABLE}"
            "${PROJECT_SOURCE_DIR}/cmake/run_legacy_text_generator.py"
            "${PROJECT_SOURCE_DIR}/src/mongo/db/fts/unicode/${_unicode_script}"
            "${_unicode_input}" "${_unicode_output}"
        DEPENDS
            "${PROJECT_SOURCE_DIR}/cmake/run_legacy_text_generator.py"
            "${PROJECT_SOURCE_DIR}/src/mongo/db/fts/unicode/${_unicode_script}"
            "${PROJECT_SOURCE_DIR}/src/mongo/db/fts/unicode/gen_helper.py"
            "${_unicode_input}"
        COMMENT "Generating MongoDB Unicode ${_kind} table")
    list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_unicode_output}")
endforeach()

if("mongo/shell/mongo.cpp" IN_LIST ELOQDOC_MONGO_SOURCES)
    set(_shell_error_codes "${ELOQDOC_GENERATED_ROOT}/mongo/shell/error_codes.js")
    _eloqdoc_codegen(
        OUTPUT "${_shell_error_codes}"
        COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/generate_error_codes.py"
            "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.err"
            "${PROJECT_SOURCE_DIR}/src/mongo/shell/error_codes.tpl.js=${_shell_error_codes}"
        DEPENDS
            "${PROJECT_SOURCE_DIR}/cmake/generate_error_codes.py"
            "${PROJECT_SOURCE_DIR}/src/mongo/base/error_codes.err"
            "${PROJECT_SOURCE_DIR}/src/mongo/shell/error_codes.tpl.js"
        COMMENT "Generating shell error-code JavaScript")

    set(_mongo_js "${ELOQDOC_GENERATED_ROOT}/mongo/shell/mongo.cpp")
    set(_mongo_js_inputs
        assert.js bulk_api.js collection.js crud_api.js db.js explainable.js explain_query.js
        mongo.js mr.js query.js session.js types.js utils_auth.js utils.js utils_sh.js)
    set(_mongo_js_paths)
    foreach(_file IN LISTS _mongo_js_inputs)
        list(APPEND _mongo_js_paths "${PROJECT_SOURCE_DIR}/src/mongo/shell/${_file}")
    endforeach()
    _eloqdoc_codegen(
        OUTPUT "${_mongo_js}"
        COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/generate_js.py"
            "${PROJECT_SOURCE_DIR}/scons/site_tools/jstoh.py"
            "${_mongo_js}" ${_mongo_js_paths} "${_shell_error_codes}"
        DEPENDS
            "${PROJECT_SOURCE_DIR}/cmake/generate_js.py"
            "${PROJECT_SOURCE_DIR}/scons/site_tools/jstoh.py"
            ${_mongo_js_paths} "${_shell_error_codes}"
        COMMENT "Embedding MongoDB server JavaScript helpers")
    list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_shell_error_codes}" "${_mongo_js}")
endif()

if("mongo/scripting/mozjs/mongohelpers_js.cpp" IN_LIST ELOQDOC_MONGO_SOURCES)
    set(_mongohelpers_js "${ELOQDOC_GENERATED_ROOT}/mongo/scripting/mozjs/mongohelpers_js.cpp")
    _eloqdoc_codegen(
        OUTPUT "${_mongohelpers_js}"
        MKDIR "${ELOQDOC_GENERATED_ROOT}/mongo/scripting/mozjs"
        COMMAND "${Python3_EXECUTABLE}" "${PROJECT_SOURCE_DIR}/cmake/generate_js.py"
            "${PROJECT_SOURCE_DIR}/scons/site_tools/jstoh.py"
            "${_mongohelpers_js}" "${PROJECT_SOURCE_DIR}/src/mongo/scripting/mozjs/mongohelpers.js"
        DEPENDS "${PROJECT_SOURCE_DIR}/cmake/generate_js.py"
            "${PROJECT_SOURCE_DIR}/scons/site_tools/jstoh.py"
            "${PROJECT_SOURCE_DIR}/src/mongo/scripting/mozjs/mongohelpers.js"
        COMMENT "Embedding MongoDB mongohelpers JavaScript")
    list(APPEND ELOQDOC_GENERATED_OUTPUTS "${_mongohelpers_js}")
endif()

function(eloqdoc_resolve_mongo_sources output)
    set(_resolved)
    foreach(_source IN LISTS ARGN)
        if(_source MATCHES "_gen\\.cpp$"
            OR _source STREQUAL "mongo/base/error_codes.cpp"
            OR _source STREQUAL "mongo/db/auth/action_type.cpp"
            OR _source STREQUAL "mongo/db/fts/stop_words_list.cpp"
            OR _source MATCHES "^mongo/db/fts/unicode/codepoints_(casefold|delimiter_list|diacritic_list)\\.cpp$"
            OR _source STREQUAL "mongo/shell/mongo.cpp"
            OR _source STREQUAL "mongo/util/icu_init.cpp"
            OR _source STREQUAL "mongo/scripting/mozjs/mongohelpers_js.cpp")
            list(APPEND _resolved "${ELOQDOC_GENERATED_ROOT}/${_source}")
        else()
            list(APPEND _resolved "${PROJECT_SOURCE_DIR}/src/${_source}")
        endif()
    endforeach()
    set(${output} "${_resolved}" PARENT_SCOPE)
endfunction()

eloqdoc_resolve_mongo_sources(ELOQDOC_COMPILE_SOURCES ${ELOQDOC_MONGO_SOURCES})

set_source_files_properties(${ELOQDOC_GENERATED_OUTPUTS} PROPERTIES GENERATED TRUE)
add_custom_target(eloqdoc-generated-sources DEPENDS ${ELOQDOC_GENERATED_OUTPUTS})

# Configure-time bootstrap.
#
# The rules above only run when something is built. That leaves a freshly
# configured tree with an empty generated/ directory, so every translation unit
# that includes a generated header (mongo/base/error_codes.h, the IDL headers,
# ...) fails to parse. compile_commands.json already points at those headers, so
# clangd reports the whole tree as broken until the first build.
#
# Generate anything that is missing now. Outputs that already exist are left
# alone, so this costs nothing on repeat configures and never competes with the
# build-time rules, which remain responsible for regenerating stale outputs.
if(ELOQDOC_CODEGEN_AT_CONFIGURE)
    get_property(_eloqdoc_codegen_count GLOBAL PROPERTY ELOQDOC_CODEGEN_COUNT)
    if(NOT _eloqdoc_codegen_count)
        set(_eloqdoc_codegen_count 0)
    endif()

    set(_eloqdoc_bootstrapped 0)
    if(_eloqdoc_codegen_count GREATER 0)
        math(EXPR _eloqdoc_codegen_last "${_eloqdoc_codegen_count} - 1")
        foreach(_i RANGE ${_eloqdoc_codegen_last})
            get_property(_outputs GLOBAL PROPERTY ELOQDOC_CODEGEN_${_i}_OUTPUT)
            set(_needs_generating FALSE)
            foreach(_output IN LISTS _outputs)
                if(NOT EXISTS "${_output}")
                    set(_needs_generating TRUE)
                    break()
                endif()
            endforeach()
            if(NOT _needs_generating)
                continue()
            endif()

            get_property(_command GLOBAL PROPERTY ELOQDOC_CODEGEN_${_i}_COMMAND)
            get_property(_mkdir GLOBAL PROPERTY ELOQDOC_CODEGEN_${_i}_MKDIR)
            get_property(_comment GLOBAL PROPERTY ELOQDOC_CODEGEN_${_i}_COMMENT)
            if(_mkdir)
                file(MAKE_DIRECTORY "${_mkdir}")
            endif()
            execute_process(
                COMMAND ${_command}
                RESULT_VARIABLE _codegen_status
                ERROR_VARIABLE _codegen_error)
            if(NOT _codegen_status EQUAL 0)
                message(FATAL_ERROR
                    "Configure-time code generation failed (${_comment}):\n${_codegen_error}")
            endif()
            math(EXPR _eloqdoc_bootstrapped "${_eloqdoc_bootstrapped} + 1")
        endforeach()
    endif()

    if(_eloqdoc_bootstrapped GREATER 0)
        message(STATUS
            "EloqDoc: generated ${_eloqdoc_bootstrapped} missing source group(s) at configure time")
    endif()
endif()
