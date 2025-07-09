# This file centralizes the compilation logic for Protocol Buffer files.
# It expects Protobuf_PROTOC_EXECUTABLE to be available (via find_package(Protobuf REQUIRED) in caller).

# Defines a function to compile all .proto files in a given directory.
#
# Usage:
# compile_protos_in_directory(<directory_path>)
#
# <directory_path>: Absolute or relative path to the directory containing .proto files.
#
# The function will set COMPILED_PROTO_CC_FILES in the PARENT_SCOPE with a list
# of generated .pb.cc file paths. The caller should copy this value to their
# desired variable immediately after calling the function.
#
# Requires find_package(Protobuf REQUIRED) to be called by the includer
# so that Protobuf_PROTOC_EXECUTABLE is available.

find_package(Protobuf REQUIRED)
# Check if protoc is available
if(NOT Protobuf_PROTOC_EXECUTABLE)
    message(FATAL_ERROR "Protobuf_PROTOC_EXECUTABLE is not set. find_package(Protobuf REQUIRED) might have failed or was not called.")
endif()



function(compile_protos_in_directory PROTO_DIRECTORY_PATH)
    # Ensure PROTO_DIRECTORY_PATH is absolute
    if(NOT IS_ABSOLUTE "${PROTO_DIRECTORY_PATH}")
        get_filename_component(PROTO_DIRECTORY_PATH "${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_DIRECTORY_PATH}" ABSOLUTE)
    endif()



    # Find all .proto files in the specified directory (non-recursive)
    file(GLOB _proto_file_basenames RELATIVE "${PROTO_DIRECTORY_PATH}" "${PROTO_DIRECTORY_PATH}/*.proto")

    set(_list_of_generated_cc_files "")

    if(NOT _proto_file_basenames)
        message(STATUS "No .proto files found in ${PROTO_DIRECTORY_PATH}.")
        set(COMPILED_PROTO_CC_FILES "" PARENT_SCOPE) # Set the output variable
        return()
    endif()

    foreach(_current_proto_basename ${_proto_file_basenames})
        # Derive .pb.cc and .pb.h filenames
        string(REGEX REPLACE "\\.proto$" "" _proto_name_no_ext ${_current_proto_basename})
        set(_generated_cc_file "${PROTO_DIRECTORY_PATH}/${_proto_name_no_ext}.pb.cc")
        set(_generated_h_file "${PROTO_DIRECTORY_PATH}/${_proto_name_no_ext}.pb.h")

        list(APPEND _list_of_generated_cc_files ${_generated_cc_file})

        # Add custom command to compile this proto file
        add_custom_command(
            OUTPUT ${_generated_cc_file} ${_generated_h_file}
            COMMAND ${Protobuf_PROTOC_EXECUTABLE}
            --cpp_out=. # Output to the working directory
            --proto_path=. # Look for imports in the working directory
            ${_current_proto_basename} # The input proto file (relative to working dir)
            WORKING_DIRECTORY ${PROTO_DIRECTORY_PATH}
            DEPENDS "${PROTO_DIRECTORY_PATH}/${_current_proto_basename}" ${Protobuf_PROTOC_EXECUTABLE}
            COMMENT "Compiling proto ${_current_proto_basename} from ${PROTO_DIRECTORY_PATH}"
            VERBATIM
        )
        message(STATUS "Configured compilation for ${_current_proto_basename} -> ${_generated_cc_file}, ${_generated_h_file}")
    endforeach()

    # Set the list of generated .cc files in the parent scope using a fixed variable name
    set(COMPILED_PROTO_CC_FILES ${_list_of_generated_cc_files} PARENT_SCOPE)
endfunction()
