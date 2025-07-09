#!/bin/bash

# Default build type
BUILD_TYPE="Debug"

# Display usage information
usage() {
    echo "Usage: $0 [-t build_type]"
    echo "  -t  Specify build type (RelWithDebInfo, Debug, Release). Default is Debug."
    exit 0
}

# Parse command-line arguments
while getopts ":t:" opt; do
    case $opt in
        t)
            BUILD_TYPE="$OPTARG"
            ;;
        *)
            usage
            ;;
    esac
done

# Validate the build type
if [[ "$BUILD_TYPE" != "RelWithDebInfo" && "$BUILD_TYPE" != "Debug" && "$BUILD_TYPE" != "Release" ]]; then
    echo "Error: Invalid build type. Must be one of: RelWithDebInfo, Debug, Release."
    usage
fi

# Define the build directory path
BUILD_DIR="build/$BUILD_TYPE"

# Check if the build directory exists
if [ ! -d "$BUILD_DIR" ]; then
    echo "Error: Build directory '$BUILD_DIR' does not exist."
    exit 1
fi

# Define a function to process each library
process_lib() {
    local lib_file="$1"
    echo "Processing $lib_file..."
    gdb-add-index "$lib_file"
    if [ $? -eq 0 ]; then
        echo "Successfully added index to $lib_file"
    else
        echo "Failed to add index to $lib_file"
    fi
}

export -f process_lib
export BUILD_TYPE # Export for subshells if needed, though not strictly for gdb-add-index

# Find all .so files in the build directory and process them in parallel
# Get the number of available processors
NUM_PROCS=$(nproc)
echo "Using up to $NUM_PROCS parallel processes."

find "$BUILD_DIR" -type f -name "*.so" -print0 | xargs -0 -P "$NUM_PROCS" -I {} bash -c 'process_lib "{}"'

echo "Finished processing all libraries."