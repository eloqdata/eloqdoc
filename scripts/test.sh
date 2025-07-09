#!/bin/bash
set -euxo pipefail

# Function to display suite selection menu and gather all parameters
select_parameters() {
    echo "Please select a test suite:"
    echo "1) core_eloq"
    echo "2) 4.0_dbaas_aggregation"
    echo "3) 4.0_dbaas_change_streams"
    echo "4) 4.0_dbaas_core_txns"
    echo "5) 4.0_dbaas_core"
    echo "6) 4.0_dbaas_decimal"
    echo "7) 4.0_dbaas_json_schema"
    echo "8) 4.0_dbaas_geospatial"
    echo "9) 4.0_dbaas_fts"
    echo -n "Enter your choice (1-9): "
    read choice
    
    case $choice in
        1) SUITES="core_eloq" ;;
        2) SUITES="4.0_dbaas_aggregation" ;;
        3) SUITES="4.0_dbaas_change_streams" ;;
        4) SUITES="4.0_dbaas_core_txns" ;;
        5) SUITES="4.0_dbaas_core" ;;
        6) SUITES="4.0_dbaas_decimal" ;;
        7) SUITES="4.0_dbaas_json_schema" ;;
        8) SUITES="4.0_dbaas_geospatial" ;;
        9) SUITES="4.0_dbaas_fts" ;; # Add case for new option
        *) echo "Invalid choice, using default: core_op_query_eloq"
           SUITES="core_op_query_eloq" ;;
    esac
    
    echo -n "Enter host (default: 127.0.0.1): "
    read input_host
    HOST=${input_host:-127.0.0.1}
    
    echo -n "Enter port (default: 27017): "
    read input_port
    PORT=${input_port:-27017}

    echo -n "Enter log file path (default: ./<suite_name>.log): "
    read input_log_file
    LOG_FILE=${input_log_file:-"./${SUITES}.log"}
}

# Check if no arguments were provided
if [ $# -eq 0 ]; then
    # Turn off trace mode temporarily for cleaner menu display
    set +x
    select_parameters
    # Turn trace mode back on
    set -x
else
    # Use command line arguments with defaults
    SUITES="${1:-core_eloq}"
    HOST="${2:-127.0.0.1}"
    PORT="${3:-27017}"
    LOG_FILE="${4:-./${SUITES}.log}"
fi

python ./buildscripts/resmoke.py \
    --mongo=./mongo \
    --shellConnString="mongodb://${HOST}:${PORT}" \
    --continueOnFailure \
    --reportFile="./${SUITES}.json" \
    --suites=${SUITES} 2>&1 | tee "${LOG_FILE}"