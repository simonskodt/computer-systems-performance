#!/bin/bash

# Get the directory of the current script
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Define the path
BUILD_PARTITION="$SCRIPT_DIR/../build/partition"

AFFINITY_FLAG=0
ARGS=("$@")

# Check for the affinity flag as the first argument
if [ "$1" == "-a" ] || [ "$1" == "--affinity" ]; then
    AFFINITY_FLAG=1
    # Remove the affinity flag from the arguments list
    ARGS=("${@:2}")
fi

# Check for the correct number of arguments
if [ "${#ARGS[@]}" -lt 3 ] || [ "${#ARGS[@]}" -gt 4 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 [-a|--affinity] <algorithm> <n_hash_bits> <n_threads> [n_tuples]"
    exit 1
fi

# Run make with or without the AFFINITY flag
if [ "$AFFINITY_FLAG" -eq 1 ]; then
    make -C "$SCRIPT_DIR/.." AFFINITY=1 2>&1 | grep -v "make: Nothing to be done for [\`']all[\`']."
else
    make -C "$SCRIPT_DIR/.." 2>&1 | grep -v "make: Nothing to be done for [\`']all[\`']."
fi

# Check if the affinity flag is provided
if [ "$#" -eq 5 ] && [ "$5" == "affinity" ]; then
    AFFINITY_FLAG=1
else
    AFFINITY_FLAG=0
fi

# Run make with or without the AFFINITY flag
if [ "$AFFINITY_FLAG" -eq 1 ]; then
    make -C "$SCRIPT_DIR/.." AFFINITY=1 2>&1 | grep -v "make: Nothing to be done for [\`']all[\`']."
else
    make -C "$SCRIPT_DIR/.." 2>&1 | grep -v "make: Nothing to be done for [\`']all[\`']."
fi

case "${ARGS[0]}" in
    "ind"|"independent")
        if [ -n "${ARGS[3]}" ]; then
            "$BUILD_PARTITION" independent "${ARGS[1]}" "${ARGS[2]}" "${ARGS[3]}"
        else
            "$BUILD_PARTITION" independent "${ARGS[1]}" "${ARGS[2]}"
        fi
        ;;
    "con"|"concurrent")
        if [ -n "${ARGS[3]}" ]; then
            "$BUILD_PARTITION" concurrent "${ARGS[1]}" "${ARGS[2]}" "${ARGS[3]}"
        else
            "$BUILD_PARTITION" concurrent "${ARGS[1]}" "${ARGS[2]}"
        fi
        ;;
    *)
        echo "Error: Unknown algorithm '${ARGS[0]}'"
        echo "Usage: $0 [-a|--affinity] <algorithm> <n_hash_bits> <n_threads> [n_tuples]"
        exit 1
        ;;
esac