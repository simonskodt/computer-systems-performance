#!/bin/bash

# Get the directory of the current script
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Define the path
BUILD_PARTITION="$SCRIPT_DIR/../build/partition"

# Avoids same output when files have not changed
# -C points to the make file in case script is run inside /scrips folder
make -C "$SCRIPT_DIR/.." 2>&1 | grep -v "make: Nothing to be done for [\`']all[\`']."

# Check for the correct number of arguments
if [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 <algorithm> <n_hash_bits> <n_threads> [n_tuples]"
    exit 1
fi

case "$1" in
    "ind"|"independent")
        if [ -n "$4" ]; then
            "$BUILD_PARTITION" independent "$2" "$3" "$4"
        else
            "$BUILD_PARTITION" independent "$2" "$3"
        fi
        ;;
    "con"|"concurrent")
        if [ -n "$4" ]; then
            "$BUILD_PARTITION" concurrent "$2" "$3" "$4"
        else
            "$BUILD_PARTITION" concurrent "$2" "$3"
        fi
        ;;
    *)
        echo "Error: Unknown algorithm '$1'"
        echo "Usage: $0 <algorithm> <n_hash_bits> <n_threads> [n_tuples]"
        exit 1
        ;;
esac