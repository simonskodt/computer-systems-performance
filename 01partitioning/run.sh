#!/bin/bash

# Avoids same output when files have not changed
make 2>&1 | grep -v "make: Nothing to be done for [\`']all[\`']."

# Check for the correct number of arguments
if [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Usage: $0 <algorithm> <n_hash_bits> <n_threads> [n_tuples]"
    exit 1
fi

case "$1" in
    "ind"|"independent")
        if [ -n "$4" ]; then
            ./build/partition independent "$2" "$3" "$4"
        else
            ./build/partition independent "$2" "$3"
        fi
        ;;
    "con"|"concurrent")
        if [ -n "$4" ]; then
            ./build/partition concurrent "$2" "$3" "$4"
        else
            ./build/partition concurrent "$2" "$3"
        fi
        ;;
    *)
        echo "Error: Unknown algorithm '$1'"
        echo "Usage: $0 <algorithm> <n_hash_bits> <n_threads> [n_tuples]"
        exit 1
        ;;
esac