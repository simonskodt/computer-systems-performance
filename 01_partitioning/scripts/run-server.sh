#!/bin/bash

# Get the directory of the current script
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Define the paths
RUN_SCRIPT="$SCRIPT_DIR/run.sh"
OUTPUT_DIR="$SCRIPT_DIR/../out"

NUM_ITERATIONS=8
hash_bits=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18)
threads=(1 2 4 8 16 32)

mkdir -p "$OUTPUT_DIR"

for hash in "${hash_bits[@]}"; do
    for thread in "${threads[@]}"; do
        for ((i=0; i<NUM_ITERATIONS; i++)); do
            # Independent output
            "$RUN_SCRIPT" ind $hash $thread >> "${OUTPUT_DIR}/ind_${hash}hash_${thread}thread.txt"
            perf stat -e cycles,instructions,cache-references,cache-misses "$RUN_SCRIPT" ind $hash $thread 2>> "${OUTPUT_DIR}/ind_${hash}hash_${thread}thread_perf.txt"

            # Concurrent output
            "$RUN_SCRIPT" con $hash $thread >> "${OUTPUT_DIR}/con_${hash}hash_${thread}thread.txt"
            perf stat -e cycles,instructions,cache-references,cache-misses "$RUN_SCRIPT" con $hash $thread 2>> "${OUTPUT_DIR}/con_${hash}hash_${thread}thread_perf.txt"
        done
    done
done
