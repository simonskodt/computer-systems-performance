#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")

SCRIPTS=("duckdb.py" "sqlite.py")

OUTPUT_DIR="$SCRIPT_DIR/out_perf"

for script in "${SCRIPTS[@]}"; do
    SCRIPT_PATH="$SCRIPT_DIR/$script"
    OUTPUT_FILE="$OUTPUT_DIR/${script%.py}_perf.txt"

    echo "Running perf for $script..."
    perf stat -e cycles,instructions,cache-references,cache-misses,branch-misses,context-switches,page-faults \
        python3 "$SCRIPT_PATH" 2> "$OUTPUT_FILE"
    echo "Perf results saved to $OUTPUT_FILE"
done