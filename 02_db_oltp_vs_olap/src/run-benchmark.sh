#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")

BENCHMARK_SCRIPT="$SCRIPT_DIR/db_benchmark.py"
OUTPUT_DIR="$SCRIPT_DIR/../out"

SCALE_FACTORS=(1 2 5)
NUM_ITERATIONS=5

# Run benchmarks for TPC-H
for sf in "${SCALE_FACTORS[@]}"; do
    for ((i=0; i<NUM_ITERATIONS; i++)); do
        if [ $i -eq 0 ]; then
            # First iteration without --reuse
            python3 "$BENCHMARK_SCRIPT" --benchmark TPC_H --sf "$sf"
        else
            # Subsequent iterations with --reuse
            python3 "$BENCHMARK_SCRIPT" --benchmark TPC_H --sf "$sf" --reuse
        fi
    done
done

# Run benchmarks for TPC-C
# for sf in "${SCALE_FACTORS[@]}"; do
#     for ((i=0; i<NUM_ITERATIONS; i++)); do
#         python3 "$BENCHMARK_SCRIPT" --benchmark TPC_C --sf "$sf"
#     done
# done
