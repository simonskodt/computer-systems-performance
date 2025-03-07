hash_bits=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18)
threads=(1 2 4 8 16 32)
folder_name="out"
NUM_ITERATIONS=8

mkdir -p "$folder_name"

for hash in "${hash_bits[@]}"; do
    for thread in "${threads[@]}"; do
        for ((i=0; i<NUM_ITERATIONS; i++)); do
            perf stat -e
            ./run.sh ind $hash $thread >> "${folder_name}/ind_${hash}hash_${thread}thread.txt"
            ./run.sh con $hash $thread >> "${folder_name}/con_${hash}hash_${thread}thread.txt"
        done
    done
done
