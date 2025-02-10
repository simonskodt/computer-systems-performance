#include "partitioning.h"

void print_usage() {
    printf("Usage: ./partition <algorithm> <n_tuples> <n_hash_bits> <n_threads>\n");
    printf("  algorithm:   'independent' or 'concurrent'\n");
    printf("  n_tuples:    number of tuples to partition\n");
    printf("  n_hash_bits: number of hash bits to use\n");
    printf("  n_threads:   number of threads to use\n");
}

// Examples flags
// ./partition independent 100, 4, 8
// ./partition concurrect 100, 4, 8
void main(int argc, char *argv[]) {
    if (argc != 5) {
        print_usage();
        return 1;
    }

    char *algorithm = argv[1];
    size_t n_tuples = atoi(argv[2]);
    size_t n_hash_bits = atoi(argv[3]);
    size_t n_threads = atoi(argv[4]);

    if (n_tuples <= 0 || n_hash_bits <= 0 || n_threads <= 0) {
        printf("Error: All arguments must be positive\n");
        return 1;
    }

    Tuple *tuples = malloc(n_tuples * sizeof(Tuple));

    for (size_t i = 0; i < n_tuples; i++) {
        tuples[i].key = i;
        tuples[i].value = rand() % 1000;

    }

    if (strcmp(algorithm, "independent") == 0) {
        independent_partition(tuples, n_tuples, n_hash_bits, n_threads);
    } else if (strcmp(algorithm, "concurrent") == 0) {
        concurrent_partition(tuples, n_tuples, n_hash_bits, n_threads);
    } else {
        printf("Error: Unknown algorithm '%s'\n", algorithm);
        free(tuples);
        return 1;
    }

    free(tuples);
    return 0;
}