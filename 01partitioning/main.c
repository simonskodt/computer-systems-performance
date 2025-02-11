#include "partitioning.h"

void print_usage();
Tuple* setup_tuples(size_t n_tuples);

int main(int argc, char *argv[]) {
    if (argc != 5) {
        print_usage();
        return EXIT_FAILURE;
    }

    char *algorithm = argv[1];
    size_t n_tuples = atoi(argv[2]);
    size_t n_hash_bits = atoi(argv[3]);
    size_t n_threads = atoi(argv[4]);

    if (n_tuples <= 0 || n_hash_bits <= 0 || n_threads <= 0) {
        fprintf(stderr, "Error: All arguments must be positive\n");
        return EXIT_FAILURE;
    }

    Tuple* tuples = setup_tuples(n_tuples);

    if (strcmp(algorithm, "independent") == 0) {
        independent_output(tuples, n_tuples, n_hash_bits, n_threads);
    } else if (strcmp(algorithm, "concurrent") == 0) {
        concurrent_output(tuples, n_tuples, n_hash_bits, n_threads);
    } else {
        fprintf(stderr, "Error: Unknown algorithm '%s'\n", algorithm);
        free(tuples);
        return EXIT_FAILURE;
    }

    free(tuples);
    return EXIT_SUCCESS;
}

void print_usage() {
    printf("Usage: ./partition <algorithm> <n_tuples> <n_hash_bits> <n_threads>\n");
    printf("  algorithm:   'independent' or 'concurrent'\n");
    printf("  n_tuples:    number of tuples to partition\n");
    printf("  n_hash_bits: number of hash bits to use\n");
    printf("  n_threads:   number of threads to use\n");
}

Tuple* setup_tuples(size_t n_tuples) {
    Tuple* tuples = malloc(n_tuples * sizeof(Tuple));
    if (tuples == NULL) {
        perror("Could not allocate memory for tuples");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < n_tuples; i++) {
        tuples[i].key = i;
        tuples[i].value = rand() % 1000;
    }

    return tuples;
}
