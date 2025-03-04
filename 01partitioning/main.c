#include "partitioning.h"
#include "timer.h"
#include "colors.h"

// Function prototypes
void print_usage();
Tuple* setup_tuples(size_t n_tuples);
pthread_t* setup_threads(size_t n_threads);

int main(int argc, char *argv[]) {
    if (argc != 4 && argc != 5) {
        print_usage();
        return EXIT_FAILURE;
    }

    char *algorithm = argv[1];
    size_t n_hash_bits = atoi(argv[2]);
    size_t n_threads = atoi(argv[3]);

    // Default value: 2^(24) tuples
    size_t n_tuples = 1ULL << 24;
    if (argc == 5) {
        n_tuples = atoi(argv[4]);
    }

    if (n_hash_bits <= 0 || n_threads <= 0 || n_tuples <= 0) {
        fprintf(stderr, "Error: All arguments must be positive\n");
        return EXIT_FAILURE;
    }

    // Setup tuples and threads
    Tuple* tuples = setup_tuples(n_tuples);
    pthread_t* threads = setup_threads(n_threads);

    // Start timer
    struct timespec start_time = start_timer();

    // Match on selected algorithm
    int result;
    if (strcmp(algorithm, "independent") == 0) {
        result = independent_output(tuples, n_tuples, n_hash_bits, n_threads, threads);
    } else if (strcmp(algorithm, "concurrent") == 0) {
        result = concurrent_output(tuples, n_tuples, n_hash_bits, n_threads, threads);
    } else {
        fprintf(stderr, "Error: Unknown algorithm '%s'\n", algorithm);
        free(tuples);
        free(threads);
        return EXIT_FAILURE;
    }

    // End timer
    long elapsed_time_ms = end_timer(start_time);
    size_t throughput = THROUGHPUT(n_tuples, elapsed_time_ms);
    if (result == EXIT_SUCCESS) {
        printf(COLOR_YELLOW "\n-----------------------------------\n" COLOR_RESET);
        printf(COLOR_YELLOW "Benchmark Results:\n"          COLOR_RESET);
        printf(COLOR_YELLOW "-----------------------------------\n"   COLOR_RESET);
        printf("Algorithm:     %s\n",  algorithm);
        printf("Hash bits:     %zu\n", n_hash_bits);
        printf("Threads:       %zu\n", n_threads);
        printf("Tuples:        %zu\n", n_tuples);
        printf(COLOR_YELLOW "\n-----------------------------------\n" COLOR_RESET);
        printf("Elapsed time:  %ld ms\n", elapsed_time_ms);
        printf("Throughput:    %zu million tuples/s\n", throughput / 1000000);
        printf(COLOR_YELLOW "-----------------------------------\n"   COLOR_RESET);
    }

    free(tuples);
    free(threads);
    return EXIT_SUCCESS;
}

void print_usage() {
    printf("Usage: ./partition <algorithm> <n_tuples> <n_hash_bits> <n_threads>\n");
    printf("  algorithm:   'ind' for independent or 'con' for concurrent\n");
    printf("  n_hash_bits: number of hash bits to use\n");
    printf("  n_threads:   number of threads to use\n");
    printf("  n_tuples:    number of tuples to partition (optional, default 2^(24)\n");
}

Tuple* setup_tuples(size_t n_tuples) {
    // Check for overflow of total byte allocation
    uint64_t total_bytes = (u_int64_t) n_tuples * sizeof(Tuple);
    if (total_bytes / sizeof(Tuple) != n_tuples) {
        fprintf(stderr, "Error: Overflow for total size of tuples\n");
        exit(EXIT_FAILURE);
    }

    // Allocate tuples
    Tuple* tuples = malloc(n_tuples * sizeof(Tuple));
    if (tuples == NULL) {
        perror("Could not allocate memory for tuples");
        exit(EXIT_FAILURE);
    }

    // Generate key value pair
    for (uint64_t i = 0; i < n_tuples; i++) {
        tuples[i].key = i; // might need different distribution (shuffle) later on
        tuples[i].value = rand() % 1000;
    }

    return tuples;
}

pthread_t* setup_threads(size_t n_threads) {
    pthread_t* threads = malloc(n_threads * sizeof(pthread_t));
    if (threads == NULL) {
        perror("Could not allocate memory for threads");
        exit(EXIT_FAILURE);
    }

    return threads;
}
