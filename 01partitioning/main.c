#include "partitioning.h"
#include "colors.h"
#include "tuple_generator.h"

// Function prototypes
void local_bencemark_results(char *algorithm, size_t n_hash_bits, size_t n_threads, 
    size_t n_tuples, long elapsed_time_ms, size_t throughput_millions);
void server_benchmark_results(long elapsed_time_ms, size_t throughput_millions);
void print_usage();
Tuple* setup_tuples(size_t n_tuples);

int main(int argc, char *argv[]) {
    if (argc != 3 && argc != 4) {
        fprintf(stderr, "Error: Incorrect number of arguments\n");
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
        print_usage();
        return EXIT_FAILURE;
    }

    // Setup tuples and threads
    Tuple* tuples = setup_tuples(n_tuples);

    // Match on selected algorithm
    long elapsed_time_ms;
    if (strcmp(algorithm, "independent") == 0) {
        elapsed_time_ms = independent_output(tuples, n_tuples, n_hash_bits, n_threads);
    } else if (strcmp(algorithm, "concurrent") == 0) {
        elapsed_time_ms = concurrent_output(tuples, n_tuples, n_hash_bits, n_threads);
    } else {
        fprintf(stderr, "Error: Unknown algorithm '%s'\n", algorithm);
        free(tuples);
        return EXIT_FAILURE;
    }

    size_t throughput = THROUGHPUT(n_tuples, elapsed_time_ms);
    // Rounds the throughput to the nearest million.
    // For example: throughput is 1,499,999, it will be rounded to 1 million.
    // If throughput is 1,500,000, it will be rounded to 2 million.
    size_t throughput_millions = (throughput + 500000) / 1000000; 

    server_benchmark_results(elapsed_time_ms, throughput_millions);

    free(tuples);
    return EXIT_SUCCESS;
}

void local_benchmark_results(char *algorithm, size_t n_hash_bits, size_t n_threads, 
        size_t n_tuples, long elapsed_time_ms, size_t throughput_millions) {
    printf(COLOR_YELLOW "\n-----------------------------------\n" COLOR_RESET);
    printf(COLOR_YELLOW "Benchmark Results:\n" COLOR_RESET);
    printf(COLOR_YELLOW "-----------------------------------\n" COLOR_RESET);
    printf("Algorithm:     %s\n", algorithm);
    printf("Hash bits:     %zu\n", n_hash_bits);
    printf("Threads:       %zu\n", n_threads);
    printf("Tuples:        %zu\n", n_tuples);
    printf(COLOR_YELLOW "\n-----------------------------------\n" COLOR_RESET);
    printf("Elapsed time:  %ld ms\n", elapsed_time_ms);
    printf("Throughput:    %zu million tuples/s\n", throughput_millions);
    printf(COLOR_YELLOW "-----------------------------------\n" COLOR_RESET);
}

void server_benchmark_results(long elapsed_time_ms, size_t throughput_millions) {
    printf("Elapsed time: %ld ms\n", elapsed_time_ms);
    printf("Throughput: %zu million tuples/s\n", throughput_millions);
    printf("-----------------------------------\n");
}

void print_usage() {
    printf("Usage: ./partition <algorithm> <n_tuples> <n_hash_bits> <n_threads>\n");
    printf("  algorithm:   'ind' for independent or 'con' for concurrent\n");
    printf("  n_hash_bits: number of hash bits to use\n");
    printf("  n_threads:   number of threads to use\n");
    printf("  n_tuples:    number of tuples to partition (optional, default 2^(24)\n");
}
