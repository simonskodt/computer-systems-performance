#include "partitioning.h"
#include <stdatomic.h>

typedef struct {
    int thread_id;
    size_t start_index;
    size_t end_index;
    Tuple *tuples;
    size_t n_tuples;
    size_t n_hash_bits;
    int n_partitions;
    Tuple **partitions;
    atomic_size_t *partition_sizes;
} ConcurrentThread;

void* concurrent_thread_function(void* args);

long concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads) {
    int n_partitions = COMPUTE_PARTITIONS(n_hash_bits);
    size_t tuples_per_partition = (n_tuples / n_partitions) * 1.5; // 50% larger than expected

    pthread_t* threads = malloc(n_threads * sizeof(pthread_t));
    if (threads == NULL) {
        perror("Could not allocate memory for threads");
        exit(EXIT_FAILURE);
    }

    // Thread structs
    ConcurrentThread* thread_args = malloc(n_threads * sizeof(ConcurrentThread));
    if (thread_args == NULL) {
        perror("Could not allocate memory for thread instances");
        return EXIT_FAILURE;
    }

    // Partitions
    Tuple **partitions = malloc(n_partitions * sizeof(Tuple*));
    atomic_size_t *partitions_sizes = (atomic_size_t *)calloc(n_partitions, sizeof(atomic_size_t));
    if (partitions == NULL || partitions_sizes == NULL) {
        perror("Could not allocate memory for partitions");
        free(thread_args);
        return EXIT_FAILURE;
    }

    for (int i = 0; i < n_partitions; i++) {
        partitions[i] = malloc(tuples_per_partition * sizeof(Tuple));
        if (partitions[i] == NULL) {
            perror("Could not allocate partition memory");

            // Free current partitions
            for (int j = 0; j < i; j++) {
                free(partitions[j]);
            }

            free(partitions);
            free(partitions_sizes);
            free(thread_args);
            return EXIT_FAILURE;
        }
    }

    // Start timer
    struct timespec start_time = start_timer();

    size_t tuples_per_thread = n_tuples / n_threads;
    size_t remainder = n_tuples % n_threads;
    size_t start = 0;
    for (size_t i = 0; i < n_threads; i++) {
        thread_args[i].thread_id = i+1; // 1 indexed
        thread_args[i].start_index = start;
        size_t extra = (i < remainder ? 1 : 0);
        thread_args[i].end_index = start + tuples_per_thread + extra;
        thread_args[i].tuples = tuples;
        thread_args[i].n_tuples = n_tuples;
        thread_args[i].n_hash_bits = n_hash_bits;
        thread_args[i].n_partitions = n_partitions;
        thread_args[i].partitions = partitions;
        thread_args[i].partition_sizes = partitions_sizes;
        start = thread_args[i].end_index;
    
        if (pthread_create(&threads[i], NULL, concurrent_thread_function, &thread_args[i]) != 0) {
            perror("Could not create thread");
            free(thread_args);
            return EXIT_FAILURE;
        }
    }
     
    for (size_t i = 0; i < n_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Printing the partitions and tuple keys
    // for (size_t i = 0; i < n_partitions; i++) {
    //     printf("Partition: %zu\n", i);
    //     for (size_t j = 0; j < tuples_per_partition; j++) {
    //         printf("Tuple: %llu\n", partitions[i][j].key);
    //     }
    // }

    // End timer
    long elapsed_time_ms = end_timer(start_time);

    // Free memory
    free(thread_args);
    for (int i = 0; i < n_partitions; i++) {
        free(partitions[i]);
    }
    free(partitions);
    free(partitions_sizes);

    return elapsed_time_ms;
}

void* concurrent_thread_function(void* args) {
    ConcurrentThread* c_thread = (ConcurrentThread*) args;

    // printf(COLOR_GREEN "Thread %d starting...\n" COLOR_RESET, c_thread->thread_id);

    for (size_t i = c_thread->start_index; i < c_thread->end_index; i++) {
        Tuple tuple = c_thread->tuples[i];

        uint64_t partition_index = mod_hashing(
            tuple.key,
            c_thread->n_hash_bits
        );

        size_t curr_partition_size = atomic_fetch_add(&c_thread->partition_sizes[partition_index], 1);
        c_thread->partitions[partition_index][curr_partition_size] = tuple;
    }

    // printf(COLOR_RED "Thread %d stopping...\n" COLOR_RESET, c_thread->thread_id);

    return NULL;
}
