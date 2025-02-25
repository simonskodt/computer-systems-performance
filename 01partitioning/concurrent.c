#include "partitioning.h"

typedef struct {
    int thread_id;
    Tuple *tuples;
    size_t n_tuples;
    size_t n_hash_bits;
    size_t n_threads;
    int n_partitions;
    Tuple **partitions;
    size_t *partition_sizes;
} ConcurrentThread;

void* concurrent_thread_function(void* args);

int concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads, pthread_t* threads) {
    printf(COLOR_GREEN "\nRunning concurrent partitioning..." COLOR_RESET "\n");

    int n_partitions = COMPUTE_PARTITIONS(n_hash_bits);
    size_t tuples_per_partition = (n_tuples / n_partitions) * 1.5; // 50% larger than expected

    // Thread structs
    ConcurrentThread* thread_args = malloc(n_threads * sizeof(ConcurrentThread));
    if (thread_args == NULL) {
        perror("Could not allocate memory for thread instances");
        return EXIT_FAILURE;
    }

    // Partitions
    Tuple **partitions = malloc(n_partitions * sizeof(Tuple*)); // wtf
    size_t *partitions_sizes = calloc(n_partitions, sizeof(size_t));
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

    for (size_t i = 0; i < n_threads; i++) {
        thread_args[i].thread_id = i+1; // 1 indexed
        thread_args[i].tuples = tuples;
        thread_args[i].n_tuples = n_tuples;
        thread_args[i].n_hash_bits = n_hash_bits;
        thread_args[i].n_threads = n_threads;
        thread_args[i].n_partitions = n_partitions;
        thread_args[i].partitions = partitions;
        thread_args[i].partition_sizes = partitions_sizes;
    
        if (pthread_create(&threads[i], NULL, concurrent_thread_function, &thread_args[i]) != 0) {
            perror("Could not create thread");
            free(thread_args);
            return EXIT_FAILURE;
        }
    }
    

    for (size_t i = 0; i < n_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Free memory
    free(thread_args);
    for (int i = 0; i < n_partitions; i++) {
        free(partitions[i]);
    }
    free(partitions);
    free(partitions_sizes);

    return EXIT_SUCCESS;
}

void* concurrent_thread_function(void* args) {
    ConcurrentThread* c_thread = (ConcurrentThread*) args;

    printf(COLOR_GREEN "Thread %d starting...\n" COLOR_RESET, c_thread->thread_id);

    size_t tuples_per_thread = c_thread->n_tuples / c_thread->n_threads;  // uniform distribution
    size_t start_index = c_thread->n_tuples * tuples_per_thread;
    size_t end_index = start_index + tuples_per_thread;

    for (size_t i = start_index; i < end_index; i++) {
        int partition_index = mod_hashing(
            c_thread->tuples[i].key,
            c_thread->n_hash_bits
        );

        size_t partition_size = c_thread->partition_sizes[partition_index]++;
        c_thread->partitions[partition_index][partition_size] = c_thread->tuples[i];
        printf("Partition %d: \n", partition_index);
        printf("Tuple key %llu: \n", c_thread->tuples[i].key);
    }

    printf(COLOR_RED "Thread %d stopping...\n" COLOR_RESET, c_thread->thread_id);

    return NULL;
}
