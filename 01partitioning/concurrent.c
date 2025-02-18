#include "partitioning.h"

typedef struct {
    int thread_id;
    Tuple *tuples;
    size_t n_hash_bits;
} ConcurrentThread;

void* concurrent_thread_function(void* args);

int concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads, pthread_t* threads) {
    printf(COLOR_GREEN "\nRunning concurrent partitioning..." COLOR_RESET "\n");

    int n_partitions = COMPUTE_PARTITIONS(n_hash_bits);
    size_t tuples_per_thread = n_tuples / n_threads;  // uniform distribution


    ConcurrentThread* thread_args = malloc(n_threads * sizeof(ConcurrentThread));
    if (thread_args == NULL) {
        perror("Could not allocate memory for thread instances");
        return EXIT_FAILURE;
    }

    for (size_t i = 0; i < n_threads; i++) {
        thread_args[i].thread_id = i;
        thread_args[i].tuples = tuples;
        thread_args[i].n_hash_bits = n_hash_bits;
    
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
    free(threads);

    return EXIT_SUCCESS;
}

void* concurrent_thread_function(void* args) {
    ConcurrentThread* c_thread = (ConcurrentThread*) args;

    size_t start_index = 0; // replace
    size_t end_index = 1;   // replace

    for (size_t i = start_index; i < end_index; i++) {
        int partition_index = mod_hashing(
            c_thread->tuples[i].key,
            c_thread->n_hash_bits
        );

    }

    return NULL;
}
