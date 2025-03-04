#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "partitioning.h"
#include "colors.h"

typedef struct {
    Tuple *tuples;
    size_t count;
    size_t capacity;
} PartitionBuffer;

typedef struct {
    size_t tid;
    size_t start_index;
    size_t end_index;
    Tuple *tuples;
    size_t n_tuples;
    size_t n_hash_bits;
    size_t num_partitions;
    PartitionBuffer *buffers;
} ThreadData;

void* thread_func(void* arg) {
    ThreadData *data = (ThreadData*) arg;
    for (size_t i = data->start_index; i < data->end_index; i++) {
        Tuple a = data->tuples[i];
        unsigned long long partition = COMPUTE_HASH_MOD(a.key, data->num_partitions);
        PartitionBuffer *buf = &data->buffers[partition];
        buf->tuples[buf->count] = a;
        buf->count++;
    }
    return NULL;
}

long independent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads) {
    size_t num_partitions = 1UL << n_hash_bits;

    PartitionBuffer **thread_buffers = malloc(n_threads * sizeof(PartitionBuffer*));
    if (!thread_buffers) {
        perror("malloc failed for thread_buffers");
        return -1;
    }

    double expected = ((double)(n_tuples / n_threads)) / num_partitions;
    // Is 10% enough, paper uses 50% ??
    size_t capacity = (size_t)ceil(expected * 1.1);

    for (size_t t = 0; t < n_threads; t++) {
        thread_buffers[t] = malloc(num_partitions * sizeof(PartitionBuffer));
        if (!thread_buffers[t]) {
            perror("malloc failed for thread_buffers[t]");
            return -1;
        }
        for (size_t p = 0; p < num_partitions; p++) {
            thread_buffers[t][p].capacity = capacity;
            thread_buffers[t][p].count = 0;
            thread_buffers[t][p].tuples = malloc(capacity * sizeof(Tuple));
            if (!thread_buffers[t][p].tuples) {
                perror("malloc failed for partition buffer");
                return -1;
            }
        }
    }

    pthread_t *threads = malloc(n_threads * sizeof(pthread_t));
    if (!threads) {
        perror("malloc failed for threads");
        return -1;
    }
    ThreadData *thread_data = malloc(n_threads * sizeof(ThreadData));
    if (!thread_data) {
        perror("malloc failed for thread_data");
        return -1;
    }

    // Start timer
    struct timespec start_time = start_timer();

    size_t tuples_per_thread = n_tuples / n_threads;
    size_t remainder = n_tuples % n_threads;
    size_t start = 0;
    for (size_t t = 0; t < n_threads; t++) {
        thread_data[t].tid = t;
        thread_data[t].start_index = start;
        size_t extra = (t < remainder ? 1 : 0);
        thread_data[t].end_index = start + tuples_per_thread + extra;
        thread_data[t].tuples = tuples;
        thread_data[t].n_tuples = n_tuples;
        thread_data[t].n_hash_bits = n_hash_bits;
        thread_data[t].num_partitions = num_partitions;
        thread_data[t].buffers = thread_buffers[t];
        start = thread_data[t].end_index;
    }

    for (size_t t = 0; t < n_threads; t++) {
        int rc = pthread_create(&threads[t], NULL, thread_func, &thread_data[t]);
        if (rc != 0) {
            fprintf(stderr, "Error creating thread %zu\n", t);
            return -1;
        }
    }

    for (size_t t = 0; t < n_threads; t++) {
        pthread_join(threads[t], NULL);
    }

    // End timer
    long elapsed_time_ms = end_timer(start_time);

    for (size_t t = 0; t < n_threads; t++) {
        for (size_t p = 0; p < num_partitions; p++) {
            free(thread_buffers[t][p].tuples);
        }
        free(thread_buffers[t]);
    }
    free(thread_buffers);
    free(threads);
    free(thread_data);

    return elapsed_time_ms;
}
