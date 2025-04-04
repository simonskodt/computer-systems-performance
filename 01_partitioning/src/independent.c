#include "../include/partitioning.h"
#include <math.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

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
        if (buf->count < buf->capacity) {
            buf->tuples[buf->count++] = a;
        } else {
            fprintf(stderr, "[Thread %zu] Partition %llu overflow\n", data->tid, partition);
        }
    }
    return NULL;
}

int independent_output(
    Tuple *tuples, 
    size_t n_tuples, 
    size_t n_hash_bits, 
    size_t n_threads,
    long *elapsed_time_ms
) {
    size_t num_partitions = 1UL << n_hash_bits;

    PartitionBuffer **thread_buffers = malloc(n_threads * sizeof(PartitionBuffer*));
    if (!thread_buffers) {
        perror("malloc failed for thread_buffers");
        return EXIT_FAILURE;
    }

    double expected = ((double)(n_tuples / n_threads)) / num_partitions;
    size_t capacity = (size_t)ceil(expected * 3 + 64);

    size_t threads_allocated = 0;
    for (; threads_allocated < n_threads; threads_allocated++) {
        thread_buffers[threads_allocated] = malloc(num_partitions * sizeof(PartitionBuffer));
        if (!thread_buffers[threads_allocated]) {
            perror("malloc failed for thread_buffers[t]");
            break;
        }
        for (size_t p = 0; p < num_partitions; p++) {
            thread_buffers[threads_allocated][p].capacity = capacity;
            thread_buffers[threads_allocated][p].count = 0;
            thread_buffers[threads_allocated][p].tuples = malloc(capacity * sizeof(Tuple));
            if (!thread_buffers[threads_allocated][p].tuples) {
                perror("malloc failed for partition buffer");
                for (size_t j = 0; j <= p; j++) {
                    if (thread_buffers[threads_allocated][j].tuples)
                        free(thread_buffers[threads_allocated][j].tuples);
                }
                free(thread_buffers[threads_allocated]);
                break;
            }
        }
    }

    if (threads_allocated != n_threads) {
        for (size_t t = 0; t < threads_allocated; t++) {
            for (size_t p = 0; p < num_partitions; p++) {
                if (thread_buffers[t][p].tuples)
                    free(thread_buffers[t][p].tuples);
            }
            free(thread_buffers[t]);
        }
        free(thread_buffers);
        return EXIT_FAILURE;
    }

    cpu_set_t cpuset[n_threads];

    pthread_t *threads = malloc(n_threads * sizeof(pthread_t));
    if (!threads) {
        perror("malloc failed for threads");
        goto cleanup;
    }
    ThreadData *thread_data = malloc(n_threads * sizeof(ThreadData));
    if (!thread_data) {
        perror("malloc failed for thread_data");
        free(threads);
        goto cleanup;
    }

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
        
        int thread_id = thread_ids[i];
        CPU_ZERO(&cpuset[i]);
        CPU_SET(thread_id, &cpuset[i]);
        pthread_attr_setaffinity_np(&attr[i], sizeof(cpu_set_t), &cpuset[i]);
    }

    for (size_t t = 0; t < n_threads; t++) {
        if (pthread_create(&threads[t], NULL, thread_func, &thread_data[t]) != 0) {
            fprintf(stderr, "Error creating thread %zu\n", t);
            free(thread_data);
            free(threads);
            goto cleanup;
        }
    }

    for (size_t t = 0; t < n_threads; t++) {
        pthread_join(threads[t], NULL);
    }

    *elapsed_time_ms = end_timer(start_time);

    free(thread_data);
    free(threads);

cleanup:
    for (size_t t = 0; t < n_threads; t++) {
        for (size_t p = 0; p < num_partitions; p++) {
            if (thread_buffers[t] && thread_buffers[t][p].tuples)
                free(thread_buffers[t][p].tuples);
        }
        free(thread_buffers[t]);
    }
    free(thread_buffers);

    return EXIT_SUCCESS;
}
