#ifndef PARTITIONING_H
#define PARTITIONING_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <sched.h>
#include "colors.h"
#include "hash.h"
#include "timer.h"

#ifdef AFFINITY
#include <sched.h>
#endif

#define COMPUTE_PARTITIONS(hash_bit) (1 << hash_bit)
#define THROUGHPUT(tuples, milliseconds) ((size_t)(tuples / ((milliseconds) / 1000.0)));

#ifdef AFFINITY
const int thread_ids[32] = {
    0, 16, 2, 18, 4, 20, 6, 22, 8, 24, 10, 26, 12, 28, 14, 30, // CORE 1
    1, 17, 3, 19, 5, 21, 7, 23, 9, 25, 11, 27, 13, 29, 15, 31  // CORE 2
};
#endif

typedef struct {
    uint64_t key;
    uint64_t value;
} Tuple;

long independent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);
long concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);

#endif // PARTITIONING_H
