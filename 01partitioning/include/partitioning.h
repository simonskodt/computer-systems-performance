#ifndef PARTITIONING_H
#define PARTITIONING_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include "colors.h"
#include "hash.h"
#include "timer.h"

#define COMPUTE_PARTITIONS(hash_bit) (1 << hash_bit);
#define THROUGHPUT(tuples, milliseconds) ((size_t)(tuples / ((milliseconds) / 1000.0)));

typedef struct {
    uint64_t key;
    uint64_t value;
} Tuple;

long independent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);
long concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);

#endif // PARTITIONING_H
