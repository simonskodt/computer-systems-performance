#ifndef PARTITIONING_H
#define PARTITIONING_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include "colors.h"
#include "hash.h"

#define COMPUTE_PARTITIONS(hash_bit) (1 << hash_bit);

typedef struct {
    uint64_t key;
    uint64_t value;
} Tuple;

int independent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads, pthread_t* threads);
int concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads, pthread_t* threads);

#endif // PARTITIONING_H
