#ifndef PARTITIONING_H
#define PARTITIONING_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>

#define COMPUTE_HASH_MOD(hash, hash_bit) ((hash) % (hash_bit))

typedef struct {
    uint64_t key;
    uint64_t value;
} Tuple;

int independent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);
int concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);

#endif // PARTITIONING_H
