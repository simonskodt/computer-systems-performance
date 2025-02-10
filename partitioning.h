#ifndef PARTITIONING_H
#define PARTITIONING_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define COMPUTE_HASH_MOD(hash, hash_bit) ((hash) % (hash_bit))

typedef struct {
    unsigned int key;
    int value;
} Tuple;

void independent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);
void concurrent_output(Tuple *tuples, size_t n_tuples, size_t n_hash_bits, size_t n_threads);

#endif // PARTITIONING_H
