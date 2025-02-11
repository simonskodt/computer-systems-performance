#include "partitioning.h"

int mod_hashing(uint64_t key, size_t hash_bits) {
    size_t n_partitions = 1 << hash_bits;
    return key % n_partitions;
}
