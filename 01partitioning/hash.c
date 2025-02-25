#include "hash.h"

int mod_hashing(uint64_t key, size_t hash_bits) {
    // Partitions are calculated by 2^(hash_bits), example:
    // 1      = 0000 0001
    // 1 << 4 = 0001 0000 = 16 = 2^4
    size_t n_partitions = 1 << hash_bits;
    return key % n_partitions;
}
