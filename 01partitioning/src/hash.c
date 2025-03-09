#include "../include/hash.h"
#include "../include/partitioning.h"

uint64_t mod_hashing(uint64_t key, size_t hash_bits) {
    // Partitions are calculated by 2^(hash_bits), example:
    // 1      = 0000 0001
    // 1 << 4 = 0001 0000 = 16 = 2^4
    uint64_t n_partitions = COMPUTE_PARTITIONS(hash_bits);
    return key % n_partitions;
}
