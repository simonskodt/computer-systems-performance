#ifndef HASH
#define HASH

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define COMPUTE_HASH_MOD(hash, hash_bit) ((hash) % (hash_bit));

uint64_t mod_hashing(uint64_t key, size_t hash_bits);

#endif // HASH