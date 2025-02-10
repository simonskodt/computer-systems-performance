#include "partitioning.h"

int openssl_hashing(unsigned int key, unsigned int hash_bits) {
    return 0; // key % (1 << hash_bits);
}
