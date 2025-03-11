#ifndef TUPLE_GENERATOR_H
#define TUPLE_GENERATOR_H

#include "partitioning.h"

Tuple* setup_tuples(size_t n_tuples);
void shuffle(Tuple* arr, size_t length);
void swap(Tuple *a, Tuple *b);

#endif // TUPLE_GENERATOR_H