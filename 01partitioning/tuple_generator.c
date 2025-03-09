#include "tuple_generator.h"

Tuple* setup_tuples(size_t n_tuples) {
    // Check for overflow of total byte allocation
    uint64_t total_bytes = (u_int64_t) n_tuples * sizeof(Tuple);
    if (total_bytes / sizeof(Tuple) != n_tuples) {
        fprintf(stderr, "Error: Overflow for total size of tuples\n");
        exit(EXIT_FAILURE);
    }

    // Allocate tuples
    Tuple* tuples = malloc(n_tuples * sizeof(Tuple));
    if (tuples == NULL) {
        perror("Could not allocate memory for tuples");
        exit(EXIT_FAILURE);
    }

    // Generate key value pair
    for (uint64_t i = 0; i < n_tuples; i++) {
        tuples[i].key = i; // might need different distribution (shuffle) later on
        tuples[i].value = rand() % 1000;
    }

    return tuples;
}

// Fisher-Yates shuffle algorithm
// Inspired by the following article: 
// https://www.geeksforgeeks.org/shuffle-a-given-array-using-fisher-yates-shuffle-algorithm/
void shuffle(Tuple* arr, size_t length) {
    for (size_t i = length - 1; i > 0; i--) {
        size_t j = rand() % (i + 1);
        swap(&arr[i], &arr[j]);
    }
}

// Swap two tuples
void swap(Tuple *a, Tuple *b) {
    Tuple temp = *a;
    *a = *b;
    *b = temp;
}