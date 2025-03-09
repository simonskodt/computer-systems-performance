#include "../include/timer.h"

struct timespec start_timer(void) {
    struct timespec start_time;
    clock_gettime(CLOCK_MONOTONIC_RAW, &start_time);
    return start_time;
}

long end_timer(struct timespec start_time) {
    struct timespec end_time;
    clock_gettime(CLOCK_MONOTONIC_RAW, &end_time);
    long elapsed_time_ms = (end_time.tv_sec - start_time.tv_sec) * 1000 + 
                           (end_time.tv_nsec - start_time.tv_nsec) / 1000000;
    return elapsed_time_ms;
}