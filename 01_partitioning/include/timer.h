#ifndef TIMING_H
#define TIMING_H

#include <time.h>

struct timespec start_timer(void);
long end_timer(struct timespec start_time);

#endif // TIMING_H