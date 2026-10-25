#ifndef ACCESS_PERFORMANCE_PROBE_H
#define ACCESS_PERFORMANCE_PROBE_H
#include <stdint.h>
typedef struct {
    uint64_t instant;
    uint64_t allocation_calls;
    uint64_t requested_bytes;
    uint64_t retained_bytes;
    uint64_t retained_blocks;
    uint64_t peak_retained_bytes;
    uint64_t reserved_bytes;
    uint64_t peak_resident_bytes;
    int valid;
    int observer_present;
} AccessPerformanceObservation;
typedef struct { uint64_t enabled_ns; uint64_t disabled_ns; uint64_t iterations; int valid; } AccessObservationCalibration;
uint64_t access_performance_now(void);
AccessPerformanceObservation access_performance_observe(void);
AccessObservationCalibration access_performance_calibrate(void);
#endif
