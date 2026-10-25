#include "AccessPerformanceProbe.h"
#include <dlfcn.h>
#include <malloc/malloc.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <time.h>

typedef void (*ReadCounts)(uint64_t *, uint64_t *);
typedef void (*SetEnabled)(int);
static _Atomic(void *) reader;
static _Atomic(void *) setter;

/* The process owns the injected observer until exit. These symbols have the
   exact C ABI below; stack output borrows never escape either call. */
static ReadCounts counts_reader(void) {
    void *value = atomic_load_explicit(&reader, memory_order_acquire);
    if (!value) {
        value = dlsym(RTLD_DEFAULT, "access_allocation_counts");
        if (value) atomic_store_explicit(&reader, value, memory_order_release);
    }
    return (ReadCounts)value;
}
static SetEnabled enabled_setter(void) {
    void *value = atomic_load_explicit(&setter, memory_order_acquire);
    if (!value) {
        value = dlsym(RTLD_DEFAULT, "access_allocation_set_enabled");
        if (value) atomic_store_explicit(&setter, value, memory_order_release);
    }
    return (SetEnabled)value;
}
uint64_t access_performance_now(void) {
    struct timespec instant = {0};
    if (clock_gettime(CLOCK_MONOTONIC, &instant) != 0) return 0;
    return (uint64_t)instant.tv_sec * UINT64_C(1000000000) + (uint64_t)instant.tv_nsec;
}
AccessPerformanceObservation access_performance_observe(void) {
    AccessPerformanceObservation observation = {0};
    malloc_statistics_t heap = {0};
    struct rusage process = {0};
    observation.instant = access_performance_now();
    if (!observation.instant || getrusage(RUSAGE_SELF, &process) != 0) return observation;
    malloc_zone_statistics(NULL, &heap);
    observation.retained_bytes = heap.size_in_use;
    observation.retained_blocks = heap.blocks_in_use;
    observation.peak_retained_bytes = heap.max_size_in_use;
    observation.reserved_bytes = heap.size_allocated;
    observation.peak_resident_bytes = (uint64_t)process.ru_maxrss;
    ReadCounts read = counts_reader();
    if (read) {
        read(&observation.allocation_calls, &observation.requested_bytes);
        observation.observer_present = 1;
    }
    observation.valid = 1;
    return observation;
}
static uint64_t allocation_loop(uint64_t iterations) {
    uint64_t start = access_performance_now();
    for (uint64_t i = 0; i < iterations; ++i) {
        void *volatile allocation = malloc(128);
        if (!allocation) return 0;
        free(allocation);
    }
    return access_performance_now() - start;
}
AccessObservationCalibration access_performance_calibrate(void) {
    AccessObservationCalibration result = {0};
    ReadCounts read = counts_reader(); SetEnabled enable = enabled_setter();
    if (!read || !enable) return result;
    uint64_t before = 0, ignored = 0, after = 0;
    read(&before, &ignored);
    void *volatile allocation = malloc(37);
    read(&after, &ignored);
    if (!allocation || after <= before) { free(allocation); return result; }
    free(allocation);
    result.iterations = 20000;
    enable(0); result.disabled_ns = allocation_loop(result.iterations);
    enable(1); result.enabled_ns = allocation_loop(result.iterations);
    result.valid = result.disabled_ns > 0 && result.enabled_ns > 0;
    return result;
}
