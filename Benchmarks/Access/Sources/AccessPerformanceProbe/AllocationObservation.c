#include <malloc/malloc.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

/* Only numeric atomics are retained. Every allocation keeps its original
   size/alignment, pointer ownership and deallocation; no pointer is stored.
   This image is injected into one benchmark process, never into the system. */
static _Atomic(uint64_t) allocation_calls = 0;
static _Atomic(uint64_t) requested_bytes = 0;
static _Atomic(int) enabled = 1;
static void observe(void *result, size_t bytes) {
    if (result && atomic_load_explicit(&enabled, memory_order_relaxed)) {
        atomic_fetch_add_explicit(&allocation_calls, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&requested_bytes, (uint64_t)bytes, memory_order_relaxed);
    }
}
void access_allocation_counts(uint64_t *calls, uint64_t *bytes) {
    *calls = atomic_load_explicit(&allocation_calls, memory_order_relaxed);
    *bytes = atomic_load_explicit(&requested_bytes, memory_order_relaxed);
}
void access_allocation_set_enabled(int value) { atomic_store_explicit(&enabled, value, memory_order_relaxed); }
#define INTERPOSE(replacement, original) \
    __attribute__((used)) static const struct { const void *new_function; const void *old_function; } \
    interpose_##original __attribute__((section("__DATA,__interpose"))) = { (const void *)replacement, (const void *)original }
static void *observed_malloc(size_t n) { void *p = malloc(n); observe(p, n); return p; }
static void *observed_calloc(size_t n, size_t size) {
    void *p = calloc(n, size);
    if (!size || n <= SIZE_MAX / size) observe(p, n * size);
    return p;
}
static void *observed_realloc(void *old, size_t n) { void *p = realloc(old, n); observe(p, n); return p; }
static int observed_posix_memalign(void **result, size_t alignment, size_t n) {
    int status = posix_memalign(result, alignment, n);
    if (status == 0) observe(*result, n);
    return status;
}
static void *observed_aligned_alloc(size_t alignment, size_t n) { void *p = aligned_alloc(alignment, n); observe(p, n); return p; }
static void *observed_zone_malloc(malloc_zone_t *zone, size_t n) { void *p = malloc_zone_malloc(zone, n); observe(p, n); return p; }
static void *observed_zone_calloc(malloc_zone_t *zone, size_t n, size_t size) {
    void *p = malloc_zone_calloc(zone, n, size);
    if (!size || n <= SIZE_MAX / size) observe(p, n * size);
    return p;
}
static void *observed_zone_realloc(malloc_zone_t *zone, void *old, size_t n) { void *p = malloc_zone_realloc(zone, old, n); observe(p, n); return p; }
static void *observed_zone_memalign(malloc_zone_t *zone, size_t alignment, size_t n) { void *p = malloc_zone_memalign(zone, alignment, n); observe(p, n); return p; }
INTERPOSE(observed_malloc, malloc);
INTERPOSE(observed_calloc, calloc);
INTERPOSE(observed_realloc, realloc);
INTERPOSE(observed_posix_memalign, posix_memalign);
INTERPOSE(observed_aligned_alloc, aligned_alloc);
INTERPOSE(observed_zone_malloc, malloc_zone_malloc);
INTERPOSE(observed_zone_calloc, malloc_zone_calloc);
INTERPOSE(observed_zone_realloc, malloc_zone_realloc);
INTERPOSE(observed_zone_memalign, malloc_zone_memalign);
