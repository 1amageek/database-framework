#include <dlfcn.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef void (*Read)(uint64_t *, uint64_t *);
static _Atomic(int) ready, gate, done, finish;
static void *work(void *ignored) {
    (void)ignored;
    atomic_fetch_add(&ready, 1);
    while (!atomic_load(&gate)) sched_yield();
    for (int i = 0; i < 10000; i++) {
        void *volatile pointer = malloc(73);
        if (!pointer) abort();
        free(pointer);
    }
    atomic_fetch_add(&done, 1);
    while (!atomic_load(&finish)) sched_yield();
    return NULL;
}
int main(void) {
    Read read = (Read)dlsym(RTLD_DEFAULT, "access_allocation_counts");
    if (!read) return 1;
    uint64_t before, bytes, after, bytes_after;
    read(&before, &bytes);
    void *volatile p = malloc(17);
    void *volatile q = calloc(3, 19);
    p = realloc(p, 43);
    void *r = NULL;
    if (posix_memalign(&r, 64, 128) || !p || !q || !r) return 1;
    void *volatile s = aligned_alloc(64, 128);
    read(&after, &bytes_after);
    if (!s || after - before != 5 || bytes_after - bytes != 373) return 1;
    free(p); free(q); free(r); free(s);
    pthread_t threads[4];
    for (int i = 0; i < 4; i++) if (pthread_create(&threads[i], NULL, work, NULL)) return 1;
    while (atomic_load(&ready) != 4) sched_yield();
    read(&before, &bytes); atomic_store(&gate, 1);
    while (atomic_load(&done) != 4) sched_yield();
    read(&after, &bytes_after);
    if (after - before != 40000 || bytes_after - bytes != 40000 * 73) return 1;
    atomic_store(&finish, 1);
    for (int i = 0; i < 4; i++) pthread_join(threads[i], NULL);
    puts("ACCESS_OBSERVER_PASS calls=40000 threads=4");
    return 0;
}
