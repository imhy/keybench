#define _POSIX_C_SOURCE 200809L
#include "store.h"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* Wait out transient backpressure before retrying a write that returned KV_RETRY,
   so a stall blocks the writer rather than dropping the write. The wait is a
   microsecond, just enough to yield to the engine's flush and compaction threads
   so we do not busy spin and starve its recovery, but far too small to add
   latency that would obscure the post stall throughput. */
static void store_backoff(void)
{
    struct timespec ts = {0, 1000};
    nanosleep(&ts, NULL);
}

static void *xmalloc(size_t n)
{
    void *p = malloc(n ? n : 1);
    if (!p)
    {
        fprintf(stderr, "keybench: out of memory\n");
        abort();
    }
    return p;
}

struct kv_store
{
    kv_backend *be;
    uint64_t next_checkpoint;
};

kv_store *store_open(backend_ctor inner, unsigned seed, const char *data_dir,
                     const kv_options *opts)
{
    kv_store *s = xmalloc(sizeof *s);
    s->be = inner(seed, data_dir, opts);
    s->next_checkpoint = 0;
    return s;
}

void store_close(kv_store *s)
{
    s->be->close(s->be->ctx);
    free(s->be);
    free(s);
}

const char *store_version(const kv_store *s)
{
    return s->be->version ? s->be->version(s->be->ctx) : s->be->name;
}

const char *store_datadir(const kv_store *s)
{
    return s->be->datadir ? s->be->datadir(s->be->ctx) : NULL;
}

void store_stats(kv_store *s, kv_stat_cb cb, void *arg)
{
    if (s->be->stats) s->be->stats(s->be->ctx, cb, arg);
}

int store_put(kv_store *s, const char *k, size_t klen, const char *v, size_t vlen)
{
    int rc;
    while ((rc = s->be->put(s->be->ctx, k, klen, v, vlen)) == KV_RETRY) store_backoff();
    return rc;
}

int store_putbatch(kv_store *s, const char *const *keys, const size_t *klens,
                   const char *const *vals, const size_t *vlens, int n)
{
    if (s->be->putbatch)
    {
        int rc;
        while ((rc = s->be->putbatch(s->be->ctx, keys, klens, vals, vlens, n)) == KV_RETRY)
            store_backoff();
        return rc;
    }
    int rc = 0;
    for (int i = 0; i < n; i++)
        if (store_put(s, keys[i], klens[i], vals[i], vlens[i]) != 0) rc = -1;
    return rc;
}

int store_get(kv_store *s, const char *k, size_t klen, char **vp, size_t *vlen)
{
    const char *bv;
    size_t bl;
    int found = s->be->get(s->be->ctx, k, klen, &bv, &bl);
    if (found > 0)
    {
        *vp = (char *)bv;
        *vlen = bl;
    }
    return found;
}

int store_del(kv_store *s, const char *k, size_t klen)
{
    int rc;
    while ((rc = s->be->del(s->be->ctx, k, klen)) == KV_RETRY) store_backoff();
    return rc;
}

int store_delbatch(kv_store *s, const char *const *keys, const size_t *klens, int n)
{
    if (s->be->delbatch)
    {
        int rc;
        while ((rc = s->be->delbatch(s->be->ctx, keys, klens, n)) == KV_RETRY) store_backoff();
        return rc;
    }
    int rc = 0;
    for (int i = 0; i < n; i++)
        if (store_del(s, keys[i], klens[i]) < 0) rc = -1;
    return rc;
}

int store_range(kv_store *s, const char *lo, size_t lolen, const char *hi, size_t hilen, int limit,
                kv_range_cb cb, void *arg)
{
    return s->be->range(s->be->ctx, lo, lolen, hi, hilen, limit, cb, arg);
}

uint64_t store_checkpoint(kv_store *s, uint64_t *bytes)
{
    *bytes = 0;
    if (!s->be->checkpoint) return 0;
    uint64_t id = __atomic_add_fetch(&s->next_checkpoint, 1, __ATOMIC_RELAXED);
    return s->be->checkpoint(s->be->ctx, id, bytes) == 0 ? id : 0;
}

int store_restore(kv_store *s, uint64_t id)
{
    if (!s->be->restore) return -1;
    return s->be->restore(s->be->ctx, id);
}
