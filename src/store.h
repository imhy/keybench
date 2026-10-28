#ifndef STORE_H
#define STORE_H

#include <stddef.h>

#include "bench.h"

typedef struct kv_store kv_store;

kv_store *store_open(backend_ctor inner, unsigned seed, const char *data_dir,
                     const kv_options *opts);
void store_close(kv_store *s);

const char *store_version(const kv_store *s);
const char *store_datadir(const kv_store *s);

void store_stats(kv_store *s, kv_stat_cb cb, void *arg);

int store_put(kv_store *s, const char *k, size_t klen, const char *v, size_t vlen);
int store_putbatch(kv_store *s, const char *const *keys, const size_t *klens,
                   const char *const *vals, const size_t *vlens, int n);

int store_get(kv_store *s, const char *k, size_t klen, char **vp, size_t *vlen);
int store_del(kv_store *s, const char *k, size_t klen);
int store_delbatch(kv_store *s, const char *const *keys, const size_t *klens, int n);
int store_range(kv_store *s, const char *lo, size_t lolen, const char *hi, size_t hilen, int limit,
                kv_range_cb cb, void *arg);
/* Checkpoint and restore, when the backend has them. store_checkpoint assigns the
   next checkpoint id (1, 2, ...) and returns it, or 0 when it failed or the backend
   has no checkpoints; store_restore returns 0 on success, -1 on failure or when the
   backend cannot restore. */
uint64_t store_checkpoint(kv_store *s, uint64_t *bytes);
int store_restore(kv_store *s, uint64_t id);

#endif
