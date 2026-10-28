#define _DEFAULT_SOURCE
#include <rocksdb/c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "backend_registry.h"
#include "bench.h"

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

typedef struct
{
    rocksdb_t *db;
    rocksdb_options_t *opts;
    rocksdb_writeoptions_t *wopt;
    rocksdb_readoptions_t *ropt;
    int disable_wal;
    char path[256];
} rdb;

/* RocksDB reports through a Status string. Busy is a write conflict, a Try again
   is the write path asking the caller to retry, and a Result incomplete is a no
   slowdown write stall, all transient and surfaced as KV_RETRY so the store waits
   them out. Everything else, from NotFound through IOError and the no space and
   memory limit aborts, is a real failure the store should not spin on. */
static int rdb_retryable(const char *err)
{
    return err && (strncmp(err, "Busy", 4) == 0 || strncmp(err, "Result incomplete", 17) == 0 ||
                   strncmp(err, "Operation failed. Try again.", 28) == 0);
}

static int rdb_put(void *ctx, const char *k, size_t klen, const char *v, size_t vlen)
{
    rdb *r = ctx;
    char *err = NULL;
    rocksdb_put(r->db, r->wopt, k, klen, v, vlen, &err);
    if (err)
    {
        int retry = rdb_retryable(err);
        if (!retry) fprintf(stderr, "rocksdb put: %s\n", err);
        free(err);
        return retry ? KV_RETRY : -1;
    }
    return 0;
}

static int rdb_putbatch(void *ctx, const char *const *keys, const size_t *klens,
                        const char *const *vals, const size_t *vlens, int n)
{
    rdb *r = ctx;
    rocksdb_writebatch_t *wb = rocksdb_writebatch_create();
    for (int i = 0; i < n; i++) rocksdb_writebatch_put(wb, keys[i], klens[i], vals[i], vlens[i]);
    char *err = NULL;
    rocksdb_write(r->db, r->wopt, wb, &err);
    rocksdb_writebatch_destroy(wb);
    if (err)
    {
        int retry = rdb_retryable(err);
        if (!retry) fprintf(stderr, "rocksdb putbatch: %s\n", err);
        free(err);
        return retry ? KV_RETRY : -1;
    }
    return 0;
}

static int rdb_delbatch(void *ctx, const char *const *keys, const size_t *klens, int n)
{
    rdb *r = ctx;
    rocksdb_writebatch_t *wb = rocksdb_writebatch_create();
    for (int i = 0; i < n; i++) rocksdb_writebatch_delete(wb, keys[i], klens[i]);
    char *err = NULL;
    rocksdb_write(r->db, r->wopt, wb, &err);
    rocksdb_writebatch_destroy(wb);
    if (err)
    {
        int retry = rdb_retryable(err);
        if (!retry) fprintf(stderr, "rocksdb delbatch: %s\n", err);
        free(err);
        return retry ? KV_RETRY : -1;
    }
    return 0;
}

static int rdb_get(void *ctx, const char *k, size_t klen, const char **vp, size_t *vlen)
{
    rdb *r = ctx;
    char *err = NULL;
    size_t vl = 0;
    char *v = rocksdb_get(r->db, r->ropt, k, klen, &vl, &err);
    if (err)
    {
        fprintf(stderr, "rocksdb get: %s\n", err);
        free(err);
        return KV_ERR;
    }
    if (!v) return 0;
    char *c = xmalloc(vl);
    memcpy(c, v, vl);
    rocksdb_free(v);
    *vp = c;
    *vlen = vl;
    return 1;
}

static int rdb_del(void *ctx, const char *k, size_t klen)
{
    rdb *r = ctx;
    char *err = NULL;
    rocksdb_delete(r->db, r->wopt, k, klen, &err);
    if (err)
    {
        int retry = rdb_retryable(err);
        if (!retry) fprintf(stderr, "rocksdb del: %s\n", err);
        free(err);
        return retry ? KV_RETRY : 0;
    }
    return 1;
}

static int rdb_range(void *ctx, const char *lo, size_t lolen, const char *hi, size_t hilen,
                     int limit, kv_range_cb cb, void *cbarg)
{
    rdb *r = ctx;
    rocksdb_iterator_t *it = rocksdb_create_iterator(r->db, r->ropt);
    rocksdb_iter_seek(it, lo, lolen);
    int n = 0;
    while (rocksdb_iter_valid(it) && (limit <= 0 || n < limit))
    {
        size_t kl;
        const char *key = rocksdb_iter_key(it, &kl);
        if (kv_keycmp(key, kl, hi, hilen) >= 0) break;
        size_t vl;
        const char *val = rocksdb_iter_value(it, &vl);
        n++;
        if (cb && cb(cbarg, key, kl, val, vl)) break;
        rocksdb_iter_next(it);
    }
    rocksdb_iter_destroy(it);
    return n;
}

/* KB_ROCKSDB_FLAVOR names a RocksDB fork built from the same C API, such as
   frocksdb or forst, so a report says which library actually ran. The forks keep
   RocksDB's version macros, so the flavor carries the fork's own release. */
static const char *rdb_version(void *ctx)
{
    (void)ctx;
#if defined(KB_ROCKSDB_FLAVOR) && defined(KB_ROCKSDB_VERSION)
    return KB_ROCKSDB_FLAVOR " (rocksdb " KB_ROCKSDB_VERSION ")";
#elif defined(KB_ROCKSDB_FLAVOR)
    return KB_ROCKSDB_FLAVOR;
#elif defined(KB_ROCKSDB_VERSION)
    return "rocksdb " KB_ROCKSDB_VERSION;
#else
    return "rocksdb";
#endif
}

static const char *const RDB_PROPS[] = {
    "rocksdb.num-immutable-mem-table",
    "rocksdb.mem-table-flush-pending",
    "rocksdb.compaction-pending",
    "rocksdb.background-errors",
    "rocksdb.cur-size-active-mem-table",
    "rocksdb.cur-size-all-mem-tables",
    "rocksdb.size-all-mem-tables",
    "rocksdb.num-entries-active-mem-table",
    "rocksdb.num-entries-imm-mem-tables",
    "rocksdb.num-deletes-active-mem-table",
    "rocksdb.num-deletes-imm-mem-tables",
    "rocksdb.estimate-num-keys",
    "rocksdb.estimate-table-readers-mem",
    "rocksdb.num-snapshots",
    "rocksdb.num-live-versions",
    "rocksdb.estimate-live-data-size",
    "rocksdb.total-sst-files-size",
    "rocksdb.live-sst-files-size",
    "rocksdb.estimate-pending-compaction-bytes",
    "rocksdb.num-running-compactions",
    "rocksdb.num-running-flushes",
    "rocksdb.actual-delayed-write-rate",
    "rocksdb.is-write-stopped",
    "rocksdb.block-cache-capacity",
    "rocksdb.block-cache-usage",
    "rocksdb.block-cache-pinned-usage",
    "rocksdb.num-immutable-mem-table-flushed",
    "rocksdb.num-running-compaction-sorted-runs",
    "rocksdb.base-level",
    "rocksdb.current-super-version-number",
    "rocksdb.is-file-deletions-enabled",
    "rocksdb.min-log-number-to-keep",
    "rocksdb.min-obsolete-sst-number-to-keep",
    "rocksdb.obsolete-sst-files-size",
    "rocksdb.oldest-snapshot-sequence",
    "rocksdb.oldest-snapshot-time",
    "rocksdb.estimate-oldest-key-time",
    "rocksdb.compaction-abort-count",
    "rocksdb.num-blob-files",
    "rocksdb.total-blob-file-size",
    "rocksdb.live-blob-file-size",
    "rocksdb.live-blob-file-garbage-size",
    "rocksdb.blob-cache-capacity",
    "rocksdb.blob-cache-usage",
    "rocksdb.blob-cache-pinned-usage",
};

static void rdb_stats(void *ctx, kv_stat_cb cb, void *arg)
{
    rdb *r = ctx;
    for (size_t i = 0; i < sizeof RDB_PROPS / sizeof RDB_PROPS[0]; i++)
    {
        uint64_t v;
        if (rocksdb_property_int(r->db, RDB_PROPS[i], &v) == 0)
            cb(arg, RDB_PROPS[i] + 8, (double)v);
    }
}

static void rdb_close(void *ctx)
{
    rdb *r = ctx;
    char *err = NULL;
    rocksdb_close(r->db);
    rocksdb_destroy_db(r->opts, r->path, &err);
    if (err) free(err);
    rocksdb_writeoptions_destroy(r->wopt);
    rocksdb_readoptions_destroy(r->ropt);
    rocksdb_options_destroy(r->opts);
    rmdir(r->path);
    free(r);
}

static const char *rdb_datadir(void *ctx)
{
    return ((rdb *)ctx)->path;
}

kv_backend *rocksdb_backend_open(unsigned seed, const char *data_dir, const kv_options *opts)
{
    (void)seed;
    rdb *r = xmalloc(sizeof *r);

    const char *base = kv_opt_str(opts, "data_dir", data_dir);
    if (!base || !*base) base = "/tmp";
    snprintf(r->path, sizeof r->path, "%s/keybench-rocksdb-XXXXXX", base);
    if (!mkdtemp(r->path))
    {
        perror("mkdtemp");
        abort();
    }

    r->opts = rocksdb_options_create();
    rocksdb_options_set_create_if_missing(r->opts, 1);
    r->disable_wal = 0;

    int nopt = kv_opt_count(opts);
    for (int i = 0; i < nopt; i++)
    {
        const char *k = kv_opt_key_at(opts, i);
        const char *v = kv_opt_val_at(opts, i);
        if (!strcmp(k, "data_dir")) continue;
        /* disable_wal is a write option, not a DB option, so the options string
           parser below cannot take it. A state backend whose durability comes from
           checkpoints writes no WAL, and this lets a run match that. */
        if (!strcmp(k, "disable_wal"))
        {
            r->disable_wal = atoi(v) != 0;
            continue;
        }
        char spec[512];
        snprintf(spec, sizeof spec, "%s=%s", k, v);
        rocksdb_options_t *nw = rocksdb_options_create();
        char *oerr = NULL;
        rocksdb_get_options_from_string(r->opts, spec, nw, &oerr);
        if (oerr)
        {
            fprintf(stderr, "rocksdb: skipping option '%s=%s' (%s)\n", k, v, oerr);
            free(oerr);
            rocksdb_options_destroy(nw);
        }
        else
        {
            rocksdb_options_destroy(r->opts);
            r->opts = nw;
        }
    }

    char *err = NULL;
    r->db = rocksdb_open(r->opts, r->path, &err);
    if (err)
    {
        fprintf(stderr, "rocksdb open: %s\n", err);
        abort();
    }
    r->wopt = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(r->wopt, r->disable_wal);
    r->ropt = rocksdb_readoptions_create();

    kv_backend *be = xmalloc(sizeof *be);
    memset(be, 0, sizeof *be);
    be->name = "rocksdb";
    be->ctx = r;
    be->put = rdb_put;
    be->putbatch = rdb_putbatch;
    be->get = rdb_get;
    be->del = rdb_del;
    be->delbatch = rdb_delbatch;
    be->range = rdb_range;
    be->close = rdb_close;
    be->version = rdb_version;
    be->stats = rdb_stats;
    be->datadir = rdb_datadir;
    return be;
}

KV_REGISTER_BACKEND("rocksdb", rocksdb_backend_open, 1);
