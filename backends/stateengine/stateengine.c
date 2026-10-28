#define _GNU_SOURCE
#include <errno.h>
#include <ftw.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <stateengine.h>

#include "backend_registry.h"
#include "bench.h"

/* StateEngine through libstateengine.so's C ABI (stateengine-flink src/ffi.rs), the
   same handle layer the Flink bridge drives: owner lock, pacer and bounded
   maintenance all run inside the calls.

   A Flink TaskManager runs one engine per subtask, each owning a range of key
   groups. This backend does the same: it opens `shards` engines and routes every
   key to one of them. With routing = hash (the default, so any workload runs) a
   key hashes to a shard and every engine runs engine-direct, without an owned
   key-group range. With routing = prefix a key starts with its 2-byte big-endian
   key group, as the Flink bridge writes it, and the key group picks the shard
   through Flink's KeyGroupRangeAssignment; each engine owns its range, as in
   Flink. Workloads that model Flink state use prefix.

   An engine serves one writer at a time and refuses writes while one of its scan
   cursors is open, so each shard has a mutex held across every call and across a
   whole scan. That mirrors Flink, where one task thread owns each engine. */

#define SE_MAX_SHARDS 64
#define SE_DEFAULT_SHARDS 4
#define SE_DEFAULT_MAX_PARALLELISM 128
#define SE_KEYGROUP_PREFIX 2

typedef struct
{
    stateengine_handle h;
    pthread_mutex_t mu;
    uint32_t kg_start, kg_end; /* inclusive, routing = prefix only */
} se_shard;

typedef struct
{
    se_shard shard[SE_MAX_SHARDS];
    int n;
    int route_prefix;
    uint32_t max_par;
    char path[512];
} se_ctx;

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

static void *xrealloc(void *p, size_t n)
{
    p = realloc(p, n ? n : 1);
    if (!p)
    {
        fprintf(stderr, "keybench: out of memory\n");
        abort();
    }
    return p;
}

static const char *se_status_name(int32_t rc)
{
    switch (rc)
    {
        case STATEENGINE_OK: return "ok";
        case STATEENGINE_INVALID_CONFIG: return "invalid config";
        case STATEENGINE_IO: return "io";
        case STATEENGINE_CORRUPTION: return "corruption";
        case STATEENGINE_NOT_FOUND: return "not found";
        case STATEENGINE_BACKPRESSURE: return "backpressure";
        case STATEENGINE_INVALID_HANDLE: return "invalid handle";
        case STATEENGINE_INVALID_ARGUMENT: return "invalid argument";
        case STATEENGINE_UNIMPLEMENTED: return "unimplemented";
        case STATEENGINE_PANIC: return "panic";
        default: return "unknown status";
    }
}

/* Report the first few failures, then quietly: a failing engine would otherwise
   flood the console with one line per operation. */
static int se_fail(const char *what, int32_t rc)
{
    static int reported;
    if (__atomic_fetch_add(&reported, 1, __ATOMIC_RELAXED) < 8)
        fprintf(stderr, "stateengine %s: %s (status %d)\n", what, se_status_name(rc), rc);
    return -1;
}

/* Write paths treat backpressure as transient, like the RocksDB backend's stalls. */
static int se_write_rc(const char *what, int32_t rc)
{
    if (rc == STATEENGINE_OK) return 0;
    if (rc == STATEENGINE_BACKPRESSURE) return KV_RETRY;
    return se_fail(what, rc);
}

static uint64_t fnv1a(const char *k, size_t n)
{
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < n; i++)
    {
        h ^= (unsigned char)k[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static uint32_t se_keygroup(const char *k, size_t n)
{
    unsigned hi = n > 0 ? (unsigned char)k[0] : 0;
    unsigned lo = n > 1 ? (unsigned char)k[1] : 0;
    return (uint32_t)(hi << 8 | lo);
}

/* KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup. */
static int se_shard_of_keygroup(const se_ctx *c, uint32_t kg)
{
    return (int)((uint64_t)kg * (uint64_t)c->n / c->max_par);
}

static int se_route(const se_ctx *c, const char *k, size_t klen)
{
    if (!c->route_prefix) return (int)(fnv1a(k, klen) % (uint64_t)c->n);
    uint32_t kg = se_keygroup(k, klen);
    if (klen < SE_KEYGROUP_PREFIX || kg >= c->max_par) return -1;
    return se_shard_of_keygroup(c, kg);
}

static int se_put(void *ctx, const char *k, size_t klen, const char *v, size_t vlen)
{
    se_ctx *c = ctx;
    int s = se_route(c, k, klen);
    if (s < 0) return se_fail("put: key has no valid key-group prefix", STATEENGINE_INVALID_ARGUMENT);
    pthread_mutex_lock(&c->shard[s].mu);
    int32_t rc = stateengine_engine_put(c->shard[s].h, (const uint8_t *)k, klen, (const uint8_t *)v, vlen);
    pthread_mutex_unlock(&c->shard[s].mu);
    return se_write_rc("put", rc);
}

static int se_del(void *ctx, const char *k, size_t klen)
{
    se_ctx *c = ctx;
    int s = se_route(c, k, klen);
    if (s < 0) return se_fail("del: key has no valid key-group prefix", STATEENGINE_INVALID_ARGUMENT);
    pthread_mutex_lock(&c->shard[s].mu);
    int32_t rc = stateengine_engine_delete(c->shard[s].h, (const uint8_t *)k, klen);
    pthread_mutex_unlock(&c->shard[s].mu);
    int r = se_write_rc("del", rc);
    return r == 0 ? 1 : r;
}

static __thread uint8_t *tl_buf;
static __thread size_t tl_cap;

static int se_get(void *ctx, const char *k, size_t klen, const char **vp, size_t *vlen)
{
    se_ctx *c = ctx;
    int s = se_route(c, k, klen);
    if (s < 0) return KV_ERR;
    if (!tl_buf)
    {
        tl_cap = 4096;
        tl_buf = xmalloc(tl_cap);
    }
    size_t len = 0;
    uint8_t found = 0;
    int32_t rc;
    pthread_mutex_lock(&c->shard[s].mu);
    for (;;)
    {
        rc = stateengine_engine_get(c->shard[s].h, (const uint8_t *)k, klen, tl_buf, tl_cap, &len, &found);
        if (rc != STATEENGINE_OK || !found || len <= tl_cap) break;
        tl_cap = len;
        tl_buf = xrealloc(tl_buf, tl_cap);
    }
    pthread_mutex_unlock(&c->shard[s].mu);
    if (rc != STATEENGINE_OK)
    {
        se_fail("get", rc);
        return KV_ERR;
    }
    if (!found) return 0;
    char *copy = xmalloc(len);
    memcpy(copy, tl_buf, len);
    *vp = copy;
    *vlen = len;
    return 1;
}

/* One open cursor of a range, with its current pair. */
typedef struct
{
    stateengine_scan scan;
    uint8_t *k, *v;
    size_t kcap, vcap, klen, vlen;
    int has;
} se_cursor;

static int se_cursor_next(se_cursor *cur)
{
    for (;;)
    {
        uint8_t has = 0;
        size_t kl = 0, vl = 0;
        int32_t rc = stateengine_engine_scan_next(cur->scan, cur->k, cur->kcap, &kl, cur->v, cur->vcap, &vl, &has);
        if (rc != STATEENGINE_OK) return se_fail("scan_next", rc);
        if (!has)
        {
            cur->has = 0;
            return 0;
        }
        if (kl <= cur->kcap && vl <= cur->vcap)
        {
            cur->klen = kl;
            cur->vlen = vl;
            cur->has = 1;
            return 0;
        }
        /* The pair did not fit: nothing was written and the cursor did not move. */
        if (kl > cur->kcap)
        {
            cur->kcap = kl;
            cur->k = xrealloc(cur->k, cur->kcap);
        }
        if (vl > cur->vcap)
        {
            cur->vcap = vl;
            cur->v = xrealloc(cur->v, cur->vcap);
        }
    }
}

static int se_range(void *ctx, const char *lo, size_t lolen, const char *hi, size_t hilen, int limit,
                    kv_range_cb cb, void *cbarg)
{
    se_ctx *c = ctx;
    int first = 0, last = c->n - 1;
    if (c->route_prefix)
    {
        uint32_t kg_lo = se_keygroup(lo, lolen), kg_hi = se_keygroup(hi, hilen);
        if (kg_lo >= c->max_par) return 0;
        if (kg_hi >= c->max_par) kg_hi = c->max_par - 1;
        first = se_shard_of_keygroup(c, kg_lo);
        last = se_shard_of_keygroup(c, kg_hi);
    }
    int m = last - first + 1;
    se_cursor cur[SE_MAX_SHARDS];
    memset(cur, 0, sizeof cur);

    /* Lock in ascending shard order, so two multi-shard scans cannot deadlock. */
    for (int i = 0; i < m; i++) pthread_mutex_lock(&c->shard[first + i].mu);
    int n = 0, err = 0, opened = 0;
    for (int i = 0; i < m && !err; i++)
    {
        int32_t rc = stateengine_engine_scan(c->shard[first + i].h, (const uint8_t *)lo, lolen, (const uint8_t *)hi, hilen, &cur[i].scan);
        if (rc != STATEENGINE_OK)
        {
            err = se_fail("scan", rc);
            break;
        }
        opened = i + 1;
        cur[i].kcap = 256;
        cur[i].vcap = 4096;
        cur[i].k = xmalloc(cur[i].kcap);
        cur[i].v = xmalloc(cur[i].vcap);
        if (se_cursor_next(&cur[i]) < 0) err = -1;
    }
    /* Shards own disjoint key groups (or disjoint hash buckets), so a merge by key
       alone yields every pair exactly once, in order. */
    while (!err && (limit <= 0 || n < limit))
    {
        int best = -1;
        for (int i = 0; i < opened; i++)
            if (cur[i].has && (best < 0 || kv_keycmp((const char *)cur[i].k, cur[i].klen,
                                                     (const char *)cur[best].k, cur[best].klen) < 0))
                best = i;
        if (best < 0) break;
        n++;
        if (cb && cb(cbarg, (const char *)cur[best].k, cur[best].klen, (const char *)cur[best].v, cur[best].vlen)) break;
        if (se_cursor_next(&cur[best]) < 0) err = -1;
    }
    for (int i = 0; i < opened; i++)
    {
        stateengine_engine_scan_close(cur[i].scan);
        free(cur[i].k);
        free(cur[i].v);
    }
    for (int i = m - 1; i >= 0; i--) pthread_mutex_unlock(&c->shard[first + i].mu);
    return err ? -1 : n;
}

/* The stats JSON is one flat object of "name":number members plus nested sections.
   Report every numeric member, summed across shards, under its bare name. */
typedef struct
{
    char name[96];
    double value;
} se_stat;

static void se_collect(const char *json, se_stat **stats, int *nstats, int *cap)
{
    const char *p = json;
    while ((p = strchr(p, '"')))
    {
        const char *q = strchr(p + 1, '"');
        if (!q) break;
        const char *colon = q + 1;
        if (*colon != ':' || (size_t)(q - p - 1) >= sizeof((*stats)->name))
        {
            p = q + 1;
            continue;
        }
        char *end;
        double v = strtod(colon + 1, &end);
        if (end == colon + 1)
        {
            p = q + 1;
            continue;
        }
        char name[96];
        memcpy(name, p + 1, (size_t)(q - p - 1));
        name[q - p - 1] = 0;
        int i = 0;
        while (i < *nstats && strcmp((*stats)[i].name, name)) i++;
        if (i == *nstats)
        {
            if (*nstats == *cap)
            {
                *cap = *cap ? *cap * 2 : 256;
                *stats = xrealloc(*stats, (size_t)*cap * sizeof **stats);
            }
            snprintf((*stats)[i].name, sizeof (*stats)[i].name, "%s", name);
            (*stats)[i].value = 0;
            (*nstats)++;
        }
        (*stats)[i].value += v;
        p = end;
    }
}

static void se_stats(void *ctx, kv_stat_cb cb, void *arg)
{
    se_ctx *c = ctx;
    se_stat *stats = NULL;
    int nstats = 0, cap = 0;
    size_t bcap = 1 << 16;
    uint8_t *buf = xmalloc(bcap);
    for (int s = 0; s < c->n; s++)
    {
        size_t len = 0;
        int32_t rc;
        while ((rc = stateengine_engine_stats_json(c->shard[s].h, buf, bcap - 1, &len)) == STATEENGINE_OK && len >= bcap)
        {
            bcap = len + 1;
            buf = xrealloc(buf, bcap);
        }
        if (rc != STATEENGINE_OK) continue;
        buf[len] = 0;
        se_collect((const char *)buf, &stats, &nstats, &cap);
    }
    for (int i = 0; i < nstats; i++) cb(arg, stats[i].name, stats[i].value);
    free(stats);
    free(buf);
}

static const char *se_version(void *ctx)
{
    (void)ctx;
#ifdef KB_STATEENGINE_VERSION
    return "stateengine " KB_STATEENGINE_VERSION;
#else
    return "stateengine";
#endif
}

static const char *se_datadir(void *ctx)
{
    return ((se_ctx *)ctx)->path;
}

static int se_rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

static void se_close(void *ctx)
{
    se_ctx *c = ctx;
    for (int s = 0; s < c->n; s++)
    {
        int32_t rc = stateengine_engine_close(c->shard[s].h);
        if (rc != STATEENGINE_OK) se_fail("close", rc);
        pthread_mutex_destroy(&c->shard[s].mu);
    }
    nftw(c->path, se_rm_entry, 16, FTW_DEPTH | FTW_PHYS);
    free(c);
}

static void se_mkdir(const char *path)
{
    if (mkdir(path, 0755) && errno != EEXIST)
    {
        perror(path);
        abort();
    }
}

/* Append ,"name":value for an option, or its Flink default when the option is
   unset. The defaults are those FlinkStateBackend and MaintenanceSettings send when
   nothing is configured, so an unconfigured engine here opens as one in Flink. */
static int se_member(char *dst, size_t cap, size_t at, const kv_options *o, const char *opt,
                     const char *json, const char *dflt, int quote)
{
    const char *v = kv_opt_str(o, opt, dflt);
    if (!v) return (int)at;
    int w = quote ? snprintf(dst + at, cap - at, ",\"%s\":\"%s\"", json, v)
                  : snprintf(dst + at, cap - at, ",\"%s\":%s", json, v);
    return (int)(at + (size_t)w);
}

static const struct
{
    const char *opt, *json, *dflt;
    int quote;
} SE_DEFAULTS[] = {
    {"ttl_mode", "ttl_mode", "per_key", 1},
    {"ttl_visibility", "ttl_visibility", "never_return_expired", 1},
    {"checkpoint_interval_seconds", "checkpoint_interval_seconds", "10", 0},
    {"rescale_max_referenced_sources", "rescale_max_referenced_sources", "16", 0},
    {"rescale_lock_attribution", "rescale_lock_attribution", "false", 0},
    {"checkpoint_accounting_enabled", "checkpoint_accounting_enabled", "false", 0},
    {"maintenance_enabled", "maintenance_enabled", "true", 0},
    {"maintenance_step_max_units", "maintenance_step_max_units", "4096", 0},
    {"maintenance_step_max_bytes", "maintenance_step_max_bytes", "1048576", 0},
    {"maintenance_step_max_duration_nanos", "maintenance_step_max_duration_nanos", "2000000", 0},
    {"maintenance_interval_ops", "maintenance_interval_ops", "1024", 0},
    {"maintenance_min_interval_nanos", "maintenance_min_interval_nanos", "10000000", 0},
    {"maintenance_checkpoint_complete_budget_nanos", "maintenance_checkpoint_complete_budget_nanos", "20000000", 0},
    {"maintenance_max_queued_edit_units", "maintenance_max_queued_edit_units", "4096", 0},
    {"maintenance_pressure_ops_per_step", "maintenance_pressure_ops_per_step", "1024", 0},
    {"maintenance_pause_during_upload", "maintenance_pause_during_upload", "false", 0},
    {"maintenance_background_io", "maintenance_background_io", "true", 0},
    {"checkpoint_background_base", "checkpoint_background_base", "true", 0},
    {"maintenance_idle_delay_nanos", "maintenance_idle_delay_nanos", "100000000", 0},
    {"maintenance_idle_duty_cycle", "maintenance_idle_duty_cycle", "50", 0},
    {"maintenance_kinds", "maintenance_kinds", "merge,defrag,compaction", 1},
    /* Unset in Flink unless configured; the engine applies its own default. */
    {"memory_process_budget_bytes", "memory_process_budget_bytes", NULL, 0},
    {"art_memory_budget_bytes", "art_memory_budget_bytes", NULL, 0},
    {"art_process_memory_budget_bytes", "art_process_memory_budget_bytes", NULL, 0},
    {"spill_shard_cache_fraction", "spill_shard_cache_fraction", NULL, 0},
    {"kv_separation_threshold", "kv_separation_threshold", NULL, 0},
    {"ring_buffer_ordered_index", "ring_buffer_ordered_index", NULL, 1},
};

kv_backend *stateengine_backend_open(unsigned seed, const char *data_dir, const kv_options *opts)
{
    (void)seed;
    se_ctx *c = xmalloc(sizeof *c);
    memset(c, 0, sizeof *c);
    c->n = (int)kv_opt_int(opts, "shards", SE_DEFAULT_SHARDS);
    c->max_par = (uint32_t)kv_opt_int(opts, "max_parallelism", SE_DEFAULT_MAX_PARALLELISM);
    const char *routing = kv_opt_str(opts, "routing", "hash");
    c->route_prefix = strcmp(routing, "prefix") == 0;
    if (c->n < 1 || c->n > SE_MAX_SHARDS || (c->route_prefix && (uint32_t)c->n > c->max_par) ||
        c->max_par < 1 || c->max_par > 32768)
    {
        fprintf(stderr, "stateengine: shards must be 1..%d and at most max_parallelism (1..32768)\n", SE_MAX_SHARDS);
        abort();
    }

    const char *base = kv_opt_str(opts, "data_dir", data_dir);
    if (!base || !*base) base = "/tmp";
    snprintf(c->path, sizeof c->path, "%s/keybench-stateengine-XXXXXX", base);
    if (!mkdtemp(c->path))
    {
        perror("mkdtemp");
        abort();
    }
    /* A single process budget for all shards, as in one TaskManager: the engines
       share its ART pool through the bridge's memory exchange. */
    const char *budget_mb = kv_opt_str(opts, "memory_budget_mb", NULL);

    for (int s = 0; s < c->n; s++)
    {
        se_shard *sh = &c->shard[s];
        pthread_mutex_init(&sh->mu, NULL);
        char dir[700], pages[760], vlog[760], ckpt[760];
        snprintf(dir, sizeof dir, "%s/shard-%02d", c->path, s);
        snprintf(pages, sizeof pages, "%s/pages", dir);
        snprintf(vlog, sizeof vlog, "%s/vlog", dir);
        snprintf(ckpt, sizeof ckpt, "%s/checkpoints", dir);
        se_mkdir(dir);
        se_mkdir(pages);
        se_mkdir(vlog);
        se_mkdir(ckpt);

        char cfg[8192];
        int at = snprintf(cfg, sizeof cfg, "{\"page_dir\":\"%s\",\"value_log_dir\":\"%s\",\"checkpoint_dir\":\"%s\"",
                          pages, vlog, ckpt);
        if (c->route_prefix)
        {
            /* KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex; end inclusive. */
            sh->kg_start = (uint32_t)(((uint64_t)s * c->max_par + (uint64_t)c->n - 1) / (uint64_t)c->n);
            sh->kg_end = (uint32_t)((((uint64_t)s + 1) * c->max_par - 1) / (uint64_t)c->n);
            at += snprintf(cfg + at, sizeof cfg - (size_t)at, ",\"owned_key_group_start\":%u,\"owned_key_group_end\":%u",
                           sh->kg_start, sh->kg_end);
        }
        for (size_t i = 0; i < sizeof SE_DEFAULTS / sizeof SE_DEFAULTS[0]; i++)
            at = se_member(cfg, sizeof cfg, (size_t)at, opts, SE_DEFAULTS[i].opt, SE_DEFAULTS[i].json,
                           SE_DEFAULTS[i].dflt, SE_DEFAULTS[i].quote);
        if (budget_mb)
            at += snprintf(cfg + at, sizeof cfg - (size_t)at, ",\"memory_process_budget_bytes\":%llu",
                           strtoull(budget_mb, NULL, 10) << 20);
        /* Raw extra members for knobs not listed above, e.g. config = "merge_target_pages":8 */
        const char *extra = kv_opt_str(opts, "config", NULL);
        if (extra && *extra) at += snprintf(cfg + at, sizeof cfg - (size_t)at, ",%s", extra);
        if (at + 2 >= (int)sizeof cfg)
        {
            fprintf(stderr, "stateengine: open config too long\n");
            abort();
        }
        cfg[at++] = '}';
        cfg[at] = 0;
        if (kv_opt_int(opts, "print_config", 0) && s == 0) fprintf(stderr, "stateengine open config: %s\n", cfg);

        int32_t rc = stateengine_engine_create((const uint8_t *)cfg, (size_t)at, &sh->h);
        if (rc != STATEENGINE_OK)
        {
            fprintf(stderr, "stateengine open shard %d: %s (status %d)\nconfig: %s\n", s, se_status_name(rc), rc, cfg);
            abort();
        }
    }

    kv_backend *be = xmalloc(sizeof *be);
    memset(be, 0, sizeof *be);
    be->name = "stateengine";
    be->ctx = c;
    be->put = se_put;
    be->get = se_get;
    be->del = se_del;
    be->range = se_range;
    be->close = se_close;
    be->version = se_version;
    be->stats = se_stats;
    be->datadir = se_datadir;
    return be;
}

KV_REGISTER_BACKEND("stateengine", stateengine_backend_open, 1);
