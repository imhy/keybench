#include "kvlua.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bench.h"
#include "hist.h"
#include "lauxlib.h"
#include "lua.h"
#include "lualib.h"
#include "store.h"

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

void stats_reset(op_stats *s)
{
    for (int i = 0; i < OP__COUNT; i++) hist_reset(&s->h[i]);
    s->prim_ops = 0;
    s->get_hits = s->get_misses = s->get_errors = 0;
}

void stats_merge(op_stats *dst, const op_stats *src)
{
    for (int i = 0; i < OP__COUNT; i++) hist_merge(&dst->h[i], &src->h[i]);
    dst->prim_ops += src->prim_ops;
    dst->get_hits += src->get_hits;
    dst->get_misses += src->get_misses;
    dst->get_errors += src->get_errors;
}

const char *op_name(int op)
{
    switch (op)
    {
        case OP_PUT:
            return "put";
        case OP_GET:
            return "get";
        case OP_DEL:
            return "del";
        case OP_RANGE:
            return "range";
        case OP_MGET:
            return "mget";
        case OP_MPUT:
            return "mput";
        case OP_MDEL:
            return "mdel";
        case OP_CKPT:
            return "checkpoint";
        case OP_RESTORE:
            return "restore";
        default:
            return "?";
    }
}

static const char KVLUA_CTX_KEY = 0;

static kvlua_ctx *ctx_from(lua_State *L)
{
    lua_pushlightuserdata(L, (void *)&KVLUA_CTX_KEY);
    lua_gettable(L, LUA_REGISTRYINDEX);
    kvlua_ctx *kc = lua_touserdata(L, -1);
    lua_pop(L, 1);
    return kc;
}

static int l_put(lua_State *L)
{
    size_t klen, vlen;
    const char *k = luaL_checklstring(L, 1, &klen);
    const char *v = luaL_checklstring(L, 2, &vlen);
    kvlua_ctx *kc = ctx_from(L);
    uint64_t t0 = now_ns();
    store_put(kc->store, k, klen, v, vlen);
    hist_record(&kc->stats.h[OP_PUT], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, 1);
    return 0;
}

static int l_get(lua_State *L)
{
    size_t klen;
    char *vp;
    size_t vlen;
    const char *k = luaL_checklstring(L, 1, &klen);
    kvlua_ctx *kc = ctx_from(L);
    uint64_t t0 = now_ns();
    int found = store_get(kc->store, k, klen, &vp, &vlen);
    hist_record(&kc->stats.h[OP_GET], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, 1);
    if (found > 0)
    {
        lua_pushlstring(L, vp, vlen);
        free(vp);
        kc->stats.get_hits++;
    }
    else
    {
        lua_pushnil(L);
        if (found < 0)
            kc->stats.get_errors++;
        else
            kc->stats.get_misses++;
    }
    return 1;
}

static int l_del(lua_State *L)
{
    size_t klen;
    const char *k = luaL_checklstring(L, 1, &klen);
    kvlua_ctx *kc = ctx_from(L);
    uint64_t t0 = now_ns();
    int existed = store_del(kc->store, k, klen);
    hist_record(&kc->stats.h[OP_DEL], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, 1);
    lua_pushboolean(L, existed);
    return 1;
}

typedef struct
{
    char *k;
    size_t klen;
    char *v;
    size_t vlen;
} pair;
typedef struct
{
    pair *a;
    int n, cap;
} collector;

static int collect_cb(void *arg, const char *k, size_t klen, const char *v, size_t vlen)
{
    collector *c = arg;
    if (c->n == c->cap)
    {
        c->cap = c->cap ? c->cap * 2 : 16;
        c->a = realloc(c->a, (size_t)c->cap * sizeof(pair));
        if (!c->a)
        {
            fprintf(stderr, "keybench: out of memory\n");
            abort();
        }
    }
    pair *p = &c->a[c->n++];
    p->klen = klen;
    p->k = xmalloc(klen);
    memcpy(p->k, k, klen);
    p->vlen = vlen;
    p->v = xmalloc(vlen);
    memcpy(p->v, v, vlen);
    return 0;
}

static int l_range(lua_State *L)
{
    size_t lolen, hilen;
    const char *lo = luaL_checklstring(L, 1, &lolen);
    const char *hi = luaL_checklstring(L, 2, &hilen);
    int limit = (int)luaL_optinteger(L, 3, 0);
    kvlua_ctx *kc = ctx_from(L);

    collector c = {0};
    uint64_t t0 = now_ns();
    store_range(kc->store, lo, lolen, hi, hilen, limit, collect_cb, &c);
    hist_record(&kc->stats.h[OP_RANGE], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, 1);

    lua_createtable(L, c.n, 0);
    for (int i = 0; i < c.n; i++)
    {
        lua_createtable(L, 0, 2);
        lua_pushlstring(L, c.a[i].k, c.a[i].klen);
        lua_setfield(L, -2, "key");
        lua_pushlstring(L, c.a[i].v, c.a[i].vlen);
        lua_setfield(L, -2, "val");
        lua_rawseti(L, -2, i + 1);
        free(c.a[i].k);
        free(c.a[i].v);
    }
    free(c.a);
    return 1;
}

typedef struct
{
    lua_State *L;
    int fn_idx;
    int err;
} scanstate;

static int scan_cb(void *arg, const char *k, size_t klen, const char *v, size_t vlen)
{
    scanstate *st = arg;
    lua_State *L = st->L;
    lua_pushvalue(L, st->fn_idx);
    lua_pushlstring(L, k, klen);
    lua_pushlstring(L, v, vlen);
    if (lua_pcall(L, 2, 1, 0) != LUA_OK)
    {
        st->err = 1;
        return 1;
    }
    int stop = lua_toboolean(L, -1);
    lua_pop(L, 1);
    return stop;
}

static int l_scan(lua_State *L)
{
    size_t lolen, hilen;
    const char *lo = luaL_checklstring(L, 1, &lolen);
    const char *hi = luaL_checklstring(L, 2, &hilen);
    luaL_checktype(L, 3, LUA_TFUNCTION);
    int limit = (int)luaL_optinteger(L, 4, 0);
    kvlua_ctx *kc = ctx_from(L);

    scanstate st = {L, 3, 0};
    uint64_t t0 = now_ns();
    int n = store_range(kc->store, lo, lolen, hi, hilen, limit, scan_cb, &st);
    hist_record(&kc->stats.h[OP_RANGE], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, 1);

    if (st.err) return lua_error(L);
    lua_pushinteger(L, n);
    return 1;
}

static int l_mget(lua_State *L)
{
    luaL_checktype(L, 1, LUA_TTABLE);
    kvlua_ctx *kc = ctx_from(L);
    int n = (int)luaL_len(L, 1);
    if (n < 0) n = 0;

    const char **kp = lua_newuserdatauv(L, (size_t)n * sizeof *kp, 0);
    size_t *kl = lua_newuserdatauv(L, (size_t)n * sizeof *kl, 0);
    char **vp = lua_newuserdatauv(L, (size_t)n * sizeof *vp, 0);
    size_t *vl = lua_newuserdatauv(L, (size_t)n * sizeof *vl, 0);
    int *found = lua_newuserdatauv(L, (size_t)n * sizeof *found, 0);
    for (int i = 0; i < n; i++)
    {
        lua_geti(L, 1, i + 1);
        kp[i] = luaL_checklstring(L, -1, &kl[i]);
        lua_pop(L, 1);
    }

    uint64_t t0 = now_ns();
    for (int i = 0; i < n; i++) found[i] = store_get(kc->store, kp[i], kl[i], &vp[i], &vl[i]);
    hist_record(&kc->stats.h[OP_MGET], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, (uint64_t)n);

    lua_createtable(L, n, 0);
    for (int i = 0; i < n; i++)
    {
        if (found[i] > 0)
        {
            lua_pushlstring(L, vp[i], vl[i]);
            kc->stats.get_hits++;
            free(vp[i]);
        }
        else
        {
            lua_pushnil(L);
            if (found[i] < 0)
                kc->stats.get_errors++;
            else
                kc->stats.get_misses++;
        }
        lua_rawseti(L, -2, i + 1);
    }
    return 1;
}

static int l_mput(lua_State *L)
{
    luaL_checktype(L, 1, LUA_TTABLE);
    kvlua_ctx *kc = ctx_from(L);
    int n = (int)luaL_len(L, 1);
    if (n < 0) n = 0;

    const char **kp = lua_newuserdatauv(L, (size_t)n * sizeof *kp, 0);
    size_t *kl = lua_newuserdatauv(L, (size_t)n * sizeof *kl, 0);
    const char **vp = lua_newuserdatauv(L, (size_t)n * sizeof *vp, 0);
    size_t *vl = lua_newuserdatauv(L, (size_t)n * sizeof *vl, 0);
    for (int i = 0; i < n; i++)
    {
        lua_geti(L, 1, i + 1);
        luaL_checktype(L, -1, LUA_TTABLE);
        lua_getfield(L, -1, "key");
        kp[i] = luaL_checklstring(L, -1, &kl[i]);
        lua_pop(L, 1);
        lua_getfield(L, -1, "val");
        vp[i] = luaL_checklstring(L, -1, &vl[i]);
        lua_pop(L, 1);
        lua_pop(L, 1);
    }
    uint64_t t0 = now_ns();
    store_putbatch(kc->store, kp, kl, vp, vl, n);
    hist_record(&kc->stats.h[OP_MPUT], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, (uint64_t)n);
    return 0;
}

static int l_mdel(lua_State *L)
{
    luaL_checktype(L, 1, LUA_TTABLE);
    kvlua_ctx *kc = ctx_from(L);
    int n = (int)luaL_len(L, 1);
    if (n < 0) n = 0;

    const char **kp = lua_newuserdatauv(L, (size_t)n * sizeof *kp, 0);
    size_t *kl = lua_newuserdatauv(L, (size_t)n * sizeof *kl, 0);
    for (int i = 0; i < n; i++)
    {
        lua_geti(L, 1, i + 1);
        kp[i] = luaL_checklstring(L, -1, &kl[i]);
        lua_pop(L, 1);
    }
    uint64_t t0 = now_ns();
    store_delbatch(kc->store, kp, kl, n);
    hist_record(&kc->stats.h[OP_MDEL], now_ns() - t0);
    KV_RELAXED_ADD(kc->stats.prim_ops, (uint64_t)n);

    lua_pushinteger(L, n);
    return 1;
}

/* kv.checkpoint() takes the next checkpoint and returns its id and the bytes it
   wrote, or nil when the engine has no checkpoints or the checkpoint failed. */
static int l_checkpoint(lua_State *L)
{
    kvlua_ctx *kc = ctx_from(L);
    uint64_t bytes = 0;
    uint64_t t0 = now_ns();
    uint64_t id = store_checkpoint(kc->store, &bytes);
    uint64_t dt = now_ns() - t0;
    if (!id)
    {
        lua_pushnil(L);
        return 1;
    }
    hist_record(&kc->stats.h[OP_CKPT], dt);
    lua_pushinteger(L, (lua_Integer)id);
    lua_pushinteger(L, (lua_Integer)bytes);
    return 2;
}

/* kv.restore(id) replaces the store with checkpoint id and returns true, or false
   when the engine cannot restore. It must not run beside other operations. */
static int l_restore(lua_State *L)
{
    lua_Integer id = luaL_checkinteger(L, 1);
    kvlua_ctx *kc = ctx_from(L);
    uint64_t t0 = now_ns();
    int rc = store_restore(kc->store, (uint64_t)id);
    uint64_t dt = now_ns() - t0;
    if (rc == 0) hist_record(&kc->stats.h[OP_RESTORE], dt);
    lua_pushboolean(L, rc == 0);
    return 1;
}

/* kv.now_ns() is the harness's monotonic clock, for timing inside a workload. */
static int l_now_ns(lua_State *L)
{
    lua_pushinteger(L, (lua_Integer)now_ns());
    return 1;
}

static const luaL_Reg kv_funcs[] = {{"put", l_put},     {"get", l_get},   {"del", l_del},
                                    {"range", l_range}, {"scan", l_scan}, {"mget", l_mget},
                                    {"mput", l_mput},   {"mdel", l_mdel}, {"checkpoint", l_checkpoint},
                                    {"restore", l_restore}, {"now_ns", l_now_ns}, {NULL, NULL}};

void kvlua_register(lua_State *L, kvlua_ctx *kc)
{
    lua_pushlightuserdata(L, (void *)&KVLUA_CTX_KEY);
    lua_pushlightuserdata(L, (void *)kc);
    lua_settable(L, LUA_REGISTRYINDEX);

    luaL_newlib(L, kv_funcs);
    lua_setglobal(L, "kv");
}
