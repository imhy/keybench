#ifndef KVLUA_H
#define KVLUA_H

#include "bench.h"
#include "hist.h"
#include "store.h"

struct lua_State;

enum
{
    OP_PUT,
    OP_GET,
    OP_DEL,
    OP_RANGE,
    OP_MGET,
    OP_MPUT,
    OP_MDEL,
    /* Not data operations: kept out of the primitive-op count and the overall
       latency percentiles, reported as their own rows. */
    OP_CKPT,
    OP_RESTORE,
    OP__COUNT
};

typedef struct
{
    hist h[OP__COUNT];
    uint64_t prim_ops;
    uint64_t get_hits;
    uint64_t get_misses;
    uint64_t get_errors;
} op_stats;

typedef struct
{
    op_stats stats;
    kv_store *store;
} kvlua_ctx;

void stats_reset(op_stats *s);
void stats_merge(op_stats *dst, const op_stats *src);
const char *op_name(int op);

void kvlua_register(struct lua_State *L, kvlua_ctx *kc);

#endif
