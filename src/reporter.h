#ifndef REPORTER_H
#define REPORTER_H

#include <stdint.h>
#include <stdio.h>

#define REPORT_MAX_OPS   10
#define REPORT_MAX_SINKS 8

typedef struct
{
    const char *op;
    uint64_t count, p50, p99, p999, max;
} report_op;

typedef struct
{
    const char *workload;
    const char *engine;
    int threads, repeat;
    /* When the workload sweeps a parameter of its own, sweep_param names it and
       sweep_value is the current point. sweep_param is NULL and sweep_value is 1
       when the workload sweeps nothing. */
    const char *sweep_param;
    long sweep_value;
    report_op ops[REPORT_MAX_OPS];
    int n_ops;
    int has_hit;
    double hit_rate;
    int has_err;
    double err_rate;
    double wu_med, wu_lo, wu_hi;
    double ops_med, ops_lo, ops_hi;
    uint64_t overall_p50, overall_p99;
} report_point;

typedef struct
{
    const char *engine;
    unsigned seed;
    int repeat;
    const char *threads_list;
    int nworkloads;
} report_run;

typedef struct
{
    const char *name;
    double value;
} sample_metric;
typedef struct
{
    const char *workload;
    const char *engine;
    int threads;
    const char *sweep_param;
    long sweep_value;
    /* "seed" while the untimed load fills the store, "run" during the measured
       phase, so the two timelines stay separable. */
    const char *phase;
    double elapsed_s;
    const sample_metric *m;
    int nm;
} report_sample;

typedef struct
{
    const char *key, *value;
} probe_kv;
typedef struct
{
    const char *probe;
    const probe_kv *kv;
    int n;
} report_probe;

typedef struct reporter reporter;
struct reporter
{
    const char *name;
    void *ctx;
    FILE *out;
    void (*on_run)(reporter *, const report_run *);
    void (*on_probe)(reporter *, const report_probe *);
    void (*on_point)(reporter *, const report_point *);
    void (*on_sample)(reporter *, const report_sample *);
    void (*on_close)(reporter *);
};

typedef struct
{
    reporter *r[REPORT_MAX_SINKS];
    int n;
} reporter_set;

int reporters_parse(const char *spec, reporter_set *set);
void reporters_run(reporter_set *set, const report_run *r);
void reporters_probe(reporter_set *set, const report_probe *p);
void reporters_point(reporter_set *set, const report_point *p);
void reporters_sample(reporter_set *set, const report_sample *s);
void reporters_close(reporter_set *set);

int reporters_want_samples(const reporter_set *set);

int reporters_want_probe(const reporter_set *set);

#endif
