#define _GNU_SOURCE
#include "dirdelta.h"

#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static __thread dir_snapshot *walk_into;

static int walk_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)ftw;
    if (flag != FTW_F) return 0;
    dir_snapshot *s = walk_into;
    if (s->n == s->cap)
    {
        s->cap = s->cap ? s->cap * 2 : 256;
        s->path = realloc(s->path, (size_t)s->cap * sizeof *s->path);
        s->size = realloc(s->size, (size_t)s->cap * sizeof *s->size);
        if (!s->path || !s->size) abort();
    }
    s->path[s->n] = strdup(path);
    s->size[s->n] = (uint64_t)sb->st_size;
    s->n++;
    return 0;
}

typedef struct
{
    char *path;
    uint64_t size;
} dir_entry;

static int by_path(const void *a, const void *b)
{
    return strcmp(((const dir_entry *)a)->path, ((const dir_entry *)b)->path);
}

void dir_snapshot_take(dir_snapshot *s, const char *root)
{
    memset(s, 0, sizeof *s);
    walk_into = s;
    nftw(root, walk_entry, 16, FTW_PHYS);
    walk_into = NULL;
    if (s->n < 2) return;
    dir_entry *e = malloc((size_t)s->n * sizeof *e);
    if (!e) abort();
    for (int i = 0; i < s->n; i++) e[i] = (dir_entry){s->path[i], s->size[i]};
    qsort(e, (size_t)s->n, sizeof *e, by_path);
    for (int i = 0; i < s->n; i++)
    {
        s->path[i] = e[i].path;
        s->size[i] = e[i].size;
    }
    free(e);
}

uint64_t dir_snapshot_added_bytes(const dir_snapshot *before, const dir_snapshot *after)
{
    uint64_t added = 0;
    int i = 0, j = 0;
    while (j < after->n)
    {
        int c = i < before->n ? strcmp(before->path[i], after->path[j]) : 1;
        if (c < 0)
            i++;
        else if (c == 0)
        {
            if (after->size[j] > before->size[i]) added += after->size[j] - before->size[i];
            i++;
            j++;
        }
        else
        {
            added += after->size[j];
            j++;
        }
    }
    return added;
}

void dir_snapshot_free(dir_snapshot *s)
{
    for (int i = 0; i < s->n; i++) free(s->path[i]);
    free(s->path);
    free(s->size);
    memset(s, 0, sizeof *s);
}

static int rm_entry(const char *path, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb;
    (void)flag;
    (void)ftw;
    return remove(path);
}

void dir_remove_tree(const char *root)
{
    nftw(root, rm_entry, 16, FTW_DEPTH | FTW_PHYS);
}

int64_t file_copy(const char *src, const char *dst)
{
    int in = open(src, O_RDONLY | O_CLOEXEC);
    if (in < 0) return -1;
    int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (out < 0)
    {
        close(in);
        return -1;
    }
    static __thread char *buf;
    if (!buf && !(buf = malloc(1 << 20))) abort();
    int64_t total = 0;
    for (;;)
    {
        ssize_t r = read(in, buf, 1 << 20);
        if (r < 0)
        {
            total = -1;
            break;
        }
        if (r == 0) break;
        for (ssize_t w = 0; w < r;)
        {
            ssize_t k = write(out, buf + w, (size_t)(r - w));
            if (k < 0)
            {
                total = -1;
                break;
            }
            w += k;
        }
        if (total < 0) break;
        total += r;
    }
    /* No fsync: Flink's upload to a local-filesystem checkpoint store closes the
       stream without one, and the copy stands in for that upload. */
    close(in);
    close(out);
    return total;
}
