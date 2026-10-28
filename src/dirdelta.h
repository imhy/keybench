#ifndef DIRDELTA_H
#define DIRDELTA_H

#include <stdint.h>

/* A snapshot of the regular files under a directory: path and size. Backends use
   it to report the bytes a checkpoint added to their checkpoint store, as the size
   of every new file plus the growth of every appended one, so all engines count
   checkpoint bytes the same way. */
typedef struct
{
    char **path;
    uint64_t *size;
    int n, cap;
} dir_snapshot;

void dir_snapshot_take(dir_snapshot *s, const char *root);
uint64_t dir_snapshot_added_bytes(const dir_snapshot *before, const dir_snapshot *after);
void dir_snapshot_free(dir_snapshot *s);

/* Remove a directory tree; a missing tree is not an error. */
void dir_remove_tree(const char *root);
/* Copy one regular file; returns its size, or -1 on failure. */
int64_t file_copy(const char *src, const char *dst);

#endif
