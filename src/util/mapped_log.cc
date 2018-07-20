#include "mapped_log.h"

#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#undef DEBUG

#define PAGE_SIZE 4096

int new_mapped_log(const char *path, MappedLog *log, int inc_size) {
    log->inc_size = inc_size;
    log->fd = open(path, O_RDWR|O_CREAT|O_TRUNC,
            S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if (log->fd == -1) {
        char msg[512];
        snprintf(msg, sizeof(msg), "create in new_mapped_log %s", path);
        perror(msg);
        return -1;
    }
#ifdef DEBUG
    /*fprintf(stderr, "%s fd %d\n", path, log->fd);*/
#endif

    if (ftruncate(log->fd, log->inc_size) == -1) {
        perror("ftruncate in new_mapped_log");
        return -1;
    }

    log->start = log->buf = (char *)mmap(NULL,log->inc_size, PROT_WRITE|PROT_READ,
                                 MAP_SHARED, log->fd,0);
    if (log->buf == MAP_FAILED) {
        perror("mmap in new_mapped_log");
        return -1;
    }
    log->end = log->start + log->inc_size;
    if (madvise(log->start, log->inc_size, MADV_SEQUENTIAL) == -1) {
        perror("madvise in new_mapped_log");
        return -1;
    }

    log->log_size = 0;
    return 0;
}

int open_mapped_log(const char *path, MappedLog *log) {
    log->fd = open(path, O_RDONLY);
    if (log->fd == -1) {
        char msg[512];
        snprintf(msg, sizeof(msg), "open fail %s", path);
        perror(msg);
        return -1;
    }

    struct stat sb;
    if (fstat(log->fd, &sb) == -1) {
        perror("fstat in open_mapped_log");
        exit(1);
    }

    log->start = log->buf = (char *)mmap(NULL,sb.st_size, PROT_READ, MAP_PRIVATE | MAP_FIXED, log->fd, 0);
    if (log->buf == MAP_FAILED) {
        perror("mmap in open_mapped_log");
        exit(1);
    }
    log->end = log->buf + sb.st_size;
    if (madvise(log->buf, sb.st_size, MADV_SEQUENTIAL) == -1) {
        perror("madvise in madvise");
        exit(1);
    }
    return 0;
}

int unmap_log(MappedLog *log) {
    if (munmap(log->start, log->end - log->start) == -1) {
        perror("munmap");
        return -1;
    }
    close(log->fd);
    return 0;
}

int enlarge_mapped_log(MappedLog *log) {
    struct stat sb;
    if (fstat(log->fd, &sb) == -1) {
        perror("fstat in enlarge_mapped_log");
        return -1;
    }
    off_t original_size = sb.st_size;
    assert(original_size % log->inc_size == 0);

#ifdef DEBUG
    fprintf(stderr, "fd %d unmap start %p buf %p ", log->fd, log->start, log->buf);
#endif
    if (munmap(log->start, log->end - log->start) == -1) {
        perror("munmap in enlarge_mapped_log");
        return -1;
    }

    if (ftruncate(log->fd, original_size + log->inc_size) == -1) {
        perror("ftruncate in enlarge_mapped_log");
        return -1;
    }

    int map_size = log->inc_size + PAGE_SIZE;
    log->start = (char *)mmap(NULL,map_size, PROT_WRITE|PROT_READ, MAP_SHARED,
                      log->fd, original_size - PAGE_SIZE);
    if (log->start == MAP_FAILED) {
        perror("mmap in enlarge_mapped_log");
        return -1;
    }
    log->end = log->start + map_size;
    long page_offset = (long)log->buf & 0xFFF;
    // If page offset is 0, means we need to start on the new page.
    log->buf = log->start + (page_offset ? page_offset : PAGE_SIZE);

#ifdef DEBUG
    fprintf(stderr, "new start: %p buf: %p truncate to %ld bytes\n", log->start,
        log->buf, (long)(original_size + log->inc_size));
#endif

    if (madvise(log->start, map_size, MADV_SEQUENTIAL) == -1) {
        perror("madvise in enlarge_mapped_log");
        return -1;
    }
    return 0;
}
