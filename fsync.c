#include "redisraft.h"
#include <pthread.h>
#include <string.h>
#include <assert.h>

/* returns latest fsync'd index */
raft_index_t fsyncIndex(fsyncThread *th)
{
    raft_index_t t;

    pthread_mutex_lock(&th->mtx);
    t = th->fsynced_index;
    pthread_mutex_unlock(&th->mtx);

    return t;
}

/* returns index of latest fsync request. After fsync() is called on this index,
 * it will be assigned to th->fsynced_index */
raft_index_t fsyncRequestedIndex(fsyncThread *th)
{
    raft_index_t t;

    pthread_mutex_lock(&th->mtx);
    t = th->requested_index;
    pthread_mutex_unlock(&th->mtx);

    return t;
}

/* trigger fsync */
void fsyncAddTask(fsyncThread *th, int fd, raft_index_t requested_index)
{
    int rc;

    pthread_mutex_lock(&th->mtx);

    th->requested_index = requested_index;
    th->fd = fd;
    th->need_fsync = 1;
    th->completed = 0;

    rc = pthread_cond_signal(&th->cond);
    assert(rc == 0);
    (void) rc;

    pthread_mutex_unlock(&th->mtx);
}

/* wait fsync thread completes fsync() call */
void fsyncWaitUntilCompleted(fsyncThread *th)
{
    pthread_mutex_lock(&th->mtx);
    while (th->completed == 0) {
        pthread_cond_wait(&th->cond, &th->mtx);
    }
    pthread_mutex_unlock(&th->mtx);
}

static void* fsyncLoop(void *arg)
{
    int rc, fd;
    raft_index_t request_idx;
    fsyncThread *th = arg;

    while (1) {
        pthread_mutex_lock(&th->mtx);
        while (th->need_fsync == 0) {
            rc = pthread_cond_wait(&th->cond, &th->mtx);
            assert(rc == 0);
            (void) rc;
        }

        request_idx = th->requested_index;
        fd = th->fd;
        th->need_fsync = 0;

        pthread_mutex_unlock(&th->mtx);

        uint64_t begin = monotonicNanos();
        rc = fsync(fd);
        if (rc != 0) {
            fprintf(stderr, "fsync : %s \n", strerror(errno));
            abort();
        }

        uint64_t time = (monotonicNanos() - begin) / 1000;

        th->fsync_total += time;
        th->fsync_count++;
        th->fsync_max = time > th->fsync_max ? time : th->fsync_max;

        pthread_mutex_lock(&th->mtx);
        th->fsynced_index = request_idx;
        if (th->need_fsync == 0) {
            th->completed = 1;
            pthread_cond_signal(&th->cond);
        }
        pthread_mutex_unlock(&th->mtx);

        if (write(th->wakeUpFd, "1", 1) != 1) {
            fprintf(stderr, "write : %s \n", strerror(errno));
            abort();
        }
    }
}

void startFsyncThread(fsyncThread *th, int wakeUpFd)
{
    int rc;
    pthread_attr_t attr;

    *th = (fsyncThread) {0};

    th->id = 0;
    th->completed = 1;
    th->mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    th->wakeUpFd = wakeUpFd;

    rc = pthread_cond_init(&th->cond, NULL);
    if (rc != 0) {
        fprintf(stderr, "pthread_cond_init : %s \n", strerror(rc));
        abort();
    }

    rc = pthread_attr_init(&attr);
    if (rc != 0) {
        fprintf(stderr, "pthread_attr_init : %s \n", strerror(rc));
        abort();
    }

    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    rc = pthread_create(&th->id, &attr, fsyncLoop, th);
    if (rc != 0) {
        fprintf(stderr, "pthread_create : %s \n", strerror(rc));
        abort();
    }

    pthread_attr_destroy(&attr);
}





