#include "redisraft.h"
#include <pthread.h>
#include <string.h>
#include <assert.h>


raft_index_t fsyncIndex(fsyncThread *th)
{
    raft_index_t t;
    pthread_mutex_lock(&th->mtx);
    t = th->fsynced_index;
    pthread_mutex_unlock(&th->mtx);

    return t;
}

raft_index_t fsyncRequestedIndex(fsyncThread *th)
{
    raft_index_t t;
    pthread_mutex_lock(&th->mtx);
    t = th->requested_index;
    pthread_mutex_unlock(&th->mtx);

    return t;
}

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

void fsyncWaitUntilCompleted(fsyncThread *th)
{
    pthread_mutex_lock(&th->mtx);
    while (th->completed == 0) {
        pthread_cond_wait(&th->cond, &th->mtx);
    }
    pthread_mutex_unlock(&th->mtx);
}

uint64_t sc_time_mono_ns()
{
#if defined(_WIN32) || defined(_WIN64)
    static int64_t frequency = 0;
	if (frequency == 0) {
		LARGE_INTEGER freq;
		QueryPerformanceFrequency(&freq);
		assert(freq.QuadPart != 0);
		frequency = freq.QuadPart;
	}
	LARGE_INTEGER count;
	QueryPerformanceCounter(&count);
	return (uint64_t) (count.QuadPart * 1000000000) / frequency;
#else
    int rc;
    struct timespec ts;

    rc = clock_gettime(CLOCK_MONOTONIC, &ts);
    assert(rc == 0);
    (void) rc;

    return ((uint64_t) ts.tv_sec * 1000000000 + (uint64_t) ts.tv_nsec);
#endif
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

        rc = fsync(fd);
        if (rc != 0) {
            fprintf(stderr, "fsync : %s \n", strerror(errno));
            abort();
        }

        pthread_mutex_lock(&th->mtx);
        th->fsynced_index = request_idx;
        if (th->need_fsync == 0) {
            th->completed = 1;
            pthread_cond_signal(&th->cond);

            if (write(th->wakeUpFd, "1", 1) != 1) {
                fprintf(stderr, "write : %s \n", strerror(errno));
                abort();
            }
        }
        pthread_mutex_unlock(&th->mtx);
    }
}

void startFsyncThread(fsyncThread *th, int wakeUpFd)
{
    int rc;
    pthread_attr_t attr;

    th->id = 0;
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





