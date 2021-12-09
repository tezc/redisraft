#ifndef __AE_H__
#define __AE_H__
#include "redisraft.h"

#define AE_NONE 0       /* No events registered. */
#define AE_READABLE 1   /* Fire when descriptor is readable. */
#define AE_WRITABLE 2   /* Fire when descriptor is writable. */

typedef struct aeEventLoop aeEventLoop;
typedef void aeFileProc(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);

int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                      aeFileProc *proc, void *clientData)
{
    return RedisModule_CreateFileEvent(fd, mask, (RedisModuleFileEventCallback)proc, clientData);
}

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    RedisModule_DeleteFileEvent(fd, mask);
}

#endif