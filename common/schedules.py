# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import asyncio
from fastapi.concurrency import run_in_threadpool


#===============================================================================
# Implement
#===============================================================================
async def asleep(delay):
    await asyncio.sleep(delay)


async def runBackground(coro):
    asyncio.create_task(coro)


async def runSyncAsAsync(func, *args, **kargs):
    return await run_in_threadpool(func, *args, **kargs)


class MultiTask:

    def __init__(self): self._multi_tasks_ = []

    async def __aenter__(self): return self

    async def __aexit__(self, *args): pass

    def __call__(self, ref):
        self._multi_tasks_.append(ref)
        return self

    async def wait(self):
        return await asyncio.gather(*(self._multi_tasks_))
