# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from .constants import TimeString
from .exceptions import EpException
from .utils import setEnvironment, getConfig, Logger
from .interfaces import SyncRest, AsyncRest
from .schedules import asleep, runBackground, MultiTask
from .model import ID, Profile, Tags, Metadata, TStamp, ModelStatus

#===============================================================================
# Implement
#===============================================================================
class BaseControl:
    
    def __init__(self, api, background=False):
        self._background=background
        self.api = api
        self.api.router.add_event_handler("startup", self.__startup__)
        self.api.router.add_event_handler("shutdown", self.shutdown)
    
    async def __startup__(self):
        await self.startup()
        if self._background: await runBackground(self.background())
    
    async def startup(self): pass

    async def shutdown(self): pass
    
    async def background(self):
        while True:
            LOG.INFO('background')
            asleep(1)
