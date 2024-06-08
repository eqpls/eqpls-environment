# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import time
import json
from uuid import UUID, uuid4
from pydantic import BaseModel
from typing import Union, Optional


#===============================================================================
# Implement
#===============================================================================
class ID(BaseModel):
    id: UUID = ''

    def generateID(self):
        self.id = uuid4()
        return self.id


class Profile(BaseModel):
    name: str = ''
    displayName: str = ''
    description: str = ''


class Metadata(BaseModel):
    metadata: str = ''

    def getMetadata(self): return json.loads(self.metadata)

    def setMetadata(self, **metadata): self.metadata = json.dumps(metadata, separators=(',', ':'))


class TStamp(BaseModel):
    tstamp: int = 0

    def setTStamp(self): self.tstamp = int(time.time())


class Status(BaseModel):
    status: str = ''
