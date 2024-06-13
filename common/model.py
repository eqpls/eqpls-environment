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
from typing import List, Union, Optional
from pydantic import BaseModel


#===============================================================================
# Interface
#===============================================================================
class ID:
    # id:UUID = ''

    def setID(self, id:Optional[UUID]=None):
        self.id = id if id else uuid4()
        return self


class Profile:
    # name:str = ''
    # displayName:str = ''
    # description:str = ''

    def setProfile(self, name:str, displayName:str='', description:str=''):
        self.name = name
        self.displayName = displayName
        self.description = description
        return self


class Tags:
    # tags:list = []
    
    def setTag(self, tag):
        if tag not in self.tags: self.tags.append(tag)
        return self
    
    def delTag(self, tag):
        if tag in self.tags: self.tags.pop(tag)
        return self
        

class Metadata:
    # metadata:str = '{}'
    
    def getMeta(self, key):
        metadata = self.getMetadata()
        if key in metadata: return metadata[key]
        else: None
    
    def setMeta(self, key, value):
        metadata = self.getMetadata()
        if key in metadata:
            preval = metadata[key]
            if isinstance(preval, list): preval.append(value)
            else: preval = [preval, value]
            metadata[key] = preval
        else:
            metadata[key] = value
        self.setMetadata(**metadata)
        return self

    def getMetadata(self): return json.loads(self.metadata)

    def setMetadata(self, **metadata):
        self.metadata = json.dumps(metadata, separators=(',', ':'))
        return self


class TStamp:
    # tstamp:int = 0

    def setTStamp(self):
        self.tstamp = int(time.time())
        return self


class ModelStatus(BaseModel):
    id:UUID = ''
    status:str = ''
