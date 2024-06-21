# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import json
from time import time as tstamp
from uuid import UUID, uuid4
from typing import Annotated
from pydantic import BaseModel, PlainSerializer

#===============================================================================
# Fields
#===============================================================================
ID = Annotated[UUID, PlainSerializer(lambda x: str(x), return_type=str)]
Key = Annotated[str, 'keyword']


#===============================================================================
# Pre Defined Models
#===============================================================================
class ModelStatus(BaseModel):
    id:ID = ''
    type:Key = ''
    ref:Key = ''
    status:str = ''


class ModelCount(BaseModel):
    path:str = ''
    query:str = ''
    result:int = 0


#===============================================================================
# Relation Models
#===============================================================================
class Reference(BaseModel):
    id:ID = ''
    type:Key = ''
    ref:Key = ''


#===============================================================================
# Abstract Models
#===============================================================================
class IdentSchema:
    id:ID = ''
    type:Key = ''
    ref:Key = ''

    def setID(self, path:str, type:str, id:ID | None=None):
        self.id = id if id else str(uuid4())
        self.type = type
        self.ref = f'{path}/{self.id}'
        return self


class StatusSchema:
    updateBy:Key = ''
    deleted:bool = False
    tstamp:int = 0
    
    def updateStatus(self, updateBy=None):
        self.updateBy = updateBy if updateBy else 'unknown'
        self.deleted = False
        self.tstamp = int(tstamp())
        return self


class BaseSchema(StatusSchema, IdentSchema): pass
    

class ProfSchema:
    name:Key = ''
    displayName:str = ''
    description:str = ''


class TagSchema:
    tags:list[str] = []

    def setTag(self, tag):
        if tag not in self.tags: self.tags.append(tag)
        return self

    def delTag(self, tag):
        if tag in self.tags: self.tags.pop(tag)
        return self


class MetaSchema:
    metadata:str = '{}'

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

