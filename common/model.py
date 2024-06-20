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
from typing import Annotated, List, Any
from pydantic import BaseModel, PlainSerializer
from stringcase import snakecase, camelcase, pathcase, titlecase

#===============================================================================
# Fields
#===============================================================================
ID = Annotated[UUID, PlainSerializer(lambda x: str(x), return_type=str)]
Key = Annotated[str, 'keyword']


#===============================================================================
# Internal Models
#===============================================================================
class SchemaDescription:
    
    def __init__(self, title:str, version:int, schema:Any):
        self.title = title
        self.version = version
        self.schema = schema
    
    @property
    def nameCode(self): return self.schema.__name__
    
    @property
    def nameSnake(self): return snakecase(self.schema.__name__)
    
    @property
    def nameCamel(self): return camelcase(self.schema.__name__)
    
    @property
    def moduleCode(self): return self.schema.__module__
    
    @property
    def modelCode(self): return f'{self.moduleCode}.{self.nameCode}'
    
    @property
    def moduleReference(self): return self.moduleCode.replace('schema.', '')
    
    @property
    def modelReference(self): return f'{snakecase(self.title)}.v{self.version}.{self.moduleReference}.{self.nameSnake}'
    
    @property
    def schemaPath(self): return snakecase(f'{self.title}.v{self.version}.{self.moduleReference}.{self.nameCamel}')
    
    @property
    def modelPath(self): return f'{self.moduleReference}.{self.nameCode}'
    
    @property
    def url(self): return f'/{pathcase(self.modelReference)}'
    
    @property
    def tags(self): return [titlecase(self.moduleReference)]
    
    # @property
    # def url(self): return f'/{self.apiPath}/{self.nameSnake}'

    
class SearchOption:
    
    def __init__(
        self,
        fields:List[str] | None=None,
        filter:Any | None=None,
        query:dict | None=None,
        orderBy:str | None=None,
        order:str | None=None,
        size:int | None=None,
        skip:int | None=None,
    ):
        if fields: self.fields = ['id', 'ref'] + fields
        else: self.fields = None
        self.filter = filter
        self.query = query
        self.orderBy = orderBy
        self.order = order
        self.size = size
        self.skip = skip


class ModelStatus(BaseModel):
    id:ID = ''
    status:str = ''


class ModelCount(BaseModel):
    path:str = ''
    value:int = 0


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
    author:Key = ''
    deleted:bool = False
    tstamp:int = 0
    
    def updateStatus(self, author=None):
        self.author = author if author else 'unknown'
        self.deleted = False
        self.tstamp = int(time.time())
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

