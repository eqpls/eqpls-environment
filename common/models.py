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
from typing import Annotated, Callable, TypeVar, Any, List
from pydantic import BaseModel, PlainSerializer, ConfigDict
from stringcase import snakecase, pathcase, titlecase

#===============================================================================
# SchemaDecorator
#===============================================================================
_TypeT = TypeVar('_TypeT', bound=type)


class LayerOpt(dict):

    def __init__(self, **kargs): dict.__init__(self, **kargs)


class SchemaInfo(BaseModel):

    major:int
    minor:int
    name:str
    module:str
    sref:str
    dref:str
    path:str
    tags:list[str]
    crud:str
    layer:str
    cache:Any | None = None
    cacheOption:Any | None = None
    search:Any | None = None
    searchOption:Any | None = None
    database:Any | None = None
    databaseOption:Any | None = None


def SchemaConfig(
        major:int,
        minor:int,
        crud:str='crud',
        layer:str='csd',
        cacheOption:Any | None=None,
        searchOption:Any | None=None,
        databaseOption:Any | None=None
    ) -> Callable[[_TypeT], _TypeT]:

    def inner(TypedDictClass: _TypeT, /) -> _TypeT:

        if not issubclass(TypedDictClass, BaseSchema): raise Exception(f'{TypedDictClass} is not a BaseSchema')

        name = TypedDictClass.__name__
        module = TypedDictClass.__module__
        modsrt = module.replace('schema.', '')
        lowerModule = modsrt.lower()
        sref = f'{modsrt}.{name}'
        lowerSchemaRef = sref.lower()
        dref = snakecase(f'{lowerSchemaRef}.{major}.{minor}')
        path = '/' + pathcase(f'v{major}.{lowerSchemaRef}')
        tags = [titlecase('.'.join(reversed(lowerModule.split('.'))))]

        TypedDictClass.__pydantic_config__ = ConfigDict(
            schemaInfo=SchemaInfo(
                major=major,
                minor=minor,
                name=name,
                module=module,
                sref=sref,
                dref=dref,
                path=path,
                tags=tags,
                crud=crud,
                layer=layer,
                cacheOption=cacheOption if cacheOption else LayerOpt(),
                searchOption=searchOption if searchOption else LayerOpt(),
                databaseOption=databaseOption if databaseOption else LayerOpt()
            )
        )
        return TypedDictClass

    return inner


#===============================================================================
# Interface
#===============================================================================
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
        if fields: self.fields = ['id', 'type', 'ref'] + fields
        else: self.fields = None
        self.filter = filter
        self.query = query
        self.orderBy = orderBy
        self.order = order
        self.size = size
        self.skip = skip


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
    sref:Key = ''
    uref:Key = ''


#===============================================================================
# Abstract Models
#===============================================================================
class IdentSchema:
    id:ID = ''
    sref:Key = ''
    uref:Key = ''

    @classmethod
    def getSchemaInfo(cls): return cls.__pydantic_config__['schemaInfo']

    @property
    def schemaInfo(self): return self.__class__.getSchemaInfo()

    def getReference(self):
        return Reference(id=self.id, sref=self.sref, uref=self.uref)

    def setID(self, id:ID | None=None):
        schemaInfo = self.schemaInfo
        self.id = id if id else str(uuid4())
        self.sref = schemaInfo.sref
        self.uref = f'{schemaInfo.path}/{self.id}'
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

