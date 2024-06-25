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
from typing import Annotated, Callable, TypeVar, Any, List, Literal
from pydantic import BaseModel, PlainSerializer, ConfigDict
from stringcase import snakecase, pathcase, titlecase

from .exceptions import EpException
from .interfaces import AsyncRest


#===============================================================================
# Search Option
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
# Pre-Defined Models
#===============================================================================
class Reference(BaseModel):
    id:ID = ''
    sref:Key = ''
    uref:Key = ''

    async def readModel(self):
        if not self.sref or not self.uref: raise Exception('could not find references')
        if 'schemaMap' not in Reference.__pydantic_config__: raise EpException(501, 'Could Not Find SchemaMap')
        schemaMap = Reference.__pydantic_config__['schemaMap']
        if self.sref not in schemaMap: raise EpException(501, 'Could Not Find Schema at schemaMap')
        schema = schemaMap[self.sref]
        info = schema.getSchemaInfo()
        if 'r' in info.crud:
            async with AsyncRest(info.provider) as rest:
                model = await rest.get(self.uref)
            return schema(**model)
        else: raise EpException(405, 'Could Not Read Model')


class ModelStatus(BaseModel):
    id:ID = ''
    status:str = ''


class ModelCount(BaseModel):
    path:str = ''
    query:str = ''
    result:int = 0


#===============================================================================
# Schema Info
#===============================================================================
_TypeT = TypeVar('_TypeT', bound=type)


class LayerOpt(dict):

    def __init__(self, **kargs): dict.__init__(self, **kargs)


class SchemaInfo(BaseModel):

    provider:str = ''
    service:str = ''
    major:int = 1
    minor:int = 1
    name:str = ''
    module:str = ''
    sref:str = ''
    dref:str = ''
    path:str = ''
    tags:list[str] = []
    crud:str = 'crud'
    layer:str = 'csd'
    cache:Any | None = None
    cacheOption:Any | None = None
    search:Any | None = None
    searchOption:Any | None = None
    database:Any | None = None
    databaseOption:Any | None = None


def SchemaConfig(
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
        sref = f'{modsrt}.{name}'
        tags = [titlecase('.'.join(reversed(modsrt.lower().split('.'))))]
        TypedDictClass.__pydantic_config__ = ConfigDict(
            schemaInfo=SchemaInfo(
                minor=minor,
                name=name,
                module=module,
                sref=sref,
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
# Schema Abstraction
#===============================================================================
class IdentSchema:
    id:ID = ''
    sref:Key = ''
    uref:Key = ''

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


class BaseSchema(StatusSchema, IdentSchema):

    #===========================================================================
    # schema info
    #===========================================================================
    @classmethod
    def setSchemaInfo(cls, major, service, provider=''):
        info = cls.getSchemaInfo()
        info.provider = provider
        info.service = service
        info.major = major
        lowerSchemaRef = info.sref.lower()
        info.dref = snakecase(f'{lowerSchemaRef}.{major}.{info.minor}')
        info.path = f'/{service}/' + pathcase(f'v{major}.{lowerSchemaRef}')
        if '__pydantic_config__' not in Reference.__dict__: Reference.__pydantic_config__ = ConfigDict(schemaMap={})
        Reference.__pydantic_config__['schemaMap'][info.sref] = cls

    @classmethod
    def getSchemaInfo(cls): return cls.__pydantic_config__['schemaInfo']

    @property
    def schemaInfo(self): return self.__class__.getSchemaInfo()

    #===========================================================================
    # reference
    #===========================================================================
    def makeReference(self): return Reference(id=self.id, sref=self.sref, uref=self.uref)

    #===========================================================================
    # crud
    #===========================================================================
    async def readModel(self):
        if not self.id: raise Exception('could not find url reference')
        info = self.schemaInfo
        if 'r' in info.crud:
            async with AsyncRest(info.provider) as rest:
                model = await rest.get(self.uref)
            return self.__class__(**model)
        else: raise EpException(405, 'Could Not Read Model')

    @classmethod
    async def readModelByID(cls, id:ID):
        info = cls.getSchemaInfo()
        if 'r' in info.crud:
            async with AsyncRest(info.provider) as rest: model = await rest.get(f'{info.path}/{id}')
            return cls(**model)
        else: raise EpException(405, 'Could Not Read Model')

    @classmethod
    async def searchModels(cls,
        fields:List[str] | None=None,
        filter:str | None=None,
        orderBy:str | None=None,
        order:Literal['asc', 'desc']=None,
        size:int | None=None,
        skip:int | None=None,
        archive:Literal['true', 'false']=None
    ):
        if fields: query = '&'.join([f'$f={field}' for field in fields])
        else: query = ''
        filter = f'$filter={filter}' if filter else None
        if filter: query = f'{query}&{filter}' if query else filter
        orderBy = f'$orderby={orderBy}' if orderBy else None
        if orderBy: query = f'{query}&{orderBy}' if query else orderBy
        order = f'$order={order}' if order else None
        if order: query = f'{query}&{order}' if query else order
        size = f'$size={size}' if size else None
        if size: query = f'{query}&{size}' if query else size
        skip = f'$skip={skip}' if skip else None
        if skip: query = f'{query}&{skip}' if query else skip
        archive = f'$archive={archive}' if archive else None
        if archive: query = f'{query}&{archive}' if query else f'{archive}'
        query = f'?{query}' if query else ''

        info = cls.getSchemaInfo()
        if 'r' in info.crud:
            async with AsyncRest(info.provider) as rest:
                models = await rest.get(f'{info.path}{query}')
            return [cls(**model) for model in models]
        else: raise EpException(405, 'Could Not Read Model')

    @classmethod
    async def countModels(cls,
        fields:List[str] | None=None,
        filter:str | None=None,
        orderBy:str | None=None,
        order:Literal['asc', 'desc']=None,
        size:int | None=None,
        skip:int | None=None,
        archive:Literal['true', 'false']=None
    ):
        query = '&'.join([f'$f={field}' for field in fields])
        filter += f'$filter={filter}' if filter else None
        if filter: query += f'&{filter}' if query else f'{filter}'
        orderBy = f'$orderby={orderBy}' if orderBy else None
        if orderBy: query += f'&{orderBy}' if query else f'{orderBy}'
        order = f'$order={order}' if order else None
        if order: query += f'&{order}' if query else f'{order}'
        size = f'$size={size}' if size else None
        if size: query += f'&{size}' if query else f'{size}'
        skip = f'$skip={skip}' if skip else None
        if skip: query += f'&{skip}' if query else f'{skip}'
        archive = f'$archive={archive}' if archive else None
        if archive: query += f'&{archive}' if query else f'{archive}'
        query = f'?{query}' if query else ''

        info = cls.getSchemaInfo()
        if 'r' in info.crud:
            async with AsyncRest(info.provider) as rest: count = await rest.get(f'{info.path}/count{query}')
            return ModelCount(**count)
        else: raise EpException(405, 'Could Not Read Model')

    async def createModel(self):
        info = self.schemaInfo
        if 'c' in info.crud:
            async with AsyncRest(info.provider) as rest: model = await rest.post(f'{info.path}', json=self)
            return self.__class__(**model)
        else: raise EpException(405, 'Could Not Create Model')

    async def updateModel(self):
        if not self.id: raise Exception('could not find model identifier')
        info = self.schemaInfo
        if 'u' in info.crud:
            async with AsyncRest(info.provider) as rest: model = await rest.put(f'{info.path}/{self.id}', json=self)
            return self.__class__(**model)
        else: raise EpException(405, 'Could Not Update Model')

    async def deleteModel(self, force=False):
        if not self.id: raise Exception('could not find model identifier')
        info = self.schemaInfo
        if 'd' in info.crud:
            force = '?$force=true' if force else ''
            async with AsyncRest(info.provider) as rest: status = await rest.delete(f'{info.path}/{self.id}{force}')
            return ModelStatus(**status)
        else: raise EpException(405, 'Could Not Delete Model')


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

