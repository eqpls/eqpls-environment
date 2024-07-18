# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import json

from uuid import UUID, uuid4
from time import time as tstamp
from urllib.parse import urlencode
from typing import Annotated, Callable, TypeVar, Any, Literal
from pydantic import BaseModel, PlainSerializer, ConfigDict
from stringcase import snakecase, pathcase, titlecase

from .constants import CRUD, LAYER, AAA
from .exceptions import EpException
from .interfaces import AsyncRest

#===============================================================================
# Interfaces
#===============================================================================
_REFERENCE_FIELDS = ['id', 'sref', 'uref']
_REFERENCE_SETS = set(_REFERENCE_FIELDS)


class Search:

    def __init__(
        self,
        fields:list[str] | None=None,
        filter:Any | None=None,
        orderBy:str | None=None,
        order:str | None=None,
        size:int | None=None,
        skip:int | None=None,
    ):
        if fields: self.fields = _REFERENCE_FIELDS + list(set(self.fields) - _REFERENCE_SETS)
        else: self.fields = None
        self.filter = filter
        self.orderBy = orderBy
        self.order = order
        self.size = size
        self.skip = skip


class Option(dict):

    def __init__(self, **kargs): dict.__init__(self, **kargs)


#===============================================================================
# Fields
#===============================================================================
ID = Annotated[UUID, PlainSerializer(lambda x: str(x), return_type=str)]
Key = Annotated[str, 'keyword']


#===============================================================================
# Pre-Defined Models
#===============================================================================
class ServiceHealth(BaseModel):

    title:str = ''
    status:str = ''
    healthy:bool = False
    detail:dict = {}


class Reference(BaseModel):

    id:ID = ''
    sref:Key = ''
    uref:Key = ''

    async def getModel(self, token=None, org=None):
        if not self.sref or not self.uref: raise EpException(400, 'Bad Request')
        if 'schemaMap' not in Reference.__pydantic_config__: raise EpException(500, 'Internal Server Error')
        schemaMap = Reference.__pydantic_config__['schemaMap']
        if self.sref not in schemaMap: raise EpException(501, 'Not Implemented')
        schema = schemaMap[self.sref]
        schemaInfo = schema.getSchemaInfo()
        if 'r' in schemaInfo.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token}'
            if org: headers['Organization'] = org
            async with AsyncRest(schemaInfo.provider) as rest: return schema(**(await rest.get(self.uref, headers=headers)))
        else: raise EpException(405, 'Method Not Allowed')


class ModelStatus(BaseModel):

    id:ID = ''
    sref:Key = ''
    uref:Key = ''
    status:str = ''


class ModelCount(BaseModel):

    sref:Key = ''
    uref:Key = ''
    query:str = ''
    result:int = 0


#===============================================================================
# Schema Info
#===============================================================================
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
    crud:int = CRUD.CRUD
    layer:int = LAYER.CSD
    aaa:int = AAA.FREE
    cache:Any | None = None
    search:Any | None = None
    database:Any | None = None


_TypeT = TypeVar('_TypeT', bound=type)


def SchemaConfig(
    version:int,
    crud:int = CRUD.CRUD,
    layer:int = LAYER.CSD,
    aaa:int = AAA.FREE,
    cache:Option | None=None,
    search:Option | None=None,
    database:Option | None=None
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
                minor=version,
                name=name,
                module=module,
                sref=sref,
                tags=tags,
                crud=crud,
                layer=layer,
                aaa=aaa,
                cache=cache if cache else Option(),
                search=search if search else Option(),
                database=database if database else Option()
            )
        )
        return TypedDictClass

    return inner


#===============================================================================
# Schema Abstraction
#===============================================================================
class IdentSchema:

    id:ID = None
    sref:Key = ''
    uref:Key = ''

    def setID(self, id:ID | None=None):
        schemaInfo = self.__class__.getSchemaInfo()
        self.id = id if id else str(uuid4())
        self.sref = schemaInfo.sref
        self.uref = f'{schemaInfo.path}/{self.id}'
        return self


class StatusSchema:

    org:Key = ''
    owner:Key = ''
    deleted:bool = False
    tstamp:int = 0

    def updateStatus(self, org='', owner='', deleted=False):
        self.org = org
        self.owner = owner
        self.deleted = deleted
        self.tstamp = int(tstamp())
        return self


class BaseSchema(StatusSchema, IdentSchema):

    #===========================================================================
    # schema info
    #===========================================================================
    @classmethod
    def setSchemaInfo(cls, provider, service, version):
        schemaInfo = cls.getSchemaInfo()
        schemaInfo.provider = provider if provider else ''
        schemaInfo.service = service
        schemaInfo.major = version
        lowerSchemaRef = schemaInfo.sref.lower()
        schemaInfo.dref = snakecase(f'{lowerSchemaRef}.{version}.{schemaInfo.minor}')
        schemaInfo.path = f'/{service}/' + pathcase(f'v{version}.{lowerSchemaRef}')
        if '__pydantic_config__' not in Reference.__dict__: Reference.__pydantic_config__ = ConfigDict(schemaMap={})
        Reference.__pydantic_config__['schemaMap'][schemaInfo.sref] = cls

    @classmethod
    def getSchemaInfo(cls): return cls.__pydantic_config__['schemaInfo']

    #===========================================================================
    # crud
    #===========================================================================
    async def readModel(self, token=None, org=None):
        if not self.id: raise EpException(400, 'Bad Request')
        schemaInfo = self.__class__.getSchemaInfo()
        if CRUD.checkRead(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            async with AsyncRest(schemaInfo.provider) as rest: return self.__class__(**(await rest.get(self.uref, headers=headers)))
        else: raise EpException(405, 'Method Not Allowed')

    @classmethod
    async def readModelByID(cls, id:ID, token=None, org=None):
        schemaInfo = cls.getSchemaInfo()
        if CRUD.checkRead(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            async with AsyncRest(schemaInfo.provider) as rest: return cls(**(await rest.get(f'{schemaInfo.path}/{id}', headers=headers)))
        else: raise EpException(405, 'Method Not Allowed')

    @classmethod
    async def searchModels(cls,
        filter:str | None=None,
        orderBy:str | None=None,
        order:Literal['asc', 'desc']=None,
        size:int | None=None,
        skip:int | None=None,
        archive:bool | None=None,
        token=None,
        org=None
    ):
        schemaInfo = cls.getSchemaInfo()
        if CRUD.checkRead(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            query = {}
            if filter: query['$filter'] = filter
            if orderBy and order:
                query['$orderby'] = orderBy
                query['$order'] = order
            if size: query['$size'] = size
            if skip: query['$skip'] = skip
            if archive: query['$archive'] = archive
            url = f'{schemaInfo.path}?{urlencode(query)}' if query else schemaInfo.path
            async with AsyncRest(schemaInfo.provider) as rest: models = await rest.get(url, headers=headers)
            return [cls(**model) for model in models]
        else: raise EpException(405, 'Method Not Allowed')

    @classmethod
    async def countModels(cls,
        filter:str | None=None,
        archive:bool | None=None,
        token=None,
        org=None
    ):
        schemaInfo = cls.getSchemaInfo()
        if CRUD.checkRead(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            query = {}
            if filter: query['$filter'] = filter
            if archive: query['$archive'] = archive
            url = f'{schemaInfo.path}/count?{urlencode(query)}' if query else f'{schemaInfo.path}/count'
            async with AsyncRest(schemaInfo.provider) as rest: count = await rest.get(url, headers=headers)
            return ModelCount(**count)
        else: raise EpException(405, 'Method Not Allowed')

    async def createModel(self, token=None, org=None):
        schemaInfo = self.__class__.getSchemaInfo()
        if CRUD.checkCreate(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            data = self.dict()
            data['id'] = '00000000-0000-0000-0000-000000000000'
            async with AsyncRest(schemaInfo.provider) as rest: model = await rest.post(f'{schemaInfo.path}', headers=headers, json=data)
            return self.__class__(**model)
        else: raise EpException(405, 'Method Not Allowed')

    async def updateModel(self, token=None, org=None):
        if not self.id: raise EpException(400, 'Bad Request')
        schemaInfo = self.__class__.getSchemaInfo()
        if CRUD.checkUpdate(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            async with AsyncRest(schemaInfo.provider) as rest: model = await rest.put(f'{schemaInfo.path}/{self.id}', headers=headers, json=self.dict())
            return self.__class__(**model)
        else: raise EpException(405, 'Method Not Allowed')

    async def deleteModel(self, force=False, token=None, org=None):
        if not self.id: raise EpException(400, 'Bad Request')
        schemaInfo = self.__class__.getSchemaInfo()
        if CRUD.checkDelete(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            force = '?$force=true' if force else ''
            async with AsyncRest(schemaInfo.provider) as rest: status = await rest.delete(f'{schemaInfo.path}/{self.id}{force}', headers=headers)
            return ModelStatus(**status)
        else: raise EpException(405, 'Method Not Allowed')

    @classmethod
    async def deleteModelByID(cls, id:ID, force=False, token=None, org=None):
        schemaInfo = cls.getSchemaInfo()
        if CRUD.checkDelete(schemaInfo.crud):
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if org: headers['Organization'] = org
            force = '?$force=true' if force else ''
            async with AsyncRest(schemaInfo.provider) as rest: status = await rest.delete(f'{schemaInfo.path}/{id}{force}', headers=headers)
            return ModelStatus(**status)
        else: raise EpException(405, 'Method Not Allowed')


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
