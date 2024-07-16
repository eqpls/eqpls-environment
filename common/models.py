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
from typing import Annotated, Callable, TypeVar, Any, List, Literal
from pydantic import BaseModel, PlainSerializer, ConfigDict
from stringcase import snakecase, pathcase, titlecase

from .constants import AuthLevel
from .tools import mergeArray
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
        orderBy:str | None=None,
        order:str | None=None,
        size:int | None=None,
        skip:int | None=None,
    ):
        if fields: self.fields = mergeArray(['id', 'type', 'ref'], fields)
        else: self.fields = None
        self.filter = filter
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
class ServiceHealth(BaseModel):
    title:str = ''
    status:str = ''
    healthy:bool = False
    detail:dict = {}


class Reference(BaseModel):
    id:ID = ''
    sref:Key = ''
    uref:Key = ''

    async def getModel(self):
        if not self.sref or not self.uref: raise EpException(400, 'Bad Request')
        if 'schemaMap' not in Reference.__pydantic_config__: raise EpException(501, 'Not Implemented')
        schemaMap = Reference.__pydantic_config__['schemaMap']
        if self.sref not in schemaMap: raise EpException(501, 'Not Implemented')
        schema = schemaMap[self.sref]
        info = schema.getSchemaInfo()
        if 'r' in info.crud:
            async with AsyncRest(info.provider) as rest:
                model = await rest.get(self.uref)
            return schema(**model)
        else: raise EpException(405, 'Method Not Allowed')


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
    auth:int = 0
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
    auth:int=0,
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
                auth=auth,
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
    id:ID = None
    sref:Key = ''
    uref:Key = ''

    def setID(self, id:ID | None=None):
        schemaInfo = self.schemaInfo
        self.id = id if id else str(uuid4())
        self.sref = schemaInfo.sref
        self.uref = f'{schemaInfo.path}/{self.id}'
        return self


class StatusSchema:
    realm:Key = ''
    owner:Key = ''
    deleted:bool = False
    tstamp:int = 0

    def updateStatus(self, realm='', owner='', deleted=False):
        self.realm = realm
        self.owner = owner
        self.deleted = deleted
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
    def getReference(self): return Reference(id=self.id, sref=self.sref, uref=self.uref)

    #===========================================================================
    # crud
    #===========================================================================
    async def readModel(self, token=None, realm=None):
        if not self.id: raise EpException(400, 'Bad Request')
        info = self.schemaInfo
        if 'r' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            async with AsyncRest(info.provider) as rest: return self.__class__(**(await rest.get(self.uref, headers=headers)))
        else: raise EpException(405, 'Method Not Allowed')

    @classmethod
    async def readModelByID(cls, id:ID, token=None, realm=None):
        info = cls.getSchemaInfo()
        if 'r' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            async with AsyncRest(info.provider) as rest: return cls(**(await rest.get(f'{info.path}/{id}', headers=headers)))
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
        realm=None
    ):
        info = cls.getSchemaInfo()
        if 'r' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            query = {}
            if filter: query['$filter'] = filter
            if orderBy and order:
                query['$orderby'] = orderBy
                query['$order'] = order
            if size: query['$size'] = size
            if skip: query['$skip'] = skip
            if archive: query['$archive'] = archive
            url = f'{info.path}?{urlencode(query)}' if query else info.path
            async with AsyncRest(info.provider) as rest: models = await rest.get(url, headers=headers)
            return [cls(**model) for model in models]
        else: raise EpException(405, 'Method Not Allowed')

    @classmethod
    async def countModels(cls,
        filter:str | None=None,
        archive:bool | None=None,
        token=None,
        realm=None
    ):
        info = cls.getSchemaInfo()
        if 'r' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            query = {}
            if filter: query['$filter'] = filter
            if archive: query['$archive'] = archive
            url = f'{info.path}/count?{urlencode(query)}' if query else f'{info.path}/count'
            async with AsyncRest(info.provider) as rest: count = await rest.get(url, headers=headers)
            return ModelCount(**count)
        else: raise EpException(405, 'Method Not Allowed')

    async def createModel(self, token=None, realm=None):
        info = self.schemaInfo
        if 'c' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            data = self.dict()
            data['id'] = '00000000-0000-0000-0000-000000000000'
            async with AsyncRest(info.provider) as rest: model = await rest.post(f'{info.path}', headers=headers, json=data)
            return self.__class__(**model)
        else: raise EpException(405, 'Method Not Allowed')

    async def updateModel(self, token=None, realm=None):
        if not self.id: raise EpException(400, 'Bad Request')
        info = self.schemaInfo
        if 'u' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            async with AsyncRest(info.provider) as rest: model = await rest.put(f'{info.path}/{self.id}', headers=headers, json=self.dict())
            return self.__class__(**model)
        else: raise EpException(405, 'Method Not Allowed')

    async def deleteModel(self, force=False, token=None, realm=None):
        if not self.id: raise EpException(400, 'Bad Request')
        info = self.schemaInfo
        if 'd' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            force = '?$force=true' if force else ''
            async with AsyncRest(info.provider) as rest: status = await rest.delete(f'{info.path}/{self.id}{force}', headers=headers)
            return ModelStatus(**status)
        else: raise EpException(405, 'Method Not Allowed')

    @classmethod
    async def deleteModelByID(cls, id:ID, force=False, token=None, realm=None):
        info = cls.getSchemaInfo()
        if 'd' in info.crud:
            headers = {}
            if token: headers['Authorization'] = f'Bearer {token.credentials}'
            if realm: headers['Realm'] = realm
            force = '?$force=true' if force else ''
            async with AsyncRest(info.provider) as rest: status = await rest.delete(f'{info.path}/{id}{force}', headers=headers)
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


#===============================================================================
# Auth Schema
#===============================================================================
class AuthInfo(BaseModel):
    realm:str
    username:str
    admin:bool
    policy:list[str]
    readAllowed:list[str]
    createAllowed:list[str]
    updateAllowed:list[str]
    deleteAllowed:list[str]

    def checkRealm(self, realm): return True if self.realm == realm else False

    def checkUsername(self, username): return True if self.username == username else False

    def checkAccount(self, realm, username): return True if self.realm == realm and self.owner == username else False

    def checkAdmin(self): return self.admin

    def checkReadAllowed(self, sref): return True if sref in self.readAllowed else False

    def checkCreateAllowed(self, sref): return True if sref in self.createAllowed else False

    def checkUpdateAllowed(self, sref): return True if sref in self.updateAllowed else False

    def checkDeleteAllowed(self, sref): return True if sref in self.deleteAllowed else False


@SchemaConfig(
minor=1,
auth=AuthLevel.AA,
cacheOption=LayerOpt(expire=86400),
searchOption=LayerOpt(expire=2419200))
class Policy(BaseModel, ProfSchema, BaseSchema):
    readAllowed: list[str]
    createAllowed: list[str]
    updateAllowed: list[str]
    deleteAllowed: list[str]
