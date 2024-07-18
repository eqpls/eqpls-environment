# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import traceback
from typing import Annotated, Any, List, Literal
from fastapi import FastAPI, Request, BackgroundTasks, Query
from pydantic import BaseModel
from luqum.parser import parser as parseLucene
from stringcase import snakecase, pathcase

from .constants import CRUD, LAYER, AAA, ORG_HEADER, AUTH_HEADER
from .exceptions import EpException
from .schedules import asleep, runBackground
from .models import Search, ID, BaseSchema, ServiceHealth, ModelStatus, ModelCount
from .auth import RBAC
from .utils import getConfig, Logger


#===============================================================================
# Base Control
#===============================================================================
class BaseControl:

    def __init__(self, confPath, background=False):
        self.config = getConfig(confPath)
        Logger.register(self.config)

        self._title = snakecase(self.config['default']['title'])
        self._version = int(self.config['service']['version'])
        self._uri = f'/{pathcase(self._title)}'
        self._background = background

        self._api = FastAPI(
            title=self._title,
            docs_url=f'{self._uri}/docs',
            openapi_url=f'{self._uri}/openapi.json',
            separate_input_output_schemas=False
        )

        self._api.router.add_event_handler('startup', self.__startup__)
        self._api.router.add_event_handler('shutdown', self.__shutdown__)

    @property
    def title(self): return self._title

    @property
    def version(self): return self._version

    @property
    def uri(self): return self._uri

    @property
    def api(self): return self._api

    async def __startup__(self):
        LOG.INFO('run startup')
        await self.startup()
        if self._background: await runBackground(self.__background__())
        self._api.add_api_route(
            methods=['GET'],
            path=f'/{self._title}/health',
            endpoint=self.__health__,
            response_model=ServiceHealth,
            tags=['Service'],
            name='Health'
        )
        LOG.INFO('startup finished')

    async def startup(self): pass

    async def __shutdown__(self):
        LOG.INFO('run shutdown')
        await self.shutdown()
        LOG.INFO('shutdown finished')

    async def shutdown(self): pass

    async def __background__(self):
        LOG.INFO('run background')
        while self._background: await self.background()
        LOG.INFO('background finished')

    async def background(self): await asleep(1)

    async def __health__(self) -> ServiceHealth: return await self.health()

    async def health(self) -> ServiceHealth: return ServiceHealth(title=self.title, status='OK', healthy=True)


#===============================================================================
# Mesh Control
#===============================================================================
class MeshControl(BaseControl):

    def __init__(self, confPath, background:bool=False):
        BaseControl.__init__(self, confPath, background)
        if 'providers' not in self.config: raise Exception('[providers] configuration is not in module.ini')
        self._providers = self.config['providers']

    async def registerModel(self, schema:BaseSchema, service):
        if service not in self._providers: raise Exception(f'{service} is not in [providers] configuration')
        schema.setSchemaInfo(self._providers[service], service, self._version)
        return self


#===============================================================================
# Uerp Control
#===============================================================================
class UerpControl(BaseControl):

    def __init__(self, confPath, background:bool=False, authDriver:Any=None, cacheDriver:Any=None, searchDriver:Any=None, databaseDriver:Any=None):
        BaseControl.__init__(self, confPath, background)

        self._uerpRefreshRBACInterval = int(self.config['auth']['refresh_rbac_interval'])
        self._uerpRefreshInfoInterval = int(self.config['auth']['refresh_info_interval'])
        self._uerpPathToSchemaMap = {}

        self._auth = authDriver(self.config) if authDriver else None
        self._cache = cacheDriver(self.config) if cacheDriver else None
        self._search = searchDriver(self.config) if searchDriver else None
        self._database = databaseDriver(self.config) if databaseDriver else None

    async def __startup__(self):
        if self._auth: await self._auth.connect()
        if self._database: await self._database.connect()
        if self._search: await self._search.connect()
        if self._cache: await self._cache.connect()

        await BaseControl.__startup__(self)

        self._refreshRBAC = True
        await self.registerModel(RBAC)
        await runBackground(self.__refresh_info__())
        await runBackground(self.__refresh_rbac__())

    async def __refresh_info__(self):
        while self._refreshRBAC:
            try: await self._auth.refreshInfos()
            except Exception as e:
                LOG.ERROR(e)
                traceback.print_exc()
            await asleep(self._uerpRefreshInfoInterval)

    async def __refresh_rbac__(self):
        search = Search()
        while self._refreshRBAC:
            try:
                if self._database:
                    rbacs = await self._database.search(RBAC, search)
                    if rbacs:
                        if self._auth: await self._auth.refreshRBACs(rbacs)
                        if self._cache: await self._cache.create(RBAC, *rbacs)
                        if self._search: await self._search.create(RBAC, *rbacs)
                elif self._search:
                    rbacs = await self._search.search(RBAC, search)
                    if rbacs:
                        if self._auth: await self._auth.refreshRBACs(rbacs)
                        if self._cache: await self._cache.create(RBAC, *rbacs)
            except Exception as e:
                LOG.ERROR(e)
                traceback.print_exc()
            await asleep(self._uerpRefreshRBACInterval)

    async def __shutdown__(self):
        self._refreshRBAC = False
        await BaseControl.__shutdown__(self)
        if self._database: await self._database.disconnect()
        if self._search: await self._search.disconnect()
        if self._cache: await self._cache.disconnect()
        if self._auth: await self._auth.disconnect()

    async def registerModel(self, schema:BaseSchema):
        schema.setSchemaInfo(None, self.title, self.version)
        schemaInfo = schema.getSchemaInfo()

        if LAYER.checkDatabase(schemaInfo.layer) and self._database: await self._database.registerModel(schema)
        if LAYER.checkSearch(schemaInfo.layer) and self._search: await self._search.registerModel(schema)
        if LAYER.checkCache(schemaInfo.layer) and self._cache: await self._cache.registerModel(schema)

        self._uerpPathToSchemaMap[schemaInfo.path] = schema

        if AAA.checkAuthorization(schemaInfo.aaa):
            if CRUD.checkCreate(schemaInfo.crud):
                self.__create_data_with_auth__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['POST'], path=schemaInfo.path, endpoint=self.__create_data_with_auth__, response_model=schema, tags=schemaInfo.tags, name=f'Create {schemaInfo.name}')
                self.__create_data_with_auth__.__annotations__['model'] = BaseModel
            if CRUD.checkRead(schemaInfo.crud):
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path, endpoint=self.__search_data_with_auth__, response_model=List[schema], tags=schemaInfo.tags, name=f'Search {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/count', endpoint=self.__count_data_with_auth__, response_model=ModelCount, tags=schemaInfo.tags, name=f'Count {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/{id}', endpoint=self.__read_data_with_auth__, response_model=schema, tags=schemaInfo.tags, name=f'Read {schemaInfo.name}')
            if CRUD.checkUpdate(schemaInfo.crud):
                self.__update_data_with_auth__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['PUT'], path=schemaInfo.path + '/{id}', endpoint=self.__update_data_with_auth__, response_model=schema, tags=schemaInfo.tags, name=f'Update {schemaInfo.name}')
                self.__update_data_with_auth__.__annotations__['model'] = BaseModel
            if CRUD.checkDelete(schemaInfo.crud):
                self.api.add_api_route(methods=['DELETE'], path=schemaInfo.path + '/{id}', endpoint=self.__delete_data_with_auth__, response_model=ModelStatus, tags=schemaInfo.tags, name=f'Delete {schemaInfo.name}')
        else:
            if CRUD.checkCreate(schemaInfo.crud):
                self.__create_data_with_free__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['POST'], path=schemaInfo.path, endpoint=self.__create_data_with_free__, response_model=schema, tags=schemaInfo.tags, name=f'Create {schemaInfo.name}')
                self.__create_data_with_free__.__annotations__['model'] = BaseModel
            if CRUD.checkRead(schemaInfo.crud):
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path, endpoint=self.__search_data_with_free__, response_model=List[schema], tags=schemaInfo.tags, name=f'Search {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/count', endpoint=self.__count_data_with_free__, response_model=ModelCount, tags=schemaInfo.tags, name=f'Count {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/{id}', endpoint=self.__read_data_with_free__, response_model=schema, tags=schemaInfo.tags, name=f'Read {schemaInfo.name}')
            if CRUD.checkUpdate(schemaInfo.crud):
                self.__update_data_with_free__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['PUT'], path=schemaInfo.path + '/{id}', endpoint=self.__update_data_with_free__, response_model=schema, tags=schemaInfo.tags, name=f'Update {schemaInfo.name}')
                self.__update_data_with_free__.__annotations__['model'] = BaseModel
            if CRUD.checkDelete(schemaInfo.crud):
                self.api.add_api_route(methods=['DELETE'], path=schemaInfo.path + '/{id}', endpoint=self.__delete_data_with_free__, response_model=ModelStatus, tags=schemaInfo.tags, name=f'Delete {schemaInfo.name}')

        return self

    async def __read_data_with_auth__(
        self,
        request:Request,
        background:BackgroundTasks,
        id:ID,
        token: AUTH_HEADER,
        org: ORG_HEADER=None
    ):
        id = str(id)
        schema = self._uerpPathToSchemaMap[request.scope['path'].replace(f'/{id}', '')]
        authInfo = await self._auth.getAuthInfo(token.credentials, org)
        schemaInfo = schema.getSchemaInfo()

        if AAA.checkAuthentication(schemaInfo.aaa) and not authInfo.checkAdmin() and not authInfo.checkReadACL(schemaInfo.sref): raise EpException(403, 'Forbidden')
        model = await self.__read_data__(
            background,
            schema,
            id
        )
        if not authInfo.checkOrg(model.org): raise EpException(404, 'Not Found')
        if AAA.checkAccount(schemaInfo.aaa) and not authInfo.checkUsername(model.owner): raise EpException(403, 'Forbidden')
        return model

    async def __read_data_with_free__(
        self,
        request:Request,
        background:BackgroundTasks,
        id:ID
    ):
        return await self.__read_data__(
            background,
            self._uerpPathToSchemaMap[request.scope['path'].replace(f'/{id}', '')],
            str(id)
        )

    async def __read_data__(
        self,
        background,
        schema,
        id
    ):
        schemaInfo = schema.getSchemaInfo()

        if LAYER.checkCache(schemaInfo.layer) and self._cache:
            try: return schema(**(await self._cache.read(schema, id)))
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        if LAYER.checkSearch(schemaInfo.layer) and self._search:
            try:
                model = await self._search.read(schema, id)
                if model and background and LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.create, schema, model)
                return schema(**model)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        if LAYER.checkDatabase(schemaInfo.layer) and self._database:
            try:
                model = await self._database.read(schema, id)
                if model and background:
                    if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.create, schema, model)
                    if LAYER.checkSearch(schemaInfo.layer) and self._search: background.add_task(self._search.create, schema, model)
                return schema(**model)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')

        raise EpException(501, 'Not Implemented')

    async def __search_data_with_auth__(
        self,
        request:Request,
        background:BackgroundTasks,
        token:AUTH_HEADER,
        org:ORG_HEADER=None,
        fields:Annotated[List[str] | None, Query(alias='$f', description='looking fields ex) $f=field1&$f=field2')]=None,
        filter:Annotated[str | None, Query(alias='$filter', description='lucene type filter ex) $filter=fieldName:yourSearchText')]=None,
        orderBy:Annotated[str | None, Query(alias='$orderby', description='ordered by specific field')]=None,
        order:Annotated[Literal['asc', 'desc'], Query(alias='$order', description='ordering type')]=None,
        size:Annotated[int | None, Query(alias='$size', description='retrieving model count')]=None,
        skip:Annotated[int | None, Query(alias='$skip', description='skipping model count')]=None,
        archive:Annotated[Literal['true', 'false', ''], Query(alias='$archive', description='searching from archive aka database')]=None
    ):
        query = request.query_params._dict
        if '$f' in query: query.pop('$f')
        if '$filter' in query: query.pop('$filter')
        if '$orderby' in query: query.pop('$orderby')
        if '$order' in query: query.pop('$order')
        if '$size' in query: query.pop('$size')
        if '$skip' in query: query.pop('$skip')
        if '$archive' in query: query.pop('$archive')
        if orderBy and not order: order = 'desc'
        if size: size = int(size)
        if skip: skip = int(skip)
        if archive == '': archive = True
        elif archive: archive = bool(archive)

        schema = self._uerpPathToSchemaMap[request.scope['path']]
        authInfo = await self._auth.getAuthInfo(token.credentials, org)
        schemaInfo = schema.getSchemaInfo()

        if AAA.checkAuthentication(schemaInfo.aaa) and not authInfo.checkAdmin() and not authInfo.checkReadACL(schemaInfo.sref): raise EpException(403, 'Forbidden')
        if AAA.checkAccount(schemaInfo.aaa): query['owner'] = authInfo.username
        query['org'] = authInfo.org
        qFilter = []
        for key, val in query.items(): qFilter.append(f'{key}:{val}')
        qFilter = ' AND '.join(qFilter)
        if filter: filter = f'({qFilter}) AND ({filter})'
        else: filter = qFilter
        filter = parseLucene.parse(filter)

        return await self.__search_data__(
            background,
            schema,
            Search(fields=fields, filter=filter, orderBy=orderBy, order=order, size=size, skip=skip),
            archive
        )

    async def __search_data_with_free__(
        self,
        request:Request,
        background:BackgroundTasks,
        fields:Annotated[List[str] | None, Query(alias='$f', description='looking fields ex) $f=field1&$f=field2')]=None,
        filter:Annotated[str | None, Query(alias='$filter', description='lucene type filter ex) $filter=fieldName:yourSearchText')]=None,
        orderBy:Annotated[str | None, Query(alias='$orderby', description='ordered by specific field')]=None,
        order:Annotated[Literal['asc', 'desc'], Query(alias='$order', description='ordering type')]=None,
        size:Annotated[int | None, Query(alias='$size', description='retrieving model count')]=None,
        skip:Annotated[int | None, Query(alias='$skip', description='skipping model count')]=None,
        archive:Annotated[Literal['true', 'false', ''], Query(alias='$archive', description='searching from archive aka database')]=None
    ):
        query = request.query_params._dict
        if '$f' in query: query.pop('$f')
        if '$filter' in query: query.pop('$filter')
        if '$orderby' in query: query.pop('$orderby')
        if '$order' in query: query.pop('$order')
        if '$size' in query: query.pop('$size')
        if '$skip' in query: query.pop('$skip')
        if '$archive' in query: query.pop('$archive')
        if orderBy and not order: order = 'desc'
        if size: size = int(size)
        if skip: skip = int(skip)
        if archive == '': archive = True
        elif archive: archive = bool(archive)

        if query:
            qFilter = []
            for key, val in query.items(): qFilter.append(f'{key}:{val}')
            qFilter = ' AND '.join(qFilter)
            if filter: filter = f'({qFilter}) AND ({filter})'
            else: filter = qFilter
        if filter: filter = parseLucene.parse(filter)

        return await self.__search_data__(
            background,
            self._uerpPathToSchemaMap[request.scope['path']],
            Search(fields=fields, filter=filter, orderBy=orderBy, order=order, size=size, skip=skip),
            archive
        )

    async def __search_data__(
        self,
        background,
        schema,
        search,
        archive
    ):
        schemaInfo = schema.getSchemaInfo()

        if archive and LAYER.checkDatabase(schemaInfo.layer) and self._database:
            try:
                models = await self._database.search(schema, search)
                if models and not search.fields and LAYER.checkSearch(schemaInfo.layer) and self._search: background.add_task(self._search.create, schema, *models)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if LAYER.checkSearch(schemaInfo.layer) and self._search:
                    try: models = await self._search(schema, search)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                else: raise EpException(501, 'Not Implemented')
            if models and not search.fields and LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.create, schema, *models)
            return models
        elif LAYER.checkSearch(schemaInfo.layer) and self._search:
            try: models = await self._search.search(schema, search)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if LAYER.checkDatabase(schemaInfo.layer) and self._database:
                    try: models = await self._database.search(schema, search)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                    if models and not search.fields and LAYER.checkSearch(schemaInfo.layer) and self._search: background.add_task(self._search.create, schema, *models)
                else: raise EpException(501, 'Not Implemented')
            if models and not search.fields and LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.create, schema, *models)
            return models

        raise EpException(501, 'Not Implemented')

    async def __count_data_with_auth__(
        self,
        request:Request,
        token: AUTH_HEADER,
        org: ORG_HEADER=None,
        filter:Annotated[str | None, Query(alias='$filter', description='lucene type filter ex) $filter=fieldName:yourSearchText')]=None,
        archive:Annotated[Literal['true', 'false', ''], Query(alias='$archive', description='searching from archive aka database')]=None
    ):
        query = request.query_params._dict
        if '$filter' in query: query.pop('$filter')
        if '$archive' in query: query.pop('$archive')
        if archive == '': archive = True
        elif archive: archive = bool(archive)

        uref = request.scope['path']
        path = uref.replace('/count', '')
        queryString = request.scope['query_string']

        schema = self._uerpPathToSchemaMap[path]
        authInfo = await self._auth.getAuthInfo(token.credentials, org)
        schemaInfo = schema.getSchemaInfo()
        if AAA.checkAuthentication(schemaInfo.aaa) and not authInfo.checkAdmin() and not authInfo.checkReadACL(schemaInfo.sref): raise EpException(403, 'Forbidden')
        if AAA.checkAccount(schemaInfo.aaa): query['owner'] = authInfo.username
        query['org'] = authInfo.org

        qFilter = []
        for key, val in query.items(): qFilter.append(f'{key}:{val}')
        qFilter = ' AND '.join(qFilter)
        if filter: filter = f'({qFilter}) AND ({filter})'
        else: filter = qFilter
        filter = parseLucene.parse(filter)

        return ModelCount(sref=schema.sref, uref=uref, query=queryString, result=await self.__count_data__(
            schema,
            Search(filter=filter),
            archive
        ))

    async def __count_data_with_free__(
        self,
        request:Request,
        filter:Annotated[str | None, Query(alias='$filter', description='lucene type filter ex) $filter=fieldName:yourSearchText')]=None,
        archive:Annotated[Literal['true', 'false', ''], Query(alias='$archive', description='searching from archive aka database')]=None
    ):
        query = request.query_params._dict
        if '$filter' in query: query.pop('$filter')
        if '$archive' in query: query.pop('$archive')
        if archive == '': archive = True
        elif archive: archive = bool(archive)

        uref = request.scope['path']
        path = uref.replace('/count', '')
        queryString = request.scope['query_string']

        if query:
            qFilter = []
            for key, val in query.items(): qFilter.append(f'{key}:{val}')
            qFilter = ' AND '.join(qFilter)
            if filter: filter = f'({qFilter}) AND ({filter})'
            else: filter = qFilter
        if filter: filter = parseLucene.parse(filter)

        schema = self._uerpPathToSchemaMap[path]
        return ModelCount(sref=schema.sref, uref=uref, query=queryString, result=await self.__count_data__(
            schema,
            Search(filter=filter),
            archive
        ))

    async def __count_data__(
        self,
        schema,
        search,
        archive
    ):
        schemaInfo = schema.getSchemaInfo()

        if archive and LAYER.checkDatabase(schemaInfo.layer) and self._database:
            try: return await self._database.count(schema, search)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if LAYER.checkSearch(schemaInfo.layer) and self._search:
                    try: return await self._search.count(schema, search)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                else: raise EpException(501, 'Not Implemented')
        elif LAYER.checkSearch(schemaInfo.layer) and self._search:
            try: return await self._search.count(schema, search)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if LAYER.checkDatabase(schemaInfo.layer) and self._database:
                    try: return await self._database.count(schema, search)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                else: raise EpException(501, 'Not Implemented')

        raise EpException(501, 'Not Implemented')

    async def __create_data_with_auth__(
        self,
        background:BackgroundTasks,
        model:BaseModel,
        token: AUTH_HEADER,
        org: ORG_HEADER=None
    ):
        schema = model.__class__
        authInfo = await self._auth.getAuthInfo(token.credentials, org)
        schemaInfo = schema.getSchemaInfo()
        if AAA.checkAuthentication(schemaInfo.aaa) and not authInfo.checkAdmin() and not authInfo.checkCreateACL(schemaInfo.sref): raise EpException(403, 'Forbidden')
        await self.__create_data__(background, schema, model.setID().updateStatus(org=authInfo.org, owner=authInfo.username).model_dump())
        return model

    async def __create_data_with_free__(
        self,
        background:BackgroundTasks,
        model:BaseModel
    ):
        await self.__create_data__(background, model.__class__, model.setID().updateStatus().model_dump())
        return model

    async def __create_data__(
        self,
        background,
        schema,
        data
    ):
        schemaInfo = schema.getSchemaInfo()

        if LAYER.checkDatabase(schemaInfo.layer) and self._database:
            try:
                if (await self._database.create(schema, data))[0]:
                    if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.create, schema, data)
                    if LAYER.checkSearch(schemaInfo.layer) and self._search: background.add_task(self._search.create, schema, data)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif LAYER.checkSearch(schemaInfo.layer) and self._search:
            try:
                await self._search.create(schema, data)
                if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.create, schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif LAYER.checkCache(schemaInfo.layer) and self._cache:
            try: await self._cache.create(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        else: raise EpException(501, 'Not Implemented')

    async def __update_data_with_auth__(
        self,
        background:BackgroundTasks,
        id:ID,
        model:BaseModel,
        token: AUTH_HEADER,
        org: ORG_HEADER=None
    ):
        id = str(id)
        schema = model.__class__
        authInfo = await self._auth.getAuthInfo(token.credentials, org)
        schemaInfo = schema.getSchemaInfo()
        if AAA.checkAuthentication(schemaInfo.aaa) and not authInfo.checkAdmin() and not authInfo.checkUpdateACL(schemaInfo.sref): raise EpException(403, 'Forbidden')
        origin = await self.__read_data__(None, schema, id)
        if not authInfo.checkOrg(origin.org): raise EpException(403, 'Forbidden')
        if AAA.checkAccount(schemaInfo.aaa) and not authInfo.checkUsername(origin.owner): raise EpException(403, 'Forbidden')
        await self.__update_data__(background, schema, model.setID(id).updateStatus(org=authInfo.org, owner=authInfo.username).model_dump())
        return model

    async def __update_data_with_free__(
        self,
        background:BackgroundTasks,
        id:ID,
        model:BaseModel
    ):
        schema = model.__class__
        await self.__update_data__(background, schema, model.setID(str(id)).updateStatus().model_dump())
        return model

    async def __update_data__(
        self,
        background,
        schema,
        data
    ):
        schemaInfo = schema.getSchemaInfo()

        if LAYER.checkDatabase(schemaInfo.layer) and self._database:
            try:
                if (await self._database.update(schema, data))[0]:
                    if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.update, schema, data)
                    if LAYER.checkSearch(schemaInfo.layer) and self._search: background.add_task(self._search.update, schema, data)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif LAYER.checkSearch(schemaInfo.layer) and self._search:
            try:
                await self._search.update(schema, data)
                if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.update, schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif LAYER.checkCache(schemaInfo.layer) and self._cache:
            try: await self._cache.update(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        else: raise EpException(501, 'Not Implemented')

    async def __delete_data_with_auth__(
        self,
        request:Request,
        background:BackgroundTasks,
        id:ID,
        token: AUTH_HEADER,
        org: ORG_HEADER=None,
        force:Annotated[Literal['true', 'false', ''], Query(alias='$force')]=None,
    ):
        if force == '': force = True
        elif force: force = bool(force)

        id = str(id)
        uref = request.scope['path']
        path = uref.replace(f'/{id}', '')
        schema = self._uerpPathToSchemaMap[path]
        authInfo = await self._auth.getAuthInfo(token.credentials, org)
        schemaInfo = schema.getSchemaInfo()
        if AAA.checkAuthentication(schemaInfo.aaa) and not authInfo.checkAdmin() and not authInfo.checkDeleteACL(schemaInfo.sref): raise EpException(403, 'Forbidden')
        origin = await self.__read_data__(None, schema, id)
        if not authInfo.checkOrg(origin.org): raise EpException(403, 'Forbidden')
        if AAA.checkAccount(schemaInfo.aaa) and not authInfo.checkUsername(origin.owner): raise EpException(403, 'Forbidden')
        await self.__delete_data__(background, schema, id, origin.setID(id).updateStatus(org=authInfo.org, owner=authInfo.username, deleted=True).model_dump(), force)
        return ModelStatus(id=id, sref=schemaInfo.sref, uref=uref, status='deleted')

    async def __delete_data_with_free__(
        self,
        request:Request,
        background:BackgroundTasks,
        id:ID,
        force:Annotated[Literal['true', 'false', ''], Query(alias='$force')]=None
    ):
        if force == '': force = True
        elif force: force = bool(force)

        id = str(id)
        uref = request.scope['path']
        path = uref.replace(f'/{id}', '')
        schema = self._uerpPathToSchemaMap[path]
        schemaInfo = schema.getSchemaInfo()
        model = await self.__read_data__(None, schema, id)
        await self.__delete_data__(background, schema, id, model.setID(id).updateStatus(deleted=True).model_dump(), force)
        return ModelStatus(id=id, sref=schemaInfo.sref, uref=uref, status='deleted')

    async def __delete_data__(
        self,
        background,
        schema,
        id,
        data,
        force
    ):
        schemaInfo = schema.getSchemaInfo()

        if force and LAYER.checkDatabase(schemaInfo.layer) and self._database:
            try:
                if await self._database.delete(schema, id):
                    if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.delete, schema, id)
                    if LAYER.checkSearch(schemaInfo.layer) and self._search: background.add_task(self._search.delete, schema, id)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif LAYER.checkDatabase(schemaInfo.layer) and self._database:
            try:
                if (await self._database.update(schema, data))[0]:
                    if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.delete, schema, id)
                    if LAYER.checkSearch(schemaInfo.layer) and self._search: background.add_task(self._search.delete, schema, id)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif LAYER.checkSearch(schemaInfo.layer) and self._search:
            try:
                await self._search.delete(schema, id)
                if LAYER.checkCache(schemaInfo.layer) and self._cache: background.add_task(self._cache.delete, schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif LAYER.checkCache(schemaInfo.layer) and self._cache:
            try: await self._cache.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        else: raise EpException(501, 'Not Implemented')
