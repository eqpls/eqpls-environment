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
from fastapi import Request, BackgroundTasks, Query, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, APIKeyHeader
from pydantic import BaseModel
from luqum.parser import parser as parseLucene
from stringcase import snakecase

from .constants import AuthLevel, AuthorizationHeader, RealmHeader
from .exceptions import EpException
from .schedules import asleep, runBackground
from .models import ServiceHealth, ModelStatus, ModelCount, ID, BaseSchema, SearchOption, Policy


#===============================================================================
# Base Control
#===============================================================================
class BaseControl:

    def __init__(self, api, config, background=False):
        self.api = api
        self.api.router.add_event_handler("startup", self.__startup__)
        self.api.router.add_event_handler("shutdown", self.__shutdown__)
        self.config = config
        self._background = background
        self._title = snakecase(config['default']['title'])
        self._version = int(config['service']['api_version'])

    @property
    def title(self): return self._title

    @property
    def version(self): return self._version

    async def __startup__(self):
        await self.startup()
        if self._background: await runBackground(self.__background__())
        self.api.add_api_route(
            methods=['GET'],
            path=f'/{snakecase(self.title)}/health',
            endpoint=self.__health__,
            response_model=ServiceHealth,
            tags=['Health'],
            name='Health'
        )

    async def __shutdown__(self):
        await self.shutdown()

    async def __background__(self):
        while self._background: await self.background()

    async def __health__(self) -> ServiceHealth: return await self.health()

    async def startup(self): pass

    async def shutdown(self): pass

    async def health(self) -> ServiceHealth: return ServiceHealth(title=self.title, status='OK', healthy=True)

    async def background(self):
        LOG.INFO('run background process')
        await asleep(1)


#===============================================================================
# Mesh Control
#===============================================================================
class MeshControl(BaseControl):

    def __init__(self, api, config, background:bool=False):
        BaseControl.__init__(self, api, config, background)
        if 'providers' not in self.config: raise Exception('[providers] configuration is not in module.conf')
        self.providers = self.config['providers']

    async def registerModel(self, schema:BaseSchema, service):
        if service not in self.providers: raise Exception(f'{service} is not in [providers] configuration')
        schema.setSchemaInfo(self.providers[service], service, self.version)
        return self


#===============================================================================
# Uerp Control
#===============================================================================
realmHeader = APIKeyHeader(name='Realm', auto_error=False)
authorizationHeader = HTTPBearer()


class UerpControl(BaseControl):

    def __init__(self, api, config, background:bool=False, authDriver:Any=None, cacheDriver:Any=None, searchDriver:Any=None, databaseDriver:Any=None):
        BaseControl.__init__(self, api, config, background)

        self._uerpUpdatePolicySec = int(config['service']['update_policy_sec'])

        self._uerpPathToSchemaMap = {}

        self._uerpAuthDriver = authDriver
        self._uerpCacheDriver = cacheDriver
        self._uerpSearchDriver = searchDriver
        self._uerpDatabaseDriver = databaseDriver

        self._auth = None
        self._cache = None
        self._search = None
        self._database = None

    async def __startup__(self):
        if self._uerpAuthDriver and not self._auth:
            self._auth = await self._uerpAuthDriver(self.config).connect()
        if self._uerpDatabaseDriver and not self._database:
            self._database = await self._uerpDatabaseDriver(self.config).connect()
        if self._uerpSearchDriver and not self._search:
            self._search = await self._uerpSearchDriver(self.config).connect()
        if self._uerpCacheDriver and not self._cache:
            self._cache = await self._uerpCacheDriver(self.config).connect()

        await BaseControl.__startup__(self)

        await self.registerModel(Policy)
        await runBackground(self.__load_policies__())

    async def __shutdown__(self):
        await BaseControl.__shutdown__(self)
        if self._database: await self._database.disconnect()
        if self._search: await self._search.disconnect()
        if self._cache: await self._cache.disconnect()
        if self._auth: await self._auth.disconnect()

    async def __load_policies__(self):
        while True:
            try:
                option = SearchOption()
                if self._database:
                    policies = await self._database.search(Policy, option)
                    if policies:
                        if self._search: await self._search.create(Policy, *policies)
                        if self._cache: await self._cache.create(Policy, *policies)
                        if self._auth: await self._auth.updatePolicyMap(policies)
                elif self._search:
                    policies = await self._search.search(Policy, option)
                    if policies:
                        if self._cache: await self._cache.create(Policy, *policies)
                        if self._auth: await self._auth.updatePolicyMap(policies)
            except Exception as e:
                LOG.ERROR(e)
                traceback.print_exc()
            await asleep(self._uerpUpdatePolicySec)

    async def registerModel(self, schema:BaseSchema):
        schema.setSchemaInfo(None, self.title, self.version)
        schemaInfo = schema.getSchemaInfo()

        if 'd' in schemaInfo.layer and self._database: await self._database.registerModel(schema)
        if 's' in schemaInfo.layer and self._search: await self._search.registerModel(schema)
        if 'c' in schemaInfo.layer and self._cache: await self._cache.registerModel(schema)

        self._uerpPathToSchemaMap[schemaInfo.path] = schema

        if 'c' in schemaInfo.crud:
            if AuthLevel.checkAuthorization(schemaInfo.auth):
                self.__create_data_with_auth__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['POST'], path=schemaInfo.path, endpoint=self.__create_data_with_auth__, response_model=schema, tags=schemaInfo.tags, name=f'Create {schemaInfo.name}')
                self.__create_data_with_auth__.__annotations__['model'] = BaseModel
            else:
                self.__create_data_with_free__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['POST'], path=schemaInfo.path, endpoint=self.__create_data_with_free__, response_model=schema, tags=schemaInfo.tags, name=f'Create {schemaInfo.name}')
                self.__create_data_with_free__.__annotations__['model'] = BaseModel
        if 'r' in schemaInfo.crud:
            if AuthLevel.checkAuthorization(schemaInfo.auth):
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path, endpoint=self.__search_data_with_auth__, response_model=List[Any], tags=schemaInfo.tags, name=f'Search {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/count', endpoint=self.__count_data_with_auth__, response_model=ModelCount, tags=schemaInfo.tags, name=f'Count {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/{id}', endpoint=self.__read_data_with_auth__, response_model=schema, tags=schemaInfo.tags, name=f'Read {schemaInfo.name}')
            else:
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path, endpoint=self.__search_data_with_free__, response_model=List[Any], tags=schemaInfo.tags, name=f'Search {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/count', endpoint=self.__count_data_with_free__, response_model=ModelCount, tags=schemaInfo.tags, name=f'Count {schemaInfo.name}')
                self.api.add_api_route(methods=['GET'], path=schemaInfo.path + '/{id}', endpoint=self.__read_data_with_free__, response_model=schema, tags=schemaInfo.tags, name=f'Read {schemaInfo.name}')
        if 'u' in schemaInfo.crud:
            if AuthLevel.checkAuthorization(schemaInfo.auth):
                self.__update_data_with_auth__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['PUT'], path=schemaInfo.path + '/{id}', endpoint=self.__update_data_with_auth__, response_model=schema, tags=schemaInfo.tags, name=f'Update {schemaInfo.name}')
                self.__update_data_with_auth__.__annotations__['model'] = BaseModel
            else:
                self.__update_data_with_free__.__annotations__['model'] = schema
                self.api.add_api_route(methods=['PUT'], path=schemaInfo.path + '/{id}', endpoint=self.__update_data_with_free__, response_model=schema, tags=schemaInfo.tags, name=f'Update {schemaInfo.name}')
                self.__update_data_with_free__.__annotations__['model'] = BaseModel
        if 'd' in schemaInfo.crud:
            if AuthLevel.checkAuthorization(schemaInfo.auth):
                self.api.add_api_route(methods=['DELETE'], path=schemaInfo.path + '/{id}', endpoint=self.__delete_data_with_auth__, response_model=ModelStatus, tags=schemaInfo.tags, name=f'Delete {schemaInfo.name}')
            else:
                self.api.add_api_route(methods=['DELETE'], path=schemaInfo.path + '/{id}', endpoint=self.__delete_data_with_free__, response_model=ModelStatus, tags=schemaInfo.tags, name=f'Delete {schemaInfo.name}')

        return self

    async def __read_data_with_auth__(
        self,
        request:Request,
        background:BackgroundTasks,
        id:ID,
        token: AuthorizationHeader,
        realm: RealmHeader=None
    ):
        schema = self._uerpPathToSchemaMap[request.scope['path'].replace(f'/{id}', '')]
        authInfo = await self._auth.getAuthInfo(realm, token)
        schemaInfo = schema.getSchemaInfo()

        if AuthLevel.checkAuthentication(schemaInfo.auth) and not authInfo.checkAdmin() and not authInfo.checkReadAllowed(schemaInfo.sref): raise EpException(403, 'Forbidden')
        model = await self.__read_data__(background, schema, str(id))
        if not authInfo.checkRealm(model.realm): raise EpException(404, 'Not Found')
        if AuthLevel.checkAccount(schemaInfo.auth) and not authInfo.checkUsername(model.owner): raise EpException(403, 'Forbidden')
        return model

    async def __read_data_with_free__(
        self,
        request:Request,
        background:BackgroundTasks,
        id:ID
    ):
        schema = self._uerpPathToSchemaMap[request.scope['path'].replace(f'/{id}', '')]
        return await self.__read_data__(background, schema, str(id))

    async def __read_data__(
        self,
        background,
        schema,
        id
    ):
        schemaInfo = schema.getSchemaInfo()
        if schemaInfo.cache:
            try: model = await schemaInfo.cache.read(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
            if model: return schema(**model)
        if schemaInfo.search:
            try: model = await schemaInfo.search.read(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
            if model:
                if schemaInfo.cache and background: background.add_task(schemaInfo.cache.create, schema, model)
                return schema(**model)
        if schemaInfo.database:
            try: model = await schemaInfo.database.read(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
            if model:
                if schemaInfo.cache and background: background.add_task(schemaInfo.cache.create, schema, model)
                if schemaInfo.search and background: background.add_task(schemaInfo.search.create, schema, model)
                return schema(**model)
        raise EpException(404, 'Not Found')

    async def __search_data_with_auth__(
        self,
        request:Request,
        background:BackgroundTasks,
        token: AuthorizationHeader,
        realm: RealmHeader=None,
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
        authInfo = await self._auth.getAuthInfo(realm, token)
        schemaInfo = schema.getSchemaInfo()
        if AuthLevel.checkAuthentication(schemaInfo.auth) and not authInfo.checkAdmin() and not authInfo.checkReadAllowed(schemaInfo.sref): raise EpException(403, 'Forbidden')
        if AuthLevel.checkAccount(schemaInfo.auth): query['owner'] = authInfo.username
        query['realm'] = authInfo.realm
        qFilter = []
        for key, val in query.items(): qFilter.append(f'{key}:{val}')
        qFilter = ' AND '.join(qFilter)
        if filter: filter = f'({qFilter}) AND ({filter})'
        else: filter = qFilter
        filter = parseLucene.parse(filter)
        return await self.__search_data__(
            background,
            schema,
            SearchOption(fields=fields, filter=filter, orderBy=orderBy, order=order, size=size, skip=skip),
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
        schema = self._uerpPathToSchemaMap[request.scope['path']]
        return await self.__search_data__(
            background,
            schema,
            SearchOption(fields=fields, filter=filter, orderBy=orderBy, order=order, size=size, skip=skip),
            archive
        )

    async def __search_data__(
        self,
        background,
        schema,
        option,
        archive
    ):
        schemaInfo = schema.getSchemaInfo()
        if archive and schemaInfo.database:
            try: models = await schemaInfo.database.search(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if schemaInfo.search:
                    try: models = await schemaInfo.search.search(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Not Implemented')
            else:
                if models and schemaInfo.search and not option.fields: background.add_task(schemaInfo.search.create, schema, *models)
            if models and schemaInfo.cache and not option.fields: background.add_task(schemaInfo.cache.create, schema, *models)
            return models
        elif schemaInfo.search:
            try: models = await schemaInfo.search.search(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if schemaInfo.database:
                    try: models = await schemaInfo.database.search(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                    if models and schemaInfo.search and not option.fields: background.add_task(schemaInfo.search.create, schema, *models)
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Not Implemented')
            if models and schemaInfo.cache and not option.fields: background.add_task(schemaInfo.cache.create, schema, *models)
            return models
        raise EpException(501, 'Not Implemented')

    async def __count_data_with_auth__(
        self,
        request:Request,
        token: AuthorizationHeader,
        realm: RealmHeader=None,
        filter:Annotated[str | None, Query(alias='$filter', description='lucene type filter ex) $filter=fieldName:yourSearchText')]=None,
        archive:Annotated[Literal['true', 'false', ''], Query(alias='$archive', description='searching from archive aka database')]=None
    ):
        query = request.query_params._dict
        if '$filter' in query: query.pop('$filter')
        if '$archive' in query: query.pop('$archive')
        if archive == '': archive = True
        elif archive: archive = bool(archive)

        path = request.scope['path'].replace('/count', '')
        qstr = request.scope['query_string']

        schema = self._uerpPathToSchemaMap[path]
        authInfo = await self._auth.getAuthInfo(realm, token)
        schemaInfo = schema.getSchemaInfo()
        if AuthLevel.checkAuthentication(schemaInfo.auth) and not authInfo.checkAdmin() and not authInfo.checkReadAllowed(schemaInfo.sref): raise EpException(403, 'Forbidden')
        if AuthLevel.checkAccount(schemaInfo.auth): query['owner'] = authInfo.username
        query['realm'] = authInfo.realm

        qFilter = []
        for key, val in query.items(): qFilter.append(f'{key}:{val}')
        qFilter = ' AND '.join(qFilter)
        if filter: filter = f'({qFilter}) AND ({filter})'
        else: filter = qFilter
        filter = parseLucene.parse(filter)

        result = await self.__count_data__(
            schema,
            SearchOption(filter=filter),
            archive
        )
        return ModelCount(path=path, query=qstr, result=result)

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

        path = request.scope['path'].replace('/count', '')
        qstr = request.scope['query_string']

        if query:
            qFilter = []
            for key, val in query.items(): qFilter.append(f'{key}:{val}')
            qFilter = ' AND '.join(qFilter)
            if filter: filter = f'({qFilter}) AND ({filter})'
            else: filter = qFilter
        if filter: filter = parseLucene.parse(filter)

        schema = self._uerpPathToSchemaMap[path]
        result = await self.__count_data__(
            schema,
            SearchOption(filter=filter),
            archive
        )
        return ModelCount(path=path, query=qstr, result=result)

    async def __count_data__(
        self,
        schema,
        option,
        archive
    ):
        schemaInfo = schema.getSchemaInfo()
        if archive and schemaInfo.database:
            try: return await schemaInfo.database.count(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if schemaInfo.search:
                    try: return await schemaInfo.search.count(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Not Implemented')  # no driver
        elif schemaInfo.search:
            try: return await schemaInfo.search.count(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e:
                if schemaInfo.database:
                    try: return await schemaInfo.database.count(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Not Implemented')  # no driver
        raise EpException(501, 'Not Implemented')  # no driver

    async def __create_data_with_auth__(
        self,
        background:BackgroundTasks,
        model:BaseModel,
        token: AuthorizationHeader,
        realm: RealmHeader=None
    ):
        schema = model.__class__
        authInfo = await self._auth.getAuthInfo(realm, token)
        schemaInfo = schema.getSchemaInfo()
        if AuthLevel.checkAuthentication(schemaInfo.auth) and not authInfo.checkAdmin() and not authInfo.checkCreateAllowed(schemaInfo.sref): raise EpException(403, 'Forbidden')
        await self.__create_data__(background, schema, model.setID().updateStatus(realm=authInfo.realm, owner=authInfo.username).model_dump())
        return model

    async def __create_data_with_free__(
        self,
        background:BackgroundTasks,
        model:BaseModel
    ):
        schema = model.__class__
        await self.__create_data__(background, schema, model.setID().updateStatus().model_dump())
        return model

    async def __create_data__(
        self,
        background,
        schema,
        data
    ):
        schemaInfo = schema.getSchemaInfo()
        if schemaInfo.database:
            try:
                if (await schemaInfo.database.create(schema, data))[0]:
                    if schemaInfo.cache: background.add_task(schemaInfo.cache.create, schema, data)
                    if schemaInfo.search: background.add_task(schemaInfo.search.create, schema, data)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif schemaInfo.search:
            try:
                await schemaInfo.search.create(schema, data)
                if schemaInfo.cache: background.add_task(schemaInfo.cache.create, schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif schemaInfo.cache:
            try: await schemaInfo.cache.create(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')

    async def __update_data_with_auth__(
        self,
        background:BackgroundTasks,
        id:ID,
        model:BaseModel,
        token: AuthorizationHeader,
        realm: RealmHeader=None
    ):
        id = str(id)
        schema = model.__class__
        authInfo = await self._auth.getAuthInfo(realm, token)
        schemaInfo = schema.getSchemaInfo()
        if AuthLevel.checkAuthentication(schemaInfo.auth) and not authInfo.checkAdmin() and not authInfo.checkUpdateAllowed(schemaInfo.sref): raise EpException(403, 'Forbidden')
        origin = await self.__read_data__(None, schema, id)
        if not authInfo.checkRealm(origin.realm): raise EpException(403, 'Forbidden')
        if AuthLevel.checkAccount(schemaInfo.auth) and not authInfo.checkUsername(origin.owner): raise EpException(403, 'Forbidden')
        await self.__update_data__(background, schema, model.setID(id).updateStatus(realm=authInfo.realm, owner=authInfo.username).model_dump())
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
        if schemaInfo.database:
            try:
                if (await schemaInfo.database.update(schema, data))[0]:
                    if schemaInfo.cache: background.add_task(schemaInfo.cache.update, schema, data)
                    if schemaInfo.search: background.add_task(schemaInfo.search.update, schema, data)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif schemaInfo.search:
            try:
                await schemaInfo.search.update(schema, data)
                if schemaInfo.cache: background.add_task(schemaInfo.cache.update, schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif schemaInfo.cache:
            try: await schemaInfo.cache.update(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')

    async def __delete_data_with_auth__(
        self,
        request:Request,
        background:BackgroundTasks,
        id:ID,
        token: AuthorizationHeader,
        realm: RealmHeader=None,
        force:Annotated[Literal['true', 'false', ''], Query(alias='$force')]=None,
    ):
        if force == '': force = True
        elif force: force = bool(force)

        id = str(id)
        path = request.scope['path'].replace(f'/{id}', '')
        schema = self._uerpPathToSchemaMap[path]
        authInfo = await self._auth.getAuthInfo(realm, token)
        schemaInfo = schema.getSchemaInfo()
        if AuthLevel.checkAuthentication(schemaInfo.auth) and not authInfo.checkAdmin() and not authInfo.checkDeleteAllowed(schemaInfo.sref): raise EpException(403, 'Forbidden')
        origin = await self.__read_data__(None, schema, id)
        if not authInfo.checkRealm(origin.realm): raise EpException(403, 'Forbidden')
        if AuthLevel.checkAccount(schemaInfo.auth) and not authInfo.checkUsername(origin.owner): raise EpException(403, 'Forbidden')
        await self.__delete_data__(background, schema, id, origin.setID(id).updateStatus(realm=authInfo.realm, owner=authInfo.username, deleted=True).model_dump(), force)
        return ModelStatus(id=id, status='deleted')

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
        path = request.scope['path'].replace(f'/{id}', '')
        schema = self._uerpPathToSchemaMap[path]
        model = await self.__read_data__(None, schema, id)
        await self.__delete_data__(background, schema, id, model.setID(id).updateStatus(deleted=True).model_dump(), force)
        return ModelStatus(id=id, status='deleted')

    async def __delete_data__(
        self,
        background,
        schema,
        id,
        data,
        force
    ):
        schemaInfo = schema.getSchemaInfo()
        if force and schemaInfo.database:
            try:
                if await schemaInfo.database.delete(schema, id):
                    if schemaInfo.cache: background.add_task(schemaInfo.cache.delete, schema, id)
                    if schemaInfo.search: background.add_task(schemaInfo.search.delete, schema, id)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif schemaInfo.database:
            try:
                if (await schemaInfo.database.update(schema, data))[0]:
                    if schemaInfo.cache: background.add_task(schemaInfo.cache.delete, schema, id)
                    if schemaInfo.search: background.add_task(schemaInfo.search.delete, schema, id)
                else: raise EpException(409, 'Conflict')
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif schemaInfo.search:
            try:
                await schemaInfo.search.delete(schema, id)
                if schemaInfo.cache: background.add_task(schemaInfo.cache.delete, schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
        elif schemaInfo.cache:
            try: await schemaInfo.cache.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Bad Request')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Service Unavailable')
