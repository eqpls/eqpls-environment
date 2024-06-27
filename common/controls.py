# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import traceback
from time import time as tstamp
from typing import Annotated, Any, List, Literal
from fastapi import Request, BackgroundTasks, Query
from pydantic import BaseModel
from luqum.parser import parser as parseLucene
from stringcase import snakecase

from .exceptions import EpException
from .utils import setEnvironment
from .schedules import asleep, runBackground
from .models import ServiceHealth, ModelStatus, ModelCount, ID, BaseSchema, SearchOption


#===============================================================================
# Implement
#===============================================================================
class BaseControl:

    def __init__(self, api, config, background=False):
        self._background = background
        self.config = config
        self.api = api
        self.api.router.add_event_handler("startup", self.__startup__)
        self.api.router.add_event_handler("shutdown", self.__shutdown__)
        self._title = snakecase(config['default']['title'])
        self._serviceVersion = int(config['service']['version'])

    @property
    def title(self): return self._title

    @property
    def version(self): return self._serviceVersion

    async def __startup__(self):
        await self.startup()
        if self._background: await runBackground(self.background())
        self.api.add_api_route(
            methods=['GET'],
            path=f'/{snakecase(self.title)}/_health',
            endpoint=self.__health__,
            response_model=ServiceHealth,
            tags=['Service Health'],
            name='Health'
        )

    async def __shutdown__(self):
        await self.shutdown()

    async def __background__(self):
        while self._background: self.background()

    async def __health__(self): pass

    async def startup(self): pass

    async def shutdown(self): pass

    async def health(self) -> ServiceHealth: return ServiceHealth(title=self.title, status='OK')

    async def background(self):
        LOG.INFO('run background process')
        asleep(1)


class MeshControl(BaseControl):

    def __init__(self, api, config, background:bool=False):
        BaseControl.__init__(self, api, config, background)
        if 'providers' not in self.config: raise Exception('[providers] configuration is not in module.conf')
        self.providers = self.config['providers']

    async def registerModel(self, schema:BaseSchema, service):
        if service not in self.providers: raise Exception(f'{service} is not in [providers] configuration')
        schema.setSchemaInfo(self.version, service, self.providers[service])
        return self


class UerpControl(BaseControl):

    def __init__(self, api, config, background:bool=False, cacheDriver:Any=None, searchDriver:Any=None, databaseDriver:Any=None):
        BaseControl.__init__(self, api, config, background)

        self._uerpCacheDriver = cacheDriver
        self._uerpSearchDriver = searchDriver
        self._uerpDatabaseDriver = databaseDriver

        self._uerpPathToSchemaMap = {}

    async def __shutdown__(self):
        await BaseControl.__shutdown__(self)
        if self._uerpDatabaseDriver: await self._database.close()
        if self._uerpSearchDriver: await self._search.close()
        if self._uerpCacheDriver: await self._cache.close()

    async def registerModel(self, schema:BaseSchema):
        schema.setSchemaInfo(self.version, self.title)
        info = schema.getSchemaInfo()

        try: self._database = UERP_DATABASE
        except:
            if self._uerpDatabaseDriver: database = self._uerpDatabaseDriver(self.config)
            else: database = None
            self._database = setEnvironment('UERP_DATABASE', database)

        try: self._search = UERP_SEARCH
        except:
            if self._uerpSearchDriver: search = self._uerpSearchDriver(self.config)
            else: search = None
            self._search = setEnvironment('UERP_SEARCH', search)

        try: self._cache = UERP_CACHE
        except:
            if self._uerpCacheDriver: cache = self._uerpCacheDriver(self.config)
            else: cache = None
            self._cache = setEnvironment('UERP_CACHE', cache)

        if 'd' in info.layer and self._database: await self._database.registerModel(schema)
        if 's' in info.layer and self._search: await self._search.registerModel(schema)
        if 'c' in info.layer and self._cache: await self._cache.registerModel(schema)

        self._uerpPathToSchemaMap[info.path] = schema

        if 'c' in info.crud:
            self.__create_data__.__annotations__['model'] = schema
            self.api.add_api_route(methods=['POST'], path=info.path, endpoint=self.__create_data__, response_model=schema, tags=info.tags, name=f'Create {info.name}')
            self.__create_data__.__annotations__['model'] = BaseModel
        if 'r' in info.crud:
            self.api.add_api_route(methods=['GET'], path=info.path, endpoint=self.__search_data__, response_model=List[Any], tags=info.tags, name=f'Search {info.name}')
            self.api.add_api_route(methods=['GET'], path=info.path + '/count', endpoint=self.__count_data__, response_model=ModelCount, tags=info.tags, name=f'Count {info.name}')
            self.api.add_api_route(methods=['GET'], path=info.path + '/{id}', endpoint=self.__read_data__, response_model=schema, tags=info.tags, name=f'Read {info.name}')
        if 'u' in info.crud:
            self.__update_data__.__annotations__['model'] = schema
            self.api.add_api_route(methods=['PUT'], path=info.path + '/{id}', endpoint=self.__update_data__, response_model=schema, tags=info.tags, name=f'Update {info.name}')
            self.__update_data__.__annotations__['model'] = BaseModel
        if 'd' in info.crud:
            self.api.add_api_route(methods=['DELETE'], path=info.path + '/{id}', endpoint=self.__delete_data__, response_model=ModelStatus, tags=info.tags, name=f'Delete {info.name}')

        return self

    async def __read_data__(self, request:Request, background:BackgroundTasks, id:ID):
        id = str(id)
        schema = self._uerpPathToSchemaMap[request.scope['path'].replace(f'/{id}', '')]
        info = schema.getSchemaInfo()

        if info.cache:
            try: model = await info.cache.read(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Read Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Read Data')
            if model: return schema(**model)

        if info.search:
            try: model = await info.search.read(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Read Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Read Data')
            if model:
                if info.cache: background.add_task(info.cache.create, schema, model)
                return schema(**model)

        if info.database:
            try: model = await info.database.read(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Read Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Read Data')
            if model:
                if info.cache: background.add_task(info.cache.create, schema, model)
                if info.search: background.add_task(info.search.create, schema, model)
                return schema(**model)

        raise EpException(404, 'Not Found')

    async def __search_data__(self, request:Request, background:BackgroundTasks,
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
        if filter: filter = parseLucene.parse(filter)
        if orderBy and not order: order = 'desc'
        if size: size = int(size)
        if skip: skip = int(skip)
        if archive == '': archive = True
        elif archive: archive = bool(archive)

        schema = self._uerpPathToSchemaMap[request.scope['path']]
        info = schema.getSchemaInfo()
        option = SearchOption(fields=fields, filter=filter, query=query, orderBy=orderBy, order=order, size=size, skip=skip)

        if archive and info.database:
            try: models = await info.database.search(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Search Data')
            except Exception as e:
                if info.search:
                    try: models = await info.search.search(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Search Data')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Search Data')
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Could Not Search Data')  # no driver
            else:
                if models and info.search and not option.fields: background.add_task(info.search.create, schema, *models)
            if models and info.cache and not option.fields: background.add_task(info.cache.create, schema, *models)
            return models
        elif info.search:
            try: models = await info.search.search(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Search Data')
            except Exception as e:
                if info.database:
                    try: models = await info.database.search(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Search Data')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Search Data')
                    if models and info.search and not option.fields: background.add_task(info.search.create, schema, *models)
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Could Not Search Data')  # no driver
            if models and info.cache and not option.fields: background.add_task(info.cache.create, schema, *models)
            return models

        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Search Data')  # no driver

    async def __count_data__(self, request:Request, background:BackgroundTasks,
            filter:Annotated[str | None, Query(alias='$filter')]=None,
            archive:Annotated[Literal['true', 'false'], Query(alias='$archive')]=None
        ):
        query = request.query_params._dict
        if '$filter' in query: query.pop('$filter')
        if '$archive' in query: query.pop('$archive')
        if filter: filter = parseLucene.parse(filter)
        if archive == '': archive = True
        elif archive: archive = bool(archive)

        path = request.scope['path'].replace('/count', '')
        qstr = request.scope['query_string']
        schema = self._uerpPathToSchemaMap[path]
        info = schema.getSchemaInfo()
        option = SearchOption(filter=filter, query=query)

        if archive and info.database:
            try: result = await info.database.count(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Count Data')
            except Exception as e:
                if info.search:
                    try: result = await info.search.count(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Count Data')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Count Data')
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Could Not Count Data')  # no driver
            return ModelCount(path=path, query=qstr, result=result)
        elif info.search:
            try: result = await info.search.count(schema, option)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Count Data')
            except Exception as e:
                if info.database:
                    try: result = await info.database.count(schema, option)
                    except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Count Data')
                    except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Count Data')
                else: LOG.ERROR('could not match driver'); traceback.print_exc(); raise EpException(501, 'Could Not Count Data')  # no driver
            return ModelCount(path=path, query=qstr, result=result)

        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Count Data')  # no driver

    async def __create_data__(self, model:BaseModel, background:BackgroundTasks):
        if not isinstance(model, BaseSchema): raise EpException(400, 'Bad Request')

        schema = model.__class__
        info = schema.getSchemaInfo()
        data = model.setID().updateStatus().model_dump()

        if info.database:
            try: result = (await info.database.create(schema, data))[0]
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Create Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Create Data')
            if result:
                if info.cache: background.add_task(info.cache.create, schema, data)
                if info.search: background.add_task(info.search.create, schema, data)
                return model
        elif info.search:
            try: await info.search.create(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Create Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Create Data')
            if info.cache: background.add_task(info.cache.create, schema, data)
            return model
        elif info.cache:
            try: await info.cache.create(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Create Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Create Data')
            return model

        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Create Data')  # no driver

    async def __update_data__(self, model:BaseModel, background:BackgroundTasks, id:ID):
        if not isinstance(model, BaseSchema): raise EpException(400, 'Bad Request')

        schema = model.__class__
        info = schema.getSchemaInfo()
        data = model.setID(str(id)).updateStatus().model_dump()

        print(info)

        if info.database:
            try: result = (await info.database.update(schema, data))[0]
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Update Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Update Data')
            if result:
                if info.cache: background.add_task(info.cache.update, schema, data)
                if info.search: background.add_task(info.search.update, schema, data)
                return model
        elif info.search:
            try: await info.search.update(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Update Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Update Data')
            if info.cache: background.add_task(info.cache.update, schema, data)
            return model
        elif info.cache:
            try: await info.cache.update(schema, data)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Update Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Update Data')
            return model

        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Update Data')  # no driver

    async def __delete_data__(self, request:Request, background:BackgroundTasks, id:ID, force:Annotated[Literal['true', 'false', ''], Query(alias='$force')]=None):
        if force == '': force = True
        elif force: force = bool(force)

        id = str(id)
        path = request.scope['path'].replace(f'/{id}', '')
        schema = self._uerpPathToSchemaMap[path]
        info = schema.getSchemaInfo()

        if force and info.database:
            try: data = await info.database.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Delete Data')
            if data:
                if info.cache: background.add_task(info.cache.delete, schema, id)
                if info.search: background.add_task(info.search.delete, schema, id)
                return ModelStatus(id=id, status='deleted')
            else: raise EpException(404, 'Not Found')
        elif info.database:
            try: data = await self._database.read(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Delete Data')
            if data:
                data['author'] = 'unknown'
                data['deleted'] = True
                data['tstamp'] = int(tstamp())
                try: data = (await info.database.update(schema, data))[0]
                except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Delete Data')
                except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Delete Data')
                if data:
                    if info.cache: background.add_task(info.cache.delete, schema, id)
                    if info.search: background.add_task(info.search.delete, schema, id)
                    return ModelStatus(id=id, status='deleted')
                else: raise EpException(409, 'Could Not Delete Data')  # update failed
            else: raise EpException(404, 'Not Found')
        elif info.search:
            try: await info.search.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Delete Data')
            if info.cache: background.add_task(info.cache.delete, schema, id)
            return ModelStatus(id=id, status='deleted')
        elif info.cache:
            try: await info.cache.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); traceback.print_exc(); raise EpException(503, 'Could Not Delete Data')
            return ModelStatus(id=id, status='deleted')

        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Delete Data')  # no driver
