# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from time import time as tstamp
from typing import Annotated, Any, List, Literal
from fastapi import Request, BackgroundTasks, Query
from pydantic import BaseModel
from luqum.parser import parser as parseLucene
from stringcase import snakecase, camelcase, pathcase, titlecase

from .exceptions import EpException
from .utils import setEnvironment
from .schedules import asleep, runBackground
from .models import ModelStatus, ModelCount, ID, BaseSchema


#===============================================================================
# Interface
#===============================================================================
class SchemaDescription:
    
    def __init__(self, title:str, apiVersion:int, subVersion:int, schema:Any):
        self.title = title
        self.apiVersion = apiVersion
        self.subVersion = subVersion
        self.schema = schema
    
    #===========================================================================
    # name
    #===========================================================================
    @property
    def nameCode(self): return self.schema.__name__
    
    @property
    def nameSnake(self): return snakecase(self.schema.__name__)
    
    @property
    def nameCamel(self): return camelcase(self.schema.__name__)
    
    #===========================================================================
    # module
    #===========================================================================
    @property
    def moduleCode(self): return self.schema.__module__
    
    @property
    def moduleReference(self): return self.moduleCode.replace('schema.', '')
    
    #===========================================================================
    # schema
    #===========================================================================
    @property
    def schemaCode(self): return f'{self.moduleCode}.{self.nameCode}'
    
    @property
    def schemaType(self): return f'{self.moduleReference}.{self.nameCode}'
    
    @property
    def schemaReference(self): return f'{snakecase(self.title)}.v{self.apiVersion}.{self.moduleReference}.{self.nameSnake}'
    
    #===========================================================================
    # model
    #===========================================================================
    @property
    def category(self): return snakecase(f'{self.title}.{self.apiVersion}.{self.subVersion}.{self.moduleReference}.{self.nameCamel}')    
    
    @property
    def url(self): return f'/{pathcase(self.schemaReference)}'
    
    @property
    def tags(self): return [titlecase(self.moduleReference)]


class SchemaDriver(BaseModel):
    database:Any | None
    search:Any | None
    cache:Any | None

    
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
# Implement
#===============================================================================
class BaseControl:

    def __init__(self, api, config, background=False):
        self._background = background
        self.config = config
        self.api = api
        self.api.router.add_event_handler("startup", self.__startup__)
        self.api.router.add_event_handler("shutdown", self.__shutdown__)
        
        self._serviceTitle = snakecase(config['default']['title'])
        self._serviceApiVersion = int(config['service']['api_version'])
        self._serviceSubVersion = int(config['service']['sub_version'])

    async def __startup__(self):
        await self.startup()
        if self._background: await runBackground(self.background())
    
    async def __shutdown__(self):
        await self.shutdown()
    
    async def __background__(self):
        while self._background: self.background()

    async def startup(self): pass

    async def shutdown(self): pass

    async def background(self):
        LOG.INFO('run background process')
        asleep(1)


class UerpControl(BaseControl):
    
    def __init__(self, api, config, background:bool=False, cacheDriver:Any=None, searchDriver:Any=None, databaseDriver:Any=None):
        BaseControl.__init__(self, api, config, background)
        
        self._uerpCacheDriver = cacheDriver
        self._uerpSearchDriver = searchDriver
        self._uerpDatabaseDriver = databaseDriver
        
        self._uerpUrlPathToSchemaMap = {}
        self._uerpSchemaToDriverMap = {}
        self._uerpSchemaToUrlPathMap = {}
        self._uerpSchemaToSchemaTypeMap = {}
    
    async def __shutdown__(self):
        await BaseControl.__shutdown__(self)
        if self._uerpDatabaseDriver: await self._database.close()
        if self._uerpSearchDriver: await self._search.close()
        if self._uerpCacheDriver: await self._cache.close()
        
    async def registerModel(self, schema:BaseModel, level='csd', crud:str='crud', cacheExpire:int=None, searchExpire:int=None):
        desc = SchemaDescription(title=self._serviceTitle, apiVersion=self._serviceApiVersion, subVersion=self._serviceSubVersion, schema=schema)

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
        
        if 'd' in level and self._database: await self._database.registerModel(schema, desc)
        if 's' in level and self._search: await self._search.registerModel(schema, desc, searchExpire)
        if 'c' in level and self._cache: await self._cache.registerModel(schema, desc, cacheExpire)

        self._uerpSchemaToDriverMap[schema] = SchemaDriver(database=self._database, search=self._search, cache=self._cache)
        self._uerpSchemaToSchemaTypeMap[schema] = desc.schemaType
        
        nameCode = desc.nameCode
        tags = desc.tags
        url = desc.url
        
        self._uerpUrlPathToSchemaMap[url] = schema
        self._uerpSchemaToUrlPathMap[schema] = url
        if 'c' in crud:
            self.__create_data__.__annotations__['model'] = schema
            self.api.add_api_route(methods=['POST'], path=url, endpoint=self.__create_data__, response_model=schema, tags=tags, name=f'Create {nameCode}')
            self.__create_data__.__annotations__['model'] = BaseModel
        if 'r' in crud:
            self.api.add_api_route(methods=['GET'], path=url, endpoint=self.__search_data__, response_model=List[Any], tags=tags, name=f'Search {nameCode}')
            self.api.add_api_route(methods=['GET'], path=url + '/count', endpoint=self.__count_data__, response_model=ModelCount, tags=tags, name=f'Count {nameCode}')
            self.api.add_api_route(methods=['GET'], path=url + '/{id}', endpoint=self.__read_data__, response_model=schema, tags=tags, name=f'Read {nameCode}')
        if 'u' in crud:
            self.__update_data__.__annotations__['model'] = schema
            self.api.add_api_route(methods=['PUT'], path=url + '/{id}', endpoint=self.__update_data__, response_model=schema, tags=tags, name=f'Update {nameCode}')
            self.__update_data__.__annotations__['model'] = BaseModel
        if 'd' in crud:
            self.api.add_api_route(methods=['DELETE'], path=url + '/{id}', endpoint=self.__delete_data__, response_model=ModelStatus, tags=tags, name=f'Delete {nameCode}')
        
    async def __read_data__(self, request:Request, background:BackgroundTasks, id:ID):
        id = str(id)
        schema = self._uerpUrlPathToSchemaMap[request.scope['path'].replace(f'/{id}', '')]
        driver = self._uerpSchemaToDriverMap[schema]
        model = None
        
        if driver.cache:
            try: model = await driver.cache.read(schema, id)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Read Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Read Data')
            if model: return schema(**model)

        if driver.search:
            try: model = await driver.search.read(schema, id)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Read Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Read Data')
            if model:
                if driver.cache: background.add_task(driver.cache.create, schema, model)
                return schema(**model)
        
        if driver.database:
            try: model = await driver.database.read(schema, id)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Read Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Read Data')
            if model:
                if driver.cache: background.add_task(driver.cache.create, schema, model)
                if driver.search: background.add_task(driver.search.create, schema, model)
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
        
        schema = self._uerpUrlPathToSchemaMap[request.scope['path']]
        driver = self._uerpSchemaToDriverMap[schema]
        option = SearchOption(fields=fields, filter=filter, query=query, orderBy=orderBy, order=order, size=size, skip=skip)
        
        if archive and driver.database:
            try: models = await driver.database.search(schema, option)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Search Data')
            except Exception as e:
                if driver.search:
                    try: models = await driver.search.search(schema, option)
                    except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Search Data')
                    except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Search Data')
                else: LOG.ERROR('could not match driver'); raise EpException(501, 'Could Not Search Data')  # no driver
            else:
                if models and driver.search and not option.fields: background.add_task(driver.search.create, schema, *models)
            if models and driver.cache and not option.fields: background.add_task(driver.cache.create, schema, *models)
            return models
        elif driver.search:
            try: models = await driver.search.search(schema, option)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Search Data')
            except Exception as e:
                if driver.database:
                    try: models = await driver.database.search(schema, option)
                    except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Search Data')
                    except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Search Data')
                    if models and driver.search and not option.fields: background.add_task(driver.search.create, schema, *models)
                else: LOG.ERROR('could not match driver'); raise EpException(501, 'Could Not Search Data')  # no driver
            if models and driver.cache and not option.fields: background.add_task(driver.cache.create, schema, *models)
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
        schema = self._uerpUrlPathToSchemaMap[path]
        driver = self._uerpSchemaToDriverMap[schema]
        option = SearchOption(filter=filter, query=query)
        
        if archive and driver.database:
            try: result = await driver.database.count(schema, option)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Count Data')
            except Exception as e:
                if driver.search:
                    try: result = await driver.search.count(schema, option)
                    except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Count Data')
                    except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Count Data')
                else: LOG.ERROR('could not match driver'); raise EpException(501, 'Could Not Count Data')  # no driver
            return ModelCount(path=path, query=qstr, result=result)
        elif driver.search:
            try: result = await driver.search.count(schema, option)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Count Data')
            except Exception as e:
                if driver.database:
                    try: result = await driver.database.count(schema, option)
                    except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Count Data')
                    except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Count Data')
                else: LOG.ERROR('could not match driver'); raise EpException(501, 'Could Not Count Data')  # no driver
            return ModelCount(path=path, query=str, result=result)
        
        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Count Data')  # no driver
    
    async def __create_data__(self, model:BaseModel, background:BackgroundTasks):
        if not isinstance(model, BaseSchema): raise EpException(400, 'Bad Request')
        
        schema = model.__class__
        driver = self._uerpSchemaToDriverMap[schema]
        data = model.setID(self._uerpSchemaToUrlPathMap[schema], self._uerpSchemaToSchemaTypeMap[schema]).updateStatus().model_dump()
        
        if driver.database:
            try: result = (await driver.database.create(schema, data))[0]
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Create Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Create Data')
            if result:
                if driver.cache: background.add_task(driver.cache.create, schema, data)
                if driver.search: background.add_task(driver.search.create, schema, data)
                return model
        elif driver.search:
            try: await driver.search.create(schema, data)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Create Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Create Data')
            if driver.cache: background.add_task(driver.cache.create, schema, data)
            return model
        elif driver.cache:
            try: await driver.cache.create(schema, data)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Create Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Create Data')
            return model
        
        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Create Data')  # no driver

    async def __update_data__(self, model:BaseModel, background:BackgroundTasks, id:ID):
        if not isinstance(model, BaseSchema): raise EpException(400, 'Bad Request')
        
        schema = model.__class__
        driver = self._uerpSchemaToDriverMap[schema]
        data = model.setID(self._uerpSchemaToUrlPathMap[schema], self._uerpSchemaToSchemaTypeMap[schema], str(id)).updateStatus().model_dump()
        
        if driver.database:
            try: result = (await driver.database.update(schema, data))[0]
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Update Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Update Data')
            if result:
                if driver.cache: background.add_task(driver.cache.update, schema, data)
                if driver.search: background.add_task(driver.search.update, schema, data)
                return model
        elif driver.search:
            try: await driver.search.update(schema, data)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Update Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Update Data')
            if driver.cache: background.add_task(driver.cache.update, schema, data)
            return model
        elif driver.cache:
            try: await driver.cache.update(schema, data)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Update Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Update Data')
            return model
        
        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Update Data')  # no driver

    async def __delete_data__(self, request:Request, background:BackgroundTasks, id:ID, force:Annotated[Literal['true', 'false', ''], Query(alias='$force')]=None):
        if force == '': force = True
        elif force: force = bool(force)
        
        id = str(id)
        path = request.scope['path'].replace(f'/{id}', '')
        schema = self._uerpUrlPathToSchemaMap[path]
        driver = self._uerpSchemaToDriverMap[schema]
        type = self._uerpSchemaToSchemaTypeMap[schema]
        ref = f'{path}/{id}'
        
        if force and driver.database:
            try: data = await driver.database.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Delete Data')
            if data:
                if driver.cache: background.add_task(driver.cache.delete, schema, id)
                if driver.search: background.add_task(driver.search.delete, schema, id)
                return ModelStatus(id=id, type=type, ref=ref, status='deleted')
            else: raise EpException(404, 'Not Found')
        elif driver.database:
            try: data = await self._database.read(schema, id)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Delete Data')
            if data:
                data['author'] = 'unknown'
                data['deleted'] = True
                data['tstamp'] = int(tstamp())
                try: data = (await driver.database.update(schema, data))[0]
                except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Delete Data')
                except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Delete Data')
                if data:
                    if driver.cache: background.add_task(driver.cache.delete, schema, id)
                    if driver.search: background.add_task(driver.search.delete, schema, id)
                    return ModelStatus(id=id, type=type, ref=ref, status='deleted')
                else: raise EpException(409, 'Could Not Delete Data')  # update failed
            else: raise EpException(404, 'Not Found')
        elif driver.search:
            try: await driver.search.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Delete Data')
            if driver.cache: background.add_task(driver.cache.delete, schema, id)
            return ModelStatus(id=id, type=type, ref=ref, status='deleted')
        elif driver.cache:
            try: await driver.cache.delete(schema, id)
            except LookupError as e: LOG.ERROR(e); raise EpException(400, 'Could Not Delete Data')
            except Exception as e: LOG.ERROR(e); raise EpException(503, 'Could Not Delete Data')
            return ModelStatus(id=id, type=type, ref=ref, status='deleted')
        
        LOG.ERROR('could not match driver')
        raise EpException(501, 'Could Not Delete Data')  # no driver
