# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import time
from typing import List, Literal, Optional, Union, Annotated, Any
from fastapi import Request, BackgroundTasks, Query
from fastapi._compat import ModelField
from pydantic import BaseModel, PlainSerializer
from luqum.parser import parser as parseLucene
from stringcase import snakecase, pathcase, dotcase, titlecase

from .constants import TimeString
from .exceptions import EpException
from .utils import setEnvironment, getConfig, Logger
from .interfaces import SyncRest, AsyncRest
from .schedules import asleep, runBackground, MultiTask
from .model import SchemaDescription, SearchOption, ModelStatus, ModelCount, Reference, ID, Key, BaseSchema, IdentSchema, StatusSchema, ProfSchema, TagSchema, MetaSchema 


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
    
    def __init__(self, api, config, cacheDriver:Any, searchDriver:Any, databaseDriver:Any, background:bool=False):
        BaseControl.__init__(self, api, config, background)
        self._uerpApiTitle = config['default']['title']
        self._uerpApiVersion = int(config['service']['api_version'])
        self._uerpCacheDriver = cacheDriver
        self._uerpSearchDriver = searchDriver
        self._uerpDatabaseDriver = databaseDriver
        self._uerpUrlToSchemaMap = {}
        self._uerpSchemaToModelCodeMap = {}
        self._uerpSchemaToSchemaPathMap = {}
    
    async def __shutdown__(self):
        await BaseControl.__shutdown__(self)
        await self._uerpDatabaseDriver.close()
        await self._uerpSearchDriver.close()
        await self._uerpCacheDriver.close()
        
    async def registerModel(self, schema:BaseModel, crud:str='crud', cacheExpire:int=None, searchExpire:int=None):
        desc = SchemaDescription(title=self._uerpApiTitle, version=self._uerpApiVersion, schema=schema)

        try: self._database = UERP_DATABASE
        except: self._database = setEnvironment('UERP_DATABASE', self._uerpDatabaseDriver(self.config))
        try: self._search = UERP_SEARCH
        except: self._search = setEnvironment('UERP_SEARCH', self._uerpSearchDriver(self.config))
        try: self._cache = UERP_CACHE
        except: self._cache = setEnvironment('UERP_CACHE', self._uerpCacheDriver(self.config))
        
        await self._database.registerModel(schema, desc)
        await self._search.registerModel(schema, desc, searchExpire)
        await self._cache.registerModel(schema, desc, cacheExpire)

        nameCode = desc.nameCode
        tags = desc.tags
        url = desc.url
        
        self._uerpSchemaToModelCodeMap[schema] = desc.modelCode
        self._uerpSchemaToSchemaPathMap[schema] = desc.schemaPath
        self._uerpUrlToSchemaMap[url] = schema
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
        schema = self._uerpUrlToSchemaMap[request.scope['path'].replace(f'/{id}', '')]
        model = None

        try: model = await self._cache.read(schema, id)
        except LookupError as e: raise EpException(400, e)
        except Exception as e: LOG.ERROR(e)
        if model: return schema(**model)

        try: model = await self._search.read(schema, id)
        except LookupError as e: raise EpException(400, e)
        except Exception as e: LOG.ERROR(e)
        if model:
            background.add_task(self._cache.create, schema, model)
            return schema(**model)

        try: model = await self._database.read(schema, id)
        except LookupError as e: raise EpException(400, e)
        except Exception as e: raise EpException(500, f'Could Not Process To Read Data: {e}')
        if model:
            background.add_task(self._cache.create, schema, model)
            background.add_task(self._search.create, schema, model)
            return schema(**model)
        
        raise EpException(404, 'Not Found')

    async def __search_data__(self, request:Request, background:BackgroundTasks,
            f:Annotated[List[str] | None, Query(alias='$f', description='retrieve fields ex) $f=field1&$f=field2')]=None,
            filter:Annotated[str | None, Query(alias='$filter', description='lucene type filter ex) ')]=None,
            orderBy:Annotated[str | None, Query(alias='$orderby', description='ordered by specific field')]=None,
            order:Annotated[Literal['asc', 'desc'], Query(alias='$order', description='ordering type')]=None,
            size:Annotated[int | None, Query(alias='$size', description='retrieved data count')]=None,
            skip:Annotated[int | None, Query(alias='$skip', description='skipping data count')]=None,
            archive:Annotated[Literal['true', 'false'], Query(alias='$archive', description='retrieve from persistent store')]=None
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
        if size: size = int(size)
        if skip: skip = int(skip)
        if archive: archive = bool(archive)
        
        schema = self._uerpUrlToSchemaMap[request.scope['path']]
        option = SearchOption(fields=f, filter=filter, query=query, orderBy=orderBy, order=order, size=size, skip=skip)
        
        import elastic_transport
        elastic_transport.ConnectionError
        
        if not archive:
            try: models = await self._search.search(schema, option)
            except LookupError as e: raise EpException(400, e)
            except Exception as e:
                try: models = await self._database.search(schema, option)
                except LookupError as e: raise EpException(400, e)
                except Exception as e: raise EpException(500, f'Could Not Process To Search Data: {e}')
        else:
            try: models = await self._database.search(schema, option)
            except LookupError as e: raise EpException(400, e)
            except Exception as e: raise EpException(500, f'Could Not Process To Search Data: {e}')
            if models and not option.fields: background.add_task(self._search.create, schema, *models)
        
        if models and not option.fields: background.add_task(self._cache.create, schema, *models)
        return models
    
    async def __count_data__(self, request:Request, background:BackgroundTasks,
            filter:Annotated[str | None, Query(alias='$filter')]=None,
            archive:Annotated[Literal['true', 'false'], Query(alias='$archive')]=None
        ):
        query = request.query_params._dict
        if '$filter' in query: query.pop('$filter')
        if '$archive' in query: query.pop('$archive')
        if filter: filter = parseLucene.parse(filter)
        if archive: archive = bool(archive)
        
        schema = self._uerpUrlToSchemaMap[request.scope['path'].replace('/count', '')]
        option = SearchOption(query=query, filter=filter)
        
        if not archive:
            try: count = await self._search.count(schema, option)
            except LookupError as e: raise EpException(400, e)
            except Exception as e:
                try: count = await self._database.count(schema, option)
                except LookupError as e: raise EpException(400, e)
                except Exception as e: raise EpException(500, f'Could Not Process To Count Data: {e}')
        else:
            try: count = await self._database.count(schema, option)
            except LookupError as e: raise EpException(400, e)
            except Exception as e: raise EpException(500, f'Could Not Process To Count Data: {e}')
        
        return ModelCount(path=request.scope['path'], value=count)
    
    async def __create_data__(self, request:Request, model:BaseModel, background:BackgroundTasks):
        if not isinstance(model, BaseSchema): raise EpException(400, 'Bad Request')
        schema = model.__class__
        data = model.setID(request.scope['path'], self._uerpSchemaToModelCodeMap[schema]).updateStatus().model_dump()
        
        try: result = (await self._database.create(schema, data))[0]
        except LookupError as e: raise EpException(400, e)
        except Exception as e: raise EpException(500, f'Could Not Process To Create Data: {e}')
        if result:
            background.add_task(self._cache.create, schema, data)
            background.add_task(self._search.create, schema, data)
            return model
        
        EpException(500, 'Could Not Create Data')

    async def __update_data__(self, request:Request, model:BaseModel, background:BackgroundTasks, id:ID):
        if not isinstance(model, BaseSchema): raise EpException(400, 'Bad Request')
        id = str(id)
        schema = model.__class__
        data = model.setID(request.scope['path'].replace(f'/{id}', ''), self._uerpSchemaToModelCodeMap[schema], id).updateStatus().model_dump()
        
        try: result = (await self._database.update(schema, data))[0]
        except LookupError as e: raise EpException(400, e)
        except Exception as e: raise EpException(500, f'Could Not Process To Update Data: {e}')
        if result:
            background.add_task(self._cache.update, schema, data)
            background.add_task(self._search.update, schema, data)
            return model
        EpException(400, 'Bad Request')

    async def __delete_data__(self, request:Request, background:BackgroundTasks, id:ID, force:Annotated[Literal['true', 'false'], Query(alias='$force')]=None):
        id = str(id)
        schema = self._uerpUrlToSchemaMap[request.scope['path'].replace(f'/{id}', '')]
        result = False
        
        if force and bool(force):
            try: result = await self._database.delete(schema, id)
            except LookupError as e: raise EpException(400, e)
            except Exception as e: raise EpException(500, f'Could Not Process To Delete Data: {e}')
        else:
            try: data = await self._database.read(schema, id)
            except LookupError as e: raise EpException(400, e)
            except Exception as e: raise EpException(500, f'Could Not Process To Delete Data: {e}')
            if data:
                data['author'] = 'unknown'
                data['deleted'] = True
                data['tstamp'] = int(time.time())
                try: result = (await self._database.update(schema, data))[0]
                except LookupError as e: raise EpException(400, e)
                except Exception as e: raise EpException(500, f'Could Not Process To Delete Data: {e}')
        if result:
            background.add_task(self._cache.delete, schema, id)
            background.add_task(self._search.delete, schema, id)
            return ModelStatus(id=id, status='deleted')
        raise EpException(404, 'Not Found')
