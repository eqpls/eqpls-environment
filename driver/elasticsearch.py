# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import inspect
import datetime
from uuid import UUID
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch, helpers
from luqum.elasticsearch import ElasticsearchQueryBuilder, SchemaAnalyzer

from common import EpException, SchemaDescription, SearchOption


#===============================================================================
# Implement
#===============================================================================
class ElasticSearch:

    def __init__(self, config):
        self._esHostname = config['search']['hostname']
        self._esHostport = int(config['search']['hostport'])
        self._esUsername = config['search']['username']
        self._esPassword = config['search']['password']
        self._esShards = int(config['elasticsearch']['shards'])
        self._esReplicas = int(config['elasticsearch']['replicas'])
        self._esExpire = int(config['elasticsearch']['expire'])
        self._esIndexMap = {}
        self._esQueryMap = {}
        self._es = AsyncElasticsearch(
            f'https://{self._esHostname}:{self._esHostport}',
            basic_auth=(self._esUsername, self._esPassword),
            verify_certs=False,
            ssl_show_warn=False
        )

    async def registerModel(self, schema:BaseModel, desc:SchemaDescription, shards:int=None, replicas:int=None, expire=None):

        def parseModelToMapping(schema):
            
            def parseTermToMapping(fieldType, fieldMeta):
                if fieldType == str:
                    if 'keyword' in fieldMeta: return {'type': 'keyword'}
                    else: return {'type': 'text'}
                elif fieldType == int: return {'type': 'long'}
                elif fieldType == float: return {'type': 'double'}
                elif fieldType == bool: return {'type': 'boolean'}
                elif fieldType == UUID: return {'type': 'keyword'}
                elif fieldType == datetime: return {'type': 'date'}
                return None
            
            def parseTermsToMapping(fieldType):
                if fieldType == str: return {'type': 'keyword'}
                elif fieldType == int: return {'type': 'long'}
                elif fieldType == float: return {'type': 'double'}
                elif fieldType == bool: return {'type': 'boolean'}
                elif fieldType == UUID: return {'type': 'keyword'}
                elif fieldType == datetime: return {'type': 'date'}
                return None
            
            mapping = {}
            for field in schema.model_fields.keys():
                fieldData = schema.model_fields[field]
                fieldType = fieldData.annotation
                fieldMeta = fieldData.metadata
                esFieldType = parseTermToMapping(fieldType, fieldMeta)
                if not esFieldType:
                    if inspect.isclass(fieldType) and issubclass(fieldType, BaseModel):
                        esFieldType = {'properties': parseModelToMapping(fieldType)}
                    elif getattr(fieldType, '__origin__', None) == list:
                        fieldType = fieldType.__args__[0]
                        esFieldType = parseTermsToMapping(fieldType)
                        if not esFieldType:
                            esFieldType = {'type': 'nested', 'properties': parseModelToMapping(fieldType)}
                    else: raise EpException(500, f'search.registerModel({schema}.{field}{fieldType}): could not parse schema') 
                mapping[field] = esFieldType
            return mapping

        index = desc.schemaPath
        indexSchema = {
            'settings': {
                'number_of_shards': shards if shards else self._esShards,
                'number_of_replicas': replicas if replicas else self._esReplicas
            },
            'mappings': {
                'properties': parseModelToMapping(schema)
            }
        }
        if not await self._es.indices.exists(index=index):
            await self._es.indices.create(index=index, body=indexSchema)
        self._esQueryMap[schema] = ElasticsearchQueryBuilder(**SchemaAnalyzer(indexSchema).query_builder_options())
        self._esIndexMap[schema] = index
        
        # LOG.DEBUG(f'search.register({schema}) <{index}>')

    async def close(self):
        await self._es.close()

    async def read(self, schema:BaseModel, id:str):
        try: model = (await self._es.get(index=self._esIndexMap[schema], id=id)).body['_source']
        except: model = None
        return model

    async def search(self, schema:BaseModel, option:SearchOption):
        if option.filter: filter = self._esQueryMap[schema](option.filter)
        else: filter = None
        if option.orderBy and option.order: sort = [{option.orderBy: option.order}]
        else: sort = None
        models = await self._es.search(index=self._esIndexMap[schema], source_includes=option.fields, query=filter, sort=sort, from_=option.skip, size=option.size)
        return [model['_source'] for model in models['hits']['hits']]
    
    async def count(self, schema:BaseModel, option:SearchOption):
        if option.filter: filter = self._esQueryMap[schema](option.filter)
        else: filter = None
        return (await self._es.count(index=self._esIndexMap[schema], query=filter))['count']
    
    async def __generate_bulk_data__(self, index, models):
        for model in models:
            model['_ttl'] = 1234
            yield {
                '_op_type': 'update',
                '_index': index,
                '_id': model['id'],
                # '_cache_tstamp': FIX
                'doc': model,
                'doc_as_upsert': True
            }

    async def create(self, schema:BaseModel, *models):
        if models:
            index = self._esIndexMap[schema]
            await helpers.async_bulk(self._es, self.__generate_bulk_data__(index, models))
    
    async def update(self, schema:BaseModel, *models):
        if models:
            index = self._esIndexMap[schema]
            await helpers.async_bulk(self._es, self.__generate_bulk_data__(index, models))

    async def delete(self, schema:BaseModel, id:str):
        await self._es.delete(index=self._esIndexMap[schema], id=id)
