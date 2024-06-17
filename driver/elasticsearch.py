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
from stringcase import snakecase
from elasticsearch import AsyncElasticsearch, helpers
from luqum.parser import parser
from luqum.elasticsearch import ElasticsearchQueryBuilder, SchemaAnalyzer
from common import EpException, ModelFilter


#===============================================================================
# Implement
#===============================================================================
class ElasticSearch:

    def __init__(self, config):
        self._esHostname = config['elasticsearch']['hostname']
        self._esHostport = int(config['elasticsearch']['hostport'])
        self._esUsername = config['elasticsearch']['username']
        self._esPassword = config['elasticsearch']['password']
        self._esShards = int(config['elasticsearch']['shards'])
        self._esReplicas = int(config['elasticsearch']['replicas'])
        self._esIndexMap = {}
        self._esQueryMap = {}
        self._es = AsyncElasticsearch(
            f'https://{self._esHostname}:{self._esHostport}',
            basic_auth=(self._esUsername, self._esPassword),
            verify_certs=False,
            ssl_show_warn=False
        )

    async def registerModel(self, schema:BaseModel, shards:int=None, replicas:int=None):

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

        index = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
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
        
        LOG.INFO(f'search.register({schema}) <{index}>')

    async def readDocument(self, schema:BaseModel, id:str):
        try:
            model = (await self._es.get(index=self._esIndexMap[schema], id=id)).body['_source']
            LOG.DEBUG(f'search.read({schema}) {model}')
        except: model = None
        return model

    async def searchDocuments(self, schema:BaseModel, filter:ModelFilter):
        if filter.query: query = self._esQueryMap[schema](parser.parse(filter.query))
        else: query = None
        if filter.orderBy: sort = [{filter.orderBy: filter.order}]
        else: sort = None
        models = await self._es.search(index=self._esIndexMap[schema], query=query, sort=sort, from_=filter.skip, size=filter.size)
        models = [model['_source'] for model in models['hits']['hits']]
        return models

    async def createDocument(self, schema:BaseModel, *models):
        if models:
            index = self._esIndexMap[schema]
            docs = []
            for model in models:
                docs.append({
                    '_index': index,
                    '_id': model['id'],
                    '_source': model
                })
            await helpers.async_bulk(self._es, docs)
            LOG.DEBUG(f'search.create({schema}) {models}')

    async def deleteDocument(self, schema:BaseModel, id:str):
        try:
            await self._es.delete(index=self._esIndexMap[schema], id=id)
            LOG.DEBUG(f'search.delete({schema}) {id}')
        except: pass
