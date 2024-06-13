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
from elasticsearch import AsyncElasticsearch
from elasticsearch_dsl import AsyncSearch
from common import EpException


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
        self._esTypeMap = {
            str: {'type': 'text'},
            int: {'type': 'long'},
            float: {'type': 'double'},
            bool: {'type': 'boolean'},
            UUID: {'type': 'keyword'},
            datetime: {'type': 'date'},
            list: {'type': 'keyword'}
        }
        self._es = AsyncElasticsearch(
            f'https://{self._esHostname}:{self._esHostport}',
            basic_auth=(self._esUsername, self._esPassword),
            verify_certs=False,
            ssl_show_warn=False
        )
            
    async def createIndex(self, schema:BaseModel, shards:int=None, replicas:int=None):
        
        def parseModelToMapping(schema):
            mapping = {}
            for field, fieldType in schema.__annotations__.items():
                if fieldType in self._esTypeMap: esFieldType = self._esTypeMap[fieldType]
                else:
                    if inspect.isclass(fieldType) and issubclass(fieldType, BaseModel):  # sub-model
                        esFieldType = {
                            'properties': parseModelToMapping(fieldType)
                        }
                    else:  # list-field
                        fieldType = fieldType.__args__[0]
                        if fieldType in self._esTypeMap: esFieldType = self._esTypeMap[fieldType]
                        else:
                            esFieldType = {
                                'type': 'nested',
                                'properties': parseModelToMapping(fieldType)
                            }
                mapping[field] = esFieldType
            return mapping
        
        
        index = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
        if not await self._es.indices.exists(index=index):
            await self._es.indices.create(index=index, body={
                'settings': {
                    'number_of_shards': shards if shards else self._esShards,
                    'number_of_replicas': replicas if replicas else self._esReplicas
                },
                'mappings': {
                    'properties': parseModelToMapping(schema)
                }
            })
            LOG.INFO(f'create index: {index}')
    
    async def createDocument(self, model:BaseModel):
        index = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
        try:
            await self._es.index(index=index, id=model.id, body=model.dict())
            return model
        except: raise EpException(400, 'Bad Request')

    async def readDocument(self, schema:BaseModel, id:UUID):
        index = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
        try:
            result = (await self._es.get(index=index, id=str(id))).body['_source']
            return schema(**result)
        except: raise EpException(404, 'Not Found')
    
    async def searchDocuments(self, schema:BaseModel, **query):
        index = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
        try:
            search = AsyncSearch(using=self._es, index=index)
            for key, val in query.items():
                op = key[-1]
                if op in ['+', '!']: search = search.filter('match', **{key[:-1]: val}) if op == '+' else search.exclude('match', **{key[:-1]: val})
                else: search = search.query('match', **{key: val})
            return [schema(**hit.to_dict()) for hit in await search.execute()]
        except: raise EpException(400, 'Bad Request')
    
