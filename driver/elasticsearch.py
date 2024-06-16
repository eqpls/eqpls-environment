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
from elasticsearch_dsl import AsyncSearch


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
        self._esIndexMap = {}
        self._es = AsyncElasticsearch(
            f'https://{self._esHostname}:{self._esHostport}',
            basic_auth=(self._esUsername, self._esPassword),
            verify_certs=False,
            ssl_show_warn=False
        )

    async def registerModel(self, schema:BaseModel, shards:int=None, replicas:int=None):

        def parseModelToMapping(schema):
            mapping = {}
            for field, fieldType in schema.__annotations__.items():
                if fieldType in self._esTypeMap: esFieldType = self._esTypeMap[fieldType]
                else:
                    if inspect.isclass(fieldType) and issubclass(fieldType, BaseModel):
                        esFieldType = {'properties': parseModelToMapping(fieldType)}
                    else:
                        fieldType = fieldType.__args__[0]
                        if fieldType in self._esTypeMap: esFieldType = self._esTypeMap[fieldType]
                        else: esFieldType = {'type': 'nested', 'properties': parseModelToMapping(fieldType)}
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
        self._esIndexMap[schema] = index
        LOG.INFO(f'search.register({schema}) <{index}>')

    async def readDocument(self, schema:BaseModel, id:str):
        try:
            model = (await self._es.get(index=self._esIndexMap[schema], id=id)).body['_source']
            LOG.DEBUG(f'search.read({schema}) {model}')
        except: model = None
        return model

    async def searchDocuments(self, schema:BaseModel, query:dict):
        search = AsyncSearch(using=self._es, index=self._esIndexMap[schema])
        for key, val in query.items():
            op = key[-1]
            if op in ['+', '!']: search = search.filter('match', **{key[:-1]: val}) if op == '+' else search.exclude('match', **{key[:-1]: val})
            else: search = search.query('match', **{key: val})
        models = [hit.to_dict() for hit in await search.execute()]
        if models: LOG.DEBUG(f'search.search({schema}) {models}')
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
