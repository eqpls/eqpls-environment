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
from time import time as tstamp
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch, helpers
from luqum.elasticsearch import ElasticsearchQueryBuilder, SchemaAnalyzer

from common import EpException
from common.controls import SchemaDescription, SearchOption
from common.drivers import ModelDriverBase


#===============================================================================
# Implement
#===============================================================================
class ElasticSearch(ModelDriverBase):

    def __init__(self, config):
        ModelDriverBase.__init__(self, 'elasticsearch', config)
        self._esHostname = self.config['hostname']
        self._esHostport = int(self.config['hostport'])
        self._esUsername = self.config['username']
        self._esPassword = self.config['password']
        self._esShards = int(self.config['shards'])
        self._esReplicas = int(self.config['replicas'])
        self._esExpire = int(self.config['expire'])
        self._esSchemaToIndexMap = {}
        self._esSchemaToQueryMap = {}
        self._esSchemaToExpireMap = {}
        self._es = AsyncElasticsearch(
            f'https://{self._esHostname}:{self._esHostport}',
            basic_auth=(self._esUsername, self._esPassword),
            verify_certs=False,
            ssl_show_warn=False
        )

    async def registerModel(self, schema:BaseModel, desc:SchemaDescription, *args, **kargs):
        shards = kargs['shards'] if 'shards' in kargs else None
        replicas = kargs['replicas'] if 'replicas' in kargs else None
        expire = kargs['expire'] if 'expire' in kargs else None

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

        index = desc.category
        mapping = parseModelToMapping(schema)
        mapping['_expire'] = {'type': 'long'}
        indexSchema = {
            'settings': {
                'number_of_shards': shards if shards else self._esShards,
                'number_of_replicas': replicas if replicas else self._esReplicas
            },
            'mappings': {
                'properties': mapping
            }
        }
        if not await self._es.indices.exists(index=index):
            await self._es.indices.create(index=index, body=indexSchema)
        if expire: self._esSchemaToExpireMap[schema] = expire
        else: self._esSchemaToExpireMap[schema] = self._esExpire
        self._esSchemaToQueryMap[schema] = ElasticsearchQueryBuilder(**SchemaAnalyzer(indexSchema).query_builder_options())
        self._esSchemaToIndexMap[schema] = index

    async def close(self):
        await self._es.close()

    async def read(self, schema:BaseModel, id:str):
        try: model = (await self._es.get(index=self._esSchemaToIndexMap[schema], id=id)).body['_source']
        except: model = None
        return model

    async def search(self, schema:BaseModel, option:SearchOption):
        if option.filter: filter = self._esSchemaToQueryMap[schema](option.filter)
        else: filter = None
        if option.orderBy and option.order: sort = [{option.orderBy: option.order}]
        else: sort = None
        models = await self._es.search(index=self._esSchemaToIndexMap[schema], source_includes=option.fields, query=filter, sort=sort, from_=option.skip, size=option.size)
        return [model['_source'] for model in models['hits']['hits']]

    async def count(self, schema:BaseModel, option:SearchOption):
        if option.filter: filter = self._esSchemaToQueryMap[schema](option.filter)
        else: filter = None
        return (await self._es.count(index=self._esSchemaToIndexMap[schema], query=filter))['count']

    def __set_search_expire__(self, model, expire):
        model['_expire'] = expire
        return model

    async def __generate_bulk_data__(self, schema, models):
        index = self._esSchemaToIndexMap[schema]
        expire = int(tstamp()) + self._esSchemaToExpireMap[schema]
        for model in models:
            yield {
                '_op_type': 'update',
                '_index': index,
                '_id': model['id'],
                'doc': self.__set_search_expire__(model, expire),
                'doc_as_upsert': True
            }

    async def create(self, schema:BaseModel, *models):
        if models: await helpers.async_bulk(self._es, self.__generate_bulk_data__(schema, models))

    async def update(self, schema:BaseModel, *models):
        if models: await helpers.async_bulk(self._es, self.__generate_bulk_data__(schema, models))

    async def delete(self, schema:BaseModel, id:str):
        await self._es.delete(index=self._esSchemaToIndexMap[schema], id=id)
