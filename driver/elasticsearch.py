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

from common import EpException, BaseSchema, Search, ModelDriverBase


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
        self._esConn = None

    async def connect(self, *args, **kargs):
        if not self._esConn:
            self._esConn = AsyncElasticsearch(
                f'https://{self._esHostname}:{self._esHostport}',
                basic_auth=(self._esUsername, self._esPassword),
                verify_certs=False,
                ssl_show_warn=False
            )
        return self

    async def disconnect(self):
        if self._esConn:
            try: await self._esConn.close()
            except: pass
            self._esConn = None

    async def registerModel(self, schema:BaseSchema, *args, **kargs):
        info = schema.getSchemaInfo()

        if 'shards' not in info.search or not info.search['shards']: info.search['shards'] = self._esShards
        if 'replicas' not in info.search or not info.search['replicas']: info.search['replicas'] = self._esReplicas
        if 'expire' not in info.search or not info.search['expire']: info.search['expire'] = self._esExpire

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

        mapping = parseModelToMapping(schema)
        mapping['_expireAt'] = {'type': 'long'}
        indexSchema = {
            'settings': {
                'number_of_shards': info.search['shards'],
                'number_of_replicas': info.search['replicas']
            },
            'mappings': {
                'properties': mapping
            }
        }
        if not await self._esConn.indices.exists(index=info.dref): await self._esConn.indices.create(index=info.dref, body=indexSchema)
        info.search['filter'] = ElasticsearchQueryBuilder(**SchemaAnalyzer(indexSchema).query_builder_options())

    async def read(self, schema:BaseSchema, id:str):
        try: model = (await self._esConn.get(index=schema.getSchemaInfo().dref, id=id, source_excludes=['_expireAt'])).body['_source']
        except: model = None
        return model

    async def search(self, schema:BaseSchema, search:Search):
        info = schema.getSchemaInfo()
        if search.filter: filter = info.search['filter'](search.filter)
        else: filter = {'match_all': {}}
        if search.orderBy and search.order: sort = [{search.orderBy: search.order}]
        else: sort = None

        models = await self._esConn.search(index=info.dref, source_includes=search.fields, source_excludes=['_expireAt'], query=filter, sort=sort, from_=search.skip, size=search.size)
        return [model['_source'] for model in models['hits']['hits']]

    async def count(self, schema:BaseSchema, search:Search):
        info = schema.getSchemaInfo()
        if search.filter: filter = info.search['filter'](search.filter)
        else: filter = {'match_all': {}}
        return (await self._esConn.count(index=info.dref, query=filter))['count']

    def __set_search_expire__(self, model, expire):
        model['_expireAt'] = expire
        return model

    async def __generate_bulk_data__(self, schema:BaseSchema, models):
        info = schema.getSchemaInfo()
        expire = int(tstamp()) + info.search['expire']
        for model in models:
            yield {
                '_op_type': 'update',
                '_index': info.dref,
                '_id': model['id'],
                'doc': self.__set_search_expire__(model, expire),
                'doc_as_upsert': True
            }

    async def create(self, schema:BaseSchema, *models):
        if models: await helpers.async_bulk(self._esConn, self.__generate_bulk_data__(schema, models))

    async def update(self, schema:BaseSchema, *models):
        if models: await helpers.async_bulk(self._esConn, self.__generate_bulk_data__(schema, models))

    async def delete(self, schema:BaseSchema, id:str):
        await self._esConn.delete(index=schema.getSchemaInfo().dref, id=id)
