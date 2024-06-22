# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import json
import redis.asyncio as redis
from pydantic import BaseModel

from common.controls import SchemaDescription
from common.drivers import ModelDriverBase


#===============================================================================
# Implement
#===============================================================================
class Redis(ModelDriverBase):

    def __init__(self, config):
        ModelDriverBase.__init__(self, 'redis', config)
        self._redisHostname = self.config['hostname']
        self._redisHostport = int(self.config['hostport'])
        self._redisUsername = self.config['username']
        self._redisPassword = self.config['password']
        self._redisLastIndex = int(self.config['start_db_index'])
        self._redisDefaultExpire = int(self.config['expire'])
        self._redisSchemaToConnMap = {}
        self._redisSchemaToExpireMap = {}

    async def registerModel(self, schema:BaseModel, desc:SchemaDescription, expire:int=None):
        self._redisSchemaToConnMap[schema] = await redis.Redis(
            host=self._redisHostname,
            port=self._redisHostport,
            db=self._redisLastIndex,
            decode_responses=True
        )
        if expire: self._redisSchemaToExpireMap[schema] = expire
        else: self._redisSchemaToExpireMap[schema] = self._redisDefaultExpire
        self._redisLastIndex += 1

    async def close(self):
        for conn in self._redisSchemaToConnMap.values(): await conn.aclose()

    async def read(self, schema:BaseModel, id:str):
        async with self._redisSchemaToConnMap[schema].pipeline(transaction=True) as pipeline:
            model = (await pipeline.get(id).expire(id, self._redisSchemaToExpireMap[schema]).execute())[0]
        if model: model = json.loads(model)
        return model

    async def __set_redis_data__(self, schema, models):
        expire = self._redisSchemaToExpireMap[schema]
        async with self._redisSchemaToConnMap[schema].pipeline(transaction=True) as pipeline:
            for model in models: pipeline.set(model['id'], json.dumps(model, separators=(',', ':')), expire)
            await pipeline.execute()

    async def create(self, schema:BaseModel, *models):
        if models: await self.__set_redis_data__(schema, models)

    async def update(self, schema:BaseModel, *models):
        if models: await self.__set_redis_data__(schema, models)

    async def delete(self, schema:BaseModel, id:str):
        await self._redisSchemaToConnMap[schema].delete(id)
