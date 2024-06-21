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


#===============================================================================
# Implement
#===============================================================================
class Redis:

    def __init__(self, config):
        self._redisHostname = config['cache']['hostname']
        self._redisHostport = int(config['cache']['hostport'])
        self._redisUsername = config['cache']['username']
        self._redisPassword = config['cache']['password']
        self._redisLastIndex = int(config['redis']['start_db_index'])
        self._redisDefaultExpire = int(config['redis']['expire'])
        self._redisModelMap = {}
        self._redisExpireMap = {}

    async def registerModel(self, schema:BaseModel, desc:SchemaDescription, expire:int=None):
        self._redisModelMap[schema] = await redis.Redis(
            host=self._redisHostname,
            port=self._redisHostport,
            db=self._redisLastIndex,
            decode_responses=True
        )
        if expire: self._redisExpireMap[schema] = expire
        else: self._redisExpireMap[schema] = self._redisDefaultExpire
        
        # LOG.DEBUG(f'cache.register({schema}) <{self._redisLastIndex}>')
        self._redisLastIndex += 1
    
    async def close(self):
        for conn in self._redisModelMap.values(): await conn.aclose()

    async def read(self, schema:BaseModel, id:str):
        async with self._redisModelMap[schema].pipeline(transaction=True) as pipeline:
            model = (await pipeline.get(id).expire(id, self._redisExpireMap[schema]).execute())[0]
        if model: model = json.loads(model)
        return model
    
    async def __set_redis_data__(self, schema, models):
        expire = self._redisExpireMap[schema]
        async with self._redisModelMap[schema].pipeline(transaction=True) as pipeline:
            for model in models: pipeline.set(model['id'], json.dumps(model, separators=(',', ':')), expire)
            await pipeline.execute()

    async def create(self, schema:BaseModel, *models):
        if models: await self.__set_redis_data__(schema, models)
    
    async def update(self, schema:BaseModel, *models):
        if models: await self.__set_redis_data__(schema, models)

    async def delete(self, schema:BaseModel, id:str):
        await self._redisModelMap[schema].delete(id)
