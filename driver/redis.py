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

from common import BaseSchema
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
        self._redisExpire = int(self.config['expire'])
        self._redisConnList = []

    async def registerModel(self, schema:BaseSchema, *args, **kargs):
        info = schema.getSchemaInfo()
        if 'expire' not in info.cacheOption or not info.cacheOption['expire']: info.cacheOption['expire'] = self._redisExpire
        conn = await redis.Redis(
            host=self._redisHostname,
            port=self._redisHostport,
            db=self._redisLastIndex,
            decode_responses=True
        )
        info.cacheOption['conn'] = conn
        self._redisConnList.append(conn)
        self._redisLastIndex += 1
        info.cache = self

    async def close(self):
        for conn in self._redisConnList: await conn.aclose()

    async def read(self, schema:BaseSchema, id:str):
        info = schema.getSchemaInfo()
        async with info.cacheOption['conn'].pipeline(transaction=True) as pipeline:
            model = (await pipeline.get(id).expire(id, info.cacheOption['expire']).execute())[0]
        if model: model = json.loads(model)
        return model

    async def __set_redis_data__(self, schema, models):
        info = schema.getSchemaInfo()
        async with info.cacheOption['conn'].pipeline(transaction=True) as pipeline:
            for model in models: pipeline.set(model['id'], json.dumps(model, separators=(',', ':')), info.cacheOption['expire'])
            await pipeline.execute()

    async def create(self, schema:BaseSchema, *models):
        if models: await self.__set_redis_data__(schema, models)

    async def update(self, schema:BaseSchema, *models):
        if models: await self.__set_redis_data__(schema, models)

    async def delete(self, schema:BaseSchema, id:str):
        await schema.getSchemaInfo().cacheOption['conn'].delete(id)
