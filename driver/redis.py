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
        self._redisModelIndex = int(self.config['model_index'])
        self._redisModelExpire = int(self.config['model_expire'])
        self._redisQueueIndex = int(self.config['queue_index'])
        self._redisQueueExpire = int(self.config['queue_expire'])
        self.model = None
        self.queue = None

    async def registerModel(self, schema:BaseSchema, *args, **kargs):
        info = schema.getSchemaInfo()
        if 'expire' not in info.cacheOption or not info.cacheOption['expire']: info.cacheOption['expire'] = self._redisModelExpire
        if not self.model:
            self.model = await redis.Redis(
                host=self._redisHostname,
                port=self._redisHostport,
                db=self._redisModelIndex,
                decode_responses=True
            )
        info.cache = self

    async def close(self):
        if self.model: await self.model.aclose()
        if self.queue: await self.queue.aclose()

    async def read(self, schema:BaseSchema, id:str):
        info = schema.getSchemaInfo()
        async with info.cache.model.pipeline(transaction=True) as pipeline:
            model = (await pipeline.get(id).expire(id, info.cacheOption['expire']).execute())[0]
        if model: model = json.loads(model)
        return model

    async def __set_redis_data__(self, schema, models):
        info = schema.getSchemaInfo()
        async with info.cache.model.pipeline(transaction=True) as pipeline:
            for model in models: pipeline.set(model['id'], json.dumps(model, separators=(',', ':')), info.cacheOption['expire'])
            await pipeline.execute()

    async def create(self, schema:BaseSchema, *models):
        if models: await self.__set_redis_data__(schema, models)

    async def update(self, schema:BaseSchema, *models):
        if models: await self.__set_redis_data__(schema, models)

    async def delete(self, schema:BaseSchema, id:str):
        await schema.getSchemaInfo().cache.model.delete(id)
