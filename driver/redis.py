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


#===============================================================================
# Implement
#===============================================================================
class Redis:

    def __init__(self, config):
        self._redisHostname = config['redis']['hostname']
        self._redisHostport = int(config['redis']['hostport'])
        self._redisUsername = config['redis']['username']
        self._redisPassword = config['redis']['password']
        self._redisDefaultExpire = int(config['redis']['expire'])
        self._redisLastIndex = 1
        self._redisModelMap = {}
        self._redisExpireMap = {}

    async def registerModel(self, schema:BaseModel, expire:int=None):
        self._redisModelMap[schema] = await redis.Redis(
            host=self._redisHostname,
            port=self._redisHostport,
            db=self._redisLastIndex,
            decode_responses=True
        )
        if expire: self._redisExpireMap[schema] = expire
        else: self._redisExpireMap[schema] = self._redisDefaultExpire
        LOG.INFO(f'cache.register({schema}) <{self._redisLastIndex}>')
        self._redisLastIndex += 1

    async def get(self, schema:BaseModel, id:str):
        async with self._redisModelMap[schema].pipeline(transaction=True) as pipeline:
            model = (await pipeline.get(id).expire(id, self._redisExpireMap[schema]).execute())[0]
        if model:
            model = json.loads(model)
            LOG.DEBUG(f'cache.get({schema}) {model}')
        return model

    async def set(self, schema:BaseModel, *models):
        if models:
            expire = self._redisExpireMap[schema]
            async with self._redisModelMap[schema].pipeline(transaction=True) as pipeline:
                for model in models: pipeline.set(model['id'], json.dumps(model, separators=(',', ':')), expire)
                await pipeline.execute()
            LOG.DEBUG(f'cache.set({schema}) {models}')

    async def delete(self, schema:BaseModel, id:str):
        if await self._redisModelMap[schema].delete(id):
            LOG.DEBUG(f'cache.del({schema}) {id}')
