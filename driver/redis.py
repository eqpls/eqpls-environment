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
from uuid import UUID
from pydantic import BaseModel


#===============================================================================
# Implement
#===============================================================================
class Redis:
    
    class Pipeline:
        
        def __init__(self, pipeline):
            self._pipeline = pipeline
        
        def get(self, id:UUID):
            self._pipeline.get(str(id))
            return self
        
        def set(self, model:BaseModel):
            self._pipeline.set(str(model.id), json.dumps(model.dict(), separators=(',', ':')))
            return self
        
        def expire(self, id:UUID, seconds:int):
            self._pipeline.expire(str(id), seconds)
            return self
        
        def delete(self, id:UUID):
            self._pipeline.delete(str(id))
            return self
        
        async def execute(self):
            return await self._pipeline.execute()
    
    def __init__(self, config):
        self._redisHostname = config['redis']['hostname']
        self._redisHostport = int(config['redis']['hostport'])
        self._redisUsername = config['redis']['username']
        self._redisPassword = config['redis']['password']
        self._redisLastIndex = 1
        self._redisModelMap = {}
    
    async def createDatabase(self, schema:BaseModel):
        path = f'{schema.__module__}.{schema.__name__}' 
        self._redisModelMap[path] = redis.Redis(host=self._redisHostname, port=self._redisHostport, db=self._redisLastIndex)
        LOG.INFO(f'create database on redis: {path} [{self._redisLastIndex}]')
        self._redisLastIndex += 1
    
    def pipeline(self, schema:BaseModel):
        conn = self._redisModelMap[f'{schema.__module__}.{schema.__name__}']
        return Redis.Pipeline(conn.pipeline(transaction=True))
    
    async def get(self, schema:BaseModel, id:UUID):
        conn = self._redisModelMap[f'{schema.__module__}.{schema.__name__}']
        return schema(**json.loads(await conn.get(str(id))))
    
    async def set(self, model:BaseModel):
        conn = self._redisModelMap[f'{model.__class__.__module__}.{model.__class__.__name__}']
        return await conn.set(str(model.id), json.dumps(model.dict(), separators=(',', ':')))
    
    async def expire(self, schema:BaseModel, id:UUID, seconds:int):
        conn = self._redisModelMap[f'{schema.__module__}.{schema.__name__}']
        return await conn.expire(str(id), seconds)
    
    async def delete(self, schema:BaseModel, id:UUID):
        conn = self._redisModelMap[f'{schema.__module__}.{schema.__name__}']
        return await conn.delete(str(id))
    
