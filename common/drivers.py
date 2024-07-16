# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from typing import Any
from pydantic import BaseModel


#===============================================================================
# Implement
#===============================================================================
class DriverBase:

    def __init__(self, name, config):
        try: self.config = config[f'driver:{name}']
        except: raise Exception(f'could not find driver config "[driver:{name}]')

    async def connect(self, *args, **kargs): return self

    async def disconnect(self): pass


class KeyValueDriverBase(DriverBase):

    def __init__(self, name, config): DriverBase.__init__(self, name, config)

    async def read(self, key:str, *args, **kargs): pass

    async def write(self, key:str, val:Any, *args, **kargs): pass

    async def delete(self, key:str, *args, **kargs): pass


class NetworkDriverBase(DriverBase):

    def __init__(self, name, config): DriverBase.__init__(self, name, config)

    async def listen(self, address:str, *args, **kargs): pass

    async def receive(self, *args, **kargs): pass

    async def send(self, address:str, data:Any, *args, **kargs): pass


class ModelDriverBase(DriverBase):

    def __init__(self, name, config): DriverBase.__init__(self, name, config)

    async def registerModel(self, schema:BaseModel, *args, **kargs): pass

    async def read(self, schema:BaseModel, id:str): pass

    async def create(self, schema:BaseModel, *models): pass

    async def update(self, schema:BaseModel, *models): pass

    async def delete(self, schema:BaseModel, id:str): pass
