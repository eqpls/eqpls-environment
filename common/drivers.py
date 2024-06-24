# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from pydantic import BaseModel
from .controls import SchemaDescription


#===============================================================================
# Implement
#===============================================================================
class DriverBase:

    def __init__(self, name, config):
        try: self.config = config[f'driver:{name}']
        except: raise Exception(f'could not find driver config "[driver:{name}]')


class ModelDriverBase(DriverBase):

    def __init__(self, name, config): DriverBase.__init__(self, name, config)

    async def registerModel(self, schema:BaseModel, desc:SchemaDescription, *args, **kargs): pass

    async def close(self): pass

    async def read(self, schema:BaseModel, id:str): pass

    async def create(self, schema:BaseModel, *models): pass

    async def update(self, schema:BaseModel, *models): pass

    async def delete(self, schema:BaseModel, id:str): pass
