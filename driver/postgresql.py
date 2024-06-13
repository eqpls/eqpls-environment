# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import re
import json
import inspect
import datetime
from uuid import UUID
from pydantic import BaseModel
from psycopg import AsyncConnection
from stringcase import snakecase
from common import EpException

#===============================================================================
# SingleTon
#===============================================================================
__OPERATOR_PARSER__ = re.compile(r'^(?P<key>\w+):(?P<op>\w+)$')


#===============================================================================
# Implement
#===============================================================================
class Postgresql:
    
    def __init__(self, config):
        self._psqlHostname = config['postgresql']['hostname']
        self._psqlHostport = int(config['postgresql']['hostport'])
        self._psqlUsername = config['postgresql']['username']
        self._psqlPassword = config['postgresql']['password']
        self._psqlDatabase = config['postgresql']['database']
        self._psqlValueTypeList = [str, int, float, bool]
        self._psqlStrConvertList = [UUID, datetime]
        self._psqlJsonConvertList = [dict, list]
        self._psqlModelFieldMap = {}
        self._psqlSnakeFieldMap = {}
        self._psql = None
    
    async def __late_init__(self):
        if not self._psql:
            self._psql = True
            self._psql = await AsyncConnection.connect(
                host=self._psqlHostname,
                port=self._psqlHostport,
                dbname=self._psqlDatabase,
                user=self._psqlUsername,
                password=self._psqlPassword
            )
    
    async def createTable(self, schema:BaseModel): 
        table = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
        fields = sorted(schema.__annotations__.keys())
        snakes = [snakecase(field) for field in fields]
        self._psqlModelFieldMap[table] = fields
        self._psqlSnakeFieldMap[table] = snakes
        
        index = 0
        columns = []
        for field in fields:
            fieldType = schema.__annotations__[field]
            if fieldType == str: columns.append(f'{snakes[index]} TEXT')
            elif fieldType == int: columns.append(f'{snakes[index]} INTEGER')
            elif fieldType == float: columns.append(f'{snakes[index]} DOUBLE PRECISION')
            elif fieldType == bool: columns.append(f'{snakes[index]} BOOL')
            elif fieldType == UUID: columns.append(f'id TEXT PRIMARY KEY')
            elif inspect.isclass(fieldType) and issubclass(fieldType, BaseModel) or fieldType in [list, dict]: columns.append(f'{snakes[index]} TEXT')
            elif getattr(fieldType, '__origin__', None) == list: columns.append(f'{snakes[index]} TEXT')
            else: raise EpException(500, f'could not parse schema[{schema}.{field}{fieldType}]')
            
            # if issubclass(fieldType, BaseModel) or fieldType in [list, dict]: columns.append(f'{snakes[index]} TEXT')
            # elif fieldType == UUID: columns.append(f'id TEXT PRIMARY KEY')
            # # else: raise EpException(500, f'could not parse schema[{schema}.{field}{fieldType}]')
            # elif getattr(fieldType, '__origin__', None) == list: columns.append(f'{snakes[index]} TEXT')
            # elif fieldType == int: columns.append(f'{snakes[index]} INTEGER')
            # elif fieldType == float: columns.append(f'{snakes[index]} DOUBLE PRECISION')
            # elif fieldType == bool: columns.append(f'{snakes[index]} BOOL')
            # else: columns.append(f'{snakes[index]} TEXT')
            index += 1
        
        await self.__late_init__()
        cursor = await self._psql.cursor()
        await cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({','.join(columns)});")
        await cursor.commit()
        await cursor.close()
    
    def __json_loader__(self, d): return json.loads(d)

    def __data_loader__(self, d): return d
    
    async def select(self, schema:BaseModel, transaction=None, **query):
        table = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
        fields = self._psqlModelFieldMap[table]
        
        if query:
            conditions = []
            for key, val in query.items():
                if type(val) == str: conditions.append(f"{snakecase(key)}='{val}'")
                else: conditions.append(f'{snakecase(key)}={val}')
            query = f"SELECT * FROM {table} WHERE {','.join(conditions)};"
        else: query = f'SELECT * FROM {table};'
        
        if not transaction: cursor = await self._psql.cursor()
        else: cursor = transaction
        await cursor.execute(query)
        
        loaders = []
        for field in fields:
            fieldType = schema.__annotations__[field]
            if inspect.isclass(fieldType):
                if issubclass(fieldType, BaseModel) or fieldType in [list, dict]: loaders.append(self.__json_loader__)
                else: raise EpException(f'could not parse schema[{schema}.{field}{fieldType}]')
            elif getattr(fieldType, '__origin__', None) == list: loaders.append(self.__json_loader__)
            else: loaders.append(self.__data_loader__)
        
        models = []
        for record in await cursor.fetchall():
            model = {}
            index = 0
            for column in record:
                model[fields[index]] = loaders[index](column)
                index += 1
            models.append(model)
        if not transaction: await cursor.close()
        return models
    
    async def insert(self, *models:BaseModel, transaction=None):
        if not transaction: cursor = await self._psql.cursor()
        else: cursor = transaction
        
        for model in models: 
            table = f'{snakecase(model.__class__.__module__)}_{snakecase(model.__class__.__name__)}'
            fields = self._psqlModelFieldMap[table]
            model = model.dict()
            values = []
            for field in fields:
                value = model[field]
                valueType = type(value)
                if valueType in [list, dict]: value = "'" + json.dumps(value, separators=(',', ':')).replace("'", "\'") + "'"
                elif valueType == str: value = "'" + value.replace("'", "\'") + "'"
                values.append(value)
            await cursor.execute(f"INSERT INTO {table} VALUES({','.join(values)});")
        await cursor.commit()
        if not transaction: await cursor.close()
    
    async def update(self, *models:BaseModel, transaction=None):
        if not transaction: cursor = await self._psql.cursor()
        else: cursor = transaction
        
        for model in models: 
            table = f'{snakecase(model.__class__.__module__)}_{snakecase(model.__class__.__name__)}'
            fields = self._psqlModelFieldMap[table]
            snakes = self._psqlSnakeFieldMap[table]
            model = model.dict()
            index = 0
            values = []
            for field in fields:
                value = model[field]
                valueType = type(value)
                if valueType in [list, dict]: value = "'" + json.dumps(value, separators=(',', ':')).replace("'", "\'") + "'"
                elif valueType == str: value = "'" + value.replace("'", "\'") + "'"
                values.append(f'{snakes[index]}={value}')
                index += 1
            await cursor.execute(f"UPDATE {table} SET {','.join(values)} WHERE id='{model[id]}';")
        await cursor.commit()
        if not transaction: await cursor.close()
    
    async def delete(self, *models:BaseModel, transaction=None):
        if not transaction: cursor = await self._psql.cursor()
        else: cursor = transaction
        
        for model in models: 
            table = f'{snakecase(model.__class__.__module__)}_{snakecase(model.__class__.__name__)}'
            await cursor.execute(f"DELETE FROM {table} WHERE id='{model.id}';")
        await cursor.commit()
        if not transaction: await cursor.close()
