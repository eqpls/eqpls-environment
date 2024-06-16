# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import json
import inspect
from uuid import UUID
from pydantic import BaseModel
from psycopg import AsyncConnection
from stringcase import snakecase
from common import EpException

#===============================================================================
# SingleTon
#===============================================================================


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
        self._psqlFieldNameMap = {}
        self._psqlSnakeNameMap = {}
        self._psqlDumperMap = {}
        self._psqlLoaderMap = {}
        self._psqlTableMap = {}
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

    def __json_dumper__(self, d): return "'" + json.dumps(d, separators=(',', ':')).replace("'", "\'") + "'"

    def __text_dumper__(self, d): return f"'{str(d)}'"

    def __data_dumper__(self, d): return str(d)

    def __json_loader__(self, d): return json.loads(d)

    def __data_loader__(self, d): return d

    async def registerModel(self, schema:BaseModel):
        table = f'{snakecase(schema.__module__)}_{snakecase(schema.__name__)}'
        fields = sorted(schema.__annotations__.keys())
        snakes = [snakecase(field) for field in fields]
        self._psqlFieldNameMap[schema] = fields
        self._psqlSnakeNameMap[schema] = snakes

        index = 0
        columns = []
        dumpers = []
        loaders = []
        for field in fields:
            fieldType = schema.__annotations__[field]
            if fieldType == str:
                columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__text_dumper__)
                loaders.append(self.__data_loader__)
            elif fieldType == int:
                columns.append(f'{snakes[index]} INTEGER')
                dumpers.append(self.__data_dumper__)
                loaders.append(self.__data_loader__)
            elif fieldType == float:
                columns.append(f'{snakes[index]} DOUBLE PRECISION')
                dumpers.append(self.__data_dumper__)
                loaders.append(self.__data_loader__)
            elif fieldType == bool:
                columns.append(f'{snakes[index]} BOOL')
                dumpers.append(self.__data_dumper__)
                loaders.append(self.__data_loader__)
            elif fieldType == UUID:
                if field == 'id': columns.append(f'id TEXT PRIMARY KEY')
                else: columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__text_dumper__)
                loaders.append(self.__data_loader__)
            elif (inspect.isclass(fieldType) and issubclass(fieldType, BaseModel)) or fieldType in [list, dict]:
                columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__json_dumper__)
                loaders.append(self.__json_loader__)
            elif getattr(fieldType, '__origin__', None) == list:
                columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__json_dumper__)
                loaders.append(self.__json_loader__)
            else: raise EpException(500, f'database.registerModel({schema}.{field}{fieldType}): could not parse schema')
            index += 1
        self._psqlDumperMap[schema] = dumpers
        self._psqlLoaderMap[schema] = loaders

        await self.__late_init__()
        async with self._psql.cursor() as cursor:
            await cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({','.join(columns)});")
            await self._psql.commit()
        self._psqlTableMap[schema] = table
        LOG.INFO(f'database.register({schema}) <{table}>')

    async def select(self, schema:BaseModel, query:dict, unique:bool=False, top:int=0):
        table = self._psqlTableMap[schema]
        fields = self._psqlFieldNameMap[schema]

        if query:
            conditions = []
            for key, val in query.items():
                if type(val) in [str, UUID]: conditions.append(f"{snakecase(key)}='{val}'")
                else: conditions.append(f'{snakecase(key)}={val}')
            query = f"SELECT * FROM {table} WHERE {' AND '.join(conditions)}"
        else: query = f'SELECT * FROM {table}'
        if unique: query = query + ' LIMIT 1;'
        elif top > 0: query = query + f' LIMIT {top};'
        else: query = query + ';'

        cursor = self._psql.cursor()
        await cursor.execute(query)
        if unique:
            records = await cursor.fetchone()
            if records: records = [records]
            else: records = []
        else: records = await cursor.fetchall()
        await cursor.close()

        loaders = self._psqlLoaderMap[schema]
        models = []
        for record in records:
            index = 0
            model = {}
            for column in record:
                model[fields[index]] = loaders[index](column)
                index += 1
            models.append(model)

        LOG.DEBUG(f'database.select({query}) {models}')
        return models

    async def count(self, schema:BaseModel, query:dict):
        table = self._psqlTableMap[schema]

        if query:
            conditions = []
            for key, val in query.items():
                if type(val) in [str, UUID]: conditions.append(f"{snakecase(key)}='{val}'")
                else: conditions.append(f'{snakecase(key)}={val}')
            query = f"SELECT COUNT(*) FROM {table} WHERE {' AND '.join(conditions)};"
        else: query = f'SELECT COUNT(*) FROM {table};'

        cursor = self._psql.cursor()
        await cursor.execute(query)
        count = int((await cursor.fetchall())[0])
        await cursor.close()
        return count

    async def insert(self, schema:BaseModel, *models):
        if models:
            table = self._psqlTableMap[schema]
            fields = self._psqlFieldNameMap[schema]
            dumpers = self._psqlDumperMap[schema]
            cursor = self._psql.cursor()

            for model in models:
                index = 0
                values = []
                for field in fields:
                    values.append(dumpers[index](model[field]))
                    index += 1
                query = f"INSERT INTO {table} VALUES({','.join(values)});"
                await cursor.execute(query)
                await cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id='{model['id']}';")
                LOG.DEBUG(f'database.insert({query})')

            results = [bool(result) for result in await cursor.fetchall()]
            await self._psql.commit()
            await cursor.close()
            return results
        return []

    async def update(self, schema:BaseModel, *models):
        if models:
            table = self._psqlTableMap[schema]
            fields = self._psqlFieldNameMap[schema]
            snakes = self._psqlSnakeNameMap[schema]
            dumpers = self._psqlDumperMap[schema]
            cursor = self._psql.cursor()

            for model in models:
                id = model['id']
                index = 0
                values = []
                for field in fields:
                    value = dumpers[index](model[field])
                    values.append(f'{snakes[index]}={value}')
                    index += 1
                query = f"UPDATE {table} SET {','.join(values)} WHERE id='{id}';"
                await cursor.execute(query)
                await cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id='{id}';")
                LOG.DEBUG(f'database.update({query})')

            results = [bool(result) for result in await cursor.fetchall()]
            await self._psql.commit()
            await cursor.close()
            return results
        return []

    async def delete(self, schema:BaseModel, id:str):
        table = self._psqlTableMap[schema]
        cursor = self._psql.cursor()
        query = f"DELETE FROM {table} WHERE id='{id}';"
        await cursor.execute(query)
        await cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id='{id}';")
        result = [bool(not result[0]) for result in await cursor.fetchall()][0]
        await self._psql.commit()
        await cursor.close()
        LOG.DEBUG(f'database.delete({query})')
        return result
