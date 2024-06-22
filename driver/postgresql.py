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
from stringcase import snakecase
from psycopg import AsyncConnection
from luqum.tree import Item, Term, SearchField, Group, FieldGroup, Range, From, To, AndOperation, OrOperation, Not, UnknownOperation

from common import asleep, runBackground, EpException
from common.controls import SchemaDescription, SearchOption
from common.drivers import ModelDriverBase


#===============================================================================
# Implement
#===============================================================================
class PostgreSql(ModelDriverBase):

    def __init__(self, config):
        ModelDriverBase.__init__(self, 'postgresql', config)
        self._psqlWriterHostname = self.config['writer_hostname']
        self._psqlWriterHostport = int(self.config['writer_hostport'])
        self._psqlReaderHostname = self.config['reader_hostname']
        self._psqlReaderHostport = int(self.config['reader_hostport'])
        self._psqlUsername = self.config['username']
        self._psqlPassword = self.config['password']
        self._psqlDatabase = self.config['database']
        self._psqlSchemaToFieldIndexMap = {}
        self._psqlSchemaToFieldNameMap = {}
        self._psqlSchemaToSnakeNameMap = {}
        self._psqlSchemaToDumperMap = {}
        self._psqlSchemaToLoaderMap = {}
        self._psqlSchemaToTableMap = {}
        self._psqlWriter = None
        self._psqlReader = None
        self._psqlRestore = False

    async def __connect__(self):
        if not self._psqlWriter:
            self._psqlWriter = await AsyncConnection.connect(
                host=self._psqlWriterHostname,
                port=self._psqlWriterHostport,
                dbname=self._psqlDatabase,
                user=self._psqlUsername,
                password=self._psqlPassword
            )

        if not self._psqlReader:
            self._psqlReader = await AsyncConnection.connect(
                host=self._psqlReaderHostname,
                port=self._psqlReaderHostport,
                dbname=self._psqlDatabase,
                user=self._psqlUsername,
                password=self._psqlPassword
            )

    async def __restore__(self):
        if not self._psqlRestore:
            self._psqlResotre = True
            await runBackground(self.__restore_background__())

    async def __restore_background__(self):
        while True:
            await asleep(1)
            if self._psqlWriter:
                try: await self._psqlWriter.close()
                except: pass
                self._psqlWriter = None
            if self._psqlReader:
                try: await self._psqlReader.close()
                except: pass
                self._psqlReader = None
            try: await self.__connect__()
            except: continue
            break
        self._psqlRestore = False

    def __parseLuceneToTsquery__(self, node:Item):
        nodeType = type(node)
        if isinstance(node, Term):
            terms = filter(None, str(node.value).strip('"').lower().split(' '))
            return f"{'|'.join(terms)}"
        elif nodeType == SearchField:
            if '.' in node.name: fieldName = snakecase(node.name.split('.')[0])
            else: fieldName = snakecase(node.name)
            exprType = type(node.expr)
            if exprType in [Range, From, To]:
                if exprType == Range: return f'{fieldName} >= {node.expr.low} AND {fieldName} <= {node.expr.high}'
                elif exprType == From: return f"{fieldName} >{'=' if node.expr.include else ''} {node.expr.a}"
                elif exprType == To: return f"{fieldName} <{'=' if node.expr.include else ''} {node.expr.a}"
            else:
                result = self.__parseLuceneToTsquery__(node.expr)
                if result: return f"{fieldName}@@'{result}'::tsquery"
            return None
        elif nodeType == Group:
            result = self.__parseLuceneToTsquery__(node.expr)
            if result: return f'({result})'
            return None
        elif nodeType == FieldGroup:
            return self.__parseLuceneToTsquery__(node.expr)
        elif nodeType == AndOperation:
            operand1 = node.operands[0]
            operand2 = node.operands[1]
            result1 = self.__parseLuceneToTsquery__(operand1)
            result2 = self.__parseLuceneToTsquery__(operand2)
            if result1 and result2:
                if (isinstance(operand1, Term) or type(operand1) == Not) and (isinstance(operand2, Term) or type(operand2) == Not): return f'{result1}&{result2}'
                else: return f'{result1} AND {result2}'
            return None
        elif nodeType == OrOperation:
            operand1 = node.operands[0]
            operand2 = node.operands[1]
            result1 = self.__parseLuceneToTsquery__(operand1)
            result2 = self.__parseLuceneToTsquery__(operand2)
            if result1 and result2:
                if (isinstance(operand1, Term) or type(operand1) == Not) and (isinstance(operand2, Term) or type(operand2) == Not): return f'{result1}|{result2}'
                else: return f'{result1} OR {result2}'
            return None
        elif nodeType == Not:
            result = self.__parseLuceneToTsquery__(node.a)
            if result:
                if isinstance(node.a, Term): return f'!{result}'
                else: return f'NOT {result}'
            return None
        elif nodeType == UnknownOperation:
            if hasattr(node, 'operands'):
                operand = str(node.operands[1]).upper()
                if operand == 'AND': opermrk = '&'
                elif operand == 'OR': opermrk = '|'
                elif operand == '&':
                    opermrk = operand
                    operand = 'AND'
                elif operand == '|':
                    opermrk = operand
                    operand = 'OR'
                else: raise EpException(400, f'Could Not Parse Filter: {node} >> {nodeType}{node.__dict__}')
                operand1 = node.operands[0]
                operand2 = node.operands[2]
                if (isinstance(operand1, Term) or type(operand1) == Not) and (isinstance(operand2, Term) or type(operand2) == Not):
                    return f"{self.__parseLuceneToTsquery__(operand1)}{opermrk}{self.__parseLuceneToTsquery__(operand2)}"
                else:
                    return f"{self.__parseLuceneToTsquery__(operand1)} {operand} {self.__parseLuceneToTsquery__(operand2)}"
        raise EpException(400, f'Could Not Parse Filter: {node} >> {nodeType}{node.__dict__}')

    def __json_dumper__(self, d): return "'" + json.dumps(d, separators=(',', ':')).replace("'", "\'") + "'"

    def __text_dumper__(self, d): return f"'{str(d)}'"

    def __data_dumper__(self, d): return str(d)

    def __json_loader__(self, d): return json.loads(d)

    def __data_loader__(self, d): return d

    async def registerModel(self, schema:BaseModel, desc:SchemaDescription, expire=None):
        table = desc.category
        fields = sorted(schema.model_fields.keys())
        snakes = [snakecase(field) for field in fields]
        self._psqlSchemaToFieldNameMap[schema] = fields
        self._psqlSchemaToSnakeNameMap[schema] = snakes

        index = 0
        columns = []
        dumpers = []
        loaders = []
        indices = {}
        for field in fields:
            fieldType = schema.model_fields[field].annotation
            if fieldType == str:
                columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__text_dumper__)
                loaders.append(self.__data_loader__)
                indices[fields[index]] = index
            elif fieldType == int:
                columns.append(f'{snakes[index]} INTEGER')
                dumpers.append(self.__data_dumper__)
                loaders.append(self.__data_loader__)
                indices[fields[index]] = index
            elif fieldType == float:
                columns.append(f'{snakes[index]} DOUBLE PRECISION')
                dumpers.append(self.__data_dumper__)
                loaders.append(self.__data_loader__)
                indices[fields[index]] = index
            elif fieldType == bool:
                columns.append(f'{snakes[index]} BOOL')
                dumpers.append(self.__data_dumper__)
                loaders.append(self.__data_loader__)
                indices[fields[index]] = index
            elif fieldType == UUID:
                if field == 'id': columns.append(f'id TEXT PRIMARY KEY')
                else: columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__text_dumper__)
                loaders.append(self.__data_loader__)
                indices[fields[index]] = index
            elif (inspect.isclass(fieldType) and issubclass(fieldType, BaseModel)):
                columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__json_dumper__)
                loaders.append(self.__json_loader__)
                indices[fields[index]] = index
            elif fieldType in [list, dict]:
                columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__json_dumper__)
                loaders.append(self.__json_loader__)
                indices[fields[index]] = index
            elif getattr(fieldType, '__origin__', None) in [list, dict]:
                columns.append(f'{snakes[index]} TEXT')
                dumpers.append(self.__json_dumper__)
                loaders.append(self.__json_loader__)
                indices[fields[index]] = index
            else: raise EpException(500, f'database.registerModel({schema}.{field}{fieldType}): could not parse schema')
            index += 1
        self._psqlSchemaToDumperMap[schema] = dumpers
        self._psqlSchemaToLoaderMap[schema] = loaders
        self._psqlSchemaToFieldIndexMap[schema] = indices

        try: await self.__connect__()
        except: exit(1)
        async with self._psqlWriter.cursor() as cursor:
            await cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({','.join(columns)});")
            await self._psqlWriter.commit()
        self._psqlSchemaToTableMap[schema] = table

    async def close(self):
        await self._psqlWriter.close()
        await self._psqlReader.close()

    async def read(self, schema:BaseModel, id:str):
        table = self._psqlSchemaToTableMap[schema]
        fields = self._psqlSchemaToFieldNameMap[schema]

        query = f"SELECT * FROM {table} WHERE id='{id}' AND deleted=FALSE LIMIT 1;"
        cursor = self._psqlReader.cursor()
        try:
            await cursor.execute(query)
            record = await cursor.fetchone()
        except Exception as e:
            await cursor.close()
            await self.__restore__()
            raise e
        await cursor.close()

        if record:
            loaders = self._psqlSchemaToLoaderMap[schema]
            index = 0
            model = {}
            for column in record:
                model[fields[index]] = loaders[index](column)
                index += 1
            return model
        return None

    async def search(self, schema:BaseModel, option:SearchOption):
        table = self._psqlSchemaToTableMap[schema]
        fields = self._psqlSchemaToFieldNameMap[schema]
        unique = False

        if option.fields:
            termFields = [field.split('.')[0] for field in option.fields]
            columns = ','.join([snakecase(field) for field in termFields])
        else: columns = '*'
        if option.query:
            query = option.query
            conditions = []
            for key, val in query.items():
                if type(val) in [str, UUID]: conditions.append(f"{snakecase(key)}='{val}'")
                else: conditions.append(f'{snakecase(key)}={val}')
            query = [f"{' AND '.join(conditions)}"]
        else: query = []
        if option.filter:
            filter = self.__parseLuceneToTsquery__(option.filter)
            if filter: filter = [filter]
            else: filter = []
        else: filter = []
        condition = ' AND '.join(query + filter)
        if condition: condition = f' AND {condition}'
        if option.orderBy and option.order: condition = f'{condition} ORDER BY {snakecase(option.orderBy)} {option.order.upper()}'
        if option.size:
            if option.size == 1: unique = True
            condition = f'{condition} LIMIT {option.size}'
        if option.skip: condition = f'{condition} OFFSET {option.skip}'
        query = f'SELECT {columns} FROM {table} WHERE deleted=FALSE{condition};'

        cursor = self._psqlReader.cursor()
        try:
            await cursor.execute(query)
            if unique:
                records = await cursor.fetchone()
                if records: records = [records]
                else: records = []
            else: records = await cursor.fetchall()
        except Exception as e:
            await cursor.close()
            await self.__restore__()
            raise e
        await cursor.close()

        loaders = self._psqlSchemaToLoaderMap[schema]
        models = []
        if option.fields:
            indices = self._psqlSchemaToFieldIndexMap[schema]
            for record in records:
                index = 0
                model = {}
                for column in record:
                    fieldIndex = indices[termFields[index]]
                    model[fields[fieldIndex]] = loaders[fieldIndex](column)
                    index += 1
                models.append(model)
        else:
            for record in records:
                index = 0
                model = {}
                for column in record:
                    model[fields[index]] = loaders[index](column)
                    index += 1
                models.append(model)
        return models

    async def count(self, schema:BaseModel, option:SearchOption):
        table = self._psqlSchemaToTableMap[schema]
        fields = self._psqlSchemaToFieldNameMap[schema]

        if option.query:
            query = option.query
            conditions = []
            for key, val in query.items():
                if type(val) in [str, UUID]: conditions.append(f"{snakecase(key)}='{val}'")
                else: conditions.append(f'{snakecase(key)}={val}')
            query = [f"{' AND '.join(conditions)}"]
        else: query = []
        if option.filter: filter = [self.__parseLuceneToTsquery__(option.filter)]
        else: filter = []
        condition = ' AND '.join(query + filter)
        if condition: condition = f' AND {condition}'
        query = f'SELECT COUNT(*) FROM {table} WHERE deleted=FALSE{condition};'

        cursor = self._psqlReader.cursor()
        try:
            await cursor.execute(query)
            count = await cursor.fetchone()
        except Exception as e:
            await cursor.close()
            await self.__restore__()
            raise e
        await cursor.close()
        return count[0]

    async def create(self, schema:BaseModel, *models):
        if models:
            table = self._psqlSchemaToTableMap[schema]
            fields = self._psqlSchemaToFieldNameMap[schema]
            dumpers = self._psqlSchemaToDumperMap[schema]
            cursor = self._psqlWriter.cursor()

            try:
                for model in models:
                    index = 0
                    values = []
                    for field in fields:
                        values.append(dumpers[index](model[field]))
                        index += 1
                    query = f"INSERT INTO {table} VALUES({','.join(values)});"
                    await cursor.execute(query)
                    await cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id='{model['id']}';")
                results = [bool(result) for result in await cursor.fetchall()]
                await self._psqlWriter.commit()
            except Exception as e:
                await cursor.close()
                await self.__restore__()
                raise e
            await cursor.close()
            return results
        return []

    async def update(self, schema:BaseModel, *models):
        if models:
            table = self._psqlSchemaToTableMap[schema]
            fields = self._psqlSchemaToFieldNameMap[schema]
            snakes = self._psqlSchemaToSnakeNameMap[schema]
            dumpers = self._psqlSchemaToDumperMap[schema]
            cursor = self._psqlWriter.cursor()

            try:
                for model in models:
                    id = model['id']
                    index = 0
                    values = []
                    for field in fields:
                        value = dumpers[index](model[field])
                        values.append(f'{snakes[index]}={value}')
                        index += 1
                    query = f"UPDATE {table} SET {','.join(values)} WHERE id='{id}' AND deleted=FALSE;"
                    await cursor.execute(query)
                    await cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id='{id}' AND deleted=FALSE;")

                results = [bool(result) for result in await cursor.fetchall()]
                await self._psqlWriter.commit()
            except Exception as e:
                await cursor.close()
                await self.__restore__()
                raise e
            await cursor.close()
            return results
        return []

    async def delete(self, schema:BaseModel, id:str):
        table = self._psqlSchemaToTableMap[schema]
        query = f"DELETE FROM {table} WHERE id='{id}';"
        cursor = self._psqlWriter.cursor()
        try:
            await cursor.execute(query)
            await cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE id='{id}';")
            result = [bool(not result[0]) for result in await cursor.fetchall()][0]
            await self._psqlWriter.commit()
        except Exception as e:
            await cursor.close()
            await self.__restore__()
            raise e
        await cursor.close()
        return result
