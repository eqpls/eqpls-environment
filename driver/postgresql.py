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

from common import asleep, runBackground, EpException, SchemaDescription, SearchOption

#===============================================================================
# SingleTon
#===============================================================================


#===============================================================================
# Implement
#===============================================================================
class Postgresql:
    
    def __init__(self, config):
        self._psqlWriterHostname = config['database']['writer_hostname']
        self._psqlWriterHostport = int(config['database']['writer_hostport'])
        self._psqlReaderHostname = config['database']['reader_hostname']
        self._psqlReaderHostport = int(config['database']['reader_hostport'])
        self._psqlUsername = config['database']['username']
        self._psqlPassword = config['database']['password']
        self._psqlDatabase = config['postgresql']['database']
        self._psqlFieldIndexMap = {}
        self._psqlFieldNameMap = {}
        self._psqlSnakeNameMap = {}
        self._psqlDumperMap = {}
        self._psqlLoaderMap = {}
        self._psqlTableMap = {}
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
            # LOG.DEBUG('try to restore database connection')
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
        # LOG.INFO('database connection is restored')
    
    def __parseLuceneToTsquery__(self, node:Item):
        nodeType = type(node)
        
        print(nodeType, node.__dict__)
        
        if isinstance(node, Term):
            terms = filter(None, str(node.value).strip('"').lower().split(' '))
            return f"{'|'.join(terms)}"
        elif nodeType == SearchField:
            if '.' in node.name: fieldName = node.name.split('.')[0]
            else: fieldName = node.name
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
        table = desc.schemaPath
        fields = sorted(schema.model_fields.keys())
        snakes = [snakecase(field) for field in fields]
        self._psqlFieldNameMap[schema] = fields
        self._psqlSnakeNameMap[schema] = snakes

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
        self._psqlDumperMap[schema] = dumpers
        self._psqlLoaderMap[schema] = loaders
        self._psqlFieldIndexMap[schema] = indices

        try: await self.__connect__()
        except: exit(1)
        async with self._psqlWriter.cursor() as cursor:
            await cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({','.join(columns)});")
            await self._psqlWriter.commit()
        self._psqlTableMap[schema] = table
        # LOG.DEBUG(f'database.register({schema}) <{table}>')
    
    async def close(self):
        await self._psqlWriter.close()
        await self._psqlReader.close()
    
    async def read(self, schema:BaseModel, id:str):
        table = self._psqlTableMap[schema]
        fields = self._psqlFieldNameMap[schema]
        
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
            loaders = self._psqlLoaderMap[schema]
            index = 0
            model = {}
            for column in record:
                model[fields[index]] = loaders[index](column)
                index += 1
            return model
        return None

    async def search(self, schema:BaseModel, option:SearchOption):
        table = self._psqlTableMap[schema]
        fields = self._psqlFieldNameMap[schema]
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
        
        LOG.DEBUG(query)
        
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

        loaders = self._psqlLoaderMap[schema]
        models = []
        if option.fields:
            indices = self._psqlFieldIndexMap[schema]
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
        table = self._psqlTableMap[schema]
        fields = self._psqlFieldNameMap[schema]
        
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
            table = self._psqlTableMap[schema]
            fields = self._psqlFieldNameMap[schema]
            dumpers = self._psqlDumperMap[schema]
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
            table = self._psqlTableMap[schema]
            fields = self._psqlFieldNameMap[schema]
            snakes = self._psqlSnakeNameMap[schema]
            dumpers = self._psqlDumperMap[schema]
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
        table = self._psqlTableMap[schema]
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
