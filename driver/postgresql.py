# -*- coding: utf-8 -*-
'''
Created on 2024. 2. 8.
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import re
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
class Table:

    def __init__(self, name, fields):
        self.name = name
        self.codeName = snakecase(name)

        self.fieldKeys = [f[0] for f in fields]
        self.fieldTypes = [f[1] for f in fields]
        self.fieldQuotes = {}

        self.primaryKey = None
        self.insertParams = []
        self.updateParams = []
        for field in fields:
            k, t = field
            k = snakecase(k)
            if t == 'char':
                self.fieldQuotes[k] = True
                self.insertParams.append("'{%s}'" % k)
                self.updateParams.append("%s='{%s}'" % (k, k))
            elif t == 'int':
                self.fieldQuotes[k] = False
                self.insertParams.append("{%s}" % k)
                self.updateParams.append("%s={%s}" % (k, k))
            elif t == 'float':
                self.fieldQuotes[k] = False
                self.insertParams.append("{%s}" % k)
                self.updateParams.append("%s={%s}" % (k, k))
            elif t == 'pkey-char':
                self.fieldQuotes[k] = True
                self.primaryKeyQuotes = True
                self.insertParams.append("'{%s}'" % k)
                self.primaryKey = k
            elif t == 'pkey-int':
                self.fieldQuotes[k] = False
                self.primaryKeyQuotes = False
                self.insertParams.append("{%s}" % k)
                self.primaryKey = k
            elif t == 'pkey-default':
                self.fieldQuotes[k] = False
                self.primaryKeyQuotes = False
                self.insertParams.append("DEFAULT")
                self.primaryKey = k
        if not self.primaryKey:
            k, t = fields[0]
            k = snakecase(k)
            self.primaryKey = k
            if t == 'char': self.primaryKeyQuotes = True
            elif t == 'int': self.primaryKeyQuotes = False
            elif t == 'float': self.primaryKeyQuotes = False

        self.querySelect = 'SELECT * FROM %s{};' % self.codeName
        self.queryCount = 'SELECT count(*) FROM %s{};' % self.codeName
        self.queryInsert = 'INSERT INTO %s VALUES(%s);' % (self.codeName, ','.join(self.insertParams))
        if self.primaryKeyQuotes:
            self.queryUpdate = "UPDATE %s SET %s WHERE %s='{%s}';" % (self.codeName, ','.join(self.updateParams), self.primaryKey, self.primaryKey)
            self.queryDelete = "DELETE FROM %s WHERE %s='{%s}';" % (self.codeName, self.primaryKey, self.primaryKey)
        else:
            self.queryUpdate = 'UPDATE %s SET %s WHERE %s={%s};' % (self.codeName, ','.join(self.updateParams), self.primaryKey, self.primaryKey)
            self.queryDelete = 'DELETE FROM %s WHERE %s={%s};' % (self.codeName, self.primaryKey, self.primaryKey)

    def cursor(self): return Database.Cursor(self)


class Database:

    class Cursor:

        def __init__(self, table):
            self.table = table
            self.database = table.database

        async def __aenter__(self):
            try: self.cursor = self.database._connection.cursor()
            except:
                await self.database.connect()
                self.cursor = self.database._connection.cursor()
            return self

        async def __aexit__(self, *args):
            try: await self.cursor.close()
            except: pass

        #=======================================================================
        # Level 1 Methods
        #=======================================================================
        async def execute(self, query, **kargs):
            try:
                await self.cursor.execute(query, kargs)
                return self
            except Exception as e:
                try: await self.cursor.close()
                except: pass
                raise e

        async def commit(self):
            try:
                await self.database._connection.commit()
                return self
            except Exception as e:
                try: await self.cursor.close()
                except: pass
                raise e

        async def fetchAll(self):
            try:
                return await self.cursor.fetchall()
            except Exception as e:
                try: await self.cursor.close()
                except: pass
                raise e

        async def fetchOne(self):
            try:
                return await self.cursor.fetchone()
            except Exception as e:
                try: await self.cursor.close()
                except: pass
                raise e

        #=======================================================================
        # Level 2 Methods
        #=======================================================================
        def parseWhere(self, conditions):
            if conditions:
                where = []
                for k, v in conditions.items():
                    ko = __OPERATOR_PARSER__.findall(k)
                    if ko: k, o = ko[0]
                    else: k, o = (k, 'eq')
                    k = snakecase(k)
                    o = o.lower()
                    if o == 'eq': o = '='
                    elif o == 'ne': o = '!='
                    elif o == 'gt': o = '>'
                    elif o == 'ge': o = '>='
                    elif o == 'lt': o = '<'
                    elif o == 'le': o = '<='
                    elif o == 'like': o = 'LIKE'
                    else: o = '='
                    if self.table.fieldQuotes[k]: where.append(f"{k} {o} '{v}'")
                    else: where.append(f"{k} {o} {v}")
                return f" WHERE {' AND '.join(where)}"
            return ''

        async def getRecords(self, **conditions):
            try:
                await self.execute(self.table.querySelect.format(self.parseWhere(conditions)))
                results = []
                for record in await self.fetchAll():
                    result = {}
                    for i in range(0, len(record)):
                        result[self.table.fieldKeys[i]] = record[i]
                    results.append(result)
                return results
            except Exception as e: raise EpException(500, f'PSQL: {str(e)}')

        async def getRecordOne(self, **conditions):
            try:
                await self.execute(self.table.querySelect.format(self.parseWhere(conditions)))
                record = await self.fetchOne()
                if record:
                    result = {}
                    for i in range(0, len(record)): result[self.table.fieldKeys[i]] = record[i]
                    return result
                return None
            except Exception as e:
                raise EpException(500, f'PSQL: {str(e)}')

        async def getRecordCount(self, **conditions):
            try:
                await self.execute(self.table.queryCount.format(self.parseWhere(conditions)))
                return int((await self.fetchOne())[0])
            except Exception as e: raise EpException(500, f'PSQL: {str(e)}')

        async def createRecord(self, **record):
            try:
                snakeRecord = {}
                for k, v in record.items(): snakeRecord[snakecase(k)] = v
                await self.execute(self.table.queryInsert.format(**snakeRecord))
                return self
            except Exception as e: raise EpException(500, f'PSQL: {str(e)}')

        async def updateRecord(self, **record):
            try:
                snakeRecord = {}
                for k, v in record.items(): snakeRecord[snakecase(k)] = v
                await self.execute(self.table.queryUpdate.format(**snakeRecord))
                return self
            except Exception as e: raise EpException(500, f'PSQL: {str(e)}')

        async def deleteRecord(self, **record):
            try:
                snakeRecord = {}
                for k, v in record.items(): snakeRecord[snakecase(k)] = v
                await self.execute(self.table.queryDelete.format(**snakeRecord))
                return self
            except Exception as e: raise EpException(500, f'PSQL: {str(e)}')

    def __init__(self, config):
        self._systemAccessKey = config['default']['system_access_key'].lower()
        self._systemSecretKey = config['default']['system_secret_key']

        self._hostname = config['psql']['hostname']
        self._hostport = int(config['psql']['hostport'])
        self._name = snakecase(config['psql']['database'])

        self._connection = None

    async def init(self, *tables:Table):
        await self.connect()

        for table in tables:
            table.database = self
            self.__setattr__(table.name, table)
        return self

    async def connect(self):
        await self.disconnect()
        try:
            self._connection = await AsyncConnection.connect(host=self._hostname, port=self._hostport, dbname=self._name, user=self._systemAccessKey, password=self._systemSecretKey)
            LOG.INFO(f'Database [{self._systemAccessKey}@{self._hostname}:{self._hostport}/{self._name}] is connected')
        except:
            await self.disconnect()
            LOG.ERROR(f'Database [{self._systemAccessKey}@{self._hostname}:{self._hostport}/{self._name}] is disconnected')
            exit(1)
        return self

    async def disconnect(self):
        try: await self._connection.close()
        except: pass

