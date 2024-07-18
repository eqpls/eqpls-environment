# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, APIKeyHeader


#===============================================================================
# Implement
#===============================================================================
class SECONDS:
    # Seconds
    SEC = 1
    # Minutes
    MIN = 60
    # Hours
    HOUR = 3600
    # Day
    DAY = 86400
    # Week
    WEEK = 604800
    # Month
    MONTH = 2592000
    # Year
    YEAR = 31536000


class CRUD:

    CREATE = 1
    READ = 2
    UPDATE = 4
    DELETE = 8

    C = 1
    R = 2
    U = 4
    D = 8

    CR = 3
    CU = 5
    CD = 9

    RU = 6
    RD = 10

    UD = 12

    CRU = 7
    CRD = 11
    CUD = 13

    RUD = 14

    CRUD = 15

    @classmethod
    def checkCreate(cls, crud): return crud & 1

    @classmethod
    def checkRead(cls, crud): return crud & 2

    @classmethod
    def checkUpdate(cls, crud): return crud & 4

    @classmethod
    def checkDelete(cls, crud): return crud & 8


class LAYER:

    DATABASE = 1
    SEARCH = 2
    CACHE = 4

    D = 1
    S = 2
    C = 4

    SD = 3
    CD = 5
    CS = 6

    CSD = 7

    @classmethod
    def checkCache(cls, layer): return layer & 4

    @classmethod
    def checkSearch(cls, layer): return layer & 2

    @classmethod
    def checkDatabase(cls, layer): return layer & 1


class AAA:

    FREE = 0

    A = 1
    AA = 101
    AAA = 1001

    @classmethod
    def checkAuthorization(cls, aaa): return True if aaa > 0 else False

    @classmethod
    def checkAuthentication(cls, aaa): return True if aaa > 100 else False

    @classmethod
    def checkAccount(cls, aaa): return True if aaa > 1000 else False


ORG_HEADER = Annotated[str | None, Depends(APIKeyHeader(name='Organization', auto_error=False))]

AUTH_HEADER = Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())]
