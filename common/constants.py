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
class TimeString:
    # Seconds
    SEC = 1
    S1 = 1
    S2 = 2
    S3 = 3
    S5 = 5
    S10 = 10
    S15 = 15
    S20 = 20
    S30 = 30
    S60 = 60
    # Minutes
    MIN = 60
    M1 = 60
    M2 = 120
    M3 = 180
    M5 = 300
    M10 = 600
    M15 = 900
    M20 = 1200
    M30 = 1800
    M60 = 3600
    # Hours
    HOUR = 3600
    H1 = 3600
    H2 = 7200
    H3 = 10800
    H6 = 21600
    H8 = 28800
    # Day
    DAY = 86400
    D1 = 86400
    WEEK = 604800
    W1 = 604800
    # Year
    YEAR = 31536000
    Y1 = 31536000

    @classmethod
    def str2int(cls, key):
        try: return cls.__getattribute__(cls, key)
        except: return str(key)


class AuthLevel:

    A = 1
    AA = 11
    AAA = 101

    @classmethod
    def checkAuthorization(cls, checker): return True if checker > 0 else False

    @classmethod
    def checkAuthentication(cls, checker): return True if checker > 10 else False

    @classmethod
    def checkAccount(cls, checker): return True if checker > 100 else False

# RealmHeader = APIKeyHeader(name='Realm', auto_error=False)
# AuthorizationHeader = HTTPBearer()

RealmHeader = Annotated[str | None, Depends(APIKeyHeader(name='Realm', auto_error=False))]
AuthorizationHeader = Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())]
