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


class AAA:

    A = 1
    AA = 101
    AAA = 1001

    @classmethod
    def checkAuthorization(cls, aaa): return True if aaa > 0 else False

    @classmethod
    def checkAuthentication(cls, aaa): return True if aaa > 100 else False

    @classmethod
    def checkAccount(cls, aaa): return True if aaa > 1000 else False


REALM_HEADER = Annotated[str | None, Depends(APIKeyHeader(name='Realm', auto_error=False))]

AUTH_HEADER = Annotated[HTTPAuthorizationCredentials, Depends(HTTPBearer())]
