# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from fastapi import HTTPException


#===============================================================================
# Implement
#===============================================================================
class EpException(HTTPException):

    def __init__(self, status_code, message):
        HTTPException.__init__(self, status_code, {'message':str(message)})
        LOG.ERROR(f'{status_code}: {str(message)}')
