# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from pydantic import BaseModel

from .constants import SECONDS, AAA
from .models import Option, SchemaConfig, BaseSchema, ProfSchema

#===============================================================================
# Implement
#===============================================================================
@SchemaConfig(
version=1,
aaa=AAA.AA,
cache=Option(expire=SECONDS.HOUR),
search=Option(expire=SECONDS.DAY))
class RBAC(BaseModel, ProfSchema, BaseSchema):

    aclRead: list[str] = []
    aclCreate: list[str] = []
    aclUpdate: list[str] = []
    aclDelete: list[str] = []


class AuthInfo(BaseModel):

    org:str
    username:str
    admin:bool
    role:list[str]
    aclRead:list[str]
    aclCreate:list[str]
    aclUpdate:list[str]
    aclDelete:list[str]

    def checkOrg(self, org): return True if self.org == org else False

    def checkUsername(self, username): return True if self.username == username else False

    def checkAccount(self, realm, username): return True if self.realm == realm and self.owner == username else False

    def checkAdmin(self): return self.admin

    def checkRole(self, role): return True if role in self.role else False

    def checkReadACL(self, sref): return True if sref in self.aclRead else False

    def checkCreateACL(self, sref): return True if sref in self.aclCreate else False

    def checkUpdateACL(self, sref): return True if sref in self.aclUpdate else False

    def checkDeleteACL(self, sref): return True if sref in self.aclDelete else False
