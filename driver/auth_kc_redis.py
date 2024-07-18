# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from common import mergeArray, AuthInfo, AuthDriverBase
from driver.keyclock import KeyCloak
from driver.redis import RedisAuthn


#===============================================================================
# Implement
#===============================================================================
class AuthKeyCloakRedis(AuthDriverBase):

    def __init__(self, config):
        self._authKeyCloak = KeyCloak(config)
        self._authRedis = RedisAuthn(config)
        self._authRBACAttribute = self._authKeyCloak._kcRBACAttribute
        self._authDefaultOrg = self._authKeyCloak._kcDefaultRealm
        self._authRBACMap = {}
        self._authInfoMap = {}

    async def connect(self):
        await self._authKeyCloak.connect()
        await self._authRedis.connect()
        return self

    async def disconnect(self):
        await self._authRedis.disconnect()
        await self._authKeyCloak.disconnect()

    async def refreshInfos(self): self._authInfoMap = {}

    async def refreshRBACs(self, rbacs):
        rbacMap = {}
        for rbac in rbacs:
            rbacMap[rbac['name']] = {
                'aclRead': rbac['aclRead'],
                'aclCreate': rbac['aclCreate'],
                'aclUpdate': rbac['aclUpdate'],
                'aclDelete': rbac['aclDelete']
            }
        self._authRBACMap = rbacMap

    async def getAuthInfo(self, token, org):
        org = org if org else self._authDefaultOrg
        rbacMap = self._authRBACMap
        if token in self._authInfoMap: return self._authInfoMap[token]
        else:
            authInfo = await self._authRedis.read(token)
            if authInfo: return AuthInfo(**authInfo)
            else:
                userInfo = await self._authKeyCloak.getUserInfo(org, token)
                rbacs = userInfo[self._authRBACAttribute]
                admin = False
                aclRead = []
                aclCreate = []
                aclUpdate = []
                aclDelete = []
                for rbac in rbacs:
                    if rbac == 'admin': admin = True
                    elif rbac in rbacMap:
                        acl = rbacMap[rbac]
                        aclRead = mergeArray(aclRead, acl['aclRead'])
                        aclCreate = mergeArray(aclCreate, acl['aclCreate'])
                        aclUpdate = mergeArray(aclUpdate, acl['aclUpdate'])
                        aclDelete = mergeArray(aclDelete, acl['aclDelete'])
                authInfoDict = {
                    'org': org,
                    'username': userInfo['preferred_username'],
                    'admin': admin,
                    'role': rbacs,
                    'aclRead': aclRead,
                    'aclCreate': aclCreate,
                    'aclUpdate': aclUpdate,
                    'aclDelete': aclDelete
                }
                authInfo = AuthInfo(**authInfoDict)
                self._authInfoMap[token] = authInfo
                await self._authRedis.write(token, authInfoDict)
                return authInfo
