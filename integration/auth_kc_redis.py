# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from common import mergeArray, AuthInfo
from driver import KeyCloak, RedisAuthn


#===============================================================================
# Implement
#===============================================================================
class AuthDriver:

    def __init__(self, config):
        self._authKeyCloak = KeyCloak(config)
        self._authRedis = RedisAuthn(config)
        self._authDefaultRealm = self._authKeyCloak._kcDefaultRealm
        self._authAllowedMap = {}
        self._authInfoMap = {}

    async def connect(self):
        await self._authKeyCloak.connect()
        await self._authRedis.connect()
        return self

    async def disconnect(self):
        await self._authRedis.disconnect()
        await self._authKeyCloak.disconnect()

    async def updatePolicyMap(self, policies):
        allowedMap = {}
        for policy in policies:
            allowedMap[policy['name']] = {
                'readAllowed': policy['readAllowed'],
                'createAllowed': policy['createAllowed'],
                'updateAllowed': policy['updateAllowed'],
                'deleteAllowed': policy['deleteAllowed']
            }
        self._authAllowedMap = allowedMap
        self._authInfoMap = {}

    async def getAuthInfo(self, realm, token):
        realm = realm if realm else self._authDefaultRealm
        token = token.credentials
        allowedMap = self._authAllowedMap
        if token in self._authInfoMap: return self._authInfoMap[token]
        else:
            authInfo = await self._authRedis.read(token)
            if authInfo: return AuthInfo(**authInfo)
            else:
                userInfo = await self._authKeyCloak.getUserInfo(realm, token)
                policies = userInfo['policy']
                admin = False
                readAllowed = []
                createAllowed = []
                updateAllowed = []
                deleteAllowed = []
                for policy in policies:
                    if policy == 'admin': admin = True
                    elif policy in allowedMap:
                        allowed = allowedMap[policy]
                        readAllowed = mergeArray(readAllowed, allowed['readAllowed'])
                        createAllowed = mergeArray(createAllowed, allowed['createAllowed'])
                        updateAllowed = mergeArray(updateAllowed, allowed['updateAllowed'])
                        deleteAllowed = mergeArray(deleteAllowed, allowed['deleteAllowed'])
                authInfoDict = {
                    'realm': realm,
                    'username': userInfo['preferred_username'],
                    'admin': admin,
                    'policy': policies,
                    'readAllowed': readAllowed,
                    'createAllowed': createAllowed,
                    'updateAllowed': updateAllowed,
                    'deleteAllowed': deleteAllowed
                }
                authInfo = AuthInfo(**authInfoDict)
                self._authInfoMap[token] = authInfo
                await self._authRedis.write(token, authInfoDict)
                return authInfo
