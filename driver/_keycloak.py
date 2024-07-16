# -*- coding: utf-8 -*-
'''
Created on 2024. 2. 8.
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from typing import Optional
from pydantic import BaseModel, PrivateAttr
from fastapi import Request
from common import AsyncRest, EpException


#===============================================================================
# Implement
#===============================================================================
class KeyCloak(BaseModel):

    baseUrl: str
    systemAccessKey: str
    systemSecretKey: str

    hostname: str
    hostport: int
    hostUrl: str

    allowedUrl: str
    cookieAccessRealm: str
    cookieAccessToken: str
    cookieRefreshToken: str
    headerAccessRealm: str
    headerAccessToken: str
    headerRefreshToken: str
    adminRealm: str
    adminUsername: str
    adminPassword: str

    _headers: str = PrivateAttr()
    _refreshToken: str = PrivateAttr()

    @classmethod
    async def connect(cls, config):
        baseUrl = config['default']['base_url']
        systemAccessKey = config['default']['system_access_key']
        systemSecretKey = config['default']['system_secret_key']

        hostname = config['keycloak']['hostname']
        hostport = int(config['keycloak']['hostport'])
        hostUrl = f'http://{hostname}:{hostport}'

        allowedUrl = config['auth']['allowed_url']
        cookieAccessRealm = config['auth']['cookie_access_realm']
        cookieAccessToken = config['auth']['cookie_access_token']
        cookieRefreshToken = config['auth']['cookie_refresh_token']
        headerAccessRealm = config['auth']['header_access_realm']
        headerAccessToken = config['auth']['header_access_token']
        headerRefreshToken = config['auth']['header_refresh_token']
        adminRealm = config['auth']['admin_realm']
        adminUsername = config['auth']['admin_username']
        adminPassword = config['auth']['admin_password']

        # logging
        LOG.INFO('Init KeyCloak')
        LOG.INFO(LOG.KEYVAL('baseUrl', baseUrl))
        LOG.INFO(LOG.KEYVAL('systemAccessKey', systemAccessKey))
        LOG.INFO(LOG.KEYVAL('systemSecretKey', systemSecretKey))
        LOG.INFO(LOG.KEYVAL('hostname', hostname))
        LOG.INFO(LOG.KEYVAL('hostport', hostport))
        LOG.INFO(LOG.KEYVAL('hostUrl', hostUrl))
        LOG.INFO(LOG.KEYVAL('allowedUrl', allowedUrl))
        LOG.INFO(LOG.KEYVAL('cookieAccessRealm', cookieAccessRealm))
        LOG.INFO(LOG.KEYVAL('cookieAccessToken', cookieAccessToken))
        LOG.INFO(LOG.KEYVAL('cookieRefreshToken', cookieRefreshToken))
        LOG.INFO(LOG.KEYVAL('headerAccessRealm', headerAccessRealm))
        LOG.INFO(LOG.KEYVAL('headerAccessToken', headerAccessToken))
        LOG.INFO(LOG.KEYVAL('headerRefreshToken', headerRefreshToken))
        LOG.INFO(LOG.KEYVAL('adminRealm', adminRealm))
        LOG.INFO(LOG.KEYVAL('adminUsername', adminUsername))
        LOG.INFO(LOG.KEYVAL('adminPassword', adminPassword))

        conn = await (cls(
            baseUrl=baseUrl,
            systemAccessKey=systemAccessKey,
            systemSecretKey=systemSecretKey,
            hostname=hostname,
            hostport=hostport,
            hostUrl=hostUrl,
            allowedUrl=allowedUrl,
            cookieAccessRealm=cookieAccessRealm,
            cookieAccessToken=cookieAccessToken,
            cookieRefreshToken=cookieRefreshToken,
            headerAccessRealm=headerAccessRealm,
            headerAccessToken=headerAccessToken,
            headerRefreshToken=headerRefreshToken,
            adminRealm=adminRealm,
            adminUsername=adminUsername,
            adminPassword=adminPassword
        )).session()

        try:
            await conn.createRealmPrivileged(adminRealm, adminRealm)
            await conn.createUser(adminRealm, adminUsername, adminUsername, adminPassword)
        except: pass
        return conn

    async def disconnect(self):
        async with AsyncRest(self.hostUrl) as s:
            await s.post(
                f'/realms/master/protocol/openid-connect/logout',
                data=f'client_id=admin-cli&refresh_token={self._refreshToken}',
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )

    async def session(self):
        try:
            async with AsyncRest(self.hostUrl) as s:
                result = await s.post(
                    f'/realms/master/protocol/openid-connect/token',
                    data=f'client_id=admin-cli&grant_type=password&username={self.systemAccessKey}&password={self.systemSecretKey}',
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
        except:
            LOG.ERROR(f'Could not connect to KeyCloak [{self.systemAccessKey}@{self.hostname}:{self.hostport}]')
            exit(1)
        self._headers = {
            'Authorization': f'Bearer {result["access_token"]}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        self._refreshToken = result['refresh_token']
        self._refreshToken = result['refresh_token']
        LOG.INFO(f'KeyCloak [{self.systemAccessKey}@{self.hostname}:{self.hostport}] is connected')
        return self

    #===========================================================================
    # Basic Rest Methods
    #===========================================================================
    async def get(self, url):
        async with AsyncRest(self.hostUrl) as s:
            try: return await s.get(url, headers=self._headers)
            except EpException as e:
                if e.status_code == 401:
                    await self.session()
                    return await self.get(url)
                else: raise e

    async def post(self, url, payload):
        async with AsyncRest(self.hostUrl) as s:
            try: return await s.post(url, json=payload, headers=self._headers)
            except EpException as e:
                if e.status_code == 401:
                    await self.session()
                    return await self.post(url, payload)
                else: raise e

    async def put(self, url, payload):
        async with AsyncRest(self.hostUrl) as s:
            try: return await s.put(url, json=payload, headers=self._headers)
            except EpException as e:
                if e.status_code == 401:
                    await self.session()
                    return await self.put(url, payload)
                else: raise e

    async def patch(self, url, payload):
        async with AsyncRest(self.hostUrl) as s:
            try: return await s.patch(url, json=payload, headers=self._headers)
            except EpException as e:
                if e.status_code == 401:
                    await self.session()
                    return await self.patch(url, payload)
                else: raise e

    async def delete(self, url):
        async with AsyncRest(self.hostUrl) as s:
            try: return await s.delete(url, headers=self._headers)
            except EpException as e:
                if e.status_code == 401:
                    await self.session()
                    return await self.delete(url)
                else: raise e

    #===========================================================================
    # Object Api Methods
    #===========================================================================
    # Check UserInfo ###########################################################
    async def userinfo(self, request:Request, admin=False):
        try:
            realm = request.cookies[self.cookieAccessRealm] if self.cookieAccessRealm in request.cookies else request.headers[self.headerAccessRealm]
            token = request.cookies[self.cookieAccessToken] if self.cookieAccessToken in request.cookies else request.headers[self.headerAccessToken]
        except Exception as e: raise EpException(401, str(e))
        if admin and realm != self.adminRealm: raise EpException(401, f'{realm} is not admin realm')
        async with AsyncRest(self.hostUrl) as s:
            userinfo = await s.get(f'/realms/{realm}/protocol/openid-connect/userinfo', { 'Authorization': f'Bearer {token}' })
        userinfo['admin'] = admin
        return userinfo

    # Realm ####################################################################
    async def getRealmList(self):
        results = []
        for realm in await self.get(f'/admin/realms'):
            if realm['realm'] not in ['master', 'admin']: results.append(realm)
        return results

    async def getRealm(self, realm):
        if id not in ['master', 'admin']: return await self.get(f'/admin/realms/{realm}')
        return None

    async def createRealm(self, realm:str, displayName:str):
        if realm in ['master', 'admin']: raise EpException(400, 'Could not create realm with predefined name')
        return await self.createRealmPrivileged(realm, displayName)

    async def createRealmPrivileged(self, realm:str, displayName:str):
        await self.post(f'/admin/realms', {
            'realm': realm,
            'displayName': displayName,
            'enabled': True
        })
        await self.post(f'/admin/realms/{realm}/client-scopes', {
            'name': 'openid',
            'description': '',
            'attributes': {
                'consent.screen.text': '',
                'display.on.consent.screen': 'true',
                'include.in.token.scope': 'true',
                'gui.order': ''
            },
            'type': 'default',
            'protocol': 'openid-connect'
        })
        for scope in await self.get(f'/admin/realms/{realm}/client-scopes'):
            if scope['name'] == 'openid': scopeId = scope['id']; break
        else: raise EpException(404, 'Could not find client scope')
        await self.delete(f'/admin/realms/{realm}/default-default-client-scopes/{scopeId}')
        await self.put(f'/admin/realms/{realm}/default-default-client-scopes/{scopeId}', {})
        await self.post(f'/admin/realms/{realm}/clients', {
            "protocol": "openid-connect",
            "clientId": 'ep-api',
            "name": 'ep-api',
            "description": "",
            "publicClient": True,
            "authorizationServicesEnabled": False,
            "serviceAccountsEnabled": False,
            "implicitFlowEnabled": False,
            "directAccessGrantsEnabled": True,
            "standardFlowEnabled": True,
            "frontchannelLogout": True,
            "attributes": {
                "saml_idp_initiated_sso_url_name": "",
                "oauth2.device.authorization.grant.enabled": False,
                "oidc.ciba.grant.enabled": False
            },
            "alwaysDisplayInConsole": False,
            "rootUrl": "",
            "baseUrl": self.baseUrl,
            "redirectUris": [self.allowedUrl]
        })
        for client in await self.get(f'/admin/realms/{realm}/clients'):
            if client['clientId'] == 'ep-api': clientId = client['id']; break
        else: raise EpException(404, 'Could not find client')
        await self.put(f'/admin/realms/{realm}/clients/{clientId}/default-client-scopes/{scopeId}', {})
        await self.put(f'/admin/realms/{realm}', {'accessTokenLifespan': 1800})
        return True

    async def setRealmDisplayName(self, realm:str, displayName:str):
        await self.put(f'/admin/realms/{realm}', {'displayName': displayName})
        return True

    async def deleteRealm(self, realm:str):
        await self.delete(f'/admin/realms/{realm}')
        return True

    # Group ####################################################################
    async def getGroupList(self, realm:str):
        return await self.get(f'/admin/realms/{realm}/groups')

    async def getGroup(self, realm:str, id:str):
        await self.get(f'/admin/realms/{realm}/groups/{id}')

    async def createGroup(self, realm:str, name:str):
        await self.post(f'/admin/realms/{realm}/groups', {'name': name})
        return True

    async def updateGroup(self, realm:str, id:str, name:str):
        await self.put(f'/admin/realms/{realm}/groups/{id}', {'name': name})
        return True

    async def deleteGroup(self, realm:str, id:str):
        await self.delete(f'/admin/realms/{realm}/groups/{id}')
        return True

    # User #####################################################################
    async def getUserList(self, realm:str):
        return await self.get(f'/admin/realms/{realm}/users')

    async def getGroupUserList(self, realm:str, id:str):
        return await self.get(f'/admin/realms/{realm}/groups/{id}/members')

    async def getUser(self, realm:str, id:str):
        return await self.get(f'/admin/realms/{realm}/users/{id}')

    async def findUser(self, realm:str, username:str):
        results = await self.get(f'/admin/realms/{realm}/users?username={username}')
        for result in results:
            if result['username'] == username: return result
        return None

    async def createUser(self, realm:str, username:str, firstName:str, password:str, lastName:Optional[str]=None, groupId:Optional[str]=None, enabled:bool=True):
        await self.post(f'/admin/realms/{realm}/users', {
            'username': username,
            'firstName': firstName,
            'lastName': lastName if lastName else ''
        })
        user = await self.findUser(realm, username)
        id = user['id']
        await self.put(f'/admin/realms/{realm}/users/{id}/reset-password', {
            'temporary': False,
            'type': 'password',
            'value': password
        })
        if groupId: await self.put(f'/admin/realms/{realm}/users/{id}/groups/{groupId}', {})
        if enabled: await self.put(f'/admin/realms/{realm}/users/{id}', {'enabled': True})
        return user

    async def setUserPassword(self, realm:str, id:str, password:str):
        await self.put(f'/admin/realms/{realm}/users/{id}/reset-password', {
            'temporary': False,
            'type': 'password',
            'value': password
        })
        return True

    async def setUserDisplayName(self, realm:str, id:str, displayName:str):
        await self.put(f'/admin/realms/{realm}/users/{id}', {'firstName': displayName})
        return True

    async def registerUserToGroup(self, realm:str, id:str, groupId:str):
        await self.put(f'/admin/realms/{realm}/users/{id}/groups/{groupId}', {})
        return True

    async def unregisterUserFromGroup(self, realm:str, id:str, groupId:str):
        await self.delete(f'/admin/realms/{realm}/users/{id}/groups/{groupId}')
        return True

    async def deleteUser(self, realm:str, id:str):
        await self.delete(f'/admin/realms/{realm}/users/{id}')
        return True
