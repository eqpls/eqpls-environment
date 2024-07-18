# -*- coding: utf-8 -*-
'''
Created on 2024. 2. 8.
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
from typing import Optional
from common import EpException, AsyncRest, DriverBase


#===============================================================================
# Implement
#===============================================================================
class KeyCloak(DriverBase):

    def __init__(self, config):
        DriverBase.__init__(self, 'keycloak', config)
        self._kcHostname = self.config['hostname']
        self._kcHostport = int(self.config['hostport'])
        self._kcUsername = self.config['username']
        self._kcPassword = self.config['password']
        self._kcFrontend = self.config['frontend']
        self._kcRBACAttribute = self.config['rbac_attribute']
        self._kcDomain = self.config['domain']
        self._kcDefaultRealm = self.config['default_realm']
        self._kcAdminUsername = self.config['admin_username']
        self._kcAdminPassword = self.config['admin_password']
        self._kcBaseUrl = f'http://{self._kcHostname}:{self._kcHostport}'
        self._kcAccessToken = None
        self._kcRefreshToken = None
        self._kcHeaders = None
        self._kcInitialized = False

    async def connect(self, *args, **kargs):
        async with AsyncRest(self._kcBaseUrl) as rest:
            tokens = await rest.post(f'/realms/master/protocol/openid-connect/token',
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data=f'client_id=admin-cli&grant_type=password&username={self._kcUsername}&password={self._kcPassword}'
            )
        self._kcAccessToken = tokens['access_token']
        self._kcRefreshToken = tokens['refresh_token']
        self._kcHeaders = {
            'Authorization': f'Bearer {self._kcAccessToken}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        if not self._kcInitialized:
            try: await self.getRealm(self._kcDefaultRealm)
            except EpException as e:
                if e.status_code == 404:
                    if await self.createRealm(self._kcDefaultRealm, self._kcDefaultRealm, self._kcRBACAttribute):
                        adminGroup = await self.createGroup(self._kcDefaultRealm, 'Administrator', {self._kcRBACAttribute: ['admin']})
                        userGroup = await self.createGroup(self._kcDefaultRealm, 'Users', {self._kcRBACAttribute: ['user']})
                        systemUser = await self.createUser(self._kcDefaultRealm, self._kcUsername, self._kcPassword, f'{self._kcUsername}@{self._kcDomain}', self._kcUsername, self._kcUsername)
                        adminUser = await self.createUser(self._kcDefaultRealm, self._kcAdminUsername, self._kcAdminPassword, f'{self._kcAdminUsername}@{self._kcDomain}', self._kcAdminUsername, self._kcAdminUsername)
                        await self.registerUserToGroup(self._kcDefaultRealm, systemUser['id'], adminGroup['id'])
                        await self.registerUserToGroup(self._kcDefaultRealm, adminUser['id'], adminGroup['id'])
                        await self.registerUserToGroup(self._kcDefaultRealm, adminUser['id'], userGroup['id'])
                else: raise e
            self._kcInitialized = True
        return self

    async def disconnect(self):
        async with AsyncRest(self._kcBaseUrl) as rest:
            await rest.post(f'/realms/master/protocol/openid-connect/logout',
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data=f'client_id=admin-cli&refresh_token={self._kcRefreshToken}'
            )

    #===========================================================================
    # Basic Rest Methods
    #===========================================================================
    async def get(self, url):
        async with AsyncRest(self._kcBaseUrl) as s:
            try: return await s.get(url, headers=self._kcHeaders)
            except EpException as e:
                if e.status_code == 401: return await (await self.connect()).get(url)
                else: raise e

    async def post(self, url, payload):
        async with AsyncRest(self._kcBaseUrl) as s:
            try: return await s.post(url, json=payload, headers=self._kcHeaders)
            except EpException as e:
                if e.status_code == 401: return await (await self.connect()).post(url, payload)
                else: raise e

    async def put(self, url, payload):
        async with AsyncRest(self._kcBaseUrl) as s:
            try: return await s.put(url, json=payload, headers=self._kcHeaders)
            except EpException as e:
                if e.status_code == 401: return await (await self.connect()).put(url, payload)
                else: raise e

    async def patch(self, url, payload):
        async with AsyncRest(self._kcBaseUrl) as s:
            try: return await s.patch(url, json=payload, headers=self._kcHeaders)
            except EpException as e:
                if e.status_code == 401: return await (await self.connect()).patch(url, payload)
                else: raise e

    async def delete(self, url):
        async with AsyncRest(self._kcBaseUrl) as s:
            try: return await s.delete(url, headers=self._kcHeaders)
            except EpException as e:
                if e.status_code == 401: return await (await self.connect()).delete(url)
                else: raise e

    #===========================================================================
    # Master Interface
    #===========================================================================
    # Realm ####################################################################
    async def getRealmList(self):
        results = []
        for realm in await self.get(f'/admin/realms'):
            if realm['realm'] != 'master': results.append(realm)
        return results

    async def getRealm(self, realm:str):
        if realm != 'master': return await self.get(f'/admin/realms/{realm}')
        return None

    async def createRealm(self, realm:str, displayName:str, rbacAttribute:str):
        if realm == 'master': raise EpException(400, 'Could not create realm with predefined name')
        return await self.createRealmPrivileged(realm, displayName, rbacAttribute)

    async def createRealmPrivileged(self, realm:str, displayName:str, rbacAttribute:str):
        await self.post(f'/admin/realms', {
            'realm': realm,
            'displayName': displayName,
            'enabled': True
        })
        await self.post(f'/admin/realms/{realm}/client-scopes', {
            'name': 'openid-client-scope',
            'description': 'openid-client-scope',
            'type': 'default',
            'protocol': 'openid-connect',
            'attributes': {
                'consent.screen.text': '',
                'display.on.consent.screen': True,
                'include.in.token.scope': True,
                'gui.order': ''
            }
        })
        for scope in await self.get(f'/admin/realms/{realm}/client-scopes'):
            if scope['name'] == 'openid-client-scope': scopeId = scope['id']; break
        else: raise EpException(404, 'Could not find client scope')
        await self.post(f'/admin/realms/{realm}/client-scopes/{scopeId}/protocol-mappers/models', {
            'name': rbacAttribute,
            'protocol': 'openid-connect',
            'protocolMapper': 'oidc-usermodel-attribute-mapper',
            'config': {
                'claim.name': rbacAttribute,
                'user.attribute': rbacAttribute,
                'jsonType.label': 'String',
                'multivalued': True,
                'aggregate.attrs': True,
                'id.token.claim': True,
                'access.token.claim': True,
                'lightweight.claim': False,
                'userinfo.token.claim': True,
                'introspection.token.claim': True
            }
        })
        await self.delete(f'/admin/realms/{realm}/default-default-client-scopes/{scopeId}')
        await self.put(f'/admin/realms/{realm}/default-default-client-scopes/{scopeId}', {})
        await self.post(f'/admin/realms/{realm}/clients', {
            'clientId': 'openid',
            'name': 'openid',
            'description': 'openid',
            'protocol': 'openid-connect',
            'publicClient': True,
            'rootUrl': self._kcFrontend,
            'baseUrl': self._kcFrontend,
            'redirectUris': ['*'],
            'authorizationServicesEnabled': False,
            'serviceAccountsEnabled': False,
            'implicitFlowEnabled': False,
            'directAccessGrantsEnabled': True,
            'standardFlowEnabled': True,
            'frontchannelLogout': True,
            'alwaysDisplayInConsole': True,
            'attributes': {
                'saml_idp_initiated_sso_url_name': '',
                'oauth2.device.authorization.grant.enabled': False,
                'oidc.ciba.grant.enabled': False,
                'post.logout.redirect.uris': '+'
            }
        })
        for client in await self.get(f'/admin/realms/{realm}/clients'):
            if client['clientId'] == 'openid': clientId = client['id']; break
        else: raise EpException(404, 'Could not find client')
        await self.put(f'/admin/realms/{realm}/clients/{clientId}/default-client-scopes/{scopeId}', {})
        await self.put(f'/admin/realms/{realm}', {
            'accessTokenLifespan': 1800,
            'revokeRefreshToken': True
        })
        return await self.getRealm(realm)

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

    async def findGroup(self, realm:str, groupname:str):
        for result in await self.get(f'/admin/realms/{realm}/groups?name={groupname}'):
            if result['name'] == groupname: return result
        return None

    async def createGroup(self, realm:str, groupname:str, attributes:Optional[dict]=None):
        if attributes: await self.post(f'/admin/realms/{realm}/groups', {'name': groupname, 'attributes': attributes})
        else: await self.post(f'/admin/realms/{realm}/groups', {'name': groupname})
        return await self.findGroup(realm, groupname)

    async def updateGroup(self, realm:str, id:str, groupname:str, attributes:Optional[dict]=None):
        if attributes: await self.put(f'/admin/realms/{realm}/groups/{id}', {'name': groupname, 'attributes': attributes})
        else: await self.put(f'/admin/realms/{realm}/groups/{id}', {'name': groupname})
        return await self.findGroup(realm, groupname)

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
        for result in await self.get(f'/admin/realms/{realm}/users?username={username}'):
            if result['username'] == username: return result
        return None

    async def createUser(self, realm:str, username:str, password:str, email:str, firstName:str, lastName:str, enabled:bool=True):
        await self.post(f'/admin/realms/{realm}/users', {
            'username': username,
            'email': email,
            'firstName': firstName,
            'lastName': lastName
        })
        user = await self.findUser(realm, username)
        id = user['id']
        await self.put(f'/admin/realms/{realm}/users/{id}/reset-password', {
            'temporary': False,
            'type': 'password',
            'value': password
        })
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

    async def registerUserToGroup(self, realm:str, userId:str, groupId:str):
        await self.put(f'/admin/realms/{realm}/users/{userId}/groups/{groupId}', {})
        return True

    async def unregisterUserFromGroup(self, realm:str, userId:str, groupId:str):
        await self.delete(f'/admin/realms/{realm}/users/{userId}/groups/{groupId}')
        return True

    async def deleteUser(self, realm:str, id:str):
        await self.delete(f'/admin/realms/{realm}/users/{id}')
        return True

    #===========================================================================
    # Account Interface
    #===========================================================================
    async def getUserInfo(self, realm:str, token:str):
        async with AsyncRest(self._kcBaseUrl) as rest:
            return await rest.get(f'/realms/{realm}/protocol/openid-connect/userinfo', headers={
                'Authorization': f'Bearer {token}'
            })
