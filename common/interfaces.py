# -*- coding: utf-8 -*-
'''
Equal Plus
@author: Hye-Churn Jang
'''

#===============================================================================
# Import
#===============================================================================
import json as JSON
import urllib3
import asyncio
import aiohttp
import requests
from fastapi import Request
from aiohttp.client_exceptions import ClientResponseError
from .constants import EpException

#===============================================================================
# Setting
#===============================================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


#===============================================================================
# Implement
#===============================================================================
class TaskOperator:

    def __init__(self): self._task_operator_tasks = []

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb): pass

    def do(self, ref):
        self._task_operator_tasks.append(ref)
        return self

    async def wait(self):
        return await asyncio.gather(*(self._task_operator_tasks))


class SyncRest:

    def __init__(self, baseUrl=''):
        self.baseUrl = baseUrl

    def __enter__(self):
        self.session = requests.Session()
        return self

    def __exit__(self, *args):
        self.session.close()

    def get(self, url, headers=None):
        res = self.session.get(f'{self.baseUrl}{url}', headers=headers, verify=False)
        res.raise_for_status()
        try: return res.json()
        except: res.text

    def post(self, url, data=None, json=None, headers=None):
        res = self.session.post(f'{self.baseUrl}{url}', data=data, json=json, headers=headers, verify=False)
        res.raise_for_status()
        try: return res.json()
        except: res.text

    def put(self, url, data=None, json=None, headers=None):
        res = self.session.put(f'{self.baseUrl}{url}', data=data, json=json, headers=headers, verify=False)
        res.raise_for_status()
        try: return res.json()
        except: res.text

    def patch(self, url, data=None, json=None, headers=None):
        res = self.session.patch(f'{self.baseUrl}{url}', data=data, json=json, headers=headers, verify=False)
        res.raise_for_status()
        try: return res.json()
        except: res.text

    def delete(self, url, headers=None):
        res = self.session.delete(f'{self.baseUrl}{url}', headers=headers, verify=False)
        res.raise_for_status()
        try: return res.json()
        except: res.text


class AsyncRest:

    def __init__(self, baseUrl=''):
        self.baseUrl = baseUrl

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), raise_for_status=True)
        return self

    async def __aexit__(self, *args):
        await self.session.close()

    async def proxy(self, request:Request):
        method = request.scope['method']
        url = request.scope['path']
        if request.scope['query_string']:
            url = f'{url}?{request.scope["query_string"].decode("latin-1")}'
        _method = self.__getattribute__(method.lower())
        try:
            if method in ['GET', 'DELETE']: return await _method(url)
            if method in ['POST', 'PUT', 'PATCH']: return await _method(url, json=(await request.json()))
        except ClientResponseError as e:
            raise EpException(e.status, e.message)

    async def get(self, url, headers=None):
        try:
            async with self.session.get(f'{self.baseUrl}{url}', headers=headers) as res:
                data = await res.text()
                try: return JSON.loads(data)
                except: return data
        except ClientResponseError as e:
            raise EpException(e.status, e.message)

    async def post(self, url, data=None, json=None, headers=None):
        try:
            async with self.session.post(f'{self.baseUrl}{url}', data=data, json=json, headers=headers) as res:
                data = await res.text()
                try: return JSON.loads(data)
                except: return data
        except ClientResponseError as e:
            raise EpException(e.status, e.message)

    async def put(self, url, data=None, json=None, headers=None):
        try:
            async with self.session.put(f'{self.baseUrl}{url}', data=data, json=json, headers=headers) as res:
                data = await res.text()
                try: return JSON.loads(data)
                except: return data
        except ClientResponseError as e:
            raise EpException(e.status, e.message)

    async def patch(self, url, data=None, json=None, headers=None):
        try:
            async with self.session.patch(f'{self.baseUrl}{url}', data=data, json=json, headers=headers) as res:
                data = await res.text()
                try: return JSON.loads(data)
                except: return data
        except ClientResponseError as e:
            raise EpException(e.status, e.message)

    async def delete(self, url, headers=None):
        try:
            async with self.session.delete(f'{self.baseUrl}{url}', headers=headers) as res:
                data = await res.text()
                try: return JSON.loads(data)
                except: return data
        except ClientResponseError as e:
            raise EpException(e.status, e.message)
