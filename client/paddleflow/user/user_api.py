"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

#!/usr/bin/env python3
# -*- coding:utf8 -*-

import json
from urllib import parse
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.utils import api_client
from paddleflow.common import api
from paddleflow.user.user_info import UserInfo


class UserServiceApi(object):
    """user service api"""
    def __init__(self):
        """
        """

    @classmethod
    def add_user(self, host, name, password, header=None):
        """call add user api"""
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        body = {
            "username": name,
            "password": password
        }
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_USER),
                                       json=body, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "add user failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def del_user(self, host, name, header=None):
        """call del user api"""
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="DELETE", url=parse.urljoin(host, api.PADDLE_FLOW_USER + "/%s" % name),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete user failed due to HTTPError")
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if data and 'message' in data:
            return False, data['message']
        return True, None

    @classmethod        
    def list_user(self, host, header=None, maxsize=100):
        """call list user api"""
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "maxsize should be int and greater than 0")
        userList = []
        response = None
        params = {
            "maxKeys": maxsize 
        }
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_USER),
                                       headers=header, params=params)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "list user failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        if data['userList'] and len(data['userList']):
            for user in data['userList']:
                userinfo = UserInfo(user['name'], user['createTime'])
                userList.append(userinfo)
        return True, userList

    @classmethod
    def update_password(self, host, name, password, header=None):
        """call update """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {
            "password": password
        }
        response = api_client.call_api(method="PUT", url=parse.urljoin(host, api.PADDLE_FLOW_USER + "/%s" % name),
                                       headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "update failed due to HTTPError")
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if data and 'message' in data:
            return False, data['message']
        return True, None