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
from paddleflow.queue.queue_info import QueueInfo
from paddleflow.queue.queue_info import GrantInfo

class QueueServiceApi(object):
    """queue service api"""
    def __init__(self):
        """
        """

    @classmethod
    def add_queue(self, host, name, namespace, clusterName, maxResources, minResources=None,
                    schedulingPolicy=None, location=None, quotaType=None, header=None):
        """
        add queue 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {
            "namespace": namespace,
            "name": name,
            "clusterName": clusterName,
            "maxResources": maxResources,
        }
        if minResources:
            body['minResources'] = minResources
        if schedulingPolicy:
            body['schedulingPolicy'] = schedulingPolicy
        if location:
            body['location'] = location
        if quotaType:
            body['quotaType'] = quotaType
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_QUEUE), headers=header,
                                       json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "add queue failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def update_queue(self, host, queuename, maxResources, minResources=None, schedulingPolicy=None,
                        location=None, header=None):
        """
        update queue
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {}
        if maxResources:
            body['maxResources'] = maxResources
        if minResources:
            body['minResources'] = minResources
        if schedulingPolicy:
            body['schedulingPolicy'] = schedulingPolicy
        if location:
            body['location'] = location
        response = api_client.call_api(method="PUT", url=parse.urljoin(host, api.PADDLE_FLOW_QUEUE+ "/%s" % queuename),
                                        headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "update queue failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def grant_queue(self, host, username, queuename, header=None):
        """
        grant queue
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {
            "username": username,
            "resourceType": "queue",
            "resourceID": queuename
        }
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_GRANT),
                                       headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "grant queue failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def ungrant_queue(self, host, username, queuename, header=None):
        """
        ungrant queue
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        ## call grant 
        params = {
            "username": username,
            "resourceType": "queue",
            "resourceID": queuename
        }
        response = api_client.call_api(method="DELETE", url=parse.urljoin(host, api.PADDLE_FLOW_GRANT),
                                       headers=header, params=params)
        if not response.text:
            return True, None
        if not response:
            raise PaddleFlowSDKException("Connection Error", "ungrant queue failed due to HTTPError")
        data = json.loads(response.text)
        if data and 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def del_queue(self, host, queuename, header=None):
        """
        delete queue
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        
        response = api_client.call_api(method="DELETE", url=parse.urljoin(host, api.PADDLE_FLOW_QUEUE + 
                                                                       "/%s" % queuename),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete queue failed due to HTTPError")
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def list_queue(self, host, header=None, maxsize=100, marker=None):
        """
        list queue
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "maxsize should be int and greater than 0")
        params = {
            "maxKeys": maxsize
        }
        if marker:
            params['marker'] = marker
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_QUEUE),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "list queue failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        queueList = []        
        if len(data['queueList']):
            for queue in data['queueList']:
                queueinfo = QueueInfo(queue['name'], queue['status'], queue['namespace'], queue['clusterName'], queue['quotaType'],
                                      queue['maxResources'], queue['minResources'], None, None, None, None,
                                      queue['createTime'], queue['updateTime'])
                queueList.append(queueinfo)
        return True, queueList, data.get('nextMarker', None)

    @classmethod
    def show_queue(self, host, queuename, header=None):
        """
        show queue info 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_QUEUE + "/%s" % queuename),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "show queue failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        queueInfo = QueueInfo(data['name'], data['status'], data['namespace'], data['clusterName'], data['quotaType'],
                              data['maxResources'], data.get('minResources'), data['usedResources'], data['idleResources'],
                              data.get('location'), data.get('schedulingPolicy'), data['createTime'], data['updateTime'])
        return True, queueInfo
        
    @classmethod
    def show_grant(self, host, username=None, header=None, maxsize=100):
        """
        show grant resources
        """    
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "maxsize should be int and greater than 0")
        params = {
            "maxKeys": maxsize
        }
        if username:
            params['username'] = username
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_GRANT),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "show grant failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        grantList = []        
        if len(data['grantList']):
            for grant in data['grantList']:
                if grant['resourceType'] != "queue":
                    continue
                grantinfo = GrantInfo(grant['userName'], grant['resourceID'])
                grantList.append(grantinfo)
        return True, grantList