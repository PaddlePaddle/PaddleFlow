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
import base64
from urllib import parse
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.utils import api_client
from paddleflow.common import api
from paddleflow.cluster.cluster_info import ClusterInfo

class ClusterServiceApi(object):
    """ cluster service
    """
    def __init__(self):
        """
        """

    @classmethod
    def create_cluster(self, host, clustername, endpoint, clustertype, credential=None,
    description=None, source=None, setting=None, status=None, namespacelist=None, version=None, header=None):
        """create cluster
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {
            "clusterName": clustername,
            "endpoint": endpoint,
            "clusterType": clustertype
        }
        if credential:
            if isinstance(credential, bytes):
                body['credential']=base64.b64encode(credential).decode()
            else:
                raise PaddleFlowSDKException("InvalidRequest", "credential must be bytes type")
        if description:
            body['description']=description
        if source:
            body['source']=source
        if setting:
            body['setting']=setting
        if status:
            body['status']=status
        if version:
            body['version']=version
        if namespacelist:
            if isinstance(namespacelist, list):
                body['namespaceList']=namespacelist
            else:
                raise PaddleFlowSDKException("InvalidRequest", "namespaceList must be list type")
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_CLUSTER),
                                       headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "create cluster failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data['id']

    @classmethod
    def list_cluster(self, host, maxkeys=100, marker=None, clusternames=None, clusterstatus=None, header=None):
        """list cluster 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        if not isinstance(maxkeys, int) or maxkeys <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "maxkeys should be int and greater than 0")
        params = {
            "maxKeys": maxkeys
        }
        if marker:
            params['marker'] = marker
        if clusternames:
            params['clusterNames'] = clusternames
        if clusterstatus:
            params['clusterStatus'] = clusterstatus
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_CLUSTER),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "list cluster failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        clusterList = []
        if data['clusterList'] and len(data['clusterList']):
            for cluster in data['clusterList']:
                ci = ClusterInfo(cluster['id'], cluster['clusterName'],
                                 cluster['description'], cluster['endpoint'],
                                 cluster['source'], cluster['clusterType'],
                                 cluster['version'], cluster['status'], None,
                                 None, None,
                                 cluster['createTime'], cluster['updateTime'])
                clusterList.append(ci)
        return True, clusterList, data.get('nextMarker', None)

    @classmethod
    def show_cluster(self, host, clustername, header=None):
        """show cluster 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET",
                                        url=parse.urljoin(host, api.PADDLE_FLOW_CLUSTER + "/%s" % clustername),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "status cluster failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        try:
            credential = base64.b64decode(data['credential']).decode()
        except Exception as e:
            raise PaddleFlowSDKException("InvalidResponse", "Credential is not Base64 encoded")
        ci = ClusterInfo(data['id'], data['clusterName'], data['description'], data['endpoint'],
                data['source'], data['clusterType'], data['version'], data['status'], credential,
                data['setting'], data['namespaceList'], data['createTime'], data['updateTime'])

        return True, ci

    @classmethod
    def delete_cluster(self, host, clustername, header=None):
        """delete cluster 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="DELETE",
                                        url=parse.urljoin(host, api.PADDLE_FLOW_CLUSTER + "/%s" % clustername),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete cluster failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, clustername
        

    @classmethod
    def update_cluster(self, host, clustername, endpoint=None, credential=None, clustertype=None,
    description=None, source=None, setting=None, status=None, namespacelist=None, version=None, header=None):
        """update cluster 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body={}
        if endpoint:
            body['endpoint']=endpoint
        if credential:
            if isinstance(credential, bytes):
                body['credential']=base64.b64encode(credential).decode()
            else:
                raise PaddleFlowSDKException("InvalidRequest", "credential must be bytes type")
        if clustertype:
            body['clusterType']=clustertype
        if source:
            body['source']=source
        if setting:
            body['setting']=setting
        if status:
            body['status']=status
        if namespacelist:
            if isinstance(namespacelist, list):
                body['namespaceList']=namespacelist
            else:
                raise PaddleFlowSDKException("InvalidRequest", "namespaceList must be list type")
        if description:
            body['description']=description
        if version:
            body['version']=version

        response = api_client.call_api(method="PUT",
                                        url=parse.urljoin(host, api.PADDLE_FLOW_CLUSTER + "/%s" % clustername),
                                        json=body, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "update cluster failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data['id']

    @classmethod
    def list_cluster_resource(self, host, clustername=None, header=None):
        """list cluster resource 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {}
        if clustername:
            params['clusterNames']=clustername
        response = api_client.call_api(method="GET", 
                                        url=parse.urljoin(host, api.PADDLE_FLOW_CLUSTER + '/resource'),
                                        params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "list cluster resource failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data
