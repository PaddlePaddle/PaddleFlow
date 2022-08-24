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
from paddleflow.flavour.flavour_info import FlavourInfo


class FlavouriceApi(object):
    """flavour api"""
    def __init__(self):
        """
        """

    @classmethod
    def list_flavour(self, host, header=None, maxsize=100, marker=None, clustername="", key=""):
        """
        list flavour
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "invalid maxsize {} should be int and greater than 0".format(maxsize))
        params = {
            "maxKeys": maxsize
        }
        if marker:
            params['marker'] = marker
        if key:
            params['name'] = key
        if clustername != "":
            params['clusterName'] = clustername

        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_FLAVOUR),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "list flavour failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        flavour_list = []
        if len(data['flavourList']):
            for f in data['flavourList']:
                scalarResources = json.dumps(f['scalarResources']) if f.__contains__('scalarResources') else ""
                flavour_info = FlavourInfo(f['name'], f['cpu'], f['mem'], scalarResources,
                                           "", f['createTime'], f['updateTime'])
                flavour_list.append(flavour_info)
        return True, flavour_list, data['nextMarker'] if data.__contains__('nextMarker') else None


    @classmethod
    def show_flavour(self, host, name, header=None):
        """
        show flavour
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET",
                                        url=parse.urljoin(host, api.PADDLE_FLOW_FLAVOUR + "/%s" % name),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "status flavour failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        scalarResources = json.dumps(data['scalarResources']) if data.__contains__('scalarResources') else ""
        f = FlavourInfo(name=data['name'],
                        cpu=data['cpu'],
                        mem=data['mem'],
                        cluster_name="",
                        scalar_resources=scalarResources,
                        createtime=data['createTime'],
                        updatetime=data['updateTime'])
        return True, f

    @classmethod
    def add_flavour(self, host, name, cpu, mem, scalar_resources=None, cluster_name=None, header=None):
        """
        add flavour
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {
            "name": name,
            "cpu": cpu,
            "mem": mem,
        }
        if cluster_name:
            body['clusterName'] = cluster_name
        if scalar_resources is not None:
            body['scalarResources'] = scalar_resources
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_FLAVOUR), headers=header,
                                       json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "add flavour failed due to HTTPError")

        data = json.loads(response.text)
        if 'message' in data:
            return False, "requestBody={}\nerror message={}".format(body, data['message'])
        return True, None

    @classmethod
    def del_flavour(self, host, name, header=None):
        """
        delete flavour
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        response = api_client.call_api(method="DELETE", url=parse.urljoin(host, "{}/{}".format(api.PADDLE_FLOW_FLAVOUR, name)),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete flavour failed due to HTTPError")
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def update_flavour(self, host, name, cpu=None, mem=None, scalar_resources=None, cluster_name=None, header=None):
        """
        update flavour
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body={}
        if cpu:
            body['cpu'] = cpu
        if mem:
            body['mem'] = mem
        if scalar_resources:
            body['scalarResources'] = scalar_resources
        if cluster_name:
            body['clusterName'] = cluster_name

        response = api_client.call_api(method="PUT", url=parse.urljoin(host, "{}/{}".format(api.PADDLE_FLOW_FLAVOUR, name)),
                                        json=body, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "update flavour failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data['name']
