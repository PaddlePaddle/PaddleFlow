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
from paddleflow.pipeline.pipeline_info import PipelineInfo


class PipelineServiceApi(object):
    """pipeline service
    """
    def __init__(self):
        """
        """

    @classmethod
    def create_pipeline(self, host, fsname, yamlpath, name=None, username=None, header=None):
        """create pipeline
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        body = {"fsname": fsname, "yamlPath": yamlpath}
        if name:
            body['name'] = name
        if username:
            body['username'] = username

        response = api_client.call_api(method="POST",
                                       url=parse.urljoin(
                                           host, api.PADDLE_FLOW_PIPELINE),
                                       headers=header,
                                       json=body)
        if not response:
            raise PaddleFlowSDKException(
                "Connection Error", "create pipeline failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message'], None
        return True, data['name'], data['pipelineID']

    @classmethod
    def list_pipeline(self, host, userfilter=None, fsfilter=None, namefilter=None, maxkeys=None, 
        marker=None, header=None):
        """list pipeline
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        params = {}
        if userfilter:
            params['userFilter'] = userfilter
        if fsfilter:
            params['fsFilter'] = fsfilter
        if namefilter:
            params['nameFilter'] = namefilter
        if maxkeys:
            params['maxKeys'] = maxkeys
        if marker:
            params['marker'] = self.marker
        response = api_client.call_api(method="GET",
                                       url=parse.urljoin(
                                           host, api.PADDLE_FLOW_PIPELINE),
                                       params=params,
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException(
                "Connection Error", "list pipeline failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        pipelineList = []
        if len(data['pipelineList']):
            for pipeline in data['pipelineList']:
                pipelineInfo = PipelineInfo(
                    pipeline['pipelineID'], pipeline['name'],
                    pipeline['fsname'], pipeline['username'],
                    pipeline['pipelineYaml'], pipeline['pipelineMd5'],
                    pipeline['createTime'], pipeline['updateTime'])
                pipelineList.append(pipelineInfo)
        return True, pipelineList, data.get('nextMarker', None)

    @classmethod
    def show_pipeline(self, host, pipelineid, header=None):
        """show pipeline
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        response = api_client.call_api(
            method="GET",
            url=parse.urljoin(host,
                              api.PADDLE_FLOW_PIPELINE + "/%s" % pipelineid),
            headers=header)
        if not response:
            raise PaddleFlowSDKException(
                "Connection Error", "status pipeline failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        pi = PipelineInfo(data['pipelineID'], data['name'], data['fsname'],
                          data['username'], data['pipelineYaml'],
                          data['pipelineMd5'], data['createTime'], data.get('updatetime', None))
        return True, pi

    @classmethod
    def delete_pipeline(self, host, pipelineid, header=None):
        """delete pipeline
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        response = api_client.call_api(
            method="DELETE",
            url=parse.urljoin(host,
                              api.PADDLE_FLOW_PIPELINE + "/%s" % pipelineid),
            headers=header)
        if not response:
            raise PaddleFlowSDKException(
                "Connection Error", "delete pipeline failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, pipelineid