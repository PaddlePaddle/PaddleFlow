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
from paddleflow.pipeline.pipeline_info import PipelineInfo, PipelineVersionInfo


class PipelineServiceApi(object):
    """pipeline service
    """
    def __init__(self):
        """
        """

    @classmethod
    def create_pipeline(self, host, fs_name, yaml_path, desc=None, username=None, header=None):
        """
            create pipeline
            this method returns a pipeline brief info and a pipeline version brief list info
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        body = {"fsname": fs_name, "yamlPath": yaml_path}
        if desc:
            body['desc'] = desc
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
            return False, data['message'], None, None
        return True, data['name'], data['pipelineID'], data['pipelineVersionID']

    @classmethod
    def list_pipeline(self, host, user_filter=None, name_filter=None, max_keys=None,
                      marker=None, header=None):
        """list pipeline
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        params = {}
        if user_filter:
            params['userFilter'] = user_filter
        if name_filter:
            params['nameFilter'] = name_filter
        if max_keys:
            params['maxKeys'] = max_keys
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
                    pipeline['pipelineID'], pipeline['name'], pipeline['username'], pipeline['desc'],
                    pipeline['createTime'], pipeline['updateTime'])
                pipelineList.append(pipelineInfo)
        return True, pipelineList, data.get('nextMarker', None)

    @classmethod
    def show_pipeline(self, host, pipeline_id, fs_filter, max_keys, marker, header=None):
        """show pipeline
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        params = {}
        if fs_filter:
            params['fsFilter'] = fs_filter
        if max_keys:
            params['maxKeys'] = max_keys
        if marker:
            params['marker'] = marker

        response = api_client.call_api(
            method="GET",
            url=parse.urljoin(host,
                              api.PADDLE_FLOW_PIPELINE + "/%s" % pipeline_id),
            params=params,
            headers=header)
        if not response:
            raise PaddleFlowSDKException(
                "Connection Error", "status pipeline failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message'], None, None
        pipeline = data['pipeline']
        resPpl = PipelineInfo(pipeline['pipelineID'], pipeline['name'], pipeline['username'], pipeline['desc'],
                          pipeline['createTime'], pipeline.get('updatetime', None))

        pplVersionList = data['pplVersions']['pplVersionList']
        resPplVerList = []
        for pplVer in pplVersionList:
            resPplVer = PipelineVersionInfo(pplVer['pipelineVersionID'], pplVer['pipelineID'], pplVer['fsName'],
                                            pplVer['yamlPath'], pplVer['pipelineYaml'], pplVer['username'],
                                            pplVer['createTime'], pplVer['updateTime'])
            resPplVerList.append(resPplVer)
        return True, resPpl, resPplVerList, data['pplVersions'].get('nextMarker', None)

    @classmethod
    def delete_pipeline(self, host, pipeline_id, header=None):
        """delete pipeline
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        response = api_client.call_api(
            method="DELETE",
            url=parse.urljoin(host,
                              api.PADDLE_FLOW_PIPELINE + "/%s" % pipeline_id),
            headers=header)
        if not response:
            raise PaddleFlowSDKException(
                "Connection Error", "delete pipeline failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, pipeline_id