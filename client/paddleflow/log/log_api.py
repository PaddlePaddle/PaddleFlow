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

from paddleflow.common import api
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.log.log_info import LogInfo, LogInfoByLimit
from paddleflow.utils import api_client

DEFAULT_PAGESIZE = 100
DEFAULT_PAGENO = 1
DEFAULT_LINE_LIMIT = 1000
DEFAULT_SIZE_LIMIT = "100KB"

class LogServiceApi(object):
    """
    log service
    """

    def __init__(self):
            """
            """

    @classmethod
    def get_log_info(self, host, runid, jobid, pagesize, pageno, fileposition, header=None):
        """ get log info
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {}
        if jobid:
            params['jobID'] = jobid
        if pagesize:
            params['pageSize'] = pagesize
        if pageno:
            params['pageNo'] = pageno
        if fileposition:
            params['logFilePosition'] = fileposition
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_LOG + "/run/%s" % runid),
                                       headers=header, params=params)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "get log failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        if pagesize is None:
            pagesize = DEFAULT_PAGESIZE
        else:
            pagesize = int(pagesize)
        if pageno is None:
            pageno = DEFAULT_PAGENO
        else:
            pageno = int(pageno)
        loginfo_list = []
        for job in data['runLog']:
            for task in job['taskList']:
                loginfo = LogInfo(runid=data['runID'], jobid=job['jobID'], taskid=task['taskID'],
                                  has_next_page=task['logInfo']['hasNextPage'], truncated=task['logInfo']['truncated'],
                                  pagesize=pagesize, pageno=pageno, log_content=task['logInfo']['logContent'])
                loginfo_list.append(loginfo)
        log_info_dict = {'submitLog': data['submitLog'], 'runLog': loginfo_list}
        return True, log_info_dict


    @classmethod
    def get_log_info_by_limit(self, host, job_id=None, name=None, namespace=None,
                              cluster_name=None, read_from_tail=None, line_limit=None, size_limit=None,
                              type=None, framework=None, header=None):
        """ get logs by line_limit or size_limit
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {}
        if job_id:
            params['jobID'] = job_id
        if name:
            params['name'] = name
        if namespace:
            params['namespace'] = namespace
        if cluster_name:
            params['clusterName'] = cluster_name
        if read_from_tail:
            params['readFromTail'] = read_from_tail
        if line_limit:
            params['lineLimit'] = str(line_limit)
        if size_limit:
            params['sizeLimit'] = str(size_limit)
        if type:
            params['type'] = type
        if framework:
            params['framework'] = framework

        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_LOG + "/job"),
                                       headers=header, params=params)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "get log failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        if line_limit is None:
            line_limit = DEFAULT_LINE_LIMIT
        else:
            line_limit = int(line_limit)
        if size_limit is None:
            size_limit = DEFAULT_SIZE_LIMIT


        loginfo_list = []
        for task in data['taskList']:
            loginfo = LogInfoByLimit(job_id=data['jobID'], task_id=task['taskID'], name=data['name'],
                                     has_next_page=task['logInfo']['hasNextPage'],
                                     truncated=task['logInfo']['truncated'],
                                     line_limit=line_limit, size_limit=size_limit, log_content=task['logInfo']['logContent'])
            loginfo_list.append(loginfo)

        log_info_dict = {'eventList': data['eventList'], 'logs': loginfo_list}
        return True, log_info_dict


