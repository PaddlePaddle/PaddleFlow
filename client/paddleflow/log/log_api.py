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
from paddleflow.log.log_info import LogInfo
from paddleflow.utils import api_client

DEFAULT_PAGESIZE = 100
DEFAULT_PAGENO = 1

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
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_LOG + "/%s" % runid),
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


