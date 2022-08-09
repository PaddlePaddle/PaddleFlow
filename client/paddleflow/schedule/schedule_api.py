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

class ScheduleServiceApi(object):
    """schedule service
    """

    def __init__(self):
        """
        """

    @classmethod
    def create_schedule(self, host, header, name, pipeline_id, pipeline_version_id, crontab,
                 desc, start_time, end_time, concurrency, concurrency_policy, expire_interval,
                 catchup, username):
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        body = {
            'name': name,
            'pipelineID': pipeline_id,
            'pipelineVersionID': pipeline_version_id,
            'crontab': crontab,
        }
        if desc:
            body['desc'] = desc
        if start_time:
            body['startTime'] = start_time
        if end_time:
            body['endTime'] = end_time
        if concurrency:
            body['concurrency'] = concurrency
        if concurrency_policy:
            body['concurrencyPolicy'] = concurrency_policy
        if expire_interval:
            body['expireInterval'] = expire_interval
        if catchup:
            body['catchup'] = catchup
        if username:
            body['username'] = username

        response = api_client.call_api(
            method="POST",
            url=parse.urljoin(host, api.PADDLE_FLOW_SCHEDULE),
            headers=header,
            json=body,
        )

        if not response:
            raise PaddleFlowSDKException(
                "Connection Error", "create pipeline failed due to HTTPError")

        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']

        return True, data['scheduleID']