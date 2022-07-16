"""
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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
from paddleflow.statistics.statistics_info import StatisticsJobInfo, StatisticsJobDetailInfo


class StatisticsServiceApi(object):
    """
    the class of statistics service api
    """

    def __init__(self):
        """init"""
        pass

    @classmethod
    def get_statistics(cls, host, job_id: str, run_id: str = None, header=None):
        """
        get statistics info, run_id is not supported yet
        @param host: host url
        @param job_id: job id
        @param run_id: not supported yet
        @param header: request header
        @return: success: bool, resp: StatisticsJobInfo
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        # TODO: support run_id
        if run_id:
            raise PaddleFlowSDKException("InvalidRequest", "run_id is not supported yet")

        pram = {
            "runID": run_id,
        }

        resp = api_client.call_api(method="GET",
                                   url=parse.urljoin(host, api.PADDLE_FLOW_STATISTIC + "/job/%s" % job_id),
                                   headers=header,
                                   params=pram)
        if not resp:
            raise PaddleFlowSDKException("Connection Error", "status run failed due to HTTPError")
        data = json.loads(resp.text)
        # return error resp, return err
        if 'message' in data:
            return False, data['message']

        statistics_job_info = StatisticsJobInfo.from_json(data)

        return True, statistics_job_info

    @classmethod
    def get_statistics_detail(cls, host, job_id: str, start=None, end=None,
                              step=None, run_id=None, header=None):
        """
        get statistics detail info, run_id is not supported yet
        @param host: host urls
        @param job_id: job id
        @param run_id: not support yet
        @param header: request header
        @param start: start time
        @param end: end time
        @param step: step of time query
        @return: success: bool, resp: StatisticsDetailJobInfo
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        # TODO: support run_id
        if run_id:
            raise PaddleFlowSDKException("InvalidRequest", "run_id is not supported yet")

        pram = {
            "runID": run_id,
            "start": start,
            "end": end,
            "step": step,
        }

        resp = api_client.call_api(method="GET",
                                   url=parse.urljoin(host, api.PADDLE_FLOW_STATISTIC + "/jobDetail/%s" % job_id),
                                   headers=header,
                                   params=pram)

        if not resp:
            raise PaddleFlowSDKException("Connection Error", "status run failed due to HTTPError")

        data = json.loads(resp.text)
        # return error resp, return err
        if 'message' in data:
            return False, data['message']
        statistics_job_detail_info = StatisticsJobDetailInfo.from_json(data)
        return True, statistics_job_detail_info
