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

# !/usr/bin/env python3
# -*- coding:utf8 -*-

import json
import base64
from urllib import parse
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.utils import api_client
from paddleflow.common import api
from paddleflow.statistics.statistics_info import StatisticsJobInfo


class StatisticsServiceApi(object):
    """
    the class of statistics service api
    """

    def __init__(self):
        """init"""
        pass

    @classmethod
    def get_statistics(cls, host, job_id, run_id=None, header=None):
        """
        get statistics info, job_id is not supported yet
        @param host:
        @param job_id:
        @param run_id:
        @param header:
        @return:
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        pram = {
            "jobID": job_id,
        }

        resp = api_client.call_api(method="GET",
                                   url=parse.urljoin(host, api.PADDLE_FLOW_STATISTIC + "/job/%s" % run_id),
                                   headers=header,
                                   params=pram)
        if not resp:
            raise PaddleFlowSDKException("Connection Error", "status run failed due to HTTPError")
        data = json.loads(resp.text)
        # return error resp, return err
        if 'message' in data:
            return False, data['message']

        statistics_job_info = StatisticsJobInfo(
            cpu_usage_rate=data['cpu_usage_rate'],
            memory_usage=data['memory_usage'],
            net_receive_bytes=data['net_receive_bytes'],
            net_send_bytes=data['net_send_bytes'],
            disk_usage_bytes=data['disk_usage_bytes'],
            disk_read_rate=data['disk_read_rate'],
            disk_write_rate=data['disk_write_rate'],
            gpu_util=data['gpu_util'],
            gpu_memory_util=data['gpu_memory_util'],
        )

        return True, statistics_job_info

    @classmethod
    def get_statistics_detail(cls, host, job_id, run_id=None, header=None):
        # TODO: TBW
        pass
