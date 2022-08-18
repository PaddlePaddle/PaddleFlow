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
from .schedule_info import ScheduleInfo
from ..run.run_info import RunInfo

class ScheduleServiceApi(object):
    """schedule service
    """

    def __init__(self):
        """
        """

    @classmethod
    def create_schedule(self, host, header, name, pipeline_id, pipeline_version_id, crontab,
                 desc=None, start_time=None, end_time=None, concurrency=None, concurrency_policy=None, expire_interval=None,
                 catchup=None, username=None):
        """ create schedule """
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

    @classmethod
    def list_schedule(self, host, header, user_filter=None, ppl_filter=None, ppl_version_filter=None, schedule_filter=None,
                      name_filter=None, status_filter=None, marker=None, max_keys=None):
        """ list schedule """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        param = {}
        if user_filter:
            param['userFilter'] = user_filter
        if ppl_filter:
            param['pplFilter'] = ppl_filter
        if ppl_version_filter:
            param['pplVersionFilter'] = ppl_version_filter
        if schedule_filter:
            param['scheduleFilter'] = schedule_filter
        if name_filter:
            param['nameFilter'] = name_filter
        if status_filter:
            param['statusFilter'] = status_filter
        if marker:
            param['marker'] = marker
        if max_keys:
            param['maxKeys'] = max_keys

        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_SCHEDULE),
                                       params=param, headers=header)

        if not response:
            raise PaddleFlowSDKException("Connection Error", "list schedule failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']

        res_schedule_list = []
        schedule_list = data['scheduleList']
        if len(schedule_list) > 0:
            for schedule in schedule_list:
                schedule_info = ScheduleInfo(schedule['crontab'], schedule['name'], schedule['pipelineID'],
                                             schedule['pipelineVersionID'], schedule['desc'], schedule['username'],
                                             schedule['scheduleID'], schedule['fsConfig'], schedule['options'],
                                             schedule['startTime'], schedule['endTime'], schedule['createTime'],
                                             schedule['updateTime'], schedule['nextRunTime'],
                                             schedule['scheduleMsg'], schedule['status'])
                res_schedule_list.append(schedule_info)
        return True, {'scheduleList': res_schedule_list, 'nextMarker': data.get('nextMarker', None)}

    @classmethod
    def show_schedule(self, host, header, schedule_id, run_filter=None, status_filter=None,
             marker=None, max_keys=None):
        """ show schedule """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        param = {}
        if run_filter:
            param['runFilter'] = run_filter
        if status_filter:
            param['statusFilter'] = status_filter
        if marker:
            param['marker'] = marker
        if max_keys:
            param['maxKeys'] = max_keys

        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_SCHEDULE + "/%s" % schedule_id),
                                       params=param, headers=header)

        if not response:
            raise PaddleFlowSDKException("Connection Error", "show schedule failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']

        runs = data['runs']
        res_run_list = []
        run_list = runs['runList']
        for run in run_list:
            run_info = RunInfo(run['runID'], run['fsName'], run['username'], run['status'], run['name'],
                               run['description'], None, None, None, None, None, run['updateTime'],
                               run['source'], run['runMsg'], run['scheduleID'], run['scheduledTime'],
                               None, None, None, None, run['createTime'], run['activateTime'])
            res_run_list.append(run_info)

        schedule_info = ScheduleInfo(data['crontab'], data['name'], data['pipelineID'],
                                        data['pipelineVersionID'], data['desc'], data['username'],
                                        data['scheduleID'], data['fsConfig'], data['options'],
                                        data['startTime'], data['endTime'], data['createTime'],
                                        data['updateTime'], data['nextRunTime'],
                                        data['scheduleMsg'], data['status'])

        return True, {'scheduleInfo': schedule_info, 'runList': res_run_list, 'nextMarker': data.get('nextMarker', None)}


    @classmethod
    def stop_schedule(self, host, header, schedule_id):
        """ stop schedule """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        response = api_client.call_api(
            method="PUT",
            url=parse.urljoin(host,
                              api.PADDLE_FLOW_SCHEDULE + "/%s" % schedule_id),
            headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "stop schedule failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, None

    @classmethod
    def delete_schedule(self, host, header, schedule_id):
        """ delete schedule """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest",
                                         "paddleflow should login first")
        response = api_client.call_api(
            method="DELETE",
            url=parse.urljoin(host,
                              api.PADDLE_FLOW_SCHEDULE + "/%s" % schedule_id),
            headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "stop schedule failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, None