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
from typing import List, Mapping


class StatisticsJobInfo:
    metrics_info: Mapping[str, any]

    def __init__(self, metric_info: Mapping[str, any]):
        self.metrics_info = metric_info

    @staticmethod
    def from_json(json_dic):
        statistics_job_info = StatisticsJobInfo(
            metric_info=None,
        )
        if 'metricsInfo' in json_dic:
            metrics_info = json_dic['metricsInfo']
            statistics_job_info.metrics_info = metrics_info
        return statistics_job_info


class TaskInfo:
    metric: str
    values: List[List[any]]

    def __init__(self, metric: str, values: List[List]) -> None:
        self.metric = metric
        self.values = values

    def __str__(self):
        return "TaskInfo: metric: {}, values: {}".format(self.metric, self.values)


class Result:
    task_name: str
    task_info: List[TaskInfo]

    def __init__(self, task_name: str, task_info: List[TaskInfo]) -> None:
        self.task_name = task_name
        self.task_info = task_info

    def __str__(self):
        return "Result: task_name: {}, task_info: {}".format(self.task_name, self.task_info)


class StatisticsJobDetailInfo:
    """the class of StatisticsJobDetailInfo info"""
    result: List[Result]
    truncated: bool

    def __init__(self, result: List[Result], truncated: bool) -> None:
        self.result = result
        self.truncated = truncated

    def __str__(self) -> str:
        """ str """
        return "StatisticsJobDetailInfo: result: {}".format(self.result)

    @staticmethod
    def from_json(metric_info):
        statistics_job_detail_info = StatisticsJobDetailInfo(
            result=[],
            truncated=metric_info['truncated'],
        )
        for result_json in metric_info['result']:
            result = Result(
                task_name=result_json['taskName'],
                task_info=[]
            )
            for task_info_json in result_json['taskInfo']:
                task_info = TaskInfo(
                    metric=task_info_json['metric'],
                    values=task_info_json['values']
                )
                result.task_info.append(task_info)
            statistics_job_detail_info.result.append(result)
        return statistics_job_detail_info


class JobInfo:
    job_id: str
    card_time: float
    create_time: str
    start_time: str
    finish_time: str
    device_count: int

    def __init__(self, job_id: str, card_time: float, create_time: str, start_time: str, finish_time: str,
                 device_count: int) -> None:
        self.job_id = job_id
        self.card_time = card_time
        self.create_time = create_time
        self.start_time = start_time
        self.finish_time = finish_time
        self.device_count = device_count

    def to_json(self):
        job_info = {
            "jobId": self.job_id,
            "cardTime": self.card_time,
            "createTime": self.create_time,
            "startTime": self.start_time,
            "finishTime": self.finish_time,
            "deviceCount": self.device_count
        }
        return job_info


class Detail:
    user_name: str
    job_info_list: List[JobInfo]
    job_count: int
    total_card_time: float

    def __init__(self, user_name: str, job_info_list: List[JobInfo], job_count: int, total_card_time: float) -> None:
        self.user_name = user_name
        self.job_info_list = job_info_list
        self.job_count = job_count
        self.total_card_time = total_card_time

    def __str__(self):
        json.dumps(self.__dict__)

    def to_str(self):
        return "Detail: user name: {}, job info list: {}, job count: {}, total card time: {},". \
            format(self.user_name, self.job_info_list, self.job_count, self.total_card_time)


class CardTimeResult:
    queue_name: str
    card_time: float
    device_type: str
    detail: List[Detail]

    def __init__(self, queue_name: str, card_time: float, device_type: str, detail: List[Detail]) -> None:
        self.queue_name = queue_name
        self.card_time = card_time
        self.device_type = device_type
        self.detail = detail

    def __str__(self):
        return "Detail: queue name: {}, card time: {}, device type: {}, detail: {},". \
            format(self.queue_name, self.card_time, self.device_type, self.detail)


class CardTimeInfo:
    data: List[CardTimeResult]

    def __init__(self, data: List[CardTimeResult]) -> None:
        self.data = data

    def __str__(self) -> str:
        """ str """
        return "CardTimeInfo: data: {}".format(self.data)

    @staticmethod
    def from_json(card_time_stat_info):
        card_time_info = CardTimeInfo(
            data=[],
        )
        try:
            for result_json in card_time_stat_info['data']:
                result = CardTimeResult(
                    queue_name=result_json['queueName'],
                    card_time=result_json['cardTime'],
                    device_type=result_json['deviceType'],
                    detail=[]
                )
                try:
                    for detail_json in result_json['detail']:
                        detail = Detail(
                            user_name=detail_json['userName'],
                            job_info_list=[],
                            job_count=detail_json['jobCount'],
                            total_card_time=detail_json['totalCardTime']
                        )
                        try:
                            for job_info_json in detail_json['jobInfoList']:
                                job_info = JobInfo(
                                    job_id=job_info_json['jobId'],
                                    card_time=job_info_json['cardTime'],
                                    create_time=job_info_json['createTime'],
                                    start_time=job_info_json['startTime'],
                                    finish_time=job_info_json['finishTime'],
                                    device_count=job_info_json['deviceCount'],
                                )
                                detail.job_info_list.append(job_info)
                        except:
                            pass
                        result.detail.append(detail)
                except:
                    pass
                card_time_info.data.append(result)
        except:
            pass
        return card_time_info
