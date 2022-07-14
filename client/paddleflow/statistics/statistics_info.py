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
from typing import List


class StatisticsJobInfo:
    metrics_info: 'MetricsInfo'

    def __init__(self, metric_info: 'MetricsInfo'):
        self.metrics_info = metric_info

    @staticmethod
    def from_json(json_dic):
        statistics_job_info = StatisticsJobInfo(
            metric_info=None,
        )
        if 'metricsInfo' in json_dic:
            metrics_info = json_dic['metricsInfo']
            statistics_job_info.metrics_info = MetricsInfo(
                cpu_usage_rate=metrics_info['cpu_usage_rate'],
                memory_usage=metrics_info['memory_usage'],
                net_receive_bytes=metrics_info['net_receive_bytes'],
                net_send_bytes=metrics_info['net_send_bytes'],
                disk_usage=metrics_info['disk_usage'],
                disk_read_rate=metrics_info['disk_read_rate'],
                disk_write_rate=metrics_info['disk_write_rate'],
                gpu_util=metrics_info['gpu_util'],
                gpu_memory_util=metrics_info['gpu_memory_util'],
            )
        return statistics_job_info


class MetricsInfo:
    """
    the class of StatisticsJobInfo info
    """
    cpu_usage_rate: float
    memory_usage: float
    net_receive_bytes: int
    net_send_bytes: int
    disk_usage: int
    disk_read_rate: float
    disk_write_rate: float
    gpu_util: float
    gpu_memory_util: float

    def __init__(self, cpu_usage_rate: float, memory_usage: float, net_receive_bytes: int, net_send_bytes: int,
                 disk_usage: int, disk_read_rate: float, disk_write_rate: float, gpu_util: float,
                 gpu_memory_util: float) -> None:
        """ init """
        self.cpu_usage_rate = cpu_usage_rate
        self.memory_usage = memory_usage
        self.net_receive_bytes = net_receive_bytes
        self.net_send_bytes = net_send_bytes
        self.disk_usage = disk_usage
        self.disk_read_rate = disk_read_rate
        self.disk_write_rate = disk_write_rate
        self.gpu_util = gpu_util
        self.gpu_memory_util = gpu_memory_util

    def __str__(self) -> str:
        """ str """
        return "StatisticsJobInfo: cpu_usage_rate: {}, memory_usage: {}, net_receive_bytes: {}, net_send_bytes: {}, " \
               "disk_usage: {}, disk_read_rate: {}, disk_write_rate: {}, gpu_util: {}, gpu_memory_util: {}". \
            format(self.cpu_usage_rate, self.memory_usage, self.net_receive_bytes, self.net_send_bytes,
                   self.disk_usage, self.disk_read_rate, self.disk_write_rate, self.gpu_util,
                   self.gpu_memory_util)


class TaskInfo:
    metric: str
    values: List[List]

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

    def __init__(self, result: List[Result]) -> None:
        self.result = result

    def __str__(self) -> str:
        """ str """
        return "StatisticsJobDetailInfo: result: {}".format(self.result)

    @staticmethod
    def from_json(metric_info):
        statistics_job_detail_info = StatisticsJobDetailInfo(
            result=[]
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
