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
from typing import List


class StatisticsJobInfo:
    """
    the class of StatisticsJobInfo info
    """
    cpu_usage_rate: float
    memory_usage: float
    net_receive_bytes: int
    net_send_bytes: int
    disk_usage_bytes: int
    disk_read_rate: float
    disk_write_rate: float
    gpu_util: float
    gpu_memory_util: float

    def __init__(self, cpu_usage_rate: float, memory_usage: float, net_receive_bytes: int, net_send_bytes: int,
                 disk_usage_bytes: int, disk_read_rate: float, disk_write_rate: float, gpu_util: float,
                 gpu_memory_util: float) -> None:
        """ init """
        self.cpu_usage_rate = cpu_usage_rate
        self.memory_usage = memory_usage
        self.net_receive_bytes = net_receive_bytes
        self.net_send_bytes = net_send_bytes
        self.disk_usage_bytes = disk_usage_bytes
        self.disk_read_rate = disk_read_rate
        self.disk_write_rate = disk_write_rate
        self.gpu_util = gpu_util
        self.gpu_memory_util = gpu_memory_util

    def __str__(self) -> str:
        """ str """
        return "StatisticsJobInfo: cpu_usage_rate: {}, memory_usage: {}, net_receive_bytes: {}, net_send_bytes: {}, " \
               "disk_usage_bytes: {}, disk_read_rate: {}, disk_write_rate: {}, gpu_util: {}, gpu_memory_util: {}". \
            format(self.cpu_usage_rate, self.memory_usage, self.net_receive_bytes, self.net_send_bytes,
                   self.disk_usage_bytes, self.disk_read_rate, self.disk_write_rate, self.gpu_util,
                   self.gpu_memory_util)

    @staticmethod
    def from_json(json_dict):
        statistics_job_info = StatisticsJobInfo(
            cpu_usage_rate=json_dict['cpu_usage_rate'],
            memory_usage=json_dict['memory_usage'],
            net_receive_bytes=json_dict['net_receive_bytes'],
            net_send_bytes=json_dict['net_send_bytes'],
            disk_usage_bytes=json_dict['disk_usage_bytes'],
            disk_read_rate=json_dict['disk_read_rate'],
            disk_write_rate=json_dict['disk_write_rate'],
            gpu_util=json_dict['gpu_util'],
            gpu_memory_util=json_dict['gpu_memory_util'],
        )
        return statistics_job_info


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
    def from_json(json_dict):
        statistics_job_detail_info = StatisticsJobDetailInfo(
            result=[]
        )
        for result_json in json_dict['result']:
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
