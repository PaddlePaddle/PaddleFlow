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
    """the class of StatisticsJobInfo info"""
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


class TaskInfo:
    metric: str
    values: List[List]

    def __init__(self, metric: str, values: List[List]) -> None:
        self.metric = metric
        self.values = values


class Result:
    task_name: str
    task_info: List[TaskInfo]

    def __init__(self, task_name: str, task_info: List[TaskInfo]) -> None:
        self.task_name = task_name
        self.task_info = task_info


class StatisticsJobDetailInfo:
    """the class of StatisticsJobDetailInfo info"""
    result: List[Result]

    def __init__(self, result: List[Result]) -> None:
        self.result = result
