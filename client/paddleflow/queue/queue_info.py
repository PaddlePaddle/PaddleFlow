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

class QueueInfo(object):
    """the class of queue info"""   

    def __init__(self, name, status, namespace, clusterName, quotaType,
                    maxResources, minResources, location, schedulingPolicy, createTime, updateTime):
        """init """
        self.name = name
        self.namespace = namespace
        self.status = status
        self.clusterName = clusterName
        self.quotaType = quotaType
        self.maxResources = maxResources
        self.minResources = minResources
        self.location = location
        self.schedulingPolicy = schedulingPolicy
        self.createTime = createTime
        self.updateTime = updateTime


class GrantInfo(object):
    """the class of grant info"""

    def __init__(self, username, resourceName):
        """ init """
        self.username = username
        self.resourceName = resourceName    