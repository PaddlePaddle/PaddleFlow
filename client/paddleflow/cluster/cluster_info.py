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

class ClusterInfo(object):
    """the class of cluster info"""
    def __init__(self, clusterid, clustername, description, endpoint, source, clustertype, version, status,
    credential, setting, namespacelist, createtime, updatetime):
        """init """
        self.clusterid = clusterid
        self.clustername = clustername
        self.description = description
        self.endpoint = endpoint
        self.source = source
        self.clustertype = clustertype
        self.version = version
        self.status = status
        self.credential = credential
        self.setting = setting
        self.namespacelist = namespacelist
        self.createtime = createtime
        self.updatetime = updatetime
