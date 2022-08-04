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

class PipelineInfo(object):
    """the class of pipeline info"""
    def __init__(self, pipelineid, name, username, desc,
                createtime, updatetime):
        """init """
        self.pipelineid = pipelineid
        self.name = name
        self.username = username
        self.desc = desc
        self.createtime = createtime
        self.updatetime = updatetime

class PipelineVersionInfo(object):
    """the class of pipeline version info"""
    def __init__(self, pipelineVersionID, pipelineID, fsName, yamlPath, pipelineYaml, username,
                 createTime, updateTime):
        self.pipelineVersionID = pipelineVersionID
        self.pipelineID = pipelineID
        self.fsName = fsName
        self.yamlPath = yamlPath
        self.pipelineYaml = pipelineYaml
        self.username = username
        self.createTime = createTime
        self.updateTime = updateTime