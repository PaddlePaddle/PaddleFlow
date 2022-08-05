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
    def __init__(self, pipeline_id, name, username, desc,
                 create_time, update_time):
        """init """
        self.pipeline_id = pipeline_id
        self.name = name
        self.username = username
        self.desc = desc
        self.create_time = create_time
        self.update_time = update_time

class PipelineVersionInfo(object):
    """the class of pipeline version info"""
    def __init__(self, pipeline_version_id, pipeline_id, fs_name, yaml_path, pipeline_yaml, username,
                 create_time, update_time):
        self.pipeline_version_id = pipeline_version_id
        self.pipeline_id = pipeline_id
        self.fs_name = fs_name
        self.yaml_path = yaml_path
        self.pipeline_yaml = pipeline_yaml
        self.username = username
        self.create_time = create_time
        self.update_time = update_time