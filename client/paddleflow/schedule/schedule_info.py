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

class ScheduleInfo(object):
    """the class of schedule info"""

    def __init__(self, crontab, name, pipeline_id, pipeline_version_id,
                 desc, username, schedule_id, fs_config, options, start_time, end_time, create_time,
                 update_time, next_run_time, message, status):
        self.schedule_id = schedule_id
        self.name = name
        self.desc = desc
        self.pipeline_id = pipeline_id
        self.pipeline_version_id = pipeline_version_id
        self.username = username
        self.fs_config = fs_config
        self.crontab = crontab
        self.options = options
        self.start_time = start_time
        self.end_time = end_time
        self.create_time = create_time
        self.update_time = update_time
        self.next_run_time = next_run_time
        self.message = message
        self.status = status
        
