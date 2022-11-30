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


class LogInfo(object):

    """the class of log info"""

    def __init__(self, runid, jobid, taskid, has_next_page, truncated, pagesize, pageno, log_content):
        """init """
        # 作业run的id
        self.runid = runid
        # run下子job的id
        self.jobid = jobid
        # job下子task的id
        self.taskid = taskid
        # 日志内容是否还有下一页，为true时则有下一页，否则为最后一页
        self.has_next_page = has_next_page
        # 日志内容是否被截断，为true时则被截断，否则未截断
        self.truncated = truncated
        # 每页日志内容的行数
        self.pagesize = pagesize
        # 日志内容的页数
        self.pageno = pageno
        # 具体的日志内容
        self.log_content = log_content


class LogInfoByLimit(object):

    """the class of log info"""
    def __init__(self, job_id="", task_id="", name="", has_next_page=False, truncated=False, line_limit=0, size_limit="", log_content=""):
        """init """
        # job id
        self.job_id = job_id
        # job下子task的id
        self.task_id = task_id
        # name
        self.name = name
        # 日志内容是否还有下一页，为true时则有下一页，否则为最后一页
        self.has_next_page = has_next_page
        # 日志内容是否被截断，为true时则被截断，否则未截断
        self.truncated = truncated
        # 行数限制
        self.line_limit = line_limit
        # 字节数限制
        self.size_limit = size_limit
        # 具体的日志内容
        self.log_content = log_content