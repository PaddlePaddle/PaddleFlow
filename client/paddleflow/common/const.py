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


RUN_ACTIVE_STATUS = ['running', 'pending', 'terminating', 'initiating']
RUN_FINAL_STATUS = ['failed', 'succeeded', 'terminated', 'skipped']

JOB_TYPE_SINGLE = 'single'
JOB_TYPE_DISTRIBUTED = 'distributed'
JOB_TYPE_WORKFLOW = 'workflow'
# batch job runs as a kubernetes native batch/v1 Job, described by extensionTemplate
JOB_TYPE_BATCH = 'batch'
JOB_TYPES = (JOB_TYPE_SINGLE, JOB_TYPE_DISTRIBUTED, JOB_TYPE_WORKFLOW, JOB_TYPE_BATCH)
