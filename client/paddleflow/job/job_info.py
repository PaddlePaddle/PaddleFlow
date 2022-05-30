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


class JobInfo(object):
    """
    JobInfo
    """

    def __init__(self, job_id, job_name, labels, annotations, username, queue, priority, flavour, fs, extra_fs_list,
                 image, env, command, args_list, port, extension_template, framework, member_list, status, message,
                 accept_time, start_time, finish_time, runtime, distributed_runtime, workflow_runtime):
        """

        :param job_id:
        :param job_name:
        :param labels:
        :param annotations:
        :param username:
        :param queue:
        :param priority:
        :param flavour:
        :param fs:
        :param extra_fs_list:
        :param image:
        :param env:
        :param command:
        :param args_list:
        :param port:
        :param extension_template:
        :param framework:
        :param member_list:
        :param status:
        :param message:
        :param accept_time:
        :param start_time:
        :param finish_time:
        :param runtime:
        :param distributed_runtime:
        :param workflow_runtime:
        """
        self.job_id = job_id
        self.job_name = job_name
        self.labels = labels
        self.annotations = annotations
        self.username = username
        self.queue = queue
        self.priority = priority
        self.flavour = flavour
        self.fs = fs
        self.extra_fs_list = extra_fs_list
        self.image = image
        self.env = env
        self.command = command
        self.args_list = args_list
        self.port = port
        self.extension_template = extension_template
        self.framework = framework
        self.member_list = member_list
        self.status = status
        self.message = message
        self.accept_time = accept_time
        self.start_time = start_time
        self.finish_time = finish_time
        self.runtime = runtime
        self.distributed_runtime = distributed_runtime
        self.workflow_runtime = workflow_runtime


class JobRequest(object):
    """
    JobRequest
    """

    def __init__(self, queue, image=None, job_id=None, job_name=None, labels=None, annotations=None, priority=None,
                 flavour=None, fs=None, extra_fs_list=None, env=None, command=None, args_list=None, port=None,
                 extension_template=None, framework=None, member_list=None):
        """

        :param queue:
        :param image:
        :param job_id:
        :param job_name:
        :param labels:
        :param annotations:
        :param priority:
        :param flavour:
        :param fs:
        :param extra_fs_list:
        :param env:
        :param command:
        :param args_list:
        :param port:
        :param extension_template:
        :param framework:
        :param member_list:
        """
        self.job_id = job_id
        self.job_name = job_name
        self.labels = labels
        self.annotations = annotations
        self.queue = queue
        self.image = image
        self.priority = priority
        self.flavour = flavour
        self.fs = fs
        self.extra_fs_list = extra_fs_list
        self.env = env
        self.command = command
        self.args_list = args_list
        self.port = port
        self.extension_template = extension_template
        self.framework = framework
        self.member_list = member_list


class Member(object):
    """
    Members
    """

    def __init__(self, role, replicas, job_id=None, job_name=None, queue=None, labels=None, annotations=None, priority=None,
                 flavour=None, fs=None, extra_fs_list=None, image=None, env=None, command=None, args_list=None, port=None,
                 extension_template=None):
        """

        :param role:
        :param replicas:
        :param job_id:
        :param job_name:
        :param queue:
        :param labels:
        :param annotations:
        :param priority:
        :param flavour:
        :param fs:
        :param extra_fs_list:
        :param image:
        :param env:
        :param command:
        :param args_list:
        :param port:
        :param extension_template:
        """
        self.role = role
        self.replicas = replicas
        self.job_id = job_id
        self.job_name = job_name
        self.queue = queue
        self.labels = labels
        self.annotations = annotations
        self.priority = priority
        self.flavour = flavour
        self.fs = fs
        self.extra_fs_list = extra_fs_list
        self.image = image
        self.env = env
        self.command = command
        self.args_list = args_list
        self.port = port
        self.extension_template = extension_template


class Flavour(object):
    """
    Flavour
    """
    def __init__(self, name, cpu, memory, scalar_resources):
        """

        :param name:
        :param cpu:
        :param memory:
        :param scalar_resources:
        """
        self.name = name
        self.cpu = cpu
        self.memory = memory
        self.scalar_resources = scalar_resources


class FileSystem(object):
    """
    FileSystem
    """
    def __init__(self, name, mount_path, sub_path, read_only):
        self.name = name
        self.mount_path = mount_path
        self.sub_path = sub_path
        self.read_only = read_only




