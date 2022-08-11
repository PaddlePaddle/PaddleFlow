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

class RunInfo(object):
    """the class of run info"""

    def __init__(self, run_id, fs_name, username, status, name, description, parameters,
                 run_yaml, runtime, post_process, docker_env, update_time, source, run_msg, schedule_id, scheduled_time,
                 fs_options, failure_options, disabled, run_cached_ids, create_time, activate_time):
        """init """
        self.run_id = run_id
        self.fs_name = fs_name
        self.username = username
        self.status = status
        self.name = name
        self.description = description
        self.parameters = parameters
        self.run_yaml = run_yaml
        self.runtime = runtime
        self.post_process = post_process
        self.docker_env = docker_env
        self.update_time = update_time
        self.source = source
        self.run_msg = run_msg
        self.fs_options = fs_options
        self.failure_options = failure_options
        self.disabled = disabled
        self.run_cached_ids = run_cached_ids
        self.schedule_id = schedule_id
        self.scheduled_time = scheduled_time
        self.create_time = create_time
        self.activate_time = activate_time

class JobInfo(object):
    """ the class of job info"""

    def __init__(self, name, deps, parameters, command, env, status, start_time, end_time, docker_env, job_id,
                 comp_type, step_name, parent_dag_id, extra_fs, artifacts, cache, job_message, cache_run_id, cache_job_id):
        self.artifacts = artifacts
        self.cache = cache
        self.job_message = job_message
        self.cache_run_id = cache_run_id
        self.cache_job_id = cache_job_id
        self.extra_fs = extra_fs
        self.job_id = job_id
        self.name = name
        self.deps = deps
        self.parameters = parameters
        self.command = command
        self.env = env
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.docker_env = docker_env
        self.type = comp_type
        self.step_name = step_name
        self.parent_dag_id = parent_dag_id

    def get_dict(self):
        return {
            'artifacts': self.artifacts,
            'cache': self.cache,
            'jobMessage': self.job_message,
            'cacheRunID': self.cache_run_id,
            'cacheJobID': self.cache_job_id,
            'extraFS': self.extra_fs,
            'jobID': self.job_id,
            'name': self.name,
            'deps': self.deps,
            'parameters': self.parameters,
            'command': self.command,
            'env': self.env,
            'status': self.status,
            'startTime': self.start_time,
            'endTime': self.end_time,
            'dockerEnv': self.docker_env,
            'type': self.type,
            'stepName': self.step_name,
            'parentDagID': self.parent_dag_id,
        }

class DagInfo(object):
    """ the class of dag info"""

    def __init__(self, dag_id, name, comp_type, dag_name, parent_dag_id, deps, parameters, artifacts, start_time, end_time,
                 status, message, entry_points):
        self.dag_id = dag_id
        self.name = name
        self.type = comp_type
        self.dag_name = dag_name
        self.parent_dag_id = parent_dag_id
        self.deps = deps
        self.parameters = parameters
        self.artifacts = artifacts
        self.start_time = start_time
        self.end_time = end_time
        self.status = status
        self.message = message
        self.entry_points = entry_points

    def get_dict(self):
        return {
            'dagID': self.dag_id,
            'name': self.name,
            'type': self.type,
            'dagName': self.dag_name,
            'parentDagID': self.parent_dag_id,
            'deps': self.deps,
            'parameters': self.parameters,
            'artifacts': self.artifacts,
            'startTime': self.start_time,
            'endTime': self.end_time,
            'status': self.status,
            'message': self.message,
            'entryPoints': self.entry_points,
        }

class RunCacheInfo(object):
    """ the class of runcache info"""

    def __init__(self, cache_id, first_fp, second_fp, run_id, source, job_id, fs_name, username, expired_time, strategy, custom,
                 create_time, update_time):
        self.cache_id = cache_id
        self.first_fp = first_fp
        self.second_fp = second_fp
        self.run_id = run_id
        self.source = source
        self.job_id = job_id
        self.fs_name = fs_name
        self.username = username
        self.expired_time = expired_time
        self.strategy = strategy
        self.custom = custom
        self.create_time = create_time
        self.update_time = update_time


class ArtifactInfo(object):
    """ the class of artifact info"""

    def __init__(self, run_id, fs_name, username, artifact_path, a_type, step, artifact_name, meta,
                 create_time, update_time):
        self.run_id = run_id
        self.fs_name = fs_name
        self.username = username
        self.artifact_path = artifact_path
        self.type = a_type
        self.step = step
        self.artifact_name = artifact_name
        self.meta = meta
        self.create_time = create_time
        self.update_time = update_time