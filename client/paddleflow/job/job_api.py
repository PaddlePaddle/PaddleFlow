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
import json
from urllib import parse

from paddleflow.common import api
from paddleflow.common.exception import PaddleFlowSDKException
from paddleflow.job.job_info import JobInfo, Member
from paddleflow.utils import api_client


class JobServiceApi(object):
    """
    JobServiceApi
    """

    def __init__(self):
        """

        """
        pass

    @classmethod
    def create_job(cls, host, job_type, job_request, header=None):
        """

        :param host:
        :param job_type:
        :param job_request:
        :param header:
        :return:
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {}
        cls.convert_to_job_spec_body(body, job_request)
        if job_request.framework:
            body['framework'] = job_request.framework
        if job_request.member_list:
            body['members'] = list()
            for member in job_request.member_list:
                member_dict = dict()
                member_dict['role'] = member['role']
                member_dict['replicas'] = member['replicas']
                cls.convert_to_job_spec_body(member_dict, Member(member.get('role', None), member.get('replicas', None),
                                                                 member.get('id', None), member.get('name', None),
                                                                 member.get('schedulingPolicy', {}).get('queue', None),
                                                                 member.get('labels', None), member.get('annotations', None),
                                                                 member.get('schedulingPolicy', {}).get('priority', None),
                                                                 member.get('flavour', None), member.get('fs', None),
                                                                 member.get('extraFS', None), member.get('image', None),
                                                                 member.get('env', None), member.get('command', None),
                                                                 member.get('args', None), member.get('port', None),
                                                                 member.get('extensionTemplate', None)))
                body['members'].append(member_dict)
        response = api_client.call_api(method="POST",
                                       url=parse.urljoin(
                                           host, api.PADDLE_FLOW_JOB + "/%s" % job_type),
                                       headers=header,
                                       json=body)
        if not response:
            raise PaddleFlowSDKException("Create job error", response.text)
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data['id']

    @classmethod
    def convert_to_job_spec_body(cls, body, job_request):
        body['schedulingPolicy'] = dict()
        body['schedulingPolicy']['queue'] = job_request.queue
        if job_request.priority:
            body['schedulingPolicy']['priority'] = job_request.priority
        if job_request.image:
            body['image'] = job_request.image
        if job_request.job_id:
            body['id'] = job_request.job_id
        if job_request.job_name:
            body['name'] = job_request.job_name
        if job_request.labels:
            body['labels'] = job_request.labels
        if job_request.annotations:
            body['annotations'] = job_request.annotations
        if job_request.flavour:
            body['flavour'] = dict()
            if 'name' in job_request.flavour:
                body['flavour']['name'] = job_request.flavour['name']
            if 'cpu' in job_request.flavour:
                body['flavour']['cpu'] = job_request.flavour['cpu']
            if 'mem' in job_request.flavour:
                body['flavour']['mem'] = job_request.flavour['mem']
            if 'scalarResources' in job_request.flavour:
                body['flavour']['scalarResources'] = job_request.flavour['scalarResources']
        if job_request.fs:
            body['fs'] = dict()
            if 'name' in job_request.fs:
                body['fs']['name'] = job_request.fs['name']
            if 'mountPath' in job_request.fs:
                body['fs']['mountPath'] = job_request.fs['mountPath']
            if 'subPath' in job_request.fs:
                body['fs']['subPath'] = job_request.fs['subPath']
            if 'readOnly' in job_request.fs:
                body['fs']['readOnly'] = job_request.fs['readOnly']
        if job_request.extra_fs_list:
            body['extraFS'] = list()
            for fs in job_request.extra_fs_list:
                fs_dict = dict()
                if 'name' in fs:
                    fs_dict['name'] = fs['name']
                if 'mountPath' in fs:
                    fs_dict['mountPath'] = fs['mountPath']
                if 'subPath' in fs:
                    fs_dict['subPath'] = fs['subPath']
                if 'readOnly' in fs:
                    fs_dict['readOnly'] = fs['readOnly']
                body['extraFS'].append(fs_dict)
        if job_request.env:
            body['env'] = job_request.env
        if job_request.command:
            body['command'] = job_request.command
        if job_request.args_list:
            body['args'] = job_request.args_list
        if job_request.port:
            body['port'] = job_request.port
        if job_request.extension_template:
            body['extensionTemplate'] = job_request.extension_template



    @classmethod
    def show_job(cls, host, job_id, header=None):
        """

        :param host:
        :param job_id:
        :param header:
        :return:
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_JOB + "/%s" % job_id),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Show job error", response.text)
        data = json.loads(response.text)
        if 'message' in data and response.status_code != 200:
            return False, data['message']
        priority = None
        if 'priority' in data['schedulingPolicy']:
            priority = data['schedulingPolicy']['priority']
        framework = None
        if 'framework' in data:
            framework = data['framework']
        members = None
        if 'members' in data:
            members = data['members']
        runtime = None
        if 'runtime' in data:
            runtime = data['runtime']
        distributed_runtime = None
        if 'distributedRuntime' in data:
            distributed_runtime = data['distributedRuntime']
        workflow_runtime = None
        if 'workflowRuntime' in data:
            workflow_runtime = data['workflowRuntime']
        job_info = JobInfo(job_id=data['id'], job_name=data['name'], labels=data['labels'],
                           annotations=data['annotations'], username=data['UserName'],
                           queue=data['schedulingPolicy']['queue'], priority=priority, flavour=data['flavour'],
                           fs=data['fs'], extra_fs_list=data['extraFS'], image=data['image'],
                           env=data['env'], command=data['command'], args_list=data['args'], port=data['port'],
                           extension_template=data['extensionTemplate'], framework=framework, member_list=members,
                           status=data['status'], message=data['message'], accept_time=data['acceptTime'],
                           start_time=data['startTime'], finish_time=data['finishTime'], runtime=runtime,
                           distributed_runtime=distributed_runtime, workflow_runtime=workflow_runtime)
        return True, job_info

    @classmethod
    def list_job(cls, host, status, timestamp, start_time, queue, labels, maxsize=100, marker=None, header=None):
        """

        :param host:
        :param status:
        :param timestamp:
        :param start_time:
        :param queue:
        :param labels:
        :param maxsize:
        :param marker:
        :param header:
        :return:
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {}
        if status is not None:
            params['status'] = status
        if timestamp is not None:
            params['timestamp'] = timestamp
        if start_time is not None:
            params['startTime'] = start_time
        if queue is not None:
            params['queue'] = queue
        if labels is not None:
            params['labels'] = json.dumps(labels)
        if marker is not None:
            params['marker'] = marker
        params['maxKeys'] = maxsize
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_JOB),
                                       headers=header, params=params)
        if not response:
            raise PaddleFlowSDKException("List job error", response.text)
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message'], None
        job_list = []
        if len(data['jobList']):
            for job in data['jobList']:
                priority = None
                if 'priority' in job['schedulingPolicy']:
                    priority = job['schedulingPolicy']['priority']
                framework = None
                if 'framework' in job:
                    framework = job['framework']
                members = None
                if 'members' in job:
                    members = job['members']
                job_info = JobInfo(job_id=job['id'], job_name=job['name'], labels=job['labels'],
                                   annotations=job['annotations'], username=job['UserName'],
                                   queue=job['schedulingPolicy']['queue'], priority=priority, flavour=job['flavour'],
                                   fs=job['fs'], extra_fs_list=job['extraFS'], image=job['image'],
                                   env=job['env'], command=job['command'], args_list=job['args'], port=job['port'],
                                   extension_template=job['extensionTemplate'], framework=framework, member_list=members,
                                   status=job['status'], message=job['message'], accept_time=job['acceptTime'],
                                   start_time=job['startTime'], finish_time=job['finishTime'], runtime=None,
                                   distributed_runtime=None, workflow_runtime=None)
                job_list.append(job_info)
        return True, job_list, data.get('nextMarker', None)


    @classmethod
    def update_job(cls, host, job_id, priority, labels, annotations, header=None):
        """
        update job priority, labels, or annotations

        :param host:
        :param job_id:
        :param priority:
        :param labels:
        :param annotations:
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {
            "action": "modify"
        }
        body = {}
        if priority is not None:
            body['priority'] = priority
        if labels is not None:
            body['labels'] = labels
        if annotations is not None:
            body['annotations'] = annotations
        response = api_client.call_api(method="PUT", url=parse.urljoin(host, api.PADDLE_FLOW_JOB + "/%s" % job_id),
                                               headers=header, params=params, json=body)
        if not response:
            raise PaddleFlowSDKException("Update job error", response.text)
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None


    @classmethod
    def delete_job(cls, host, job_id, header=None):
        """

        :param host:
        :param job_id:
        :param header:
        :return:
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="DELETE", url=parse.urljoin(host, api.PADDLE_FLOW_JOB + "/%s" % job_id),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Delete job error", response.text)
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def stop_job(cls, host, job_id, header=None):
        """

        :param host:
        :param job_id:
        :param header:
        :return:
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {}
        params['action'] = 'stop'
        response = api_client.call_api(method="PUT", url=parse.urljoin(host, api.PADDLE_FLOW_JOB + "/%s" % job_id),
                                       headers=header, params=params)
        if not response:
            raise PaddleFlowSDKException("Stop job error", response.text)
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None
