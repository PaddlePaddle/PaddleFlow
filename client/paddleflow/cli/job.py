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


import sys
import subprocess
import time
import json
import click
import shutil


from paddleflow.cli.output import print_output, OutputFormat
from paddleflow.job.job_info import JobRequest
from paddleflow.utils.format_help import command_required_option_from_option


field_dict = {"id": "job id", "name": "job name", "queue": "queue", "status": "status", "acceptTime": "accept time",
              "startTime": "start time", "finishTime": "finish time", "user": "user", "runtime": "runtime",
              "distributedRuntime": "distributed runtime", "workflowRuntime": "workflow runtime", "message": "message",
              "labels": "labels", "annotations": "annotations", "priority": "priority", "flavour": "flavour",
              "fs": "file system", "extraFS": "extra file systems", "image": "image", "env": "env",
              "command": "command", "args": "args", "port": "port", "extensionTemplate": "extension template",
              "framework": "framework", "members": "members"}


@click.group()
def job():
    """manage job resources"""
    pass


@job.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('jobid')
@click.pass_context
def show(ctx, jobid, fieldlist=None):
    """

    show job\n
    JOBID: the id of the specificed job.

    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not jobid:
        click.echo('job show must provide jobid.', err=True)
        sys.exit(1)
    valid, response = client.show_job(jobid)
    if valid:
        _print_job(response, output_format)
    else:
        click.echo("show job failed with message[%s]" % response)


def _print_job(job_info, out_format):
    """
    _print_job
    """
    field_value_dict = {"id": job_info.job_id, "name": job_info.job_name, "queue": job_info.queue, "status": job_info.status,
                        "acceptTime": job_info.accept_time, "startTime": job_info.start_time,
                        "finishTime": job_info.finish_time, "user": job_info.username, "runtime": job_info.runtime,
                        "distributedRuntime": job_info.distributed_runtime, "workflowRuntime": job_info.workflow_runtime,
                        "message": job_info.message, "labels": job_info.labels, "annotations": job_info.annotations,
                        "priority": job_info.priority, "flavour": job_info.flavour, "fs": job_info.fs,
                        "extraFS": job_info.extra_fs_list, "image": job_info.image, "env": job_info.env,
                        "command": job_info.command, "args": job_info.args_list, "port": job_info.port,
                        "extensionTemplate": job_info.extension_template, "framework": job_info.framework,
                        "members": job_info.member_list}
    # print job basic info
    headers = ['job id', 'job name', 'queue', 'priority', 'status', 'accept time', 'start time', 'finish time']
    data = [[job_info.job_id, job_info.job_name, job_info.queue, job_info.priority, job_info.status,
            job_info.accept_time, job_info.start_time, job_info.finish_time]]
    print_output(data, headers, out_format, table_format='grid')
    print("job config and runtime info: ")
    # print job fs
    headers = ['flavour', 'fs', 'extraFS', 'labels', 'annotations', 'image', 'command', 'args',  'port']
    data = [[job_info.flavour, job_info.fs, job_info.extra_fs_list, job_info.labels, job_info.annotations,
            job_info.image, job_info.command, job_info.args_list, job_info.port]]
    # print distributed job config
    if job_info.member_list:
        headers.extend(['framework', 'members'])
        data[0].extend([job_info.framework, job_info.member_list])
    if job_info.extension_template is dict:
        headers.append('extensionTemplate')
        data[0].append(job_info.extension_template)
    # print job runtime info
    headers.append('message')
    data[0].append(job_info.message)
    if job_info.runtime:
        headers.append('runtime')
        data[0].append(job_info.runtime)
    if job_info.distributed_runtime:
        headers.append('distributed runtime')
        data[0].append(job_info.distributed_runtime)
    if job_info.workflow_runtime:
        headers.append('workflow runtime')
        data[0].append(job_info.workflow_runtime)
    print_output(data, headers, "json", table_format='grid')


@job.command()
@click.option('-s', '--status', help="List the job by job status.")
@click.option('-t', '--timestamp', help="List the job after the updated time.")
@click.option('-st', '--starttime', help="List the job after the start time.")
@click.option('-q', '--queue', help="List the job by the queue.")
@click.option('-l', '--labels', help="List the job by the labels.")
@click.option('-m', '--maxkeys', help="Max size of the listed job.")
@click.option('-mk', '--marker', help="Next page ")
@click.option('-fl', '--fieldlist', help="show the specificed field list")
@click.pass_context
def list(ctx, status=None, timestamp=None, starttime=None, queue=None, labels=None, maxkeys=None, marker=None, fieldlist=None):
    """
    list job\n
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if labels:
        label_list = labels.split(",")
        label_map = dict()
        for i in label_list:
            v = i.split("=")
            label_map[v[0]] = v[1]
        labels = label_map
    valid, response, nextmarker = client.list_job(status, timestamp, starttime, queue, labels, maxkeys, marker)
    if valid:
        _print_job_list(response, output_format, fieldlist)
        click.echo('marker: {}'.format(nextmarker))
    else:
        click.echo("list job failed with message[%s]" % response)


def _print_job_list(jobs, out_format, fieldlist):
    """print jobs """
    if fieldlist:
        headers = [field_dict[i] for i in fieldlist.split(",")]
    else:
        headers = ['job id', 'job name', 'queue', 'status', 'accept time', 'start time', 'finish time']
    if fieldlist:
        data = []
        for job_info in jobs:
            field_value_dict = {"id": job_info.job_id, "name": job_info.job_name, "queue": job_info.queue,
                                "status": job_info.status,
                                "acceptTime": job_info.accept_time, "startTime": job_info.start_time,
                                "finishTime": job_info.finish_time, "user": job_info.username,
                                "runtime": job_info.runtime,
                                "distributedRuntime": job_info.distributed_runtime,
                                "workflowRuntime": job_info.workflow_runtime,
                                "message": job_info.message, "labels": job_info.labels,
                                "annotations": job_info.annotations,
                                "priority": job_info.priority, "flavour": job_info.flavour, "fs": job_info.fs,
                                "extraFS": job_info.extra_fs_list, "image": job_info.image,
                                "env": job_info.env,
                                "command": job_info.command, "args": job_info.args_list, "port": job_info.port,
                                "extensionTemplate": job_info.extension_template, "framework": job_info.framework,
                                "members": job_info.member_list}
            data.append([field_value_dict[i]] for i in fieldlist.split(","))
    else:
        data = [[job.job_id, job.job_name, job.queue, job.status, job.accept_time, job.start_time, job.finish_time] for job in jobs]
    print_output(data, headers, out_format, table_format='grid')


@job.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('jobtype')
@click.argument('jsonpath')
@click.pass_context
def create(ctx, jobtype, jsonpath):
    """ create job.\n
    JOBTYPE: single, distributed or workflow.
    JSONPATH: relative path of json file under storage volume.
    """
    client = ctx.obj['client']
    if not jobtype or not jsonpath:
        click.echo('job create must provide jobtype and jsonpath.', err=True)
        sys.exit(1)
    with open(jsonpath, 'r', encoding='utf8') as read_content:
        job_request_dict = json.load(read_content)
    job_request = JobRequest(job_request_dict.get('schedulingPolicy', {}).get('queue', None), job_request_dict.get('image', None),
                             job_request_dict.get('id', None), job_request_dict.get('name', None),
                             job_request_dict.get('labels', None), job_request_dict.get('annotations', None),
                             job_request_dict.get('schedulingPolicy', {}).get('priority', None), job_request_dict.get('flavour', None),
                             job_request_dict.get('fs', None), job_request_dict.get('extraFS', None),
                             job_request_dict.get('env', None), job_request_dict.get('command', None),
                             job_request_dict.get('args', None), job_request_dict.get('port', None),
                             job_request_dict.get('extensionTemplate', None), job_request_dict.get('framework', None),
                             job_request_dict.get('members', None))
    valid, response = client.create_job(jobtype, job_request)
    if valid:
        click.echo("job create success, id[%s]" % response)
    else:
        click.echo("job create failed with message[%s]" % response)
        sys.exit(1)


@job.command()
@click.argument('jobid')
@click.option('-p', '--priority', help="Update the priority of job, such as: low, normal, high, e.g. --priority high")
@click.option('-l', '--labels', help="Update the labels of job, e.g. --labels label1=value1,label2=value2")
@click.option('-a', '--annotations', help="Update the annotations of job, e.g. --annotations anno1=value1,anno2=value2")
@click.pass_context
def update(ctx, jobid, priority, labels, annotations):
    """update job, including priority, labels, or annotations.\n
    JOBID: the id of the specificed job.
    """
    client = ctx.obj['client']
    if not jobid:
        click.echo('job update must provide jobid.', err=True)
        sys.exit(1)
    labelDict = None
    if labels:
        args = labels.split(',')
        labelDict = dict([item.split("=") for item in args])
    annotationDict = None
    if annotations:
        args = annotations.split(',')
        annotationDict = dict([item.split("=") for item in args])
    valid, response = client.update_job(jobid, priority, labelDict, annotationDict)
    if valid:
        click.echo("jobid[%s] update success" % jobid)
    else:
        click.echo("update job %s failed with message[%s]" % jobid, response)
        sys.exit(1)


@job.command()
@click.argument('jobid')
@click.pass_context
def stop(ctx, jobid):
    """stop the job.\n
    JOBID: the id of the specificed job.
    """
    client = ctx.obj['client']
    if not jobid:
        click.echo('job stop must provide jobid.', err=True)
        sys.exit(1)
    valid, response = client.stop_job(jobid)
    if valid:
        click.echo("jobid[%s] stop success" % jobid)
    else:
        click.echo("job stop failed with message[%s]" % response)
        sys.exit(1)


@job.command()
@click.argument('jobid')
@click.pass_context
def delete(ctx, jobid):
    """ delete job.\n
    JOBID: the name of job
    """
    client = ctx.obj['client']
    if not jobid:
        click.echo('job delete  must provide jobid.', err=True)
        sys.exit(1)
    valid, response = client.delete_job(jobid)
    if valid:
        click.echo("job[%s] delete success " % jobid)
    else:
        click.echo("job delete failed with message[%s]" % response)
        sys.exit(1)
