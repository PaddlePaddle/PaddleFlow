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
from paddleflow.utils.format_help import  command_required_option_from_option

@click.group()
def queue():
    """manage queue resources"""
    pass


@queue.command()
@click.option('-m', '--maxsize', default=100, help="Max size of the listed queues.")
@click.option('-mk', '--marker', help="Next page ")
@click.pass_context
def list(ctx, maxsize=100, marker=None):
    """list queue. """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.list_queue(maxsize, marker)
    if valid:
        if len(response):
            _print_queues(response, output_format)
            click.echo('marker: {}'.format(nextmarker))
        else:
            msg = "no queue found "
            click.echo(msg)
    else:
        click.echo("queue list failed with message[%s]" % response)
        sys.exit(1)


@queue.command()
@click.argument('queuename')
@click.pass_context
def show(ctx, queuename):
    """ show queue info.\n
    QUEUENAME: the name of queue
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not queuename:
        click.echo('queue show  must provide queuename.', err=True)
        sys.exit(1)
    valid, response = client.show_queue(queuename)
    if valid:
        _print_queue_info(response, output_format)
    else:
        click.echo("queue show failed with message[%s]" % response)
        sys.exit(1)


def _print_queues(queues, out_format):
    """print queues """
    headers = ['name', 'namespace', 'status', 'cluster name', 'create time', 'update time']
    data = [[queue.name, queue.namespace, queue.status, queue.clusterName,
             queue.createTime, queue.updateTime] for queue in queues]
    print_output(data, headers, out_format, table_format='grid')


def _print_queue_info(queue, out_format):
    """print queue info"""
    headers = ['name', 'namespace', 'status', 'quota type', 'cluster name', 'create time', 'update time']
    data = [[queue.name, queue.namespace, queue.status, queue.quotaType, queue.clusterName, queue.createTime, queue.updateTime]]
    print_output(data, headers, out_format, table_format='grid')
    print("queue info: ")
    headers = ['max resources']
    data = [[queue.maxResources]]
    if queue.minResources:
        headers.append('min resources')
        data[0].append(queue.minResources)
    if queue.usedResources:
        headers.append('used resources')
        data[0].append(queue.usedResources)
    if queue.idleResources:
        headers.append('idle resources')
        data[0].append(queue.idleResources)
    if queue.schedulingPolicy:
        headers.append('scheduling policy')
        data[0].append(queue.schedulingPolicy)
    if queue.location:
        headers.append('location')
        data[0].append(queue.location)
    print_output(data, headers, "json", table_format='grid')