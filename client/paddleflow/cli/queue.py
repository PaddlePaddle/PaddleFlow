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


@queue.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('name')
@click.argument('namespace')
@click.argument('cpu')
@click.argument('mem')
@click.argument('clustername')
@click.pass_context
def create(ctx, name, namespace, cpu, mem, clustername):
    """ create queue.\n
    NAME: the name of queue.
    NAMESPACE: The namespace to which it belongs.
    CPU: CPU occupied by queue.
    MEN: Men occupied by queue.
    CLUSTERNAME: cluster name.
    """
    client = ctx.obj['client']
    if not name:
        click.echo('queue create  must provide name.', err=True)
        sys.exit(1)
    if not namespace:
        click.echo('queue create  must provide namespace.', err=True)
        sys.exit(1)
    if not cpu:
        click.echo('queue create  must provide cpu.', err=True)
        sys.exit(1)
    if not mem:
        click.echo('queue create  must provide mem.', err=True)
        sys.exit(1)
    if not clustername:
        click.echo('queue create  must provide clustername.', err=True)
        sys.exit(1)
    valid, response = client.add_queue(name, namespace, cpu, mem, clustername)
    if valid:
        click.echo("queue[%s] create success " % name)
    else:
        click.echo("queue create failed with message[%s]" % response)
        sys.exit(1)


@queue.command()
@click.argument('queuename')
@click.pass_context
def delete(ctx, queuename):
    """ delete queue.\n
    QUEUENAME: the name of queue
    """
    client = ctx.obj['client']
    if not queuename:
        click.echo('queue delete  must provide queuename.', err=True)
        sys.exit(1)
    valid, response = client.del_queue(queuename)
    if valid:
        click.echo("queue[%s] delete success " % queuename)
    else:
        click.echo("queue delete failed with message[%s]" % response)
        sys.exit(1)


@queue.command()
@click.argument('queuename')
@click.pass_context
def stop(ctx, queuename, action=None):
    """ stop queue .\n
    QUEUENAME: the name of queue
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not queuename:
        click.echo('queue show  must provide queuename.', err=True)
        sys.exit(1)
    valid, response = client.stop_queue(queuename)
    if valid:
        click.echo("queue[%s] stop success " % queuename)
    else:
        click.echo("queue stop failed with message[%s]" % response)
        sys.exit(1)


@queue.command()
@click.argument('username')
@click.argument('queuename')
@click.pass_context
def grant(ctx, username, queuename):
    """ add grant. \n
    USERNAME:  the user's name\n
    QUEUENAME: the queue's name
    """
    client = ctx.obj['client']
    if not username or not queuename:
        click.echo('queue add  must provide username and queuename.', err=True)
        sys.exit(1)
    valid, response = client.grant_queue(username, queuename)
    if valid:
        click.echo("queue[%s] add username[%s] success" % (queuename, username))
    else:
        click.echo("queue add failed with message[%s]" % response)
        sys.exit(1)


@queue.command()
@click.option('-m', '--maxsize', default=100, help="Max size of the listed queues.")
@click.option('-u', '--username', help="List the specified fs by username, only useful for root.")
@click.pass_context
def grantlist(ctx, username=None, maxsize=100):
    """list grant """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.show_queue_grant(username, maxsize)
    if valid:
        if len(response):
            _print_grants(response, output_format)
        else:
            msg = "no grantlist found "
            click.echo(msg)
    else:
        click.echo("grant list failed with message[%s]" % response)
        sys.exit(1)


@queue.command()
@click.argument('username')
@click.argument('queuename')
@click.pass_context
def ungrant(ctx, username, queuename):
    """ delete grant.\n
    USERNAME:  the user's name\n
    QUEUENAME: the queue's name
    """
    client = ctx.obj['client']
    if not username or not queuename:
        click.echo('queue delete must provide username and queuename.', err=True)
        sys.exit(1)
    valid, response = client.ungrant_queue(username, queuename)
    if valid:
        click.echo("queue[%s] delete username[%s] success" % (queuename, username))
    else:
        click.echo("queue delete failed with message[%s]" % response)
        sys.exit(1)


def _print_queues(queues, out_format):
    """print queues """
    headers = ['name', 'status', 'create time', 'update time']
    data = [[queue.name, queue.status, queue.createTime, queue.updateTime] for queue in queues]
    print_output(data, headers, out_format, table_format='grid')


def _print_queue_info(queue, out_format):
    """print queue info"""
    headers = ['name', 'status', 'namespace', 'mem', 'cpu', 'cluster name', 'create time', 'update time']
    data = [[queue.name, queue.status, queue.namespace, queue.mem, queue.cpu, queue.clustername or None,
            queue.createTime, queue.updateTime]]
    print_output(data, headers, out_format, table_format='grid')


def _print_grants(grants, out_format):
    """print grant info"""
    headers = ['user name', 'queue name']
    data = [[grant.username, grant.resourceName] for grant in grants]
    print_output(data, headers, out_format, table_format='grid')


def _print_flavour_info(res, out_format):
    """print flavour list"""
    headers = ['name', 'cpu', 'mem', 'scalarResources']
    data = []
    for i in res:
        data.append([i.get('name', ''), i.get('cpu', ''), i.get('mem', ''), i.get('scalarResources', None),])
    print_output(data, headers, out_format, table_format='grid')