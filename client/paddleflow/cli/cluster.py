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
def cluster():
    """manage cluster resources"""
    pass


@cluster.command()
@click.option('-m', '--maxkeys', help="Max size of the listed cluster.", default=100)
@click.option('-mk', '--marker', help="Next page.")
@click.option('-cn', '--clusternames', help="List the cluster by name")
@click.option('-cs', '--clusterstatus', help="List the cluster by status")
@click.pass_context
def list(ctx, maxkeys, marker=None, clusternames=None, clusterstatus=None):
    """list cluster. """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.list_cluster(maxkeys, marker, clusternames,
                                          clusterstatus)
    if valid:
        if len(response):
            _print_cluster(response, output_format)
            click.echo('marker: {}'.format(nextmarker))
        else:
            msg = "no cluster found "
            click.echo(msg)
    else:
        click.echo("cluster list failed with message[%s]" % response)
        sys.exit(1)


@cluster.command()
@click.argument('clustername')
@click.pass_context
def show(ctx, clustername):
    """ show cluster info.\n
    CLUSTERNAME: the name of cluster.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not clustername:
        click.echo('cluster show must provide name.', err=True)
        sys.exit(1)
    valid, response = client.show_cluster(clustername)
    if valid:
        _print_cluster_info(response, output_format)
    else:
        click.echo("cluster show failed with message[%s]" % response)
        sys.exit(1)


@cluster.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('clustername')
@click.argument('endpoint')
@click.argument('clustertype')
@click.option('-c', '--credential', help="Base64 encoded voucher information.Pass in base64 or absolute path str.")
@click.option('-d', '--description', help="Cluster description.")
@click.option('--source', help="Source, such as AWS, CCE, etc.")
@click.option('--setting', help="Additional configuration information.")
@click.option('--status', help="Cluster status.")
@click.option('-ns', '--namespacelist', help="Namespace list, such as ['NS1','NS2'].")
@click.option('--version', help="Cluster version, such as v1.16.3, etc.")
@click.pass_context
def create(ctx, clustername, endpoint, clustertype, credential=None, description=None,
        source=None, setting=None, status=None, namespacelist=None, version=None):
    """ create cluster.\n
    CLUSTERNAME:custom cluster name.
    ENDPOINT: cluster endpoint, http://0.0.0.0:8080.
    CLUSTERTYPE: cluster type , Kubernetes, Local, etc.
    """
    client = ctx.obj['client']
    if not clustername or not endpoint or not clustertype:
        click.echo(
            'cluster create  must provide clustername or endpoint or clustertype.',
            err=True)
        sys.exit(1)
    if credential:
        if not credential.startswith('/'):
            credential = credential.encode()
        else:
            with open(credential, 'rb') as f:
                credential = f.read()
    if namespacelist:
        namespacelist = namespacelist.split(',')
    valid, response = client.create_cluster(clustername, endpoint, clustertype,
                                            credential, description,
                                            source, setting, status,
                                            namespacelist, version)
    if valid:
        click.echo("cluster[%s] create  success, id[%s]" %
                   (clustername, response))
    else:
        click.echo("cluster create failed with message[%s]" % response)
        sys.exit(1)


@cluster.command()
@click.argument('clustername')
@click.pass_context
def delete(ctx, clustername):
    """ delete cluster.\n
    CLUSTERNAME: cluster name.
    """
    client = ctx.obj['client']
    if not clustername:
        click.echo('delete cluster must  clustername.', err=True)
        sys.exit(1)
    valid, response = client.delete_cluster(clustername)
    if valid:
        click.echo('cluster[%s] delete success' % response)
    else:
        click.echo("cluster delete failed with message[%s]" % response)
        sys.exit(1)


@cluster.command()
@click.argument('clustername')
@click.option('-e', '--endpoint', help="Cluster endpoint.")
@click.option('-c', '--credential', help="Credential information used to store clusters.")
@click.option('-t', '--clustertype', help="Cluster type.")
@click.option('-d', '--description', help="Cluster description.")
@click.option('--source', help="Source, such as AWS, CCE, etc.")
@click.option('--setting', help="Additional configuration information.")
@click.option('--status', help="Cluster status.")
@click.option('-ns', '--namespacelist', help="Namespace list, such as ['NS1','NS2'].")
@click.option('--version', help="Cluster version, such as v1.16.3, etc.")
@click.pass_context
def update(ctx, clustername, endpoint=None, credential=None, clustertype=None,
    description=None, source=None, setting=None, status=None, namespacelist=None, version=None):
    """ update info from clustername.\n
    CLUSTERNAME: cluster name.
    """
    client = ctx.obj['client']
    if not clustername:
        click.echo('update must provide clustername.', err=True)
        sys.exit(1)
    if credential:
        if not credential.startswith('/'):
            credential = credential.encode()
        else:
            with open(credential, 'rb') as f:
                credential = f.read()
    if namespacelist:
        namespacelist = namespacelist.split(',')
    valid, response = client.update_cluster(clustername, endpoint, credential,
                                            clustertype, description, source, setting,
                                            status, namespacelist, version)
    if valid:
        click.echo("cluster[%s] update success, id[%s]" %
                   (clustername, response))
    else:
        click.echo("cluster update failed with message[%s]" % response)
        sys.exit(1)


@cluster.command()
@click.option('-cn', '--clustername', help="luster name. example:clusterName1,clusterName2")
@click.pass_context
def resource(ctx, clustername=None):
    """ Get the remaining resource information of the cluster.\n
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.list_cluster_resource(clustername)
    if valid:
        _print_cluster_resource(response, output_format)
    else:
        click.echo("get cluster resource failed with message[%s]" % response)
        sys.exit(1)


def _print_cluster(clusters, out_format):
    """print queues """
    headers = [
        'cluster id', 'cluster name', 'description', 'cluster type', 'version', 'status',
        'create time', 'update time'
    ]
    data = [[cluster.clusterid, cluster.clustername, cluster.description, cluster.clustertype, cluster.version,
    cluster.status, cluster.createtime, cluster.updatetime] for cluster in clusters]
    print_output(data, headers, out_format, table_format='grid')


def _print_cluster_info(cluster, out_format):
    """print queue info"""
    headers = [
        'cluster id', 'cluster name', 'description', 'endpoint', 'source',
        'cluster type', 'version', 'status', 'setting', 'namespace list',
        'create time', 'update time'
    ]
    data = [[
        cluster.clusterid, cluster.clustername, cluster.description,
        cluster.endpoint, cluster.source, cluster.clustertype, cluster.version, cluster.status,
        cluster.setting, cluster.namespacelist, cluster.createtime,
        cluster.updatetime
    ]]
    print_output(data, headers, out_format, table_format='grid')
    # print_output([[cluster.credential]], ['credential'],
    #              out_format,
    #              table_format='yaml')
    click.echo('credential value:\n------------------------\n{}'.format(cluster.credential))



def _print_cluster_resource(cluster, out_format):
    """print cluster resource info"""
    headers = ['cluster name', 'cluster info']
    data = []
    for k, v in cluster.items():
        data.append([k, json.dumps(v, indent=4)])
    print_output(data, headers, out_format, table_format='grid')
