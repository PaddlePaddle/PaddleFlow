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


@click.group()
def flavour():
    """manage flavour resources"""
    pass


@flavour.command()
@click.option('-m', '--maxkeys', help="Max size of the listed cluster.", default=100)
@click.option('--marker', help="Next page.")
@click.pass_context
def list(ctx, maxkeys, marker=None, clustername="", key=""):
    """ list flavour."""
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.list_flavour(maxkeys, marker, clustername, key)
    if valid:
        if len(response)>0:
            _print_flavour_list(response, output_format)
        else:
            click.echo("not find flavour")
        sys.exit(1)
    else:
        click.echo("flavour list failed with message[%s]" % response)
        sys.exit(1)


@flavour.command()
@click.option('-n', "--flavourname", required=True)
@click.pass_context
def show(ctx, flavourname):
    """ show flavour info.\n
    flavourname: the name of flavour
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']

    valid, response = client.show_flavour(flavourname)
    if valid:
        _print_flavour_info(response, output_format)
    else:
        click.echo("get flavour failed with message[%s]" % response)
        sys.exit(1)


@flavour.command()
@click.option('-n',"--flavourname", required=True)
@click.option('-c', '--cpu', help="CPU, e.g. --cpu 4")
@click.option('-m', '--memory', help="Memory, e.g. --memroy 10G")
@click.option('-s','--scalar', help='The scalar resource of flavour, e.g. --scalar a=b,c=d')
@click.option('-cn', '--clusterName', help="Cluster name that flavour belongs to.")
@click.pass_context
def update(ctx, flavourname, cpu=None, memory=None, scalar=None, clustername=None):
    """ update info from flavourname.\n
    flavourname: flavour name.
    """
    client = ctx.obj['client']
    # scalar_resources
    scalar_resources = {}
    if scalar:
        args = scalar.split(',')
        scalar_resources['scalarResources'] = dict([item.split('=') for item in args])
    # call update_flavour
    valid, response = client.update_flavour(flavourname, cpu, memory, scalar_resources, clustername)
    if valid:
        click.echo("cluster[%s] update success, id[%s]" %
                   (clustername, response))
    else:
        click.echo("cluster update failed with message[%s]" % response)
        sys.exit(1)


@flavour.command()
@click.option('-n', "--flavourname", required=True)
@click.option('-c', '--cpu', help="CPU, e.g. --cpu 4", required=True)
@click.option('-m', '--memory', help="Memory, e.g. --memroy 10G", required=True)
@click.option('-s', '--scalar', help='The scalar resource of flavour, e.g. --scalar a=b,c=d')
@click.option('-cn', '--clustername', help='Cluster name that flavour belongs to.')
@click.pass_context
def create(ctx, flavourname, cpu, memory, scalar=None, clustername=None):
    """ create flavour.\n
    NAME: the name of flavour.\n
    CPU: the CPU of flavour.\n
    MEM: the Memory of flavour.\n
    Scalar: the scalar resource of flavour.\n
    CLUSTERNAME: the cluster name.
   """
    client = ctx.obj['client']

    scalar_resources = None
    if scalar:
        args = scalar.split(',')
        scalar_resources = dict([item.split('=') for item in args])
        click.echo("scalar_resources={}".format(scalar_resources))

    valid, response = client.add_flavour(name=flavourname, cpu=cpu, memory=memory, scalar_resources=scalar_resources, cluster_name=clustername)
    if valid:
        click.echo("flavour[%s] create success " % flavourname)
    else:
        click.echo("flavour create failed with message[%s]" % response)
        sys.exit(1)


@flavour.command()
@click.option('-n',"--flavourname", required=True)
@click.pass_context
def delete(ctx, flavourname):
    """ delete flavour.\n
    flavour_name: the name of flavour
    """
    client = ctx.obj['client']
    valid, response = client.del_flavour(flavourname)
    if valid:
        click.echo("flavour[%s] delete success " % flavourname)
    else:
        click.echo("flavour delete failed with message[%s]" % response)
        sys.exit(1)


def _print_flavour_list(res, out_format):
    """print flavour list"""
    headers = ['name', 'cpu', 'mem', 'scalarResources', 'clusterName']

    data = [[
            flavour_info.name,
            flavour_info.cpu,
            flavour_info.mem,
            flavour_info.scalar_resources,
     ] for flavour_info in res]

    print_output(data, headers, out_format, table_format='grid')


def _print_flavour_info(flavour_info, out_format):
    """print flavour list"""
    headers = ['name', 'cpu', 'mem', 'scalarResources', 'clusterName']
    data = [[
            flavour_info.name,
            flavour_info.cpu,
            flavour_info.mem,
            flavour_info.scalar_resources,
     ]]

    print_output(data, headers, out_format, table_format='grid')

