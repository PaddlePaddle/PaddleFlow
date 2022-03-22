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
def pipeline():
    """manage pipeline resources"""
    pass


@pipeline.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('fsname')
@click.argument('yamlpath')
@click.option('-n', '--name', help="Custom pipeline name.")
@click.option('-u', '--username', help="Only the root user can specify other users.")
@click.pass_context
def create(ctx, fsname, yamlpath, name=None, username=None):
    """ create pipeline.\n
    FSNAME: specified name. 
    YAMLPATH: relative path of yaml file under storage volume.
    """
    client = ctx.obj['client']
    if not fsname or not yamlpath:
        click.echo('pipelinecreate  must provide fsname or yamlpath .', err=True)
        sys.exit(1)
    valid, response, id = client.create_pipeline(fsname, yamlpath, name, username)
    if valid:
        click.echo("pipeline[%s] create  success, id[%s]" % (response, id))
    else:
        click.echo("pipeline create failed with message[%s]" % response)
        sys.exit(1)


@pipeline.command()
@click.option('-u', '--userfilter', help="List the pipeline by user.")
@click.option('-f', '--fsfilter', help="List the pipeline by fs.")
@click.option('-n', '--namefilter', help="List the pipeline by name.")
@click.option('-m', '--maxkeys', help="Max size of the listed pipeline.")
@click.option('-mk', '--marker', help="Next page ")
@click.pass_context
def list(ctx, userfilter=None, fsfilter=None, namefilter=None, maxkeys=None, marker=None):
    """list pipeline. \n"""
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.list_pipeline(userfilter, fsfilter, namefilter, maxkeys, marker)
    if valid:
        if len(response):
            _print_pipeline(response, output_format)
            click.echo('marker: {}'.format(nextmarker))
        else:
            msg = "no pipeline found "
            click.echo(msg)
    else:
        click.echo("pipeline list failed with message[%s]" % response)
        sys.exit(1)


@pipeline.command()
@click.argument('pipelineid')
@click.pass_context
def show(ctx, pipelineid):
    """ show pipeline info.\n
    PIPELINEID: the id of pipeline.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not pipelineid:
        click.echo('pipeline show  must pipeline id.', err=True)
        sys.exit(1)
    valid, response = client.show_pipeline(pipelineid)
    if valid:
        _print_pipeline_info(response, output_format)
    else:
        click.echo("pipeline show failed with message[%s]" % response)
        sys.exit(1)


@pipeline.command()
@click.argument('pipelineid')
@click.pass_context
def delete(ctx, pipelineid):
    """ delete pipeline. \n
    PIPELINEID: the id of pipeline.
    """
    client = ctx.obj['client']
    if not pipelineid:
        click.echo('delete must provide pipelineid.', err=True)
        sys.exit(1)
    valid, response = client.delete_pipeline(pipelineid)
    if valid:
        click.echo('pipelineid[%s] delete success' % pipelineid)
    else:
        click.echo("pipeline delete failed with message[%s]" % response)
        sys.exit(1)


def _print_pipeline(pipelines, out_format):
    """print pipelines """
    headers = [
        'pipeline id', 'name', 'fsname', 'username', 
        'pipeline md5', 'create time', 'update time'
    ]
    data = [[pipeline.pipelineid, pipeline.name, pipeline.fsname, pipeline.username, pipeline.pipelinemd5, 
        pipeline.createtime, pipeline.updatetime] for pipeline in pipelines]
    print_output(data, headers, out_format, table_format='grid')


def _print_pipeline_info(pipeline, out_format):
    """print pipeline info"""
    headers = [
        'pipeline id', 'name', 'fsname', 'username', 
        'pipeline md5', 'create time', 'update time'
    ]
    data = [[
        pipeline.pipelineid, pipeline.name, pipeline.fsname,
        pipeline.username, pipeline.pipelinemd5, pipeline.createtime,
        pipeline.updatetime
    ]]
    print_output(data, headers, out_format, table_format='grid')
    print_output([[pipeline.pipelineyaml]], ['pipeline yaml'],
                 out_format,
                 table_format='grid')


