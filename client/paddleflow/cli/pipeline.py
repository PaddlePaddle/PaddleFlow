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
@click.option('-d', '--desc', help="description of the pipeline.")
@click.option('-u', '--username', help="Only the root user can specify other users.")
@click.pass_context
def create(ctx, fsname, yamlpath, desc=None, username=None):
    """ create pipeline.\n
    FSNAME: specified name. 
    YAMLPATH: relative path of yaml file under storage volume.
    """
    client = ctx.obj['client']
    if not fsname or not yamlpath:
        click.echo('pipelinecreate  must provide fsname or yamlpath .', err=True)
        sys.exit(1)
    valid, response, id, versionID = client.create_pipeline(fsname, yamlpath, desc, username)
    if valid:
        click.echo("pipeline[%s] create  success, id[%s], versionID[%s]" % (response, id, versionID))
    else:
        click.echo("pipeline create failed with message[%s]" % response)
        sys.exit(1)


@pipeline.command()
@click.option('-u', '--userfilter', help="List the pipeline by user.")
@click.option('-n', '--namefilter', help="List the pipeline by name.")
@click.option('-m', '--maxkeys', help="Max size of the listed pipeline.")
@click.option('-mk', '--marker', help="Next page ")
@click.pass_context
def list(ctx, userfilter=None, namefilter=None, maxkeys=None, marker=None):
    """list pipeline. \n"""
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.list_pipeline(userfilter, namefilter, maxkeys, marker)
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
@click.argument('-f', '--fsfilter', help='list ppl version by fs')
@click.argument('-m', '--maxkeys', help='Max size of the listed ppl version')
@click.argument('-mk', '--marker', help='Next page')
@click.pass_context
def show(ctx, pipelineid, fsfilter=None, maxkeys=None, marker=None):
    """ show pipeline info.\n
    PIPELINEID: the id of pipeline.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not pipelineid:
        click.echo('pipeline show  must pipeline id.', err=True)
        sys.exit(1)
    valid, response, pplVerList, nextmarker = client.show_pipeline(pipelineid, fsfilter, maxkeys, marker)
    if valid:
        _print_pipeline_info(response, pplVerList, nextmarker, output_format)
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
        'pipeline id', 'name', 'username', 'desc',
        'create time', 'update time'
    ]
    data = [[pipeline.pipeline_id, pipeline.name, pipeline.username, pipeline.desc,
             pipeline.create_time, pipeline.update_time] for pipeline in pipelines]
    print_output(data, headers, out_format, table_format='grid')


def _print_pipeline_info(pipeline, pplVerList, nextmarker, out_format):
    """print pipeline info"""
    headers = [
        'pipeline id', 'name', 'username',
        'pipeline desc', 'create time', 'update time'
    ]
    data = [[
        pipeline.pipeline_id, pipeline.name,
        pipeline.username, pipeline.desc, pipeline.create_time,
        pipeline.update_time
    ]]
    print_output(data, headers, out_format, table_format='grid')

    dataPplInfo = []
    dataPplYaml = []
    for pplVer in pplVerList:
        dataPplInfo.append([pplVer.pipeline_version_id, pplVer.fs_name, pplVer.yaml_path, pplVer.username,
                            pplVer.create_time, pplVer.update_time])
        dataPplYaml.append([pplVer.pipeline_version_id, pplVer.pipeline_yaml])

    headers = ['ppl ver id', 'fs name', 'yaml path', 'username',
               'create time', 'update time']
    print_output(dataPplInfo, headers, out_format, table_format='grid')

    headers = ['ppl ver id', 'pipeline yaml']
    print_output(dataPplYaml, headers, out_format, table_format='grid')

    if nextmarker:
        click.echo('marker: {}'.format(nextmarker))



