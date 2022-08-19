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
@click.argument('fs_name')
@click.option('-yp', '--yamlpath', 'yaml_path', help="relative path of yaml file under storage volume.")
@click.option('-d', '--desc', help="description of the pipeline.")
@click.option('-u', '--username', help="Only the root user can specify other users.")
@click.pass_context
def create(ctx, fs_name, yaml_path=None, desc=None, username=None):
    """ create pipeline.\n
    FS_NAME: specified name.
    """
    client = ctx.obj['client']
    if not fs_name:
        click.echo('pipeline create  must provide fs name.', err=True)
        sys.exit(1)
    valid, response = client.create_pipeline(fs_name, yaml_path, desc, username)
    if valid:
        name, ppl_id, ppl_ver_id = response['name'], response['pplID'], response['pplVerID']
        click.echo("pipeline[%s] create  success, id[%s], versionID[%s]" % (name, ppl_id, ppl_ver_id))
    else:
        click.echo("pipeline create failed with message[%s]" % response)
        sys.exit(1)


@pipeline.command()
@click.option('-u', '--userfilter', 'user_filter', help="List the pipeline by user.")
@click.option('-n', '--namefilter', 'name_filter', help="List the pipeline by name.")
@click.option('-m', '--maxkeys', 'max_keys', help="Max size of the listed pipeline.")
@click.option('-mk', '--marker', help="Next page ")
@click.pass_context
def list(ctx, user_filter=None, name_filter=None, max_keys=None, marker=None):
    """list pipeline. \n"""
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.list_pipeline(user_filter, name_filter, max_keys, marker)
    if valid:
        pipeline_list, next_marker = response['pipelineList'], response['nextMarker']
        if len(pipeline_list):
            _print_pipeline(pipeline_list, output_format)
            click.echo('marker: {}'.format(next_marker))
        else:
            msg = "no pipeline found "
            click.echo(msg)
    else:
        click.echo("pipeline list failed with message[%s]" % response)
        sys.exit(1)


@pipeline.command()
@click.argument('pipeline_id')
@click.option('-f', '--fsfilter', 'fs_filter', help='list ppl version by fs')
@click.option('-m', '--maxkeys', 'max_keys', help='Max size of the listed ppl version')
@click.option('-mk', '--marker', help='Next page')
@click.pass_context
def show(ctx, pipeline_id, fs_filter=None, max_keys=None, marker=None):
    """ show pipeline info.\n
    PIPELINE_ID: the id of pipeline.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not pipeline_id:
        click.echo('pipeline show  must pipeline id.', err=True)
        sys.exit(1)
    valid, response = client.show_pipeline(pipeline_id, fs_filter, max_keys, marker)
    if valid:
        ppl_info, ppl_ver_list = response['pipelineInfo'], response['pipelineVersionList']
        next_marker = response['nextMarker']
        _print_pipeline_info(ppl_info, ppl_ver_list, next_marker, output_format, False)
    else:
        click.echo("pipeline show failed with message[%s]" % response)
        sys.exit(1)


@pipeline.command()
@click.argument('pipeline_id')
@click.pass_context
def delete(ctx, pipeline_id):
    """ delete pipeline. \n
    PIPELINE_ID: the id of pipeline.
    """
    client = ctx.obj['client']
    if not pipeline_id:
        click.echo('delete must provide pipeline id.', err=True)
        sys.exit(1)
    valid, response = client.delete_pipeline(pipeline_id)
    if valid:
        click.echo('pipeline id [%s] delete success' % pipeline_id)
    else:
        click.echo("pipeline delete failed with message[%s]" % response)
        sys.exit(1)

@pipeline.command()
@click.argument('pipeline_id')
@click.argument('fs_name')
@click.argument('yaml_path')
@click.option('-u', '--username', help='choose a user to use its fs if you are root')
@click.option('-d', '--desc', help='description of new pipeline version')
@click.pass_context
def update(ctx, pipeline_id, fs_name, yaml_path, username, desc):
    """ update pipeline (create pipeline version).
        PIPELINE_ID: pipeline you want to create a new version.
        FS_NAME: specified fs name.
        YAML_PATH: relative path of yaml file under storage volume."""
    client = ctx.obj['client']
    if not pipeline_id or not fs_name or not yaml_path:
        click.echo('pipeline update must provide pipeline_id, fs_name and yaml_path .', err=True)
        sys.exit(1)
    valid, response = client.update_pipeline(pipeline_id, fs_name, yaml_path, username, desc)
    if valid:
        ppl_id, ppl_version_id = response['pipelineID'], response['pipelineVersionID']
        click.echo("pipeline[%s] update success, new version id [%s]" % (ppl_id, ppl_version_id))
    else:
        click.echo("pipeline update failed with message[%s]" % response)
        sys.exit(1)

@pipeline.command(name='showversion')
@click.argument('pipeline_id')
@click.argument('pipeline_version_id')
@click.pass_context
def show_version(ctx, pipeline_id, pipeline_version_id):
    """ show pipeline version info.\n
    PIPELINE_ID: the id of pipeline.\n
    PIPELINE_VERSION_ID: the id of pipeline version.\n
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not pipeline_id or not pipeline_version_id:
        click.echo('pipeline show must set pipeline id, pipeline version id.', err=True)
        sys.exit(1)
    valid, response = client.show_pipeline_version(pipeline_id, pipeline_version_id)
    if valid:
        pipeline_info, ppl_ver_info = response['pipelineInfo'], response['pipelineVersionInfo']
        ppl_ver_list = [ppl_ver_info]
        _print_pipeline_info(pipeline_info, ppl_ver_list, None, output_format, True)
    else:
        click.echo("pipeline version show failed with message[%s]" % response)
        sys.exit(1)

@pipeline.command(name='deleteversion')
@click.argument('pipeline_id')
@click.argument('pipeline_version_id')
@click.pass_context
def delete_version(ctx, pipeline_id, pipeline_version_id):
    """ delete pipeline version. \n
        PIPELINE_ID: the id of pipeline.
        PIPELINE_VERSION_ID: the id of pipeline version.
    """
    client = ctx.obj['client']
    if not pipeline_id or not pipeline_version_id:
        click.echo('delete pipeline version must provide pipeline_id, pipeline_version_id.', err=True)
        sys.exit(1)
    valid, response = client.delete_pipeline_version(pipeline_id, pipeline_version_id)
    if valid:
        click.echo('pipeline version [%s] of pipeline [%s] delete success' % (pipeline_id, pipeline_version_id))
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


def _print_pipeline_info(pipeline, ppl_ver_list, next_marker, out_format, is_print_yaml):
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

    data_ppl_info = []
    data_ppl_yaml = []
    for pplVer in ppl_ver_list:
        data_ppl_info.append([pplVer.pipeline_version_id, pplVer.fs_name, pplVer.yaml_path, pplVer.username,
                            pplVer.create_time, pplVer.update_time])
        if is_print_yaml:
            data_ppl_yaml.append([pplVer.pipeline_version_id, pplVer.pipeline_yaml])

    headers = ['ppl ver id', 'fs name', 'yaml path', 'username',
               'create time', 'update time']
    print_output(data_ppl_info, headers, out_format, table_format='grid')

    if is_print_yaml:
        headers = ['ppl ver id', 'pipeline yaml']
        print_output(data_ppl_yaml, headers, out_format, table_format='grid')

    if next_marker:
        click.echo('marker: {}'.format(next_marker))



