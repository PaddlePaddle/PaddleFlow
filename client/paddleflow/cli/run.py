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
import base64
from ..run.run_info import RunInfo, DagInfo, JobInfo
from ..common.const import RUN_FINAL_STATUS, RUN_ACTIVE_STATUS

from paddleflow.cli.output import print_output, OutputFormat

@click.group()
def run():
    """manage run resources"""
    pass


@run.command()
@click.option('-f', '--fsname', 'fs_name', help='The name of fs')
@click.option('-n', '--name', help='The name of run.')
@click.option('-d', '--desc', help='The description of run.')
@click.option('-u', '--username', help='Run the specified run by username, only useful for root.')
@click.option('-p', '--param', multiple=True, help="Run pipeline params, example: -p regularization=xxx .")
@click.option('-yp', '--runyamlpath', 'run_yaml_path', help='Run yaml file path, example ./run.yaml .')
@click.option('-yr', '--runyamlraw', 'run_yaml_raw', help='Run yaml file raw, local absolute path .')
@click.option('-pplid', '--pipelineid', 'pipeline_id', help='Pipeline ID, example ppl-000666')
@click.option('-pplver', '--pipelineversionid', 'pipeline_version_id', help='Pipeline Version ID, example 1')
@click.option('--disabled', multiple=True, help="the name of step which need to be disabled.")
@click.option('-de', '--dockerenv', 'docker_env', help='a global dockerEnv used by all steps which have no dockerEnv')
@click.pass_context
def create(ctx, fs_name=None, name=None, desc=None, username=None, run_yaml_path=None, run_yaml_raw=None,
           param="", pipeline_id=None, pipeline_version_id=None, disabled=None, docker_env=None):
    """create a new run.\n
    """
    client = ctx.obj['client']
    param_dict = {}
    for k in param:
        split_txt = k.split("=", 1)
        param_dict[split_txt[0]] = split_txt[1]
    if run_yaml_raw:
        with open(run_yaml_raw, 'rb') as f:
            run_yaml_raw = f.read()
    
    if disabled is not None:
        disabled = ",".join(disabled)

    valid, response = client.create_run(fs_name, username, name, desc, run_yaml_path, run_yaml_raw,
                                        pipeline_id, pipeline_version_id, param_dict, disabled=disabled, docker_env=docker_env)

    if valid:
        click.echo("run[%s] create success with run id[%s]" % (fs_name, response))
    else:
        click.echo("run create failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.option('-f', '--fsname', 'fs_name', help='List the specified run by fsname.')
@click.option('-u', '--username', help='List the specified run by username, only useful for root.')
@click.option('-r', '--runid', 'run_id', help='List the specified run by runid')
@click.option('-n', '--name', help='List the specified run by run name')
@click.option('-s', '--status', help='List the specified run by run status')
@click.option('-m', '--maxsize', 'max_size', default=100, help="Max size of the listed users.")
@click.option('-mk', '--marker', help="Next page.")
@click.pass_context
def list(ctx, fs_name=None, username=None, run_id=None, name=None, status=None, max_size=100, marker=None):
    """list run.\n """
    client = ctx.obj['client']
    output_format = ctx.obj['output']

    # 处理statusFilters
    status_processed = ''
    if status:
        status_filters = status.split(sep=',')
        status_list = []
        for status_filter in status_filters:
            if status_filter == 'active':
                status_list.extend(RUN_ACTIVE_STATUS)
            elif status_filter == 'final':
                status_list.extend(RUN_FINAL_STATUS)
            else:
                status_list.append(status_filter)
        status_processed = ','.join(status_list)

    valid, response = client.list_run(fs_name, username, run_id, name, status_processed, max_size, marker)
    if valid:
        run_list, next_marker = response['runList'], response['nextMarker']
        if len(run_list):
            click.echo("{} runs shown under:".format(len(run_list)))
            _print_run_list(run_list, output_format)
            click.echo('marker: {}'.format(next_marker))
        else:
            msg = "no run found "
            click.echo(msg)
    else:
        click.echo("run list failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('run_id')
@click.pass_context
def show(ctx, run_id):
    """ show detail info of run. \n
    RUN_ID: the id of the specified run.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not run_id:
        click.echo('run show must provide run id.', err=True)
        sys.exit(1)
    valid, response = client.show_run(run_id)
    if valid:
        _print_run(response, output_format)
    else:
        click.echo("run show failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('run_id')
@click.option('-f', '--force', is_flag=True,
    help="Whether to forcibly stop the task. Forcibly stop will also stop step in post_process")
@click.pass_context
def stop(ctx, run_id, force):
    """stop the run.\n
    RUN_ID: the id of the specified run.
    """
    client = ctx.obj['client']
    if not run_id:
        click.echo('run stop must provide runid.', err=True)
        sys.exit(1)
    valid, response = client.stop_run(run_id, force=force)
    if valid:
        click.echo("runid[%s] stop success" % run_id)
    else:
        click.echo("run stop failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('run_id')
@click.pass_context
def retry(ctx, run_id):
    """retry the run.\n
    RUN_ID: the id of the specificed run.
    """
    client = ctx.obj['client']
    if not run_id:
        click.echo('run retry must provide run id.', err=True)
        sys.exit(1)
    valid, response = client.retry_run(run_id)
    if valid:
        click.echo("run id[%s] retry success, new run id is [%s]" % (run_id, response))
    else:
        click.echo("run retry failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('run_id')
@click.option('-not-cc', '--notcheckcache', 'not_check_cache', is_flag=True, show_default=True,
                help='set force to True if you want to delete a cached run')
@click.pass_context
def delete(ctx, run_id, not_check_cache):
    """ delete run .\n
    RUN_ID: the id of the specified run.
    """
    client = ctx.obj['client']
    if not run_id:
        click.echo('delete run provide run id.', err=True)
        sys.exit(1)
    checkcache = not not_check_cache
    valid, response = client.delete_run(run_id, checkcache)
    if valid:
        click.echo('run id [%s] delete success' % run_id)
    else:
        click.echo("run delete failed with message[%s]" % response)
        sys.exit(1)


@run.command(name='listcache')
@click.option('-u', '--userfilter', 'user_filter', help="List the artifactEventList by user.")
@click.option('-f', '--fsfilter', 'fs_filter', help="List the artifactEventList by fs.")
@click.option('-r', '--runfilter', 'run_filter', help="List the artifactEventList by run.")
@click.option('-m', '--maxkeys', 'max_keys', help="Max size of the listed artifactEventList.")
@click.option('-mk', '--marker', help="Next page.")
@click.pass_context
def list_cache(ctx, user_filter=None, fs_filter=None, run_filter=None, max_keys=None, marker=None):
    """list cache .\n """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.list_cache(user_filter, fs_filter, run_filter, max_keys, marker)
    if valid:
        run_cache_list, next_marker = response['runCacheList'], response['nextMarker']
        if len(run_cache_list):
            _print_run_cache(run_cache_list, output_format)
            click.echo('marker: {}'.format(next_marker))
        else:
            msg = "no run found "
            click.echo(msg)
    else:
        click.echo("cache list failed with message[%s]" % response)
        sys.exit(1)


@run.command(name='showcache')
@click.argument('cache_id')
@click.pass_context
def show_cache(ctx, cache_id):
    """detail info of cache. \n
    CACHE_ID: the id of the specified cache.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not cache_id:
        click.echo('cache show must provide cacheid.', err=True)
        sys.exit(1)
    valid, response = client.show_cache(cache_id)
    if valid:
        _print_run_cache_info(response, output_format)
    else:
        click.echo("cache list failed with message[%s]" % response)
        sys.exit(1)


@run.command(name='deletecache')
@click.argument('cache_id')
@click.pass_context
def delete_cache(ctx, cache_id):
    """delete the cache.\n
    CACHE_ID: the id of the specified cache.
    """
    client = ctx.obj['client']
    if not cache_id:
        click.echo('run stop must provide cacheid.', err=True)
        sys.exit(1)
    valid, response = client.delete_cache(cache_id)
    if valid:
        click.echo("cache id [%s] delete success" % cache_id)
    else:
        click.echo("cache delete failed with message[%s]" % response)
        sys.exit(1)


@run.command(name='listartifact')
@click.option('-u', '--userfilter', 'user_filter', help="List the artifactEventList by user.")
@click.option('-f', '--fsfilter', 'fs_filter', help="List the artifactEventList by fs.")
@click.option('-r', '--runfilter', 'run_filter', help="List the artifactEventList by run.")
@click.option('-t', '--typefilter', 'type_filter', help="List the artifactEventList by type.")
@click.option('-p', '--pathfilter', 'path_filter', help="List the artifactEventList by path.")
@click.option('-m', '--maxkeys', 'max_keys', help="Max size of the listed artifactEventList.")
@click.option('-mk', '--marker', help="next page.")
@click.pass_context
def list_artifact(ctx, user_filter=None, fs_filter=None, run_filter=None, type_filter=None, path_filter=None,
                  max_keys=100, marker=None):
    """list artifact. \n"""
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.list_artifact(user_filter, fs_filter, run_filter, type_filter,
                                                        path_filter, max_keys, marker)
    if valid:
        artifact_list, next_marker = response['artifactList'], response['nextMarker']
        if len(artifact_list):
            _print_artifact(artifact_list, output_format)
            click.echo('marker: {}'.format(next_marker))
        else:
            msg = "no artifact found "
            click.echo(msg)
    else:
        click.echo("artifact list failed with message[%s]" % response)
        sys.exit(1)


def _print_run_list(run_list, out_format):
    """print run list """

    headers = ['run id', 'fs name', 'username', 'status', 'name', 'description', 'run msg', 'source',
               'schedule id', 'scheduled time', 'create time', 'activate time', 'update time']
    data = [[run.run_id, run.fs_name, run.username, run.status, run.name, run.description, run.run_msg, run.source,
             run.schedule_id, run.scheduled_time, run.create_time, run.activate_time, run.update_time] for run in run_list]
    print_output(data, headers, out_format, table_format='grid')


def _print_run_cache(caches, out_format):
    """print cache list """

    headers = ['cache id', 'run id', 'job id', 'fsname', 'username', 'expired time',
                'create time', 'update time']
    data = [[cache.cache_id, cache.run_id, cache.job_id, cache.fs_name, cache.username,
             cache.expired_time, cache.create_time, cache.update_time] for cache in caches]
    print_output(data, headers, out_format, table_format='grid')


def _print_run_cache_info(cache, out_format):
    """print cache info """

    headers = ['cache id', 'run id', 'job id', 'fsname', 'username', 'expired time',
                'strategy', 'custom', 'create time', 'update time']
    data = [[cache.cache_id, cache.run_id, cache.job_id, cache.fs_name, cache.username, cache.expired_time,
             cache.strategy, cache.custom, cache.create_time, cache.update_time]]
    print_output(data, headers, out_format, table_format='grid')
    print_output([[cache.first_fp, cache.second_fp]], ['first fp', 'second fp'], out_format, table_format='grid')


def _print_run(run, out_format):
    """ print run info"""
    headers = ['run id', 'status', 'name', 'desc', 'param', 'source', 'run msg',
    'create time', 'update time', 'activate time']
    data = [[run.run_id, run.status, run.name, run.description, run.parameters, run.source,
             run.run_msg, run.create_time, run.update_time, run.activate_time]]
    print_output(data, headers, out_format, table_format='grid')

    headers = ['fs name', 'username', 'docker env', 'schedule id', 'fs options(json)', "failure options(json)",
               'disabled', 'run cached ids']
    data = [[run.fs_name, run.username, run.docker_env, run.schedule_id, run.fs_options, run.failure_options,
             run.disabled, run.run_cached_ids]]
    print_output(data, headers, out_format, table_format='grid')

    if run.run_yaml:
        headers = ['run yaml detail']
        data = [[run.run_yaml]]
        print_output(data, headers, out_format, table_format='grid')
    if (not run.runtime or not len(run.runtime)) and (not run.post_process or not len(run.post_process)):
        click.echo("no job found")
        return
    if run.runtime and len(run.runtime):
        headers = ['runtime in json']
        runtime_dict = _trans_comps_to_dict(run.runtime)
        data = [[json.dumps(runtime_dict, indent=2)]]
        print_output(data, headers, out_format, table_format='grid')
    if run.post_process and len(run.post_process):
        headers = ['postProcess in json']
        for key in run.post_process:
            run.post_process[key] = run.post_process[key].get_dict()
        data = [[json.dumps(run.post_process, indent=2)]]
        print_output(data, headers, out_format, table_format='grid')

def _trans_comps_to_dict(comps):
    res_comps = {}
    for name, comp_list in comps.items():
        new_comp_list = []
        for comp in comp_list:
            if hasattr(comp, 'entry_points'):
                comp.entry_points = _trans_comps_to_dict(comp.entry_points)
            new_comp_list.append(comp.get_dict())
        res_comps[name] = new_comp_list
    return res_comps


def _print_artifact(runs, out_format):
    """ print artifact info"""
    headers = ['run id', 'fsname', 'username', 'artifact path', 'type', 'step', 'artifact name', 'meta',  
            'create time', 'update time']
    data = [[run.run_id, run.fs_name, run.username, run.artifact_path, run.type, run.step, run.artifact_name,
             run.meta, run.create_time, run.update_time] for run in runs]
    print_output(data, headers, out_format, table_format='grid')
    
