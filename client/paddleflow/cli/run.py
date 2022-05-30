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

from paddleflow.cli.output import print_output, OutputFormat

@click.group()
def run():
    """manage run resources"""
    pass


@run.command()
@click.option('-f', '--fsname', help='The name of fs')
@click.option('-n', '--name', help='The name of run.')
@click.option('-d', '--desc', help='The description of run.')
@click.option('-u', '--username', help='Run the specified run by username, only useful for root.')
@click.option('-p', '--param', multiple=True, help="Run pipeline params, example: -p regularization=xxx .")
@click.option('-yp', '--runyamlpath', help='Run yaml file path, example ./run.yaml .')
@click.option('-yr', '--runyamlraw', help='Run yaml file raw, local absolute path .')
@click.option('-pplid', '--pipelineid', help='Pipeline ID, example ppl-000666')
@click.option('--disabled', multiple=True, help="the name of step which need to be disabled.")
@click.option('-de', '--dockerenv', help='a global dockerEnv used by all steps which have no dockerEnv')
@click.pass_context
def create(ctx, fsname=None, name=None, desc=None, username=None, runyamlpath=None, runyamlraw=None,
        param="", pipelineid=None, disabled=None, dockerenv=None):
    """create a new run.\n
    FSNAME: the name of the fs.
    """
    client = ctx.obj['client']
    param_dict = {}
    for k in param:
        splitTxt = k.split("=", 1)
        param_dict[splitTxt[0]] = splitTxt[1]
    if runyamlraw:
        with open(runyamlraw, 'rb') as f:
            runyamlraw = f.read()
    
    if disabled is not None:
        disabled = ",".join(disabled)

    valid, response = client.create_run(fsname, username, name, desc, runyamlpath, runyamlraw, pipelineid,
                            param_dict, disabled=disabled, dockerenv=dockerenv)

    if valid:
        click.echo("run[%s] create success with runid[%s]" % (fsname, response))
    else:
        click.echo("run create failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.option('-f', '--fsname', help='List the specified run by fsname.')
@click.option('-u', '--username', help='List the specified run by username, only useful for root.')
@click.option('-r', '--runid', help='List the specified run by runid')
@click.option('-n', '--name', help='List the specified run by run name')
@click.option('-m', '--maxsize', default=100, help="Max size of the listed users.")
@click.option('-mk', '--marker', help="Next page.")
@click.pass_context
def list(ctx, fsname=None, username=None, runid=None, name=None, maxsize=100, marker=None):
    """list run.\n """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.list_run(fsname, username, runid, name, maxsize, marker)
    if valid:
        if len(response):
            _print_runlist(response, output_format)
            click.echo('marker: {}'.format(nextmarker))
        else:
            msg = "no run found "
            click.echo(msg)
    else:
        click.echo("run list failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('runid')
@click.pass_context
def status(ctx, runid):
    """detail info of run. \n
    RUNID: the id of the specified run.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not runid:
        click.echo('run status must provide runid.', err=True)
        sys.exit(1)
    valid, response = client.status_run(runid)
    if valid:
        _print_run(response, output_format)
    else:
        click.echo("run status failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('runid')
@click.option('-f', '--force', is_flag=True,
    help="Whether to forcibly stop the task. Forcibly stop will also stop step in post_process")
@click.pass_context
def stop(ctx, runid, force):
    """stop the run.\n
    RUNID: the id of the specificed run.
    """
    client = ctx.obj['client']
    if not runid:
        click.echo('run stop must provide runid.', err=True)
        sys.exit(1)
    valid, response = client.stop_run(runid, force=force)
    if valid:
        click.echo("runid[%s] stop success" % runid)
    else:
        click.echo("run stop failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('runid')
@click.pass_context
def retry(ctx, runid):
    """retry the run.\n
    RUNID: the id of the specificed run.
    """
    client = ctx.obj['client']
    if not runid:
        click.echo('run retry must provide runid.', err=True)
        sys.exit(1)
    valid, response = client.retry_run(runid)
    if valid:
        click.echo("runid[%s] retry success" % runid)
    else:
        click.echo("run retry failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('runid')
@click.pass_context
def delete(ctx, runid):
    """ delete run .\n
    RUNID: the id of the specificed run.
    """
    client = ctx.obj['client']
    if not runid:
        click.echo('delete run provide runid.', err=True)
        sys.exit(1)
    valid, response = client.delete_run(runid)
    if valid:
        click.echo('runid[%s] delete success' % runid)
    else:
        click.echo("run delete failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.option('-u', '--userfilter', help="List the artifactEventList by user.")
@click.option('-f', '--fsfilter', help="List the artifactEventList by fs.")
@click.option('-r', '--runfilter', help="List the artifactEventList by run.")
@click.option('-m', '--maxkeys', help="Max size of the listed artifactEventList.")
@click.option('-mk', '--marker', help="Next page.")
@click.pass_context
def listcache(ctx, userfilter=None, fsfilter=None, runfilter=None, maxkeys=None, marker=None):
    """list cache .\n """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.list_cache(userfilter, fsfilter, runfilter, maxkeys, marker)
    if valid:
        if len(response):
            _print_runcache(response, output_format)
            click.echo('marker: {}'.format(nextmarker))
        else:
            msg = "no run found "
            click.echo(msg)
    else:
        click.echo("cache list failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('cacheid')
@click.pass_context
def showcache(ctx, cacheid):
    """detail info of cache. \n
    CACHEID: the id of the specified cache.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not cacheid:
        click.echo('cache show must provide cacheid.', err=True)
        sys.exit(1)
    valid, response = client.show_cache(cacheid)
    if valid:
        _print_runcache_info(response, output_format)
    else:
        click.echo("cache list failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.argument('cacheid')
@click.pass_context
def delcache(ctx, cacheid):
    """delete the cache.\n
    CACHEID: the id of the specificed cache.
    """
    client = ctx.obj['client']
    if not cacheid:
        click.echo('run stop must provide cacheid.', err=True)
        sys.exit(1)
    valid, response = client.delete_cache(cacheid)
    if valid:
        click.echo("cacheid[%s] delete success" % cacheid)
    else:
        click.echo("cache delete failed with message[%s]" % response)
        sys.exit(1)


@run.command()
@click.option('-u', '--userfilter', help="List the artifactEventList by user.")
@click.option('-f', '--fsfilter', help="List the artifactEventList by fs.")
@click.option('-r', '--runfilter', help="List the artifactEventList by run.")
@click.option('-t', '--typefilter', help="List the artifactEventList by type.")
@click.option('-p', '--pathfilter', help="List the artifactEventList by path.")
@click.option('-m', '--maxkeys', help="Max size of the listed artifactEventList.")
@click.option('-mk', '--marker', help="next page.")
@click.pass_context
def artifact(ctx, userfilter=None, fsfilter=None, runfilter=None, typefilter=None, pathfilter=None, 
                maxkeys=100, marker=None):
    """list artifact. \n"""
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response, nextmarker = client.artifact(userfilter, fsfilter, runfilter, typefilter, 
                pathfilter, maxkeys, marker)
    if valid:
        if len(response):
            _print_artiface(response, output_format)
            click.echo('marker: {}'.format(nextmarker))
        else:
            msg = "no artifact found "
            click.echo(msg)
    else:
        click.echo("artifact list failed with message[%s]" % response)
        sys.exit(1)


def _print_runlist(runlist, out_format):
    """print run list """

    headers = ['run id', 'fsname', 'username', 'status', 'name']
    data = [[run.runId, run.fsname, run.username, run.status, run.name] for run in runlist]
    print_output(data, headers, out_format, table_format='grid')


def _print_runcache(caches, out_format):
    """print cache list """

    headers = ['cache id', 'run id', 'step', 'fsname', 'username', 'expired time', 
                'create time', 'update time']
    data = [[cache.cacheid, cache.runid, cache.step, cache.fsname, cache.username, 
            cache.expiredtime, cache.createtime, cache.updatetime] for cache in caches]
    print_output(data, headers, out_format, table_format='grid')


def _print_runcache_info(cache, out_format):
    """print cache info """

    headers = ['cache id', 'run id', 'step', 'fsname', 'username', 'expired time', 
                'strategy', 'custom', 'create time', 'update time']
    data = [[cache.cacheid, cache.runid, cache.step, cache.fsname, cache.username, cache.expiredtime, 
            cache.strategy, cache.custom, cache.createtime, cache.updatetime]]
    print_output(data, headers, out_format, table_format='grid')
    print_output([[cache.firstfp, cache.secondfp]], ['first fp', 'second fp'], out_format, table_format='grid')


def _print_run(run, out_format):
    """ print run info"""
    headers = ['run id', 'status', 'name', 'desc', 'entry', 'param', 'source', 'run msg', 
    'create time', 'update time', 'activate time']
    data = [[run.runId, run.status, run.name, run.description, run.entry, run.parameters, run.source, 
             run.runMsg, run.createTime, run.updateTime, run.activateTime]]
    print_output(data, headers, out_format, table_format='grid')
    if run.runYaml:
        headers = ['run yaml detail']
        data = [[run.runYaml]]
        print_output(data, headers, out_format, table_format='grid')
    if (not run.runtime or not len(run.runtime)) and (not run.postProcess or not len(run.postProcess)):
        click.echo("no job found")
        return
    if run.runtime and len(run.runtime):
        print_output([], ["Runtime Details"], out_format)
        headers = ['job id', 'name', 'status', 'deps', 'start time', 'end time', 'dockerEnv']
        data = [[job.jobId, job.name, job.status, job.deps, job.start_time, job.end_time, job.dockerEnv] 
                for job in run.runtime]
        print_output(data, headers, out_format, table_format='grid')
    if run.postProcess and len(run.postProcess):
        print_output([], ["PostProcess Details"], out_format)
        headers = ['job id', 'name', 'status', 'deps', 'start time', 'end time', 'dockerEnv']
        data = [[job.jobId, job.name, job.status, job.deps, job.start_time, job.end_time, job.dockerEnv] 
                for job in run.postProcess]
        print_output(data, headers, out_format, table_format='grid')


def _print_artiface(runs, out_format):
    """ print artifact info"""
    headers = ['run id', 'fsname', 'username', 'artifact path', 'type', 'step', 'artifact name', 'meta',  
            'create time', 'update time']
    data = [[run.runid, run.fsname, run.username, run.artifactpath, run.type, run.step, run.artifactname, 
            run.meta, run.createtime, run.updatetime] for run in runs]
    print_output(data, headers, out_format, table_format='grid')
    
