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
from ..common.const import RUN_ACTIVE_STATUS, RUN_FINAL_STATUS

from paddleflow.cli.output import print_output, OutputFormat

@click.group()
def schedule():
    """manage schedule resources"""
    pass

@schedule.command()
@click.argument('name')
@click.argument('ppl_id')
@click.argument('ppl_ver_id')
@click.argument('crontab')
@click.option('-d', '--desc', help='Description of the schedule.')
@click.option('-s', '--starttime', 'start_time', help='StartTime like "YYYY-MM-DD hh-mm-ss", Start immediately if not set.')
@click.option('-e', '--endtime', 'end_time', help='EndTime like "YYYY-MM-DD hh-mm-ss", Never end if not set')
@click.option('-c', '--concurrency', help='num of run running at the sametime at most')
@click.option('-cp', '--concurrencypolicy', 'concurrency_policy',
              help='policy when run ready to start greater than concurrency,'
                   'should be set as "suspend", "replace" or "replace"')
@click.option('-ei', '--expireinterval', 'expire_interval',
              help='time interval before retrying schedule, always retry when setting 0')
@click.option('--catchup', is_flag=True, help='if use catchup')
@click.option('-u', '--username', help='only root can set username')
@click.pass_context
def create(ctx, name, ppl_id, ppl_ver_id, crontab,
            desc=None, start_time=None, end_time=None, concurrency=None, concurrency_policy=None, expire_interval=None,
            catchup=None, username=None):
    """ create run.\n
        NAME: Name of schedule.
        PPL_ID: ID of Pipeline used to create schedule.
        PPL_VER_ID: ID of Pipeline Version used to create schedule.
        CRONTAB: Crontab expression to specify how often the schedule create a run.
    """
    client = ctx.obj['client']

    valid, response = client.create_schedule(name, ppl_id, ppl_ver_id, crontab,
                                                desc, start_time, end_time, concurrency, concurrency_policy,
                                                expire_interval, catchup, username)

    if valid:
        click.echo("schedule [%s] create success with schedule id[%s]" % (name, response))
    else:
        click.echo("schedule create failed with message[%s]" % response)
        sys.exit(1)

@schedule.command()
@click.option('-u', '--userfilter', 'user_filter', help='List schedule with user specified.')
@click.option('-n', '--namefilter', 'name_filter', help='List schedule with name specified.')
@click.option('-p', '--pplfilter', 'ppl_filter', help='List schedule with pipeline_id specified.')
@click.option('-pv', '--pplverfilter', 'ppl_ver_filter', help='List schedule with pipeline_version_id specified.')
@click.option('-s', '--statusfilter', 'status_filter', help='List schedule with status specified.')
@click.option('-m', '--maxkeys', 'max_keys', help='Max size of list of schedule.')
@click.option('-mk', '--marker', help='Next page.')
@click.pass_context
def list(ctx, user_filter=None, ppl_filter=None, ppl_ver_filter=None, schedule_filter=None,
            name_filter=None, status_filter=None, marker=None, max_keys=None):
    """ List run. """
    client = ctx.obj['client']
    output_format = ctx.obj['output']

    # 处理statusFilters
    status_processed = ''
    if status_filter:
        status_filters = status_filter.split(sep=',')
        status_list = []
        for status in status_filters:
            if status == 'active':
                status_list.extend(RUN_ACTIVE_STATUS)
            elif status == 'final':
                status_list.extend(RUN_FINAL_STATUS)
            else:
                status_list.append(status)
        status_processed = ','.join(status_list)

    valid, response = client.list_schedule(user_filter, ppl_filter, ppl_ver_filter, schedule_filter, name_filter,
                                           status_processed, marker, max_keys)

    if valid:
        schedule_list, next_marker = response['scheduleList'], response['nextMarker']
        if len(schedule_list):
            _print_schedule_list(schedule_list, output_format)
            click.echo('marker: {}'.format(next_marker))
        else:
            msg = "No schedule found."
            click.echo(msg)
    else:
        click.echo("schedule list failed with message[%s]" % response)
        sys.exit(1)

@schedule.command()
@click.argument('schedule_id')
@click.option('-r', '--runfilter', 'run_filter', help='List schedule with run_id specified.')
@click.option('-s', '--statusfilter', 'status_filter', help='List schedule with status specified.')
@click.option('-m', '--maxkeys', 'max_keys', help='Max size of list of schedule.')
@click.option('-mk', '--marker', help='Next page.')
@click.pass_context
def show(ctx, schedule_id, run_filter=None, status_filter=None, max_keys=None, marker=None):
    """
    show schedule.
    SCHEDULE_ID: ID of schedule to show.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']

    valid, response = client.show_schedule(schedule_id, run_filter, status_filter, marker, max_keys)

    if valid:
        schedule_info, run_list, next_marker = response['scheduleInfo'], response['runList'], response['nextMarker']
        _print_schedule(schedule_info, run_list, next_marker, output_format)
    else:
        click.echo("schedule show failed with message[%s]" % response)
        sys.exit(1)


@schedule.command()
@click.argument('schedule_id')
@click.pass_context
def stop(ctx, schedule_id):
    """
    stop schedule.
    SCHEDULE_ID: ID of schedule to stop.
    """
    client = ctx.obj['client']

    valid, response = client.stop_schedule(schedule_id)

    if valid:
        click.echo("schedule with id [%s] stop success" % schedule_id)
    else:
        click.echo("schedule stop failed with message[%s]" % response)
        sys.exit(1)

@schedule.command()
@click.argument('schedule_id')
@click.pass_context
def delete(ctx, schedule_id):
    """
    delete schedule.
    SCHEDULE_ID: ID of schedule to delete.
    """
    client = ctx.obj['client']

    valid, response = client.delete_schedule(schedule_id)

    if valid:
        click.echo("schedule with id [%s] delete success" % schedule_id)
    else:
        click.echo("schedule stop failed with message[%s]" % response)
        sys.exit(1)

def _print_schedule_list(schedule_list, output_format):
    headers = ['schedule id', 'name', 'desc', 'pipeline id', 'pipeline version id', 'crontab',
               'username', 'fs config', 'options', 'message', 'status']
    data = [[schedule.schedule_id, schedule.name, schedule.desc, schedule.pipeline_id, schedule.pipeline_version_id, schedule.crontab,
             schedule.username, schedule.fs_config, schedule.options, schedule.message, schedule.status]
                for schedule in schedule_list]
    print_output(data, headers, output_format, table_format='grid')

    headers = ['schedule id', 'start time', 'end time', 'create time', 'update time', 'next run time']
    data = [[schedule.schedule_id, schedule.start_time, schedule.end_time, schedule.create_time, schedule.update_time, schedule.next_run_time]
            for schedule in schedule_list]
    print_output(data, headers, output_format, table_format='grid')


def _print_schedule(schedule_info, run_list, next_marker, output_format):
    _print_schedule_list([schedule_info], output_format)

    if len(run_list) > 0:
        headers = ['run id', 'fs name', 'username', 'status', 'name', 'description', 'run msg', 'source',
                   'schedule id', 'scheduled time', 'create time', 'activate time', 'update time']
        data = [[run.run_id, run.fs_name, run.username, run.status, run.name, run.description, run.run_msg, run.source,
                 run.schedule_id, run.scheduled_time, run.create_time, run.activate_time, run.update_time] for run in
                run_list]
        print_output(data, headers, output_format, table_format='grid')
    else:
        click.echo('no run created by this schedule')

    if next_marker:
        click.echo('marker: {}'.format(next_marker))


