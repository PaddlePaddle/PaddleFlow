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
def log():
    """manage log resources"""
    pass


@log.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('runid')
@click.option('-j', '--jobid', help="job id")
@click.option('-ps', '--pagesize', help="content lines in one page; max size 100; default value 100")
@click.option('-pn', '--pageno', help="page number;default value 1")
@click.option('-fp', '--logfileposition', help="read log from the beginning or the end; "
                                               "optional value: begin, end; default value end")
@click.pass_context
def show(ctx, runid, jobid=None, pagesize=None, pageno=None, logfileposition=None):
    """

    show run log\n
    RUNID: the id of the specificed run.

    """
    client = ctx.obj['client']
    output_format = 'text'
    if not runid:
        click.echo('log show must provide runid.', err=True)
        sys.exit(1)
    valid, response = client.show_log(runid, jobid, pagesize, pageno, logfileposition)
    if valid:
        if jobid is None and len(response['runLog']) > 0:
            response['runLog'] = [response['runLog'][0]]
        _print_run_log(response, output_format)
    else:
        click.echo("show run log failed with message[%s]" % response)
        sys.exit(1)


def _print_run_log(loginfo, out_format):
    """print run log """
    submit_loginfo = loginfo['submitLog']
    run_loginfo = loginfo['runLog']
    if submit_loginfo == '':
        data = [["submit log:"], [""], ["run log:"]]
    else:
        data = [["submit log:"], [submit_loginfo], [""], ["run log:"]]
    if len(run_loginfo) == 0:
        data.append([""])
    for index, item in enumerate(run_loginfo):
        line = ["runid:" + item.runid, "jobid:" + item.jobid, "taskid:" + item.taskid, "has_next_page:" + str(item.has_next_page),
                "truncated:" + str(item.truncated), "page_no:" + str(item.pageno), "page_size:" + str(item.pagesize)]
        line_log = []
        line_log.append(item.log_content)
        data.append(line)
        data.append(line_log)
        if index != len(run_loginfo) - 1:
            data.append(['\n'])
    print_output(data, [], out_format)
