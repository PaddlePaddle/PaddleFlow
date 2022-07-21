import sys
import traceback

import click
from paddleflow.cli.output import print_output
from paddleflow.statistics import StatisticsJobInfo, StatisticsJobDetailInfo


# !/usr/bin/env python3
# -*- coding:utf8 -*-

@click.group()
def statistics():
    """show resources statistics """
    pass


@statistics.command()
@click.pass_context
@click.argument('jobid')
@click.option('-d', '--detail', is_flag=True, help="show detail statistics")
@click.option('-s', '--start', help="start time", type=int)
@click.option('-e', '--end', help="end time", type=int)
@click.option('-st', '--step', help="step", type=int)
def job(ctx, jobid, detail, start, end, step):
    """ show statistics info.\n
    JOBID: the id of job you want to show.
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    if not jobid:
        click.echo("jobid is required")
        sys.exit(1)

    if not detail and (start or end or step):
        click.echo("please use -d or --detail option to show detail statistics")
        sys.exit(1)

    if detail:
        _get_job_statistics_detail(client, output_format, jobid, start, end, step)
    else:
        _get_job_statistics(client, output_format, jobid)


def _get_job_statistics(cli, output_format, jobid):
    valid, response = cli.get_statistics(jobid)
    if valid:
        _print_job_statistics(jobid, response, output_format)
    else:
        click.echo("get job statistics failed with message[%s]" % response)
        sys.exit(1)


def _get_job_statistics_detail(cli, output_format, jobid, start, end, step):
    valid, response, truncated = cli.get_statistics_detail(jobid, start, end, step)
    if valid:
        _print_job_statistics_detail(response, output_format)
        if truncated:
            click.echo("results has been truncated due to server side limitation")
    else:
        click.echo("get job statistics detail failed with message[%s]" % response)
        sys.exit(1)


def _print_job_statistics(jobid, info: StatisticsJobInfo, output_format):
    """print job statistics info."""
    if info.metrics_info is None:
        click.echo("no data")
        return
    info = info.metrics_info
    headers = [k.replace("_", " ") for k in info]
    data = [[v for v in info.values()]]

    print_output(data, headers, output_format, table_format='grid')


def _print_job_statistics_detail(job_statistics_detail_info: StatisticsJobDetailInfo, output_format):
    if len(job_statistics_detail_info.result) == 0:
        click.echo("no data")
        return

    # only show the first result
    result = job_statistics_detail_info.result[0]
    headers = ['timestamp']
    data = []
    ts_map = {}

    for i, info in enumerate(result.task_info):
        headers.append(info.metric.replace("_", " "))
        ts_set = set()
        # add metric to timestamp map
        for value in info.values:
            if not value[0] in ts_map:
                # add absent values to timestamp map
                ts_map[value[0]] = ["N/A" for _ in range(i)]
                ts_map[value[0]].append(value[1])
            else:
                ts_map[value[0]].append(value[1])
            ts_set.add(value[0])

        # add absent values to timestamp map
        for k in ts_map:
            if k not in ts_set:
                ts_map[k].append("N/A")

    for k, v in sorted(ts_map.items(), key=lambda x: x[0]):
        v.insert(0, k)
        data.append(v)

    print_output(data, headers, output_format, table_format='grid')
