import sys

import click
from paddleflow.cli.output import print_output, OutputFormat
from paddleflow.statistics import StatisticsJobInfo


@click.group()
def statistics():
    """show resources statistics """
    pass


@statistics.command()
@click.pass_context
@click.argument('jobid')
@click.option('-d', '--detail', is_flag=True, help="show detail statistics")
@click.option('-s', '--start', help="start time")
@click.option('-e', '--end', help="end time")
@click.option('-t', '--step', help="step")
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
        click.echo("detail is required")
        sys.exit(1)

    if detail:
        _get_job_statistics_detail(client, output_format, jobid, start, end, step)
    else:
        _get_job_statistics(client, output_format, jobid)
    pass


def _get_job_statistics(cli, output_format, jobid):
    valid, response = cli.get_statistics(jobid)
    if valid:
        _print_job_statistics(jobid, response, output_format)
    else:
        click.echo("get job statistics failed with message[%s]" % response)
        sys.exit(1)


def _get_job_statistics_detail(cli, output_format, jobid, start, end, step):
    valid, response = cli.get_statistics_detail(jobid, start, end, step)
    if valid:
        _print_job_statistics_detail(jobid, response, output_format)
    pass


def _print_job_statistics(jobid, info: StatisticsJobInfo, output_format):
    """print job statistics info."""
    headers = [
        "jobid",
        "cpu usage rate",
        "memory usage",
        "net receive bytes",
        "net send bytes",
        "disk usage bytes",
        "disk read rate",
        "disk write rate",
        "gpu util",
        "gpu memory util",
    ]
    data = [[
        jobid,
        info.cpu_usage_rate,
        info.memory_usage,
        info.net_receive_bytes,
        info.net_send_bytes,
        info.disk_usage_bytes,
        info.disk_read_rate,
        info.disk_write_rate,
        info.gpu_util,
        info.gpu_memory_util,
    ]]

    print_output(data, headers, output_format, table_format='grid')


def _print_job_statistics_detail(jobid, job_statistics_detail_info, output_format):
    pass
