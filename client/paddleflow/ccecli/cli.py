"""
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

import os
import click
import logging
import sys
import configparser

from paddleflow.client import Client
from paddleflow.cli.output import OutputFormat
from paddleflow.cli.user import user
from paddleflow.ccecli.queue import queue
from paddleflow.cli.fs import fs
from paddleflow.cli.job import job
from paddleflow.cli.log import log
from paddleflow.cli.run import run
from paddleflow.cli.pipeline import pipeline
from paddleflow.cli.flavour import flavour
from paddleflow.cli.statistics import statistics
from paddleflow.common.util import get_default_config_path

DEFAULT_PADDLEFLOW_PORT = 8999


@click.group()
@click.option('--pf_config', help='the path of default config.')
@click.option('--output', type=click.Choice(list(map(lambda x: x.name, OutputFormat))),
              default=OutputFormat.table.name, show_default=True,
              help='The formatting style for command output.')
@click.pass_context
def cli(ctx, pf_config=None, output=OutputFormat.table.name):
    """paddleflow is the command line interface to paddleflow service.\n
       provide `user`, `queue`, `fs`, `run`, `pipeline`, `flavour` operation commands
    """
    if pf_config:
        config_file = pf_config
    else:
        config_file = get_default_config_path()
    if not os.access(config_file, os.R_OK):
        click.echo("no config file in %s" % config_file, err=True)
        sys.exit(1)
    config = configparser.RawConfigParser()
    config.read(config_file, encoding='UTF-8')
    if 'user' not in config or 'server' not in config:
        click.echo("no user or server conf in %s" % config_file, err=True)
        sys.exit(1)
    if 'password' not in config['user'] or 'name' not in config['user']:
        click.echo("no name or password conf['user'] in %s" % config_file, err=True)
        sys.exit(1)
    if 'paddleflow_server_host' not in config['server']:
        click.echo("no paddleflow_server_host in %s" % config_file, err=True)
        sys.exit(1)
    paddleflow_server_host = config['server']['paddleflow_server_host']
    if 'paddleflow_server_port' in config['server']:
        paddleflow_server_port = config['server']['paddleflow_server_port']
    else:
        paddleflow_server_port = DEFAULT_PADDLEFLOW_PORT
    ctx.obj['client'] = Client(paddleflow_server_host, config['user']['name'], config['user']['password'],
                               paddleflow_server_port)
    name = config['user']['name']
    password = config['user']['password']
    ctx.obj['client'].login(name, password)
    ctx.obj['output'] = output


def main():
    """main
    """
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    cli.add_command(user)
    cli.add_command(queue)
    cli.add_command(fs)
    cli.add_command(run)
    cli.add_command(pipeline)
    cli.add_command(flavour)
    cli.add_command(log)
    cli.add_command(job)
    cli.add_command(statistics)
    try:
        cli(obj={}, auto_envvar_prefix='paddleflow')
    except Exception as e:
        click.echo(str(e), err=True)
        sys.exit(1)
