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

@click.group()
def flavour():
    """manage flavour resources"""
    pass


@flavour.command()
@click.pass_context
def list(ctx):
    """ list flavour.\n
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.flavour()
    if valid:
        if len(response)>0:
            _print_flavour_info(response, output_format)
        else:
            click.echo("not find flavour")
        sys.exit(1)
    else:
        click.echo("flavour list failed with message[%s]" % response)
        sys.exit(1)


def _print_flavour_info(res, out_format):
    """print flavour list"""
    headers = ['name', 'cpu', 'mem', 'scalarResources']
    data = []
    for i in res:
        data.append([i.get('name', ''), i.get('cpu', ''), i.get('mem', ''), i.get('scalarResources', None),])
    print_output(data, headers, out_format, table_format='grid')