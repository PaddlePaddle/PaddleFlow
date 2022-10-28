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

import sys
import click


from paddleflow.cli.output import print_output

@click.group()
def version():
    """show paddleflow server version"""
    pass

@version.command()
@click.pass_context
def show(ctx):
    """show paddleflow server version\n
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.get_version()
    if valid:
        _print_version_info(response, output_format)
    else:
        click.echo("get paddleflow server version failed with message[%s]" % response)
        sys.exit(1)


def _print_version_info(response, out_format):
    """print server version info """
    headers = ['PaddleFlow Server Version']
    data = [[response]]
    print_output(data, headers, "json", table_format='grid')