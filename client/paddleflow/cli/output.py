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

import click
import json
from enum import Enum, unique
from typing import Union

from tabulate import tabulate


@unique
class OutputFormat(Enum):
    """Enumerated class with the allowed output format constants."""
    table = "table"
    json = "json"
    text = "text"


def print_output(data: Union[list, dict], headers: list, output_format: str, table_format: str = "simple"):
    """Prints the output from the cli command execution based on the specified format.
    Args:
        data (Union[list, dict]): Nested list of values representing the rows to be printed.
        headers (list): List of values representing the column names to be printed
            for the ``data``.
        output_format (str): The desired formatting of the text from the command output.
        table_format (str): The formatting for the table ``output_format``.
            Default value set to ``simple``.
    Returns:
        None: Prints the output.
    Raises:
        NotImplementedError: If the ``output_format`` is unknown.
    """
    if output_format == OutputFormat.table.name:
        click.echo(tabulate(data, headers=headers, tablefmt=table_format))
    elif output_format == OutputFormat.json.name:
        if not headers:
            output = data
        else:
            output = []
            # if list size is 1, then convert list to object
            if len(data) <= 1:
                output = dict(zip(headers, data[0]))
            else:
                for row in data:
                    output.append(dict(zip(headers, row)))
    elif output_format == OutputFormat.text.name:
        output = ''
        for index, item in enumerate(data):
            for i in item:
                output += i + ' \t'
            if index != len(data) - 1:
                output += '\n'
        click.echo(output)
    else:
        raise NotImplementedError("Unknown Output Format: {}".format(output_format))