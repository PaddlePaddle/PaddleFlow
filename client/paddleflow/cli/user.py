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
def user():
    """manage user resources"""
    pass


@user.command()
@click.option('-m', '--maxsize', default=100, help="Max size of the listed users.")
@click.pass_context
def list(ctx, maxsize=100):
    """list user """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.list_user(maxsize)
    if valid:
        if len(response):
            _print_users(response, output_format)
        else:
            msg = "no users found "
            click.echo(msg)
    else:
        click.echo("user list failed with message[%s]" % response)


@user.command()
@click.argument('username')
@click.argument('password')
@click.pass_context
def add(ctx, username, password):
    """
    add user arguments.\n
    USERNAME: the new user's name \n
    PASSWORD: the password of new user
    """
    client = ctx.obj['client']
    if not username or not password:
        click.echo('user add  must provide username and password.', err=True)
        sys.exit(1)
    valid, response = client.add_user(username, password)
    if valid:
        click.echo("user[%s] add success" % username)
    else:
        click.echo("user[%s] add failed with message[%s]" % (username, response))
        sys.exit(1)


@user.command()
@click.argument('username')
@click.argument('password')
@click.pass_context
def set(ctx, username, password):
    """update user's password.\n
    USERNAME: the user's name \n
    PASSWORD: the new password of the user
    """
    client = ctx.obj['client']
    if not username or not password:
        click.echo('user set  must provide username and password.', err=True)
        sys.exit(1)
    valid, response = client.update_password(username, password)
    if valid:
        click.echo("user[%s] update success" % username)
    else:
        click.echo("user[%s] update failed with message[%s]" % (username, response))
        sys.exit(1)


@user.command()
@click.argument('username')
@click.pass_context
def delete(ctx, username):
    """delete user.\n
    USERNAME: the user's name \n
    """
    client = ctx.obj['client']
    if not username:
        click.echo('user delete  must provide username.', err=True)
        sys.exit(1)
    valid, response = client.del_user(username)
    if valid:
        click.echo("user[%s] delete success" % username)
    else:
        click.echo("user[%s] delete failed with message[%s]" % (username, response))
        sys.exit(1)


def _print_users(users, out_format):
    """print users """
    headers = ['name', 'create time']
    data = [[user.name, user.create_time] for user in users]
    print_output(data, headers, out_format, table_format='grid')