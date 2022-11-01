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

# !/usr/bin/env python3
# -*- coding:utf8 -*-

import sys
import subprocess
import time
import json
import click
import shutil

from paddleflow.cli.output import print_output, OutputFormat
from paddleflow.utils.format_help import command_required_option_from_option


@click.group()
def fs():
    """manage fs resources"""
    pass


@fs.command()
@click.option('-u', '--username', help='List the specified fs by username, only useful for root.')
@click.option('-m', '--maxsize', default=100, help="Max size of the listed fs.")
@click.pass_context
def list(ctx, username=None, maxsize=100):
    """list fs """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.list_fs(username, maxsize)
    if valid:
        if len(response):
            _print_fs(response, output_format)
        else:
            msg = "no fs found "
            click.echo(msg)
    else:
        click.echo("fs list failed with message[%s]" % response)
        sys.exit(1)


@fs.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('fsname')
@click.argument('url')
@click.option('-u', '--username', help='Create fs by username, only useful for root.')
@click.option('-o', '--o', multiple=True, help="""
Create file-sysystem properties example and rules:   
SFTP: 
  -o user: sftp user [required] 
  -o password: sftp password [required] 
  Example: -o user=your-username -o password=your-password
  
S3:   
  -o endpoint: s3 endpoint [required] 
  -o region: s3 region [not required] 
  -o accessKey: s3 accessKey [required]
  -o secretKey: s3 secretKey [required]
  -o s3ForcePathStyle: set this to `true` to force the request to use path-style addressing
  -o insecureSkipVerify: insecureSkipVerify controls whether a client verifies the server's certificate chain and host
   name. If insecureSkipVerify is `true`, crypto/tls accepts any certificate presented by the server and any host name in 
   that certificate. In this mode, TLS is susceptible to machine-in-the-middle attacks unless custom verification is 
   used.
  -o dir-mode: Permission bits for directories. (default: 0755)
  -o file-mode: Permission bits for files. (default: 0644)
  Example: -o endpoint=your-endpoint -o region=your-region 
 
HDFS:  
  -o user: hdfs user name [required] 
  -o group: hdfs user group [required] 
  Example: -o user=your-hdfs-user -o group=your-hdfs-group 
 
HDFSWithKerberos: 
  -o keyTabData: base64 your hdfs.keytab file [required] 
  -o principal: kerberos principal, example: hdfs/xxx [required]
  -o realm: kerberos realm, example: EXAMPLE.COM [required] 
  -o kdc: kerberos kdc, example: 192.101.101.101 [required]
  -o nameNodePrincipal: kerberos name node principal, example: hdfs/xxx@EXAMPLE.COM [required]
  Example: -o keyTabData=base64data -o principal=hdfs/xxx -o realm=EXAMPLE.COM -o kdc=192.101.101.101 -o nameNodePrincipal=hdfs/xxx@EXAMPLE.COM
""")
@click.pass_context
def create(ctx, fsname, url, o="", username=None):
    """
    Create fs arguments
    FSNAME: your custom fs name
    URL: File System URL, examlpe:
      SFTP: sftp://192.168.1.2:22/myfs/data
      S3: s3://yourbucket/subpath
      HDFS: hdfs://192.168.1.2:9000/myfs/data
      HDFSWithKerberos: hdfs://192.168.1.2:9000/myfs/data
    """
    client = ctx.obj['client']
    if not fsname or not url:
        click.echo('fs create must provide fsname and url.', err=True)
        sys.exit(1)
    properties = {}
    for k in o:
        splitTxt = k.split("=", 1)
        for k in o:
            splitTxt = k.split("=", 1)
            if len(splitTxt) < 2:
                click.echo("-o params must follow with {}=\"\"".format(k))
                return
            properties[splitTxt[0]] = splitTxt[1]
    valid, response = client.add_fs(fsname, url, username, properties)
    if valid:
        click.echo("fs[%s] create success" % fsname)
    else:
        click.echo("fs create failed with message[%s]" % response)
        sys.exit(1)


@fs.command()
@click.argument('fsname')
@click.option('-u', '--username', help='Show the specified fs by username, only useful for root.')
@click.pass_context
def show(ctx, fsname, username=None):
    """
    show fs arguments\n
    FSNAME: fs name
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.show_fs(fsname, username)
    if valid:
        if len(response):
            _print_fs(response, output_format)
        else:
            msg = "no fs found "
            click.echo(msg)
    else:
        click.echo("describe fs failed with message[%s]" % response)
        sys.exit(1)


@fs.command()
@click.argument('fsname')
@click.option('-u', '--username', help='List the specified fs by username,only useful for root.')
@click.pass_context
def delete(ctx, fsname, username=None):
    """
    delete fs\n
    FSNAME: fs name
    """
    client = ctx.obj['client']
    if not fsname:
        click.echo('fs delete must provide fsname.', err=True)
        sys.exit(1)
    valid, response = client.delete_fs(fsname, username)
    if valid:
        click.echo("fs[%s] delete success" % fsname)
    else:
        click.echo("fs delete failed with message[%s]" % response)
        sys.exit(1)


@fs.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('fsname')
@click.argument('path')
@click.option('-u', '--username', help='Mount the specified fs by username, only useful for root.')
@click.option('-o', '--o', multiple=True, help="""
mount options: 
  -o block-size: data cache block size (default: 0), if block-size equals to 0, it means that no data cache is used 
  -o data-cache-path: directory path of local data cache  (default:"/var/cache/pfs-cache-dir/data-cache") 
  -o data-cache-expire: file data cache timeout (default 0s)
  -o meta-cache-path: directory path of meta cache  (default:"/var/cache/pfs-cache-dir/meta-cache") 
  -o meta-cache-driver: meta driver type (e.g. mem, disk)",
  -o meta-cache-expire: file meta cache timeout (default 5s)
  -o entry-cache-expire: file entry cache timeout (default 5s)
""")
@click.pass_context
def mount(ctx, fsname, path, o="", username=None):
    """
    mount fs\n
    FSNAME: mount fs name which contain mount fs and mount path\n
    PATH: your local mount path
    """
    client = ctx.obj['client']
    if not fsname:
        click.echo('fs mount must provide fsname.', err=True)
        sys.exit(1)
    if not path:
        click.echo('fs mount must provide path.', err=True)
        sys.exit(1)
    mountOptions = {}
    for k in o:
        splitTxt = k.split("=", 1)
        if len(splitTxt) < 2:
            click.echo("-o params must follow with {}=\"\"".format(k))
            return
        mountOptions[splitTxt[0]] = splitTxt[1]
    valid, response = client.mount(fsname, path, mountOptions, username)
    if not valid:
        if response is not None:
            click.echo(response)
        else:
            log = "mount-{}.err.log".format(fsname)
            click.echo("Mount failed. Please check the log file for more details, the log file is {}".format(log))
    else:
        click.echo("mount success")
    sys.exit(1)


@fs.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('fsname')
@click.option('-u', '--username', help='specify fs under other user. for root use only.')
@click.option('-o', '--o', multiple=True, help="""
cacheConfig options: 
  -o cacheDir: cache dir on host node. Two sub-directories "data-cache" and "meta-cache" will be created under it.
  -o metaDriver: meta driver type (e.g. default, mem, disk)",
  -o blockSize: data cache block size (default: 0), if block-size equals to 0, it means that no data cache is used
""")
@click.pass_context
def createfscache(ctx, fsname, o="", username=None):
    """
    create a cache config for your fs\n
    FSNAME: fs name to set cache config\n
    """
    client = ctx.obj['client']
    if not fsname:
        click.echo('create fs cache must provide fsname.', err=True)
        sys.exit(1)
    params = {}
    for k in o:
        params_kv = k.split("=", 1)
        if len(params_kv) < 2:
            click.echo("-o params must follow with {}=\"\"".format(k))
            return
        params[params_kv[0]] = params_kv[1]
    if params["blockSize"]:
        params["blockSize"] = int(params["blockSize"])
    valid, response = client.create_cache(fsname, params, username)
    if valid:
        click.echo("create fs cache success")
    else:
        if response is not None:
            click.echo(response)
        else:
            click.echo("create fs cache config failed with no error response")
        sys.exit(1)

@fs.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('fsname')
@click.option('-u', '--username', help='specify fs under other user. for root use only.')
@click.pass_context
def getfscache(ctx, fsname, username=None):
    """
    get cache config info for a filesystem\n
    FSNAME: fs name to set cache config\n
    """
    client = ctx.obj['client']
    if not fsname:
        click.echo('must provide fsname.', err=True)
        sys.exit(1)
    valid, response = client.get_fs_cache(fsname, username)
    if valid:
        _print_cache(response, ctx.obj['output'])
    else:
        click.echo("get fs cache config failed with message[%s]" % response)
        sys.exit(1)


@fs.command()
@click.argument('fsname')
@click.option('-u', '--username', help='List the specified fs by username,only useful for root.')
@click.pass_context
def deletefscache(ctx, fsname, username=None):
    """
    delete fs cache config\n
    FSNAME: fs name
    """
    client = ctx.obj['client']
    if not fsname:
        click.echo('fs delete must provide fsname.', err=True)
        sys.exit(1)
    valid, response = client.delete_fs_cache(fsname, username)
    if valid:
        click.echo("fs[%s] cache config delete success" % fsname)
    else:
        click.echo("fs cache config delete failed with message[%s]" % response)
        sys.exit(1)


@fs.command(context_settings=dict(max_content_width=2000), cls=command_required_option_from_option())
@click.argument('fsname')
@click.argument('fspath')
@click.argument('url')
@click.option('-u', '--username', help='Create link by username.')
@click.option('-o', '--o', multiple=True, help="""
Create file-sysystem link properties example and rules:   
SFTP: 
  -o user: sftp user [required] 
  -o password: sftp password [required] 
  Example: -o user=your-username -o password=your-password
  
S3:   
  -o endpoint: s3 endpoint [required] 
  -o region: s3 regin [not required] 
  -o accessKey: s3 accessKey [required]
  -o secretKey: s3 secretKey [required]
  -o s3ForcePathStyle: set this to `true` to force the request to use path-style addressing
  -o insecureSkipVerify: insecureSkipVerify controls whether a client verifies the server's certificate chain and host
   name. If insecureSkipVerify is `true`, crypto/tls accepts any certificate presented by the server and any host name in 
   that certificate. In this mode, TLS is susceptible to machine-in-the-middle attacks unless custom verification is 
   used.
  -o dir-mode: Permission bits for directories. (default: 0755)
  -o file-mode: Permission bits for files. (default: 0644)
  Example: -o endpoint=your-endpoint -o region=your-regin 
 
HDFS:  
  -o user: hdfs user name [required] 
  -o group: hdfs user group [required] 
  Example: -o user=your-hdfs-user -o group=your-hdfs-group 
 
HDFSWithKerberos: 
  -o keyTabData: base64 your hdfs.keytab file [required] 
  -o principal: kerberos principal, example: hdfs/xxx [required]
  -o realm: kerberos realm, example: EXAMPLE.COM [required] 
  -o kdc: kerberos kdc, example: 192.101.101.101 [required]
  -o nameNodePrincipal: kerberos name node principal, example: hdfs/xxx@EXAMPLE.COM [required]
  Example: -o keyTabData=base64data -o principal=hdfs/xxx -o realm=EXAMPLE.COM -o kdc=192.101.101.101 -o nameNodePrincipal=hdfs/xxx@EXAMPLE.COM
""")
@click.pass_context
def link(ctx, fsname, fspath, url, o="", username=None):
    """
    Create link arguments
    FSNAME: your custom fs name
    FSPATH: fs path
    URL: File System URL, examlpe:
      SFTP: sftp://192.168.1.2:22/myfs/data
      S3: s3://yourbucket/subpath
      HDFS: hdfs://192.168.1.2:9000/myfs/data
      HDFSWithKerberos: hdfs://192.168.1.2:9000/myfs/data
    """
    client = ctx.obj['client']
    if not fsname or not url:
        click.echo('fs create must provide fsname and url.', err=True)
        sys.exit(1)
    properties = {}
    for k in o:
        splitTxt = k.split("=", 1)
        if len(splitTxt) < 2:
            click.echo("-o params must follow with {}=\"\"".format(k))
            return
        properties[splitTxt[0]] = splitTxt[1]
    valid, response = client.add_link(fsname, fspath, url, username, properties)
    if valid:
        click.echo("fs[%s] create link success" % fsname)
    else:
        click.echo("fs create link failed with message[%s]" % response)
        sys.exit(1)


@fs.command()
@click.argument('fsname')
@click.argument('fspath')
@click.option('-u', '--username', help='Show the specified fs by username.')
@click.pass_context
def showlink(ctx, fsname, fspath, username=None):
    """
    show link arguments\n
    FSNAME: fs name
    FSPATH: fs path
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.show_link(fsname, fspath, username)
    if valid:
        if len(response):
            _print_link(response, output_format)
        else:
            msg = "no link found "
            click.echo(msg)
    else:
        click.echo("describe link failed with message[%s]" % response)
        sys.exit(1)


@fs.command()
@click.argument('fsname')
@click.argument('fspath')
@click.option('-u', '--username', help='List the specified fs by username.')
@click.pass_context
def unlink(ctx, fsname, fspath, username=None):
    """
    delete link\n
    FSNAME: fs name
    FSPATH: fs path
    """
    client = ctx.obj['client']
    if not fsname:
        click.echo('fs delete must provide fsname.', err=True)
        sys.exit(1)
    valid, response = client.delete_link(fsname, fspath, username)
    if valid:
        click.echo("fs[%s] delete link success" % fsname)
    else:
        click.echo("link delete failed with message[%s]" % response)
        sys.exit(1)


@fs.command()
@click.argument('fsname')
@click.option('-u', '--username', help='List the specified fs by username.')
@click.option('-m', '--maxsize', default=100, help="Max size of the listed fs.")
@click.pass_context
def listlink(ctx, fsname, username=None, maxsize=100):
    """
    list link\n
    FSNAME: fs name
    """
    client = ctx.obj['client']
    output_format = ctx.obj['output']
    valid, response = client.list_link(fsname, username, maxsize)
    if valid:
        if len(response):
            _print_link(response, output_format)
        else:
            msg = "no link found"
            click.echo(msg)
    else:
        click.echo("link list failed with message[%s]" % response)
        sys.exit(1)


def _print_fs(fslist, out_format):
    """print fs """
    headers = ['name', 'owner', 'type', 'server address', 'sub path', 'properties']
    data = [[fs.name, fs.owner, fs.fstype, fs.server_adddress, fs.subpath, fs.properties] for fs in fslist]
    print_output(data, headers, out_format, table_format='grid')


def _print_link(linklist, out_format):
    """print link """
    headers = ['name', 'owner', 'type', 'fs path', 'server address', 'sub path', 'properties']
    data = [[link.name, link.owner, link.fstype, link.fspath, link.server_adddress, link.subpath, link.properties]
            for link in linklist]
    print_output(data, headers, out_format, table_format='grid')


def _print_cache(cacheconfig, out_format):
    """print fs cache config """
    headers = ['fsname', 'owner', 'cache dir', 'meta driver', 'block size']
    data = [[cacheconfig.fsname, cacheconfig.username, cacheconfig.cachedir, cacheconfig.metadriver, cacheconfig.blocksize]]
    print_output(data, headers, out_format, table_format='grid')
