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


class FSInfo(object):
    """the class of fs info"""
    def __init__(self, name, owner, fstype, server_address, subpath, properties):
        """init """
        self.name = name
        self.owner = owner
        self.fstype = fstype
        self.server_adddress = server_address
        self.subpath = subpath
        self.properties = properties


class LinkInfo(object):
    """the class of link info"""
    def __init__(self, name, owner, fstype, fspath, server_address, subpath, properties):
        """init """
        self.name = name
        self.owner = owner
        self.fstype = fstype
        self.fspath = fspath
        self.server_adddress = server_address
        self.subpath = subpath
        self.properties = properties


class CacheConfigInfo(object):
    """the class of fs cache config"""
    def __init__(self, fsname, username, cachedir, metadriver, blocksize):
        """init """
        self.fsname = fsname
        self.username = username
        self.cachedir = cachedir
        self.metadriver = metadriver
        self.blocksize = blocksize