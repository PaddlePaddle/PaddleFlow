#!/usr/bin/env python3
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
from typing import List

from .options import Options

from paddleflow.common.exception import PaddleFlowSDKException
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError


class FSScope(object):
    """ the paths involved in the cachekey calculation (mainly to calculate whether the content under the path has changed). 
    """
    def __init__(
            self,
            name: str,
            path: str=None
            ):
        """ create an new instance of FSScope

        Args:
            name: the name of paddleflow filesystem
            path: the paths involved in the cachekey calculation (mainly to calculate whether the content under the path has changed). Multiple paths are divided by ',' such as "/code,/data"
        """
        self.name = name
        self.path = path

    def compile(self):
        """ trans to dict
        """
        result = {}

        if not self.name:
            raise PaddleFlowSDKException(PipelineDSLError, "FSScope's name attribute cannot empty")

        result["name"] = self.name

        if self.path:
            result["path"] = self.path

        return result


class CacheOptions(Options):
    """ The Cache Options of Pipeline or Step
    """
    COMPILE_ATTR_MAP = {
        "enable": "enable",
        "fs_scope": "fs_scope",
        "max_expired_time": "max_expired_time",
    }

    def __init__(
            self,
            enable: bool=False,
            fs_scope: List[FSScope]=None,
            max_expired_time: int=-1,
            ):
        """ create a new instance of CacheOptions

        Args:
            enable (bool): indicate use cache or not, default is False
            fs_scope (List[FSScope]): the paths involved in the cachekey calculation (mainly to calculate whether the content under the path has changed). Multiple paths are divided by ',' such as "/code,/data"
            max_expired_time (int): the maximum expiration time of the cache, in seconds, - 1 means permanently valid, default is -1
        """
        # cache 的默认处理由 server 侧决定
        if enable is None:
            self.enable = enable
        else:
            self.enable = bool(enable)

        if fs_scope is None:
            self.fs_scope = fs_scope
        else:
            if not isinstance(fs_scope, list):
                fs_scope = [fs_scope]

            self.fs_scope = []
            for fs in fs_scope:
                self.fs_scope.append(fs)

        if max_expired_time is None: 
            self.max_expired_time = max_expired_time
        else:
            self.max_expired_time = int(max_expired_time)

    def _validate(self):
        """ validate
        """
        if self.fs_scope:
            if not isinstance(self.fs_scope, List):
                self.fs_scope = [self.fs_scope]
            
            for sp in self.fs_scope:
                if not isinstance(sp, FSScope):
                    raise PaddleFlowSDKException(PipelineDSLError, 
                        "the [fs_scope] attirbute of CacheOptions should be a list of FSScope")
                        