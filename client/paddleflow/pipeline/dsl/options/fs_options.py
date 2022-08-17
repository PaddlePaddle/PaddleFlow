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

from .options import Options
from typing import List

from paddleflow.common.exception import PaddleFlowSDKException
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError

class ExtraFS(object):
    """ extra paddleflow filesystem which used by step
    """
    def __init__(
            self, 
            name: str,
            sub_path: str=None,
            mount_path: str=None,
            read_only: bool=False 
            ):
        """ create an new instance for ExtraFS

        Args:
            name (str): the name of paddleflow filesystem
            sub_path (str): the sub path on paddleflow filesystem which would be mount on pods
            mount_path (str): the path on pod where paddleflow filesystem mount on
            read_only (bool): the access right of mount_path
        """
        self.name = name 
        self.sub_path = sub_path
        self.mount_path = mount_path
        self.read_only = read_only

    def compile(self):
        """ trans to dict 
        """ 
        result = {}

        if not self.name:
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of paddleflow filesystem cannot be empty")

        result["name"] = self.name

        if self.sub_path:
            result["sub_path"] = self.sub_path

        if self.mount_path:
            result["mount_path"] = self.mount_path

        if not isinstance(self.read_only, bool):
            raise PaddleFlowSDKException(PipelineDSLError, f"the attribute[read_only] of ExtraFS should be an instance of bool")

        result["read_only"] = bool(self.read_only)

        return result


class MainFS(ExtraFS):
    """ the main paddleflow filesystem used by pipeline. artifact of producted by step would be save in this filesystem
    """
    def __init__(
            self, 
            name: str,
            sub_path: str=None,
            mount_path: str=None,
            ):
        """ create an new instance for ExtraFS

        Args:
            name (str): the name of paddleflow filesystem
            sub_path (str): the sub path on paddleflow filesystem which would be mount on pods
            mount_path (str): the path on pod where paddleflow filesystem mount on
        """
        super().__init__(name, sub_path, mount_path)
    
    def compile(self):
        """ trans to dict 
        """ 
        result = super().compile()
        result.pop("read_only")
        return result


class FSOptions(Options):
    """ the paddleflow filesystem info which used by pipeline
    """
    COMPILE_ATTR_MAP = {
        "main_fs": "main_fs",
        "extra_fs": "extra_fs"
        }

    def __init__(
            self,
            main_fs: MainFS=None,
            extra_fs: List[ExtraFS]=None,
        ):
        """create an new instance for FSOptions

        Args:
            main_fs (MainFS): the main paddleflow filesystem used by pipeline. artifact of producted by step would be save in this filesystem
            extra_fs (List[ExtraFS]): extra paddleflow filesystem which used by step
        """
        if main_fs:
            if not isinstance(main_fs, MainFS):
                raise PaddleFlowSDKException(PipelineDSLError,
                    "FSOpitons' main_fs attribute should be an instance of MainFS")
            
            self.main_fs = main_fs
        else:
            self.main_fs = None
        
        if extra_fs:
            if not isinstance(extra_fs, List):
                extra_fs = [extra_fs]
            
            for extra in extra_fs:
                if not isinstance(extra, ExtraFS):
                    raise PaddleFlowSDKException(PipelineDSLError, 
                        "FSOpitons' extra_fs attribute should be a list of ExtraFS instance")

            self.extra_fs = extra_fs
        else:
            self.extra_fs = None

    def _validate(self):
        """ validate
        """
        if self.extra_fs:
            if not isinstance(self.extra_fs, List):
                self.extra_fs = [self.extra_fs]
        
            for fs in self.extra_fs:
                if not isinstance(fs, ExtraFS):
                    raise PaddleFlowSDKException(PipelineDSLError, 
                        "FSOpitons' extra_fs attribute should be a list of ExtraFS instance")

        if self.main_fs:
            if not isinstance(self.main_fs, MainFS):
                raise PaddleFlowSDKException(PipelineDSLError,
                    "FSOpitons' main_fs attribute should be an instance of MainFS")
