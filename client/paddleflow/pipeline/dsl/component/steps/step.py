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
import copy
from pathlib import Path
from typing import Dict
from typing import Any 
from typing import List 
from typing import Union

from paddleflow.pipeline.dsl.component import Component
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.options import ExtraFS
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class Step(Component):
    """ Step is the basic scheduling unit in pipeline
    """
    def __init__(
            self, 
            name: str, 
            inputs: Dict[str, Artifact]=None,
            outputs: Dict[str, Artifact]=None,
            parameters: Dict[str, Any]=None,
            cache_options: CacheOptions=None,
            condition: str=None,
            loop_argument: Union[List, Parameter, Artifact, str]=None,
            extra_fs: List[ExtraFS] = None
            ):
        """ create a new instance of Step

        Args:
            name (str): the name of Step
            inputs (Dict[str, Artifact]): input artifact, the key is the name of artifact, and the value should be upstream Step's output artifact. 
            outputs (Dict[str, Artifact]): output artifact, the key is the name of artifact, and the value should be an instance of Artifact
            parameters (str, Any): Parameter of step, the key is the name of this parameter, and the value could be int, string, Paramter, or upstream Step's Parameter
            cache_options (CacheOptions): the cache options of step
            condition (str): the condition. when schedule component, would be calculate condition, if the result of it is False, then the status of this component would be set "skipped"
            loop_argument (Union[List, Parameter, Artifact, str]): the loop arugment, when schedule this component, would be be traversed, and the runtime will be created once for each item in it
            extra_fs (List[ExtraFS]): the paddleflow filesystem used by Step
        Raises:
            PaddleFlowSDKException: if some args is illegal
        """
        super().__init__(name, inputs, outputs, parameters, condition, loop_argument)
        self._type = "step"

        self.cache_options = cache_options


        if extra_fs and not isinstance(extra_fs, list):
            extra_fs = [extra_fs]
        
        if isinstance(extra_fs, list):
            self.extra_fs = []
            for extra in extra_fs:
                if not isinstance(extra, ExtraFS):
                    raise PaddleFlowSDKException(PipelineDSLError, 
                        self._generate_error_msg("Step's extra_fs attribute should be a list of ExtraFS instance"))

                self.extra_fs.append(extra)
        else:
            self.extra_fs = []