#!/usr/bin/env python3
"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
steps/step.pydistributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import Dict
from typing import List
from typing import Any 
from pathlib import Path
from typing import Union

from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types import EnvDict
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.options import ExtraFS
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import COMPONENT_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


from .step import Step

class ContainerStep(Step):
    """ ContainerStep: Step based on docker image
    """
    def __init__(
            self, 
            name: str, 
            docker_env: str=None,
            command: str=None,
            inputs: Dict[str, Artifact]=None,
            outputs: Dict[str, Artifact]=None,
            parameters: Dict[str, Any]=None,
            env: Dict[str, str]=None,
            cache_options: CacheOptions=None,
            condition: str=None,
            loop_argument: Union[List, Parameter, Artifact, str]=None,
            extra_fs: List[ExtraFS] = None
            ):
        """ create a new instance of ContainerStep

        Args:
            name (str): the name of ContainerStep
            docker_env (str): the address of docker image for executing Step
            command (str): the command of step to execute
            inputs (Dict[str, Artifact]): input artifact, the key is the name of artifact, and the value should be upstream Step's output artifact. 
            outputs (Dict[str, Artifact]): output artifact, the key is the name of artifact, and the value should be an instance of Artifact
            parameters (str, Any): Parameter of step, the key is the name of this parameter, and the value could be int, string, Paramter, or upstream Step's Parameter
            env (Dict[str, str]): enviroment varible for Step runtime
            cache_options (CacheOptions): the cache options of step

        Raises:
            PaddleFlowSDKException: if some args is illegal
        """
        super().__init__(
                name=name,
                inputs=inputs,
                outputs=outputs,
                parameters=parameters,
                cache_options=cache_options,
                condition=condition,
                loop_argument=loop_argument,
                extra_fs=extra_fs
                )
                
        self.docker_env = docker_env
        self.command = command

        self._env = EnvDict(self)
        self.add_env(env)


    @property
    def env(self):
        """ get env

        Returns:
            a dict while the key is the name of env and the value is the value of env
        """
        return self._env

    def add_env(
            self,
            env: Dict[str, str]
            ):
        """ add enviroment varible

        Args:
            env (Dict[str, str]): enviroment varible need to be added when executing Step.

        Raises:
            PaddleFlowSDKException: if some enviroment is illegal
        """
        if not env:
            return 

        for name, value in env.items():
            self._env[name] = value
