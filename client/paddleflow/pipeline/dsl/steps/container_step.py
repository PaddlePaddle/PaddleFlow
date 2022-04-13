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

from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.util import CaseSensitiveConfigParser 
from paddleflow.pipeline.dsl.utils.consts import STEP_NAME_REGEX
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
            image: str=None,
            command: str=None,
            inputs: Dict[str, Artifact]=None,
            outputs: Dict[str, Artifact]=None,
            params: Dict[str, Any]=None,
            env: Dict[str, str]=None,
            cache_options: CacheOptions=None,
            ):
        """ create an new instance of ContainerStep

        Args:
            name (str): the name of ContainerStep
            image (str): the address of docker image for executing Step
            command (str): the command of step to execute
            inputs (Dict[str, Artifact]): input artifact, the key is the name of artifact, and the value should be upstream Step's output artifact. 
            outputs (Dict[str, Artifact]): output artifact, the key is the name of artifact, and the value should be an instance of Artifact
            params (str, Any): Parameter of step, the key is the name of this parameter, and the value could be int, string, Paramter, or upstream Step's artifact
            env (Dict[str, str]): enviroment varible for Step runtime
            cache_options (cache_options): the cache options of step

        Raises:
            PaddleFlowSDKException: if some args is illegal
        """
        self.image = image
        self.command = command

        self._env = {} 
        self.add_env(env)

        super().__init__(name=name, inputs=inputs, outputs=outputs, params=params, cache_options=cache_options)

    @property
    def env(self):
        """ get env

        Returns:
            a dict while the key is the name of env and the value is the value of env
        """
        return self._env

    def add_env_from_file(
            self,
            file: str):
        """ add env from file

        Args:
            file (str): the path of file in ini format which has env section

        Raises:
            PaddleFlowSDKException: if the file is not exists or the format of the file is error
        """
        if not Path(file).is_file():
            err_msg = self._generate_error_msg(f"the file[{file}] is not exists or it's not a file")
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        config = CaseSensitiveConfigParser() 
        env = {}
        try:
            config.read(file)
            for key, value in config.items("env"):
                env[key] = value
            self.add_env(env)
        except PaddleFlowSDKException as e:
            raise e 
        except Exception as e:
            err_msg = self._generate_error_msg(
                    f"The file[{file}] needs to be in ini format and have [env] section")
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

    def add_env(
            self,
            env: Dict[str, str]
            ):
        """ add enviroment varible

        Args:
            env (Dict[str, str]): enviroment varible need to be set when executing Step.

        Raises:
            PaddleFlowSDKException: if some enviroment is illegal
        """
        if not env:
            return 

        for name, value in env.items():
            if not validate_string_by_regex(name, VARIBLE_NAME_REGEX):
                err_msg = self._generate_error_msg(
                        f"the name of env[{name}] is illegal, the regex used for validation is {VARIBLE_NAME_REGEX}")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)

            try:
                value = str(value)
            except Exception as e:
                err_msg = self._generate_error_msg(
                        f"the value of env[{name}] should be an instances of string")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)

            self._env[name] = value
