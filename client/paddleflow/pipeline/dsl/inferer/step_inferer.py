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
from typing import Dict

from .component_inferer import ComponentInferer

from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class ContainerStepInferer(ComponentInferer):
    """ infer parameter and artifact for Step from it's fileds
    """
    FILED_TO_PREFIX = {
        "loop_argument": "loop",
        "condition": "condition",
        "command": "command",
        "env": "env"
    }

    def __init__(self, step):
        """ create a new instance of StepInfer
        """
        super().__init__(step)

    def infer(self, env: Dict):
        """ infer artifact and parameter for step
        """     
        self._infer_from_command()
        self._infer_env(env)
        self._infer_from_env()
        self._infer_from_loop_argument()
        self._infer_from_condition()
        self._infer_from_artifact()
        self._infer_from_parameter()
        self._infer_deps()

    def _infer_env(self, env: Dict):
        """ infer env for containerstep
        """
        for key, value in env.items():
            if key not in self._component.env:
                self._component.env[key] = value

    def _infer_from_env(self):
        """ infer artifact and parameter from env
        """
        for key, value in self._component.env.items():
            tpls = self._parse_template_from_string(value)
            for tpl in tpls:
                arg = self._infer_by_template(tpl, self.FILED_TO_PREFIX["env"])
                if isinstance(arg, str):
                    self._component.env[key] = self._component.env[key].replace(tpl.group(), arg)
                elif isinstance(arg, Parameter): 
                    self._component.env[key] = self._component.env[key].replace(tpl.group(), "{{" + arg.name + "}}")
                elif isinstance(arg, Artifact):
                    raise PaddleFlowSDKException(PipelineDSLError, 
                        self._generate_error_msg(f"env cannot reference artifact"))
        
    def _infer_from_command(self):
        """  infer artifact and parameter from command field
        """
        tpls = self._parse_template_from_string(self._component.command)
        for tpl in tpls:
            arg = self._infer_by_template(tpl, self.FILED_TO_PREFIX["command"])
            if isinstance(arg, str):
                self._component.command = self._component.command.replace(tpl.group(), arg)
            else:
                self._component.command = self._component.command.replace(tpl.group(), "{{" + arg.name + "}}")

