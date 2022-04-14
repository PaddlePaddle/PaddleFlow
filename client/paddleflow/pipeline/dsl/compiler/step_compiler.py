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

from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class StepCompiler(object):
    """ Compiler: trans dsl.Pipeline to static description string
    """
    def compile(
            self,
            step
            ):
        """ trans dsl.Pipeline to static description string

        Args:
            step (dsl.Step): the Step instances which need to trans to static description string

        Returns:
            a Dict which description this Step instance

        Raises:
            PaddleFlowSDKException: if compile failed
        """
        self._step = step
        self._step_dict = {}

        # 1. compiler base info such as command, docker_env, env and so on
        self._compile_base_info() 

        # 2. compiler artifact
        self._compile_artifact()
        
        # 4. compier params
        self._compile_params()

        # 5. compiler dependences
        self._compile_dependences()

        # 6. compiler cache_options
        self._compile_cache_options()
        
        return self._step_dict
   
    def _compile_base_info(self):
        """ compiler base info such as command, docker_env, env
        """
        attr_to_filed = {
                "command": "command",
                "docker_env": "docker_env", 
                }

        for attr, filed in attr_to_filed.items():
            if getattr(self._step, attr, None):
                self._step_dict[filed] = getattr(self._step, attr, None)

        if self._step.env: 
            self._step_dict["env"] = dict(self._step.env)

    def _compile_artifact(self):
        """ compiler artifact
        """
        # 1. compile inputs artifact
        inputs = self._compile_input_artifact()
    
        # 2. compile  outputs aritfact
        outputs = self._compile_output_artifact()

        # 3. assemble arifact
        if any([inputs, outputs]):
            self._step_dict["artifacts"] = {}

            if inputs:
                self._step_dict["artifacts"]["input"] = inputs

            if outputs:
                self._step_dict["artifacts"]["output"] = outputs
    
    def _compile_input_artifact(self):
        """ compile input artifact
        """
        inputs = {}

        if not self._step.inputs:
            return inputs

        for name, art in self._step.inputs.items():
            inputs[name] = art.compile()

        return inputs

    def _compile_output_artifact(self):
        """ compile output artifact
        """
        outputs = []

        if self._step.outputs:
            for name, _ in self._step.outputs.items():
                outputs.append(name)

        return outputs

    def _compile_params(self):
        """ compile paramter
        """
        if self._step.parameters:
            self._step_dict["parameters"] = {}
            for name, param in self._step.parameters.items():
                self._step_dict["parameters"][name] = param.compile()

    def _compile_dependences(self):
        """ compile dependences
        """
        deps  = self._step.get_dependences()
        if not deps:
            return 

        self._step_dict["deps"] = []
        for dep in deps:
            self._step_dict["deps"].append(dep.name)

        self._step_dict["deps"] = ",".join(self._step_dict["deps"])
        
    def _compile_cache_options(self):
        """ compile cache_options
        """
        if self._step.cache_options and self._step.cache_options.compile():
            self._step_dict["cache"] = self._step.cache_options.compile()
