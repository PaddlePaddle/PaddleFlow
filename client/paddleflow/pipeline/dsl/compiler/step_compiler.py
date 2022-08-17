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
from .component_compiler import ComponentCompiler

from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class StepCompiler(ComponentCompiler):
    """ Compiler: trans dsl.Pipeline to static description string
    """
    def __init__(self, component):
        """ compile step
        """
        super().__init__(component)


    def compile(self):
        """ trans step to dict
        """
        super().compile()

        self._compile_base_info() 

        # compile cache_options
        self._compile_cache_options()

        # compile extra_fs
        self._compile_extra_fs()
        
        return self._dict
   
    def _compile_base_info(self):
        """ compiler base info such as command, docker_env, env
        """
        attr_to_filed = {
                "command": "command",
                "docker_env": "docker_env", 
                }

        for attr, filed in attr_to_filed.items():
            if getattr(self._component, attr, None):
                self._dict[filed] = getattr(self._component, attr, None)

        if self._component.env: 
            self._dict["env"] = dict(self._component.env)
        
    def _compile_output_artifact(self):
        """ compile output artifact
        """
        if self._component.outputs:
            self._dict.setdefault("artifacts", {})
            self._dict["artifacts"]["output"] = self._component.outputs.kes()
    
    def _compile_cache_options(self):
        """ compile cache_options
        """
        if self._component.cache_options:
            self._dict["cache"] = self._component.cache_options.compile()
        
    def _compile_extra_fs(self):
        """ compile extra_fs 
        """
        if self._component.extra_fs:
            self._dict["extra_fs"] = []

            for extra in self._component.extra_fs:
                self._dict["extra_fs"].append(extra.compile())

    def _validate(self):
        """ validate
        """
        # TODO
        pass