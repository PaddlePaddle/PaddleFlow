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

from paddleflow.pipeline.dsl.component import Component
from paddleflow.pipeline.dsl.component import DAG
from paddleflow.pipeline.dsl.component import Step
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types.loop_argument import _LoopItem
from paddleflow.pipeline.dsl.utils.util import random_code
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.pipeline.dsl.utils.consts import PARAM_NAME_CODE_LEN
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class ComponentCompiler(object):
    """ the base compiler of component
    """
    def __init__(self, component: Component):
        """ create an instance of DAGcompile

        Args:
            parent: the parent DAG of component

        """
        self._component = component
        self._dict = {}
        self._error_msg_prefix = f"compile {component.__class__.__name__}[{component.name}] failed: "

    def _generate_error_msg(self, msg: str):
        """ generate error msg

        Args:
            msg: base error msg

        """
        return self._error_msg_prefix + msg

    def compile(self):
        """ trans component to dict
        """
        # 0. validate
        self.validate_io_names()

        # 1. deps
        if self._component._dependences:
            deps = list(self._component._dependences)
            deps.sort()

            self._dict["deps"] = ",".join(deps)

        # 2. condition 
        if self._component.condition is not None:
            self._dict["condition"] = self._component.condition
    
        # 3. loop_argument
        if self._component.loop_argument is not None:
            arg = self._component.loop_argument.argument
            if isinstance(arg, Parameter) or isinstance(arg, Artifact):
                self._dict["loop_argument"] = "{{" + arg.name + "}}"
            else:
                self._dict["loop_argument"] = arg

        # 4. type
        self._dict["type"] = self._component.type
        
        # 5. parameter
        self._compile_parameters()

        # 6. artifact
        self._compile_artifacts()

        # 7. validate
        self._validatte()

    def _compile_artifacts(self):
        """ compile artifacts
        """
        self._compile_input_artifact()
        self._compile_output_artifact()

    def _compile_input_artifact(self):
        """ compile input artifact
        """
        if self._component.inputs:
            self._dict["artifacts"] = {"input": {}}
            inputs = self._dict["artifacts"]["input"]

            for name, art in self._component.inputs.items():
                ref_component = art.ref.component

                if not self._is_parent(ref_component.full_name):
                    if art.ref.name not in ref_component.outputs:
                        raise PaddleFlowSDKException(PipelineDSLError,
                            self._generate_error_msg(f"there is no output artifact named[{art.ref.name}] in " + \
                                f"{ref_component.class_name()}[{ref_component.full_name}]"))
                else:
                    if art.ref.name not in ref_component.inputs:
                        raise PaddleFlowSDKException(PipelineDSLError,
                            self._generate_error_msg(f"there is no input artifact named[{art.ref.name}] in " + \
                                f"{ref_component.class_name()}[{ref_component.full_name}]"))

                if self._is_parent(art.ref.component.full_name):
                    inputs[name] = "{{PF_PARENT." + art.ref.name + "}}"
                else:
                    inputs[name] = "{{" + f"{art.ref.component.name}.{art.ref.name}" + "}}"

    def _compile_output_artifact(self):
        """ compile output artifact
        """
        raise NotImplementedError
    
    def _compile_parameters(self):
        """ compile parameters
        """
        if self._component.parameters:
            self._dict["parameters"] = {}

            for name, param in self._component.parameters.items():
                if isinstance(param.ref, Parameter):
                    ref_component = param.ref.component
                    if param.ref.name not in ref_component.parameters:
                        raise PaddleFlowSDKException(PipelineDSLError,
                            self._generate_error_msg(f"there is no parameter named[{param.ref.name}] in " + \
                                f"{ref_component.class_name()}[{ref_component.full_name}]"))

                    if self._is_parent(param.ref.component.full_name):
                        self._dict["parameters"][name] = "{{PF_PARENT." + param.ref.name + "}}"
                    else:
                        self._dict["parameters"][name] = "{{" + f"{param.ref.component.name}.{param.ref.name}" + "}}"
                elif isinstance(param.ref, _LoopItem):
                    if param.ref.component is self._component:
                        raise PaddleFlowSDKException(PipelineDSLError, 
                            self._generate_error_msg("component cannot reference its loop item as its parameter"))
                    
                    self._dict["parameters"][name] = "{{PF_PARENT.PF_LOOP_ARGUMENT}}"
                elif param.ref is not None:
                    self._dict["parameters"][name] = param.ref
                else:
                    if param.default is not None:
                        self._dict["parameters"].setdefault(name, {})["default"] = param.default
                    if param.type is not None:
                        self._dict["parameters"].setdefault(name, {})["type"] = param.type


    def _is_parent(self, component_full_name:str):
        """ is parent
        """
        return self._component.full_name.startswith(component_full_name + ".")

    def _validatte(self):
        """ validate
        """
        self.validate_io_names()

    def validate_io_names(self):
        """ Ensure that the input / output aritact and parameter of the step have different names

        Raises:
            PaddleFlowSDKException: if the input / output artifact or parameter has the same name
        """
        inputs_names = set(self._component.inputs.keys())
        outputs_names = set(self._component.outputs.keys())
        params_names = set(self._component.parameters.keys())

        if len(inputs_names | outputs_names | params_names) != \
                len(inputs_names) + len(outputs_names) + len(params_names):
            dup_names = (inputs_names & outputs_names) | (inputs_names & params_names) | (outputs_names & params_names)

            err_msg = self._generate_error_msg(f"the input/output aritacts and parameters of the same Step or DAG " + \
                    f" should have different names, duplicate name is [{dup_names}]")
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
