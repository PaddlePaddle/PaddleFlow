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

from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types.dicts import InputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import OutputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import ParameterDict
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import STEP_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class Step(object):
    """ Step is the basic scheduling unit in pipeline
    """
    def __init__(
            self, 
            name: str, 
            inputs: Dict[str, Artifact]=None,
            outputs: Dict[str, Artifact]=None,
            parameters: Dict[str, Any]=None,
            cache_options: CacheOptions=None,
            ):
        """ create a new instance of Step

        Args:
            name (str): the name of Step
            inputs (Dict[str, Artifact]): input artifact, the key is the name of artifact, and the value should be upstream Step's output artifact. 
            outputs (Dict[str, Artifact]): output artifact, the key is the name of artifact, and the value should be an instance of Artifact
            parameters (str, Any): Parameter of step, the key is the name of this parameter, and the value could be int, string, Paramter, or upstream Step's Parameter
            cache_options (CacheOptions): the cache options of step
        Raises:
            PaddleFlowSDKException: if some args is illegal
        """
        if not validate_string_by_regex(name, STEP_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of Step[{name}] is is illegal" + \
                    f"the regex used for validation is {STEP_NAME_REGEX}")

        self.name = name
        register_step_handler(self)

        self.__error_msg_prefix = f"error occurred in step[{self.name}]: "
        self.cache_options = cache_options

        self._dependences = []

        self._set_inputs(inputs)
        self._set_outputs(outputs)
        self._set_params(parameters)
        
    @property
    def name(self):
        """ get the name of step

        Returns:
            a string which indicate the name of this step
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """ set the name of step

        Args:
            name (str): the name of step
        Raises:
            PaddleFlowPaddleFlowSDKExceptionSDKException: if name is illegal
        """
        if not validate_string_by_regex(name, STEP_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of Step[{name}] is is illegal " + \
                    f"the regex used for validation is {STEP_NAME_REGEX}")
            
        self._name = name
        
    def _generate_error_msg(self, msg: str):
        """ generate error msg

        Args:
            msg: base error msg

        """
        return self.__error_msg_prefix + msg
    
    @property
    def inputs(self):
        """ get inputs arifacts
        
        Returns:
            an instance of InputArtifactDict which manager the input artifacts of this step
        """
        return self._inputs

    def _set_inputs(self, inputs: Dict[str, Artifact]):
        """ set inputs artifact

        Args:
            inputs: input artifact which need to set

        Raises:
            PaddleFlowSDKException: if the value of inputs is not upstream Steps's output artifact or the key is illegal
        """
        # to ensure self._input flexibility and legitimacy, so use InputArtifactDict
        # flexibility: user can change self._input content at any time and anywhere
        # legitimacy: all value of self._input should be an output artifact of upstream step
        self._inputs = InputArtifactDict(self)
        if not inputs:
            return 

        if inputs and not isinstance(inputs, Dict):
            err_msg = self._generate_error_msg("the inputs of step should be an instance of Dict, " + \
                    "and the value should be the output artifact of upstream Step")
            raise PaddleFlowSDKException(PipelineDSLError, err_msg) 

        for name, art in inputs.items():
            self._inputs[name] =  art

    @property
    def outputs(self):
        """ get output artifact 
        
        Returns:
            an instance of OutputArtifact which manager the output aritfacts of this step
        """
        return self._outputs


    def _set_outputs(self, outputs: Dict[str, Artifact]):
        """ set outputs artifact

        Args:
            outputs (Dict[str, Artifact]): output artifacts which need to set

        Raises:
            PaddleFlowSDKException: if the value of outputs is not equal to Artifact() or the key is illegal
        """
        # same reason as self._inputs
        self._outputs = OutputArtifactDict(self)

        if not outputs:
            return 

        if outputs and not isinstance(outputs, Dict):
            err_msg = self._generate_error_msg("outputs of Step should be an instance of Dict")
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        # OutputArtifactDict would change the value of outputs, so we need deepcopy it to support reuse
        outputs = copy.deepcopy(outputs)
        for name, art in outputs.items():
            self._outputs[name] = art

    @property
    def parameters(self):
        """ get params

        Returns:
            an instance of ParameterDict which manager the parameters of this step
        """
        return self._params 

    def _set_params(self, params: Dict[str, Any]):
        """ set parameters

        Args:
            outputs (Dict[str, Any]): parameters which need to set

        Raises:
            PaddleFlowSDKException: if the value or the key is illegal
        """
        # same reason as input 
        self._params = ParameterDict(self)
        if not params:
            return 

        if params and not isinstance(params, Dict):
            err_msg = self._generate_error_msg("the params of step should be an instance of Dict")
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        # to avoid changing the value of params
        params = copy.deepcopy(params)
        for key, value in params.items():
            self._params[key] = value
    
    def after(
            self, 
            *upstream_steps):
        """ specify upstream Step

        Args:
            *upstream_steps (List[Step]): a list of upstream Step. 
        
        Returns:
            An instances of Step which indicate itself

        Raises:
            PaddleFlowSDKException: if any upsream step is not an instance of Step
        """
        for step in upstream_steps:
            if not isinstance(step, Step):
                err_msg = self._generate_error_msg("all upstream step should be an instance of Step")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        self._dependences += upstream_steps

        return self
    
    def get_dependences(self):
        """ get upstream Steps

        Returns:
            a list of upstream Steps
        """
        # 1. get deps from self.dependces
        deps = set(self._dependences)

        # 2. parse deps from input artifact
        for art in self.inputs.values():
            deps.add(art.step)

        # 3. parse deps from parameter
        for param in self.parameters.values():
            if param.ref and isinstance(param.ref, Parameter):
                deps.add(param.ref.step)

        self._dependences = list(deps)
        return self._dependences

    def validate_io_names(self):
        """ Ensure that the input / output aritact and parameter of the step have different names
        
        Raises:
            PaddleFlowSDKException: if the input / output artifact or parameter has the same name
        """
        inputs_names = set(self.inputs.keys())
        outputs_names = set(self.outputs.keys())
        params_names = set(self.parameters.keys())

        if len(inputs_names | outputs_names | params_names) != \
                len(inputs_names) + len(outputs_names) + len(params_names):
            dup_names = (inputs_names & outputs_names) | (inputs_names & params_names) | (outputs_names & params_names)
            
            err_msg = self._generate_error_msg(f"the input/output aritacts and parameters of one " + \
                    f"Step should have different names, duplicate name is [{dup_names}]")
            raise PaddleFlowSDKException(PipelineDSLError, err_msg) 


def register_step_handler(step: Step):
    """ register step to Pipeline or something which composed of step
    """
    pass
