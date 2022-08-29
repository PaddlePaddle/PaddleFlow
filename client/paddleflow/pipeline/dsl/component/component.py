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

from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types.loop_argument import _LoopArgument
from paddleflow.pipeline.dsl.io_types.dicts import InputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import OutputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import ParameterDict
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.util import random_code
from paddleflow.pipeline.dsl.utils.consts import COMPONENT_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PARAM_NAME_CODE_LEN
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import ENTRY_POINT_NAME
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

def register_component_handler(component):
    """ register step to Pipeline or something which composed of step
    """
    pass

class Component(object):
    """ Step is the basic scheduling unit in pipeline
    """

    REGISTER_HANDLER = register_component_handler
    def __init__(
            self, 
            name: str, 
            inputs: Dict[str, Artifact]=None,
            outputs: Dict[str, Artifact]=None,
            parameters: Dict[str, Any]=None,
            condition: str=None,
            loop_argument: Union[List, Parameter, Artifact, str]=None
            ):
        """ create a new instance of Step

        Args:
            name (str): the name of Step
            inputs (Dict[str, Artifact]): input artifact, the key is the name of artifact, and the value should be upstream Step's output artifact. 
            outputs (Dict[str, Artifact]): output artifact, the key is the name of artifact, and the value should be an instance of Artifact
            parameters (str, Any): Parameter of step, the key is the name of this parameter, and the value could be int, string, Paramter, or upstream Step's Parameter
            condition (str): the condition. when schedule component, would be calculate condition, if the result of it is False, then the status of this component would be set "skipped"
            loop_argument (Union[List, Parameter, Artifact, str]): the loop arugment, when schedule this component, would be be traversed, and the runtime will be created once for each item in it
            condition (str): the condition. when schedule component, would be calculate condition, if the result of it is False, then the status of this component would be set "skipped"
            loop_argument (Union[List, Parameter, Artifact, str]): the loop arugment, when schedule this component, would be be traversed, and the runtime will be created once for each item in it
            extra_fs (List[ExtraFS]): the paddleflow filesystem used by Step
        Raises:
            PaddleFlowSDKException: if some args is illegal
        """
        if not validate_string_by_regex(name, COMPONENT_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of {self.class_name()}[{name}] is is illegal" + \
                    f"the regex used for validation is {COMPONENT_NAME_REGEX}")

        self.name = name
        self._full_name = ""
        self._type = "component"

        register_component_handler(self)

        if self.name != ENTRY_POINT_NAME:
            self._error_msg_prefix = f"error occurred in {self.class_name()}[{self.name}]: "
        else:
            self._error_msg_prefix = f"error occurred in Pipeline: "
        self.loop_argument = loop_argument
        self.condition = condition        

        self._dependences = set()
        self._io_names = []

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
        if not validate_string_by_regex(name, COMPONENT_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of Dag[{name}] is is illegal " + \
                    f"the regex used for validation is {COMPONENT_NAME_REGEX}")
            
        self._name = name
        
    def _generate_error_msg(self, msg: str):
        """ generate error msg

        Args:
            msg: base error msg

        """
        return self._error_msg_prefix + msg
    
    @property
    def type(self):
        """ get the component type
        """
        return self._type

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
        # legitimacy: all value of self._input should be an output artifact of upstream step or an input artifact from it's parent
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
        
        params = copy.deepcopy(params)
        for key, value in params.items():
            self._params[key] = value
    
    def after(
            self, 
            *upstream):
        """ specify upstream Step

        Args:
            *upstream (List[Component]): a list of upstream Step. 
        
        Returns:
            An instances of Step which indicate itself

        Raises:
            PaddleFlowSDKException: if any upsream step is not an instance of Step
        """
        if not upstream:
            raise PaddleFlowSDKException(PipelineDSLError, 
                self._generate_error_msg("in after(), at leaset one upstream Step or DAG needs to be set"))

        for cp in upstream:
            if not isinstance(cp, Component):
                err_msg = self._generate_error_msg("all upstream should be an instance of Component")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
            self._dependences.add(cp.full_name)

        return self

    @property
    def full_name(self):
        """ get the full name of component
        """
        if self._full_name == "":
            self._full_name = self.name
            
        return self._full_name 

    
    def _set_full_name(self, parent_name: str):
        """ set the full name

        .. note:: This is not used directly by the users!!!
        """
        # if not parent_name or parent_name == ENTRY_POINT_NAME:
        if not parent_name:
            self._full_name = self.name
        else:            
            self._full_name = ".".join([parent_name, self.name])

    @property
    def loop_argument(self):
        """ get the loop_arugment attribute
        """
        return self._loop_argument

    @loop_argument.setter
    def loop_argument(self, argument):
        """ set loop_argument
        """
        if argument is None:
            self._loop_argument = None
        else:
            self._loop_argument = _LoopArgument(argument, self)
        
    @property
    def condition(self):
        """ get the condition contribute
        """
        return self._condition

    @condition.setter
    def condition(self, condition: str):
        """ set condition attribute
        """
        if condition is not None and not isinstance(condition, str):
            raise PaddleFlowSDKException(PipelineDSLError,
                self._generate_error_msg("the condition attribute of component should be an instance of str"))
            
        self._condition = condition
    
    def class_name(self):
        """ get __class__.__name__
        """
        return self.__class__.__name__