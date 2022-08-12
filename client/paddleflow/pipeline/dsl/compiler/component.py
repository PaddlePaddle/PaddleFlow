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
from paddleflow.pipeline.dsl.io_types import _LoopArgument
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
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class Component(object):
    """ Step is the basic scheduling unit in pipeline
    """
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
            cache_options (CacheOptions): the cache options of step
            condition (str): the condition. when schedule component, would be calculate condition, if the result of it is False, then the status of this component would be set "skipped"
            loop_argument (Union[List, Parameter, Artifact, str]): the loop arugment, when schedule this component, would be be traversed, and the runtime will be created once for each item in it
        Raises:
            PaddleFlowSDKException: if some args is illegal
        """
        if not validate_string_by_regex(name, COMPONENT_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of Step[{name}] is is illegal" + \
                    f"the regex used for validation is {COMPONENT_NAME_REGEX}")

        self.name = name
        self._full_name = None

        register_step_handler(self)

        self.__error_msg_prefix = f"error occurred in step[{self.name}]: "
        self.cache_options = cache_options
        self.loop_argument = loop_argument



        self.condition = condition        

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
        if not validate_string_by_regex(name, COMPONENT_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of Step[{name}] is is illegal " + \
                    f"the regex used for validation is {COMPONENT_NAME_REGEX}")
            
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
        
        # to avoid changing the value of params
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
        for cp in upstream:
            if not isinstance(cp, Component):
                err_msg = self._generate_error_msg("all upstream should be an instance of Component")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        self._dependences += upstream

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

    @property
    def full_name(self):
        """ get the full name of component
        """
        return self._full_name 

    @property
    def _set_full_name(self, parent_name: str):
        """ set the full name

        .. note:: This is not used directly by the users!!!
        """
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
        # if argument ref some parameter or artifact from other components, then we need to add input artifact or parameter
        arg = self._add_art_or_param_by_loop(argument)
        if arg is None:
            self._loop_argument = _LoopArgument(argument, self)
        else:
            self._loop_argument =_LoopArgument(arg, self)

    def _add_art_or_param_by_loop(self, loop: Any):
        """ add artifact or parameter: if condition or loop_argument reference artifact or parameter which is belong to other component
        """
        if isinstance(loop, Parameter) or isinstance(loop, Artifact):
            if loop.obj is None:
                err_msg = self._generate_error_msg(f"cannot find which components the {type(loop)}[{loop}] belong to")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
            if loop.obj == self:
                return None
            else:
                name = self._generate_art_or_param_name(loop.__class__.__name__, "loop")
                if isinstance(loop, Parameter):
                    self._set_params({name, loop})
                    return self.parameters[name]
                else:
                    self._set_inputs({name, loop})
                    return self.inputs[name]
        
        # todo: loop 最多只能有引用一个模板。
        if isinstance(loop, str):
            tpls = self._parse_template_from_string(loop)
            if tpls == 0:
                return None
            elif tpls == 1:
                if tpls[0] != loop:
                    err_msg = self._generate_error_msg(f"loop_arugment filed is not support template join with other string")
                    raise PaddleFlowSDKException(PipelineDSLError, err_msg)

                return self._add_art_or_param_by_template(loop, "loop")
            else: 
                err_msg = self._generate_error_msg(f"loop_arugment filed is not support template join with other template")
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        return None
        
    @property
    def condition(self):
        """ get the condition contribute
        """
        return self._condition

    @condition.setter
    def condition(self, condition: str):
        """ set condition attribute
        """        
        if condition and not isinstance(condition, str):
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                "the condition attribute of component should be an instance of str")

        tpls = self._parse_template_from_string(condition)
        for tpl in tpls:
            arg = self._add_art_or_param_by_template(tpl, "condition")
            condition.replace(tpl, str(arg), )
            
        self._condition = condition
        # TODO: 根据_condition 判断是否需要添加输入artifact或者parameter

    def _is_sibling(self, other_full_name: str):
        """ determine whether it is a sibling component
        """
        # TODO

    def _parse_template_from_string(self, s: str):
        """ parse parameter or artifact template from string
        """
        # Only templates in the form of {{p1}} are supported. Templates in the form of {{a.p1}} are not supported
        #TODO
    
    def _add_art_or_param_by_template(self, tpl: str, filed_type: str):
        """ add parameter or input artifact according template
        """
        # template: {{parameter: $fullname.$parameter_name}} or {{artifact: $fullname.$artifact_name}}
        # TODO
    
    def _generate_art_or_param_name(self, arg_type: str, filed_type: str):
        """ generate art 
        """
        suffix = random_code(PARAM_NAME_CODE_LEN)
        return "_".join([filed_type, arg_type, suffix])
 
    

            

        

    
def register_step_handler(component: Component):
    """ register step to Pipeline or something which composed of step
    """
    pass
