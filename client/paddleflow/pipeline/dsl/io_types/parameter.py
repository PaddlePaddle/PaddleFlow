#!/usr/bin/env python3
"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
param = Parameter()you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# Parameter: Parameters are inputs to pipelines that are known before your pipeline is executed

from typing import Dict
from typing import Any

from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


TYPE_TO_STRING = {str: "string", int: "int", float: "float"}
STRING_TO_TYPE = {value: key for key, value in TYPE_TO_STRING.items()}
SUPPORT_TYPE = STRING_TO_TYPE.keys() 


class Parameter(object):
    """ Parameters are inputs to pipelines that are known before your pipeline is executed, Parameters let you change the behavior of a step, through configuration instead of code.
    """
    def __init__(
            self,
            default: Any=None,
            type: str=None,
            ):
        """ create a new instance of Paramter
        
        Args:
            type (str): the type of Parameter, type check before execution 
            default (Any): the default or Paramter

        Raise:
            PaddleFlowSDKException: if all of "type" and "default" are None or "type" is not supported or "type" and "default" are not match
        """
        if type and not isinstance(type, str):
            self._type = self._trans_type_to_str(type)
        else:
            self._type = type
        
        self._default = default
        if all([self._type, self._default]):
            self._default = self._validate_type(self._type, self._default)
    
        # if __ref param = Parameter()is not None, means other parameters are referenced
        self.__ref = None
        
        self.__step = None
        self.__name = None
    
    def _trans_type_to_str(self, t):
        """ trans type to str 

        Args:
            t (type): the type need to trans to str
            
        Raises:
            PaddleFlowSDKException: if t is not supported
        """
        if t not in TYPE_TO_STRING:
            raise PaddleFlowSDKException(PipelineDSLError, "the type of Parameter only support {SUPPORT_TYPE}")    

        return TYPE_TO_STRING[t]

    def _validate_type(self, t: str, value: Any=None):
        """ validate the type of it and the value
        """
        if not isinstance(value, STRING_TO_TYPE[t]):
            try:
                return STRING_TO_TYPE[t](value)
            except Exception as e:
                raise PaddleFlowSDKException(PipelineDSLError, "the type of Parameter is not match the default value of it")
        
        return value

    def set_base_info(self, name: str, step, ref: Any=None):
        """ set the step that this paramter instances was belong to and set the name of it

        Args:
            step (Step): the step that this paramter instances was belong to
            name (str): the name of it
            ref (Any): the refrence parameter

        Raises:
            PaddleFlowSDKException: if the name is illegal
        """
        self.__step = step
        
        if not validate_string_by_regex(name, VARIBLE_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of parameter[{name}] for step[{step.name}]is illegal, " + \
                    f"the regex used for validation is {VARIBLE_NAME_REGEX}")
            
        self.__name = name 
        self.__ref = ref

    def compile(self):
        """ trans to dict while be invoked at compile stage

        Returns:
            a dict/string which can describe it

        Raises:
            PaddleFlowSDKException: if the ref attribute and the [default, type] attribute exist at the same time
        """
        if self.__ref and any([self.default, self.type]):
            raise PaddleFlowSDKException(PipelineDSLError, "the  parameter[{self._name}] for step[{step.name}]" + \
                    "have both [ref] attribute and [default | type] attribute, please contact manager")
        
        if isinstance(self.__ref, Parameter):
            return self.__ref.to_template()
        elif self.__ref:
            return self.__ref

        dicts = {}
        if self._default:
            dicts.update({"default": self._default})

        if self._type:
            dicts.update({"type": self._type})
        
        if dicts:
            return dicts
        
        return  ""

    def to_template(self):
        """ trans to template when downstream step ref this Parameter

        Returns:
            A string indicate the template of it
        """
        return "{{" + self.__step.name + "." + self.__name + '}}'

    @property
    def step(self):
        """ get the step step that this paramter instances was belong to

        Returns:
            A Step that it was belong to
        """
        return self.__step

    @property
    def name(self):
        """ get the name of it
        """
        return self.__name
    
    @property
    def type(self):
        """ get the type of it
        """
        return self._type
    
    @property
    def default(self):
        """ get the default value of it

        Returns:
            the default value of it
        """
        return self._default
    
    @default.setter
    def default(self, value: Any):
        """ set the default value of it

        Args:
            value (Any): the value to set the default of it
        
        Raises:
            PaddleFlowSDKException: if the value is not match the type of it 
        """
        if self._type:
            self._default = self._validate_type(self._type, value)
        else:
            self._default = value
        
        self.__ref = None

    @property
    def ref(self):
        """ get refrence

        Returns:
            the refrence of this instance
        """
        return self.__ref

    def __deepcopy__(self, memo):
        """ support copy.deepcopy
        """
        param = Parameter(type=self.type, default=self.default)
        if self.name:
            param.set_base_info(step=self.step, name=self.name, ref=self.ref)

        return param

    def __eq__(self, other):
        """ support  == and  != 
        """
        return self.name == other.name and self.type == other.type and self.default == other.default and \
                self.ref == other.ref and self.step == other.step
