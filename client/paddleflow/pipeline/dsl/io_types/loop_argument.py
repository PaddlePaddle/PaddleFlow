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
import re
import json
from typing import List 
from typing import Union

from .parameter import Parameter
from .parameter import TYPE_TO_STRING as PARAM_SUPPORT_TYPE
from .artifact import Artifact

from paddleflow.pipeline.dsl.sys_params import PF_LOOP_ARGUMENT
from paddleflow.pipeline.dsl.utils.consts import DSL_TEMPLATE_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class _LoopItem(object):
    """ used to refer to item in loop_argument, 
    
    .. note:: should not be created by user
    """
    def __init__(self, component):
        """ create an new instance of _LoopItem

        Args:
            obj (Component): which component this instance is belong to
        """
        self.component = component

    def __str__(self):
        """ magic func for str
        """
        return "{{" + f"loop: {self.component.full_name}.{PF_LOOP_ARGUMENT[2:-2]}" + "}}"


class _LoopArgument(object):
    """ loop arguemnt of step

        .. note:: should not be created by user
    """
    SUPPORT_TYPE = [list, Artifact, Parameter, _LoopItem, str]

    def __init__(
            self,
            argument,
            component,
            ):
        """ create an new instance of LoopArgument

        Args:
            argument (Union[List, Parameter, Artifact, str]) : the arument which would be traversed
            obj (component): the component which this instance is belong to
        """
        self._prefix = f"the loop_argument for step[{component.name}]is illegal: "
        self.component = component

        self._validate(argument)
        self._argument = argument

        self.item = _LoopItem(component)

    def _validate(self, argument):
        """ validate argument
        """
        err_msg = f"the type of loop_argument should be in [list, Artifact, Parameter, json list, _LoopItem], " + \
            f"but now is {type(argument)}"
        err_msg = self._prefix + err_msg

        if isinstance(argument, str):
            # 考虑是否为参数模板， 这里不对模板的对应的参数是否存在做校验，由server侧保证
            pattern = re.compile(DSL_TEMPLATE_REGEX)
            match = re.match(pattern, argument)
            if match:
                if match.group("type") == "loop" and match.group("component_full_name") == self.component.full_name:
                    raise PaddleFlowSDKException(PipelineDSLError, self._prefix + \
                        "loop argument cannot referece loop item which product by itself")
                else:
                    return

            try:
                argument = json.loads(argument)
            except Exception as e:
                raise PaddleFlowSDKException(PipelineDSLError, err_msg + f" error: {e}")
            
            if not isinstance(argument, list):
                raise PaddleFlowSDKException(PipelineDSLError, self._prefix + \
                    "when the type of loop_arugment is string, then it should an json list " + \
                        "or template of Parameter or Artifact")
        
        if isinstance(argument, list):
            for arg in argument:
                if type(arg) not in PARAM_SUPPORT_TYPE:
                    raise PaddleFlowSDKException(PipelineDSLError, self._prefix + \
                        "when the type of loop_arugment is [list, json list], " + \
                        f"the type of item should be in {PARAM_SUPPORT_TYPE}")
            
            return 
        
        if type(argument) not in self.SUPPORT_TYPE:
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
            
        if hasattr(argument, "component"):
            if getattr(argument, "component") is None:
                raise PaddleFlowSDKException(PipelineDSLError, self._prefix + \
                    "loop argument cannot reference [Aritfact, Parameter, " + \
                        "LoopItem] which is not belong to any Step or DAG")

        if isinstance(argument, _LoopItem):
            if argument.component == self.component:
                raise PaddleFlowSDKException(PipelineDSLError, self._prefix + \
                        "loop argument cannot referece loop item which product by itself")

    @property
    def argument(self):
        """ get argument
        """
        return self._argument