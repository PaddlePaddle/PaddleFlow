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
# refrence: https://stackoverflow.com/questions/3137685/using-property-decorator-on-dicts
from typing import Any

from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

from .artifact import Artifact 
from .parameter import Parameter

class ParameterDict(dict):
    """ ParameterDict: an dict for manager Parameter
    """
    def __init__(self, step):
        """ create an new ParameterDict instance
        
        Args:
            step (Step): the step which owner this ParameterDict instance 
        """
        self.__step = step

    def __setitem__(self, key: str, value: Any):
        """ magic function __setitem__
        """
        if isinstance(value, Parameter) and value.step is None:
                # it means this Parameter don't reference anything
                value.set_base_info(step=self.__step, name=key)
        else:
            # it means value is defined by other Step or inner type
            param = Parameter()
            param.set_base_info(step=self.__step, name=key, ref=value)
            value = param

        super().__setitem__(key, value)


class InputArtifactDict(dict):
    """ InputArtifactDict: an dict for manager input artifact
    """
    def __init__(self, step):
        """ create an new InputArtifactDict instance

        Args:
            step (Step): the step which owner this InputArtifactDict instance
        """
        self.__step = step

    def __setitem__(self, key: str, value: Artifact):
        """ magic function __setitem__
        """
        if not isinstance(value, Artifact) or value.step is None:
            err_msg = f"the value of inputs for Step[{self.__step.name}] should be an output artifact of other Step"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        if not validate_string_by_regex(key, VARIBLE_NAME_REGEX):
            err_msg = f"the name of inputs artifacts[{key}] for Step[{self.__step.name}] is is illegal" + \
                f"the regex used for validation is {VARIBLE_NAME_REGEX}"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        super().__setitem__(key, value)


class OutputArtifactDict(dict):
    """ OutputArtifactDict: an dict for manager output artifact
    """
    def __init__(self, step):
        """ create an new OutputArtifactDict instance

        Args:
            step (Step): the step which owner this OutputArtifactDict instance
        """
        self.__step = step

    def __setitem__(self, key: str, value: Artifact):
        """ magic function __setitem__
        """
        if not isinstance(value, Artifact) or value.step is not None:
            err_msg = f"the value of outputs[{key}] for step[{self.__step.name}] just can be Artifact()"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        value.set_base_info(step=self.__step, name=key)
        super().__setitem__(key, value)


class EnvDict(dict):
    """ To manager envrionment varible of step or pipeline
    """
    def __init__(self, obj: Any):
        """ create an new EnvDict instance

        Args:
            obj (Any): the step which owner this OutputArtifactDict instance
        """
        self.__obj = obj
    
    def __setitem__(self, key:str, value: str):
        """ magic function __setitem__
        """
        
        if not isinstance(key, str):
            err_msg = f"the name of envrionment varible of obj[{self.__obj.name}] should be an instance of string," + \
                f" but it is{type(key)}"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        if not isinstance(value, str):
            err_msg = f"the value of env[{key}] of obj[{self.__obj.name}] should be an instances of string"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        if not validate_string_by_regex(key, VARIBLE_NAME_REGEX):
                err_msg =  f"the name of env[{key}] of obj[{self.__obj.name}] is illegal, the regex used for validation" + \
                    f" is {VARIBLE_NAME_REGEX}"
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        super().__setitem__(key, value)
