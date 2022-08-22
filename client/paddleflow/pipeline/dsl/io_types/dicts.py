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
from typing import Union

from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

from .artifact import Artifact
from .parameter import Parameter
from .placeholder import ArtifactPlaceholder
from .placeholder import ParameterPlaceholder

class ParameterDict(dict):
    """ ParameterDict: an dict for manager Parameter
    """
    def __init__(self, component):
        """ create an new ParameterDict instance
        
        Args:
            component (component): the component which owner this ParameterDict instance 
        """
        self.__component = component

    def __setitem__(self, key: str, value: Any):
        """ magic function __setitem__
        """
        if isinstance(value, Parameter) and value.component is None:
                # it means this Parameter don't reference anything
                value.set_base_info(component=self.__component, name=key)
        else:
            if isinstance(value, Parameter) and value.component == self.__component:
                raise PaddleFlowSDKException(PipelineDSLError, 
                    "parameter cannot referene another parameter which belong to the same DAG or Step")

            # it means value is defined by other component or inner type
            param = Parameter()
            param.set_base_info(component=self.__component, name=key, ref=value)
            value = param

        super().__setitem__(key, value)


class InputArtifactDict(dict):
    """ InputArtifactDict: an dict for manager input artifact
    """
    def __init__(self, component):
        """ create an new InputArtifactDict instance

        Args:
            component (component): the component which owner this InputArtifactDict instance
        """
        self.__component = component

    def __setitem__(self, key: str, value: Union[Artifact, ArtifactPlaceholder]):
        """ magic function __setitem__
        """
        if not isinstance(value, ArtifactPlaceholder):
            if not isinstance(value, Artifact) or value.component is None or value.component is self.__component:
                err_msg = f"the value of inputs for component[{self.__component.name}] " + \
                    "should be an output artifact of other component"
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        if not validate_string_by_regex(key, VARIBLE_NAME_REGEX):
            err_msg = f"the name of inputs artifacts[{key}] for component[{self.__component.name}] is is illegal" + \
                f"the regex used for validation is {VARIBLE_NAME_REGEX}"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        art = Artifact()
        art.set_base_info(component=self.__component, name=key, ref=value)
        super().__setitem__(key, art)


class OutputArtifactDict(dict):
    """ OutputArtifactDict: an dict for manager output artifact
    """
    def __init__(self, component):
        """ create an new OutputArtifactDict instance

        Args:
            component (component): the component which owner this OutputArtifactDict instance
        """
        self.__component = component

    def __setitem__(self, key: str, value: Union[Artifact, ArtifactPlaceholder]):
        """ magic function __setitem__
        """
        from paddleflow.pipeline.dsl.component import DAG
        from paddleflow.pipeline.dsl.component import Step

        if isinstance(value, Artifact):
            if isinstance(self.__component, Step) and value.component is not None:
                err_msg = f"the value of outputs[{key}] for Step[{self.__component.name}] just can be Artifact()"
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
            elif isinstance(self.__component, DAG) and value.component is None:
                err_msg = f"the value of outputs[{key}] for DAG[{self.__component.name}] should be reference from " + \
                    "the outputs of its substep or subDAG's"
        elif isinstance(value, ArtifactPlaceholder):
            if not isinstance(self.__component, DAG):
                raise PaddleFlowSDKException(PipelineDSLError, 
                    "only DAG's artifact could be an instances of ArtifactPlaceholder")
        else:
            err_msg = f"the value of outputs for Step just can be Artifact(). and"
            err_msg += f"the value of outputs for DAG should be reference from " + \
                    "the outputs of its substep or subDAG's"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        if isinstance(self.__component, Step):
            value.set_base_info(component=self.__component, name=key)
        else:
            art = Artifact()
            art.set_base_info(component=self.__component, name=key, ref=value)
            value = art

        super().__setitem__(key, value)


class EnvDict(dict):
    """ To manager envrionment varible of component or pipeline
    """
    def __init__(self, obj: Any):
        """ create an new EnvDict instance

        Args:
            obj (Any): the component which owner this OutputArtifactDict instance
        """
        self.__obj = obj
    
    def __setitem__(self, key:str, value: str):
        """ magic function __setitem__
        """
        
        if not isinstance(key, str):
            err_msg = f"the name of envrionment varible of obj[{self.__obj.name}] should be an instance of string," + \
                f" but it is{type(key)}"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        if not isinstance(value, str) and not isinstance(value, Parameter):
            err_msg = f"the value of env[{key}] of obj[{self.__obj.name}] should be an instances of string or Parameter"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        if not validate_string_by_regex(key, VARIBLE_NAME_REGEX):
                err_msg =  f"the name of env[{key}] of obj[{self.__obj.name}] is illegal, the regex used for validation" + \
                    f" is {VARIBLE_NAME_REGEX}"
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)
        
        super().__setitem__(key, str(value))
