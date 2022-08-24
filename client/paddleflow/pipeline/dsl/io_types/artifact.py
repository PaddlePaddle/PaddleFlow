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

# Artifact: the input/output file of component
from .placeholder import ArtifactPlaceholder

from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class Artifact(object):
    """ Artifact: the input/output file/directory of component
    """
    def __init__(self):
        """ create a new instance of Artifact
        """
        self.__component = None
        self.ref = None
        self.__name = None

    def set_base_info(self, name: str, component, ref=None):
        """ set the component that this paramter instances was belong to and set the name of it

        Args:
            component (component): the component that this artifact instances was belong to
            component (component_full_name): 
            name (str): the name of it

        Raises:
            PaddleFlowSDKException: if the name is illegal

        .. note:: This is not used directly by the users
        """
        self.__component = component
        
        if not validate_string_by_regex(name, VARIBLE_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError,
                    f"the name of Artifact[{name}] for component[{component.name}]is illegal, " + \
                            f"the regex used for validation is {VARIBLE_NAME_REGEX}")
            
        self.__name = name 
        self.ref = ref

    @property
    def component(self):
        """ get the component of it

        Returns:
            A component instance whicht it was belong to
        """
        return self.__component

    @property
    def name(self):
        """ get the name of it

        Returns:
            A string which describe the name of it
        """
        return self.__name

    def __deepcopy__(self, memo):
        """ support copy.deepcopy
        """
        art = Artifact()
        if self.name:
            art.set_base_info(name=self.name, component=self.component, ref=self.ref)
        return art

    def __eq__(self, other):
        """ support ==
        """
        if not isinstance(other, Artifact):
            return False
             
        return self.name == other.name and self.component == other.component and  self.ref == other.ref

    def __str__(self):
        """ magic func for str
        """
        if not self.component:
            raise PaddleFlowSDKException(PipelineDSLError, 
                f"cannot trans Artifact to string, if the Artifact instance doesn't belong to any Step or DAG")

        return "{{" + f"artifact: {self.component.full_name}.{self.name}" + "}}"
