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

# Artifact: the input/output file of step
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError 
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class Artifact(object):
    """ Artifact: the input/output file/directory of step
    """
    def __init__(self):
        """ create a new instance of Artifact
        """
        self.__step = None
        self.__name = None

    def set_base_info(self, name: str, step):
        """ set the step that this paramter instances was belong to and set the name of it

        Args:
            step (Step): the step that this paramter instances was belong to
            name (str): the name of it

        Raises:
            PaddleFlowSDKException: if the name is illegal

        .. note:: This is not used directly by the users
        """
        self.__step = step
        
        if not validate_string_by_regex(name, VARIBLE_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError,
                    f"the name of Artifact[{name}] for step[{step.name}]is illegal, " + \
                            f"the regex used for validation is {VARIBLE_NAME_REGEX}")
            
        self.__name = name 

    def compile(self):
        """ trans to template when downstream step ref it at compile stage

        Returns:
            A string indicate the template of it

        Raises:
            PaddleFlowSDKException: if cannot trans to template
        """
        if self.__step is None or self.__name is None:
            raise PaddleFlowSDKException(PipelineDSLError,
                    "when trans Artifact to template, it's step and name cannot be None")

        return "{{" + f"{self.__step.name}.{self.__name}" + "}}"

    @property
    def step(self):
        """ get the step of it

        Returns:
            A step instance whicht it was belong to
        """
        return self.__step

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
            art.set_base_info(name=self.name, step=self.step)
        return art

    def __eq__(self, other):
        """ support ==
        """
        return self.name == other.name and self.step == other.step
