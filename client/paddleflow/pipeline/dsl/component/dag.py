"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/home/ahz/baidu/bmlc/paddleflow/client/paddleflow/pipelineSee the License for the specific language governing permissions and
limitations under the License.
"""
import os
import yaml
import copy
import configparser
from typing import Callable
from typing import List
from typing import Dict
from typing import Union
from typing import Any
from pathlib import Path

from .steps import Step
from .component import Component

from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class DAG(Component):
    """ Pipeline is a workflow which is composed of Step  
    """
    def __init__(
            self, 
            name: str, 
            condition: str=None,
            loop_argument: Union[List, Parameter, Artifact, str]=None
            ):
        """ create a new instance of DAG

        Args:
            name (str): the name of DAG
            condition (str): the condition. when schedule component, would be calculate condition, if the result of it is False, then the status of this component would be set "skipped"
            loop_argument (Union[List, Parameter, Artifact, str]): the loop arugment, when schedule this component, would be be traversed, and the runtime will be created once for each item in it
        Raises:
            PaddleFlowSDKException: if some args is illegal

        Example: ::
            
            with DAG(name):
                step1 = data_process(data_path)
                step2 = main(step1.parameters["data_file"], iteration, step1.outputs["train_data"])
                step3 = validate(step2.parameters["data_file"], step1.outputs["validate_data"], step2.outputs["train_model"])        
        """
        super().__init__(name=name, condition=condition, loop_argument=loop_argument)

        self.entry_points = {}
        self._type = "dag"

    def __enter__(self):
        """ organize pipeline
        """
        def register_component_to_dag(cp: Component):
            """ register step to pipeline, will be invoked when instantiating step
            
            Args:
                cp (Component): the component which need to register this dag instance
            """
            self.add(cp)

        from paddleflow.pipeline.dsl.component import component
        
        self.__old_register = component.register_component_handler
        component.register_component_handler = register_component_to_dag

        return self

    def __exit__(self, *args):
        """ magic func __exit__, to support with context
        """
        from paddleflow.pipeline.dsl.component import component
        component.register_component_handler = self.__old_register

    def add(
            self, 
            component: Component
            ):
        """ add step

        Args:
            step (Step):  the step which need to register to this Pipeline 
        """
        self._valide_name_is_unique(component.name)
        self.entry_points[component.name] = component
        component._set_full_name(self.full_name)

    def _valide_name_is_unique(
            self,
            name: str
            ):
        """ to ensure every step in pipeline has unique name 

        Args:
            name (str): the name of steps:
        """
        names = self.entry_points.keys()
        if name in names:
            raise PaddleFlowSDKException(PipelineDSLError, 
                self._generate_error_msg(f"there are multiple Steps or Dags with the same name[{name}]"))