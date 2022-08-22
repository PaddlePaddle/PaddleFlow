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
from typing import Dict

from .component_inferer import ComponentInferer
from .step_inferer import ContainerStepInferer

from paddleflow.pipeline.dsl.component import DAG
from paddleflow.pipeline.dsl.component import ContainerStep
from paddleflow.pipeline.dsl.io_types import ArtifactPlaceholder
from paddleflow.pipeline.dsl.io_types import ParameterPlaceholder
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class DAGInferer(ComponentInferer):
    """ infer parameter and artifact for DAG from it's fileds
    """
    def __init__(self, dag):
        """ create a new instance of DAGInferer
        """
        super().__init__(dag)
    
    def infer(self, env: Dict):
        """ infer parameter and artifact
        """
        self._infer_from_loop_argument()
        self._infer_from_condition()
        self._infer_env()
        self._infer_from_artifact()
        self._infer_from_parameter()

        self._infer_from_sub_component(env)
        self._infer_deps()
    
    def _infer_from_sub_component(self, env:Dict):
        """ infer parameter from sub_component
        """
        for _, component in self._component.entry_points.items():
            if isinstance(component, DAG):
                DAGInferer(component).infer(env)
            elif isinstance(component, ContainerStep):
                ContainerStepInferer(component).infer(env) 

        #  entry_points is out of order at this time
        for _, cp in self._component.entry_points.items():
            self._infer_parameter_from_sub_component(cp)
            self._infer_inputs_from_sub_component(cp)
            self._infer_deps_from_sub_component(cp)

        # only sub dag need to infer output from it's downstream component and just execute this function on the outermost
        if len(self._component.full_name.split(".")) == 1:
            self._infer_outputs_for_sub_dag()

    def _has_sub_componet(self, componnent_full_name:str):
        """ is there some subcomponent who's full_name == component_full_name
        """
        if componnent_full_name.rsplit(".", 1)[0] != self._component.full_name:
            return False
        
        return True
    
    def _has_descendants_component(self, component_full_name: str):
        """ is there some descendants component who's full_name == component_full_name
        """
        return component_full_name.startswith(self._component.full_name + ".")

    def _infer_parameter_from_sub_component(self, cp):
        """ infer parameter from sub_component
        """
        for name, value in cp.parameters.items():
            if isinstance(value.ref, ParameterPlaceholder):
                if self._has_sub_componet(value.ref.component_full_name):
                    upstream_name = value.ref.component_full_name.split(".")[-1]
                    cp.parameters[name]=self._component.entry_points[upstream_name].parameters[value.ref.name]
                else:
                    # parameter is come from self._component's upstream
                    param_name = self._generate_art_or_param_name("dsl", "param")
                    self._component.parameters[param_name] = value.ref
                    cp.parameters[name] = self._component.parameters[param_name]

    def _infer_inputs_from_sub_component(self, cp):
        """ infer inputs from sub_component
        """
        for name, value in cp.inputs.items():
            if isinstance(value.ref, ArtifactPlaceholder):
                if self._has_sub_componet(value.ref.component_full_name):
                    upstream_name = value.ref.component_full_name.split(".")[-1]
                    cp.inputs[name] = self._component.entry_points[upstream_name].outputs[value.ref.name]
                else:
                    art_name = self._generate_art_or_param_name("dsl", "art")
                    if self._has_descendants_component(value.ref.component_full_name):
                        upstream_name = value.ref.component_full_name.split(".")[len(cp.full_name.split(".")) -1 ]
                        self._component.entry_points[upstream_name].outputs[art_name] = value.ref
                        cp.inputs[name] = self._component.entry_points[upstream_name].outputs[art_name]
                    else:    
                        art_name = self._generate_art_or_param_name("dsl", "art")
                        self._component.inputs[art_name] = value.ref
                        cp.inputs[name] = self._component.inputs[art_name]
                
    def _infer_outputs_for_sub_dag(self):
        """ infer outputs for sub dag according to it's downstream
        """
        for key, value in self._component.outputs.items():
            if isinstance(value.ref, ArtifactPlaceholder):
                sub_name = value.ref.component_full_name.replace(self._component.full_name + ".", "", 1).split(".")[0]
                if self._has_sub_componet(value.ref.component_full_name):
                    self._component.outputs[key] = self._component.entry_points[sub_name].outputs[value.ref.name]
                else:
                    art_name = self._generate_art_or_param_name("dsl", "art")
                    self._component.entry_points[sub_name].outputs[art_name] = value.ref
                    self._component.outputs[key] = self._component.entry_points[sub_name].outputs[art_name]
                    # self._component.outputs[key].ref.ref = None

        for _, cp in self._component.entry_points.items():
            if isinstance(cp, DAG):
                DAGInferer(cp)._infer_outputs_for_sub_dag()

    def _infer_deps_from_sub_component(self, cp):
        """ infer deps from sub_component
        """
        if not cp._dependences:
            return 

        need_remove = []
        need_add = []
        for dep in cp._dependences:
            if "." not in dep:
                if dep not in self._component.entry_points:
                    raise PaddleFlowSDKException(PipelineDSLError, 
                        self._generate_error_msg(f"there is no substep or subdag named [{dep}] in " + \
                            f"DAG[{self._component.name}]"))
                else:            
                    continue

            need_remove.append(dep)
            if self._has_sub_componet(dep):
                dep = dep.rsplit(".")[-1]
                need_add.append(dep)
            elif self._has_descendants_component(dep):
                dep = dep.replace(self._component.full_name + ".", "", 1).split(".")[0]
                need_add.append(dep)
            else:
                self._component._dependences.add(dep)
        
        for dep in need_remove:
            cp._dependences.remove(dep)
        
        for dep in need_add:
            cp._dependences.add(dep)

    def _infer_env(self):
        """ infer env for subcomponent
        """
        pass
            