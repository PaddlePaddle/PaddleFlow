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

from .component_compiler import ComponentCompiler

from paddleflow.pipeline.dsl import Pipeline
from paddleflow.pipeline.dsl.component import DAG
from paddleflow.pipeline.dsl.component import Component
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class DAGCompiler(ComponentCompiler):
    """ trans DAG obj to dict
    """
    def __init__(self, pipeline: Pipeline, dag: DAG, parent: Component):
        """ create an instance of DAGcompile

        Args:
            pipeline: the pipeline which dag belong to
            dag: the dag need to compile

        """
        self._pipeline = pipeline
        self._dag = dag


def _update_and_validate_steps(self, steps: Dict[str, Step]):
        """ update steps before compile

        Args:
            steps (Dict[string, Step]): the steps need to update and validate
        """
        # 1. Synchronize environment variables of pipeline and step
        for name, step in steps.items():
            # 1. Synchronize environment variables of pipeline and step
            env = dict(self.env)
            env.update(step.env)
            step.add_env(env)

            # 2. Ensure that the input / output aritact and parameter of the step have different names
            step.validate_io_names()
        
            # 3. Resolve dependencies between steps and validate all deps are Pipeline
            for dep in step.get_dependences():
                if dep.name not in steps or dep is not steps[dep.name]:
                    raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                            f"the upstream step[{dep.name}] for step[{step.name}] is not in Pipeline[{self.name}].\n" + \
                                "Attentions: step in postProcess cannot depend on any other step")
            
            # 4. validate docker_env
            if not self.docker_env and not step.docker_env:
                raise PaddleFlowSDKException(PipelineDSLError, 
                    self.__error_msg_prefix + f"cannot set the docker_env of step[step.name]")  

    def topological_sort(self):
        """ List Steps in topological order.

        Returns:
            A list of Steps in topological order

        Raises:
            PaddleFlowSDKException: if there is a ring
        """
        topo_sort = []
        
        while len(topo_sort) < len(self._steps):
            exists_ring = True
            for step in self._steps.values():
                if step in topo_sort:
                    continue

                need_add = True

                for dep in step.get_dependences():
                    if dep not in topo_sort:
                        need_add = False
                        break

                if need_add:
                    topo_sort.append(step)
                    exists_ring = False
            
            if exists_ring:
                ring_steps = [step.name for step in self._steps.values()  if step not in topo_sort]
                raise PaddleFlowSDKException(PipelineDSLError, 
                    self.__error_msg_prefix + f"there is a ring between {ring_steps}")
        
        # append post_process  
        topo_sort += list(self._post_process.values())
        return topo_sort