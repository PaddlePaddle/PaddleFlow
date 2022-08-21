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
from .step_compiler import StepCompiler

from paddleflow.pipeline.dsl.component import DAG
from paddleflow.pipeline.dsl.component import Component
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class DAGCompiler(ComponentCompiler):
    """ trans DAG obj to dict
    """
    def __init__(self, dag: DAG):
        """ create an instance of DAGcompile

        Args:
            pipeline: the pipeline which dag belong to
            dag: the dag need to compile

        """
        super().__init__(dag)

    def compile(self):
        """ trans dag to dict
        """
        super().compile()
        topo = self._topo_sort()
        self._dict["entry_points"] = {}

        for sub in topo:
            if isinstance(sub, DAG):
                self._dict["entry_points"][sub.name] = DAGCompiler(sub).compile()
            else:
                self._dict["entry_points"][sub.name] = StepCompiler(sub).compile()
        
        return self._dict

    def _compile_output_artifact(self):
        """ _compile_output_artifact
        """
        if not self._component.outputs:
            return 

        self._dict.setdefault("artifacts", {}).setdefault("output", {})
        for name, art in self._component.outputs.items():
            self._dict["artifacts"]["output"][name] = "{{" + f"{art.ref.component.name}.{art.ref.name}" + "}}"

    def _topo_sort(self):
        """ List Steps in topological order.

        Returns:
            A list of Steps in topological order

        Raises:
            PaddleFlowSDKException: if there is a ring
        """
        topo_sort = []
        
        while len(topo_sort) < len(self._component.entry_points):
            exists_ring = True
            for sub in self._component.entry_points.values():
                if sub in topo_sort:
                    continue

                need_add = True

                for dep in sub._dependences:
                    if dep not in self._component.entry_points:
                        raise PaddleFlowSDKException(PipelineDSLError, 
                            self._generate_error_msg(f"there is no substep or subdag named [{dep}] in " + \
                                f"DAG[{self._component.full_name}]"))

                    if self._component.entry_points[dep] not in topo_sort:
                        need_add = False
                        break

                if need_add:
                    topo_sort.append(sub)
                    exists_ring = False
            
            if exists_ring:
                ring_steps = [step.name for step in self._component.entry_points.values()  if step not in topo_sort]
                raise PaddleFlowSDKException(PipelineDSLError, 
                    self._generate_error_msg(f"there is a ring between {ring_steps}"))
    
        return topo_sort