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

""" unit test for paddleflow.pipeline.dsl.compiler.step_compiler
"""
import pytest 

from paddleflow.pipeline import DAG
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline.dsl.compiler.dag_compiler import DAGCompiler
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class TestStepCompiler(object):
    """ unit test for StepCompiler
    """
    @pytest.mark.topo
    def test_topo_sort(self):
        """ get the topo sort for dag
        """ 
        with DAG(name="dag1") as dag1:
            st1 = ContainerStep("step1")
            st2 = ContainerStep("step2")
            st3 = ContainerStep("step3")
            st1._set_full_name("")
            st2._set_full_name("")
            st3._set_full_name("")

        cp = DAGCompiler(dag1)
        sorted = cp._topo_sort()
        assert len(sorted) == 3
        
        st3.after(st1)
        st2.after(st3)            
        
        sorted = cp._topo_sort()
        assert sorted == [st1, st3, st2]

        st3.after(st2)
        with pytest.raises(PaddleFlowSDKException):
            cp._topo_sort()

        st3._dependences = {st3}
        with pytest.raises(PaddleFlowSDKException):
            cp._topo_sort()

    
    @pytest.mark.dag_compile
    def test_compile(self):
        """ unit test for compile
        """
        with DAG("dag") as dag:
            step1 = ContainerStep(
                name="step1",
                parameters={"abc": 123, "def": 456},
                outputs={"out1": Artifact()}
                )

            with DAG("dag1") as dag1:
                step2 = ContainerStep(
                name="step2",
                )
                step3 = ContainerStep(
                name="step3",
                outputs = {"out1": Artifact(), "out2": Artifact()}
                )
            
            step4 = ContainerStep(
                name="step4",
                )
            
        dag1.inputs["in1"] = step1.outputs["out1"]
        dag1.parameters["abc1"] = step1.parameters["abc"]
        dag1.parameters["def"] = step1.parameters["def"]

        step2.parameters["abc"] = dag1.parameters["abc1"]

        step3.parameters["def1"] = dag1.parameters["def"]
        step3.inputs["in"] = dag1.inputs["in1"]

        dag1.outputs["out1"] = step3.outputs["out1"]
        dag1.outputs["out2"] = step3.outputs["out2"]

        step4.inputs["in1"] = dag1.outputs["out1"]
        step4.inputs["in2"] = dag1.outputs["out2"]

        cp = DAGCompiler(dag)

        dag_dict = cp.compile()
        assert len(dag_dict) == 2 and len(dag_dict["entry_points"]) == 3
        assert "step1" in dag_dict["entry_points"] and "dag1" in dag_dict["entry_points"] and \
            "step4" in dag_dict["entry_points"]
        
        dag1_dict = dag_dict["entry_points"]["dag1"]
        assert dag1_dict["parameters"]["abc1"] == "{{step1.abc}}" and \
            dag1_dict["parameters"]["def"] == "{{step1.def}}"
        assert dag1_dict["artifacts"]["input"]["in1"] == "{{step1.out1}}" and \
            dag1_dict["artifacts"]["output"]["out1"] == "{{step3.out1}}" and \
            dag1_dict["artifacts"]["output"]["out2"] == "{{step3.out2}}" 

        assert dag1_dict["entry_points"]["step2"]["parameters"]["abc"] == "{{PF_PARENT.abc1}}"
        assert dag1_dict["entry_points"]["step3"]["parameters"]["def1"] == "{{PF_PARENT.def}}"
        assert dag1_dict["entry_points"]["step3"]["artifacts"]["input"]["in"] == "{{PF_PARENT.in1}}"

        assert dag_dict["entry_points"]["step4"]["artifacts"]["input"]["in1"] == "{{dag1.out1}}"
        assert dag_dict["entry_points"]["step4"]["artifacts"]["input"]["in2"] == "{{dag1.out2}}"


        
        

