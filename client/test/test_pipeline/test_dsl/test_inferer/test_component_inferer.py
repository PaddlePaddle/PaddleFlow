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

""" unit test for paddleflow.pipeline.dsl.inferer.ComponentInfer
"""

import pytest 
from pathlib import Path
from hashlib import md5

from paddleflow.pipeline.dsl.inferer.component_inferer import ComponentInferer
from paddleflow.pipeline.dsl.component import ContainerStep
from paddleflow.pipeline.dsl.component import DAG
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types.dicts import InputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import OutputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import ParameterDict
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import COMPONENT_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX, ENTRY_POINT_NAME
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

def base_demo():
    """ base demo
    """
    with DAG(name="pf-entry-point"):
        step1 = ContainerStep(
                    name="step1",
                    parameters={
                        "nums": 1,
                        "num2": Parameter(2),
                        "num3": [1,2],
                    },
                    outputs={
                        "art1": Artifact(),
                        "art2": Artifact(),
                    },
                    )
    
        with DAG(name="dag1") as dag1:
            step2 = ContainerStep(
                    name="step2",
                    parameters={
                        "p1": step1.parameters["nums"],
                        "p2": [1,2,3]
                    },
                    inputs={
                        "in1": step1.outputs["art1"]
                    },
                    outputs={
                        "abc": Artifact(),
                    }
                    )

        step3 = ContainerStep(
                    name="step3",
                    )
    return (step1, dag1, step2, step3)

class TestComponentInferer(object):
    """ unit test for componentinfer
    """
    @pytest.mark.loop_obj
    def test_infer_loop_from_obj(self):
        """ test infer from loop
        """
        step1, dag1, step2, step3 = base_demo()
        inferer = ComponentInferer(step2)

        # 1. from parameter and artifact which not belong to any component
        with pytest.raises(PaddleFlowSDKException):
            step2.loop_argument = Parameter(23)
            inferer._infer_from_loop_argument()

        with pytest.raises(PaddleFlowSDKException):
            step2.loop_argument = Artifact()
            inferer._infer_from_loop_argument()

        # 2.  from it's self's artifact or parameter
        ## 2.1 parameter
        step2.loop_argument = step2.parameters["p2"]
        inferer._infer_from_loop_argument()
        assert step2.loop_argument.argument == step2.parameters["p2"]
        assert len(step2.parameters) == 2

        ## 2.2 artifact
        step2.loop_argument = step2.inputs["in1"]
        inferer._infer_from_loop_argument()
        assert step2.loop_argument.argument == step2.inputs["in1"]
        assert len(step2.inputs) == 1

        # 3. 直接引用其余节点的输出artifact或者parameter
        ## 3.1 artifact
        step2.loop_argument = step1.outputs["art2"]
        inferer._infer_from_loop_argument()
        assert len(step2.inputs) == 2

        new_art = ""
        for k, v in step2.inputs.items():
            if k == "in1":
                continue

            new_art = k
            m = md5()
            m.update("pf-entry-point.step1.art2".encode())
            assert int(new_art[len("loop_art_") : ], 36) == int(m.hexdigest(), 16)
            assert new_art.startswith("loop_art")
            assert v.ref.name == "art2"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.loop_argument.argument == v and v.name == new_art
        
        step2.inputs.pop(new_art)
        
        ## 3.2 parameter
        step2.loop_argument = step1.parameters["num3"]
        inferer._infer_from_loop_argument()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            assert new_param.startswith("loop_param")
            m = md5()
            m.update("pf-entry-point.step1.num3".encode())
            assert int(new_param[len("loop_param_"):], 36) == int(m.hexdigest(), 16)
            assert v.ref.name == "num3"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.loop_argument.argument == v and v.name == new_param
        
        step2.parameters.pop(new_param)

        # 4. 引用父节点的 loop_argument
        dag1.loop_argument = [1,2,3]
        step2.loop_argument = dag1.loop_argument.item
        inferer._infer_from_loop_argument()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            assert new_param.startswith("loop_param")
            m = md5()
            m.update((dag1.full_name + ".PF_LOOP_ARGUMENT").encode())
            assert int(new_param[len("loop_param_"):], 36) == int(m.hexdigest(), 16) 
            assert step2.loop_argument.argument == v and v.name == new_param and \
                v.ref == "{{PF_PARENT.PF_LOOP_ARGUMENT}}"
        
        step2.parameters.pop(new_param)

        # 5. 引用本节点的 loop item
        with pytest.raises(PaddleFlowSDKException):
            step2.loop_argument = dag1.loop_argument.item
            step2.loop_argument = step2.loop_argument.item
            inferer._infer_from_loop_argument()


        # 5. referenece parameter from sub component
        with pytest.raises(PaddleFlowSDKException):
            step3.loop_argument = step2.parameters["p1"]
            inferer3 = ComponentInferer(step3)
            inferer3._infer_from_loop_argument()

        step3.loop_argument = step2.outputs["abc"]
        inferer3 = ComponentInferer(step3)
        inferer3._infer_from_loop_argument()
        assert len(step3.inputs) == 1

    @pytest.mark.loop_str
    def test_infer_loop_from_str(self):
        """ test infer from loop
        """
        step1, dag1, step2, step3 = base_demo()
        inferer = ComponentInferer(step2)

        # 1. from parameter and artifact which not belong to any component
        with pytest.raises(PaddleFlowSDKException):
            step2.loop_argument = str(Parameter(23))
            inferer._infer_from_loop_argument()

        with pytest.raises(PaddleFlowSDKException):
            step2.loop_argument = str(Artifact())
            inferer._infer_from_loop_argument()

        # 2.  from it's self's artifact or parameter
        ## 2.1 parameter
        step2.loop_argument = str(step2.parameters["p2"])
        inferer._infer_from_loop_argument()
        assert step2.loop_argument.argument == step2.parameters["p2"]
        assert len(step2.parameters) == 2

        ## 2.2 artifact
        step2.loop_argument = str(step2.inputs["in1"])
        inferer._infer_from_loop_argument()
        assert step2.loop_argument.argument == step2.inputs["in1"]
        assert len(step2.inputs) == 1

        # 3. 直接引用其余节点的输出artifact或者parameter
        ## 3.1 artifact
        step2.loop_argument = str(step1.outputs["art2"])
        inferer._infer_from_loop_argument()
        assert len(step2.inputs) == 2

        new_art = ""
        for k, v in step2.inputs.items():
            if k == "in1":
                continue

            new_art = k
            m = md5()
            m.update("pf-entry-point.step1.art2".encode())
            assert int(new_art[len("loop_art_") : ], 36) == int(m.hexdigest(), 16)
            assert new_art.startswith("loop_art")
            assert v.ref.name == "art2"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.loop_argument.argument == v and v.name == new_art
        
        step2.inputs.pop(new_art)
        
        ## 3.2 parameter
        step2.loop_argument = str(step1.parameters["num3"])
        inferer._infer_from_loop_argument()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            m = md5()
            m.update("pf-entry-point.step1.num3".encode())
            assert int(new_param[len("loop_param_"):], 36) == int(m.hexdigest(), 16)
            assert new_param.startswith("loop_param")
            assert v.ref.name == "num3"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.loop_argument.argument == v and v.name == new_param
        
        step2.parameters.pop(new_param)

        # 4. 引用父节点的 loop_argument
        dag1.loop_argument = [1,2,3]
        step2.loop_argument = str(dag1.loop_argument.item)
        inferer._infer_from_loop_argument()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            m = md5()
            m.update((dag1.full_name + ".PF_LOOP_ARGUMENT").encode())
            assert int(new_param[len("loop_param_"):], 36) == int(m.hexdigest(), 16)
            assert new_param.startswith("loop_param")
            assert step2.loop_argument.argument == v and v.name == new_param and \
                v.ref == "{{PF_PARENT.PF_LOOP_ARGUMENT}}"
        
        step2.parameters.pop(new_param)

        # 5. 引用本节点的 loop item
        with pytest.raises(PaddleFlowSDKException):
            step2.loop_argument = [1,2,3]
            step2.loop_argument = str(step2.loop_argument.item)
            inferer._infer_from_loop_argument()


        # 5. referenece parameter from sub component
        with pytest.raises(PaddleFlowSDKException):
            step3.loop_argument = str(step2.parameters["p1"])
            inferer3 = ComponentInferer(step3)
            inferer3._infer_from_loop_argument()

        step3.loop_argument = step2.outputs["abc"]
        inferer3 = ComponentInferer(step3)
        inferer3._infer_from_loop_argument()
        assert len(step3.inputs) == 1

        with pytest.raises(PaddleFlowSDKException):
             step2.loop_argument = str(step2.loop_argument.item) + "abc"

        
        with pytest.raises(PaddleFlowSDKException):
             step2.loop_argument = str(step2.loop_argument.item) + str(step2.parameters["p1"])
        
    @pytest.mark.condition
    def test_condtion(self):
        """ test conditon
        """
        step1, dag1, step2, step3 = base_demo()
        inferer = ComponentInferer(step2)

        # 1. from parameter and artifact which not belong to any component
        with pytest.raises(PaddleFlowSDKException):
            step2.condition = f"{Parameter(23)} > 10 "
            inferer._infer_from_condition()

        with pytest.raises(PaddleFlowSDKException):
            step2.contidion = f"{Artifact()} > 10"
            inferer._infer_from_condition()

        # 2.  from it's self's artifact or parameter
        ## 2.1 parameter
        step2.condition = f"{step2.parameters['p2']} > 10"
        inferer._infer_from_condition()
        assert step2.condition == "{{p2}} > 10"
        assert len(step2.parameters) == 2

        ## 2.2 artifact
        step2.condition =  f"{step2.inputs['in1']} > 10"
        inferer._infer_from_condition()
        assert step2.condition == "{{in1}} > 10"
        assert len(step2.inputs) == 1

        # 3. 直接引用其余节点的输出artifact或者parameter
        ## 3.1 artifact
        step2.condition = f'{step1.outputs["art2"]} > 10'
        inferer._infer_from_condition()
        assert len(step2.inputs) == 2

        new_art = ""
        for k, v in step2.inputs.items():
            if k == "in1":
                continue

            new_art = k
            m = md5()
            m.update("pf-entry-point.step1.art2".encode())
            assert int(new_art[len("condition_art_") : ], 36) == int(m.hexdigest(), 16)
            assert new_art.startswith("condition_art")
            assert v.ref.name == "art2"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.condition == "{{" + f"{new_art}" + "}} > 10" and v.name == new_art
        
        step2.inputs.pop(new_art)
        
        ## 3.2 parameter
        step2.condition = f'{step1.parameters["num3"]} > 10'
        inferer._infer_from_condition()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            m = md5()
            m.update("pf-entry-point.step1.num3".encode())
            assert int(new_param[len("condition_param_") : ], 36) == int(m.hexdigest(), 16)
            assert new_param.startswith("condition_param")
            assert v.ref.name == "num3"
            assert v.ref.component_full_name == "pf-entry-point.step1"
            assert step2.condition == "{{" + f"{new_param}" + "}} > 10" and v.name == new_param
        
        step2.parameters.pop(new_param)

        # 4. 引用父节点的 loop_argument
        dag1.loop_argument = [1,2,3]
        step2.condition = f'{dag1.loop_argument.item} > 10'
        inferer._infer_from_condition()
        assert len(step2.parameters) == 3

        new_param = ""
        for k, v in step2.parameters.items():
            if k.startswith("p"):
                continue

            new_param = k
            m = md5()
            m.update((dag1.full_name + ".PF_LOOP_ARGUMENT").encode())
            assert int(new_param[len("condition_param_") : ], 36) == int(m.hexdigest(), 16)
            assert new_param.startswith("condition_param")
            assert step2.condition == "{{" + f"{new_param}" + "}} > 10" and v.name == new_param and \
                v.ref == "{{PF_PARENT.PF_LOOP_ARGUMENT}}"
        
        step2.parameters.pop(new_param)

        # 5. 引用本节点的 loop item
        step2.loop_argument = [1,2,3]
        step2.condition = f'{step2.loop_argument.item} > {step2.inputs["in1"]}'
        inferer._infer_from_condition()
        assert len(step2.parameters) == 2
        assert step2.condition == "{{PF_LOOP_ARGUMENT}} > {{in1}}"

        # 5. referenece parameter from sub component
        with pytest.raises(PaddleFlowSDKException):
            step3.condition = f'{step2.parameters["p1"]} > 10'
            inferer3 = ComponentInferer(step3)
            inferer3._infer_from_condition()

        step3.condition = f'{step2.outputs["abc"]} > 10'
        inferer3 = ComponentInferer(step3)
        inferer3._infer_from_condition()
        assert len(step3.inputs) == 1
        assert step3.condition == "{{" + list(step3.inputs.keys())[0] + "}} > 10"

    @pytest.mark.deps
    def test_infer_deps(self):
        """ test deps
        """
        with DAG(name=ENTRY_POINT_NAME) as dag:
            step1 = ContainerStep(
                    name="step1",
                    parameters={
                        "nums": 1,
                        "num2": Parameter(2),
                        "num3": [1,2],
                    },
                    outputs={
                        "art1": Artifact(),
                        "art2": Artifact(),
                    },
                    )
            step2 = ContainerStep(
                    name="step2",
                    parameters={
                        "nums": 1,
                        "num2": Parameter(2),
                        "num3": [1,2],
                    },
                    outputs={
                        "art1": Artifact(),
                        "art2": Artifact(),
                    },
                    )
            step3 = ContainerStep(
                    name="step3",
                    parameters={
                        "nums": 1,
                        "num2": Parameter(2),
                        "num3": [1,2],
                    },
                    outputs={
                        "art1": Artifact(),
                        "art2": Artifact(),
                    },
                    )
            step4 = ContainerStep(
                    name="step4",
                    )

        assert len(step2._dependences) == 0

        step2.after(step1)
        step3.parameters["p2"] = step2.parameters["nums"]
        step4.inputs["in1"] = step3.outputs["art1"]
        step4.inputs["in2"] = step2.outputs["art1"]

        dag.parameters["p1"] = 10
        step3.parameters["p3"] = dag.parameters["p1"]

        assert len(step1._dependences) == 0

        inferer2 = ComponentInferer(step2)
        inferer2._infer_deps()
        assert step2._dependences == {ENTRY_POINT_NAME + ".step1"}

        inferer3 = ComponentInferer(step3)
        inferer3._infer_from_parameter()
        inferer3._infer_from_artifact()
        inferer3._infer_deps()
        assert step3._dependences == {"step2"}

        inferer4 = ComponentInferer(step4)
        inferer4._infer_from_parameter()
        inferer4._infer_from_artifact()
        inferer4._infer_deps()
        assert step4._dependences == {"step3", "step2"}
    
    @pytest.mark.parent
    def test_parent_name(self):
        step1, dag1, step2, step3 = base_demo()
        inferer2 = ComponentInferer(step2)

        assert inferer2._is_parent(dag1.full_name)
        assert inferer2._get_parent_full_name() == dag1.full_name
