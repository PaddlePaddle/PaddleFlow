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

from .test_component_inferer import base_demo

from paddleflow.pipeline.dsl.inferer.dag_inferer import DAGInferer
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
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


def step1(num):
    return ContainerStep(
            name="step1",
            command="echo {{num}} >> result",
            parameters={"num": num},
            outputs={"result": Artifact()},
            )


def show(name, num, num2, art):
    return ContainerStep(
            name=name,
            command="echo {{num}} && echo {{num2}} >>result  && cat {{data}}",
            parameters={"num": num, "num2": num2},
            inputs={"data": art},
            outputs={"result": Artifact()},
            env={"a": "2", "b": "1"}
            )


def dag_example(num=0, num2=1):
    with DAG(name="dag-example") as dag1:
        st1 = step1(num)

        with DAG(name="dag2") as dag2:
            st2 = show("dag-show", st1.parameters["num"], num2, st1.outputs["result"])
            st3 = show("show", num, num2, st2.outputs["result"])

            with DAG(name="dag3") as dag3:
                st4 = show("show", st2.parameters["num"], num2, st2.outputs["result"])

        st5 = show("show", num, num2, st4.outputs["result"])
        st6 = ContainerStep(name="step6", command=f"cat {st5.parameters['num']}")

    return dag1, st1, dag2, st2, st3, dag3, st4, st5, st6


class TestDAGInferer(object):
    """ unit test for DAGInferer
    """
    def test_infer(self):
        """ unit test
        """
        dag1, st1, dag2, st2, st3, dag3, st4, st5, st6 = dag_example()
        dag_inferer = DAGInferer(dag1)
        dag_inferer.infer({"a": "1"})

        assert len(dag1.inputs) + len(dag1.outputs) + len(dag1.parameters) == 0
        
        # dag2:
        # 1. parameter
        assert len(dag2.parameters) == 1
        list(dag2.parameters.values())[0].ref == st1.parameters["num"]

        # 2. inputs
        assert len(dag2.inputs) == 1
        list(dag2.inputs.values())[0].ref == st1.outputs["result"]

        # 3. outputs
        assert len(dag2.outputs) == 1
        assert list(dag2.outputs.values())[0].ref.component.full_name == dag3.full_name
        assert list(dag2.outputs.values())[0].ref.name == list(dag3.outputs.keys())[0]

        # st2
        assert st2.parameters["num"].ref.component ==  dag2 and \
            st2.parameters["num"].ref.name == list(dag2.parameters.keys())[0]

        assert st2.inputs["data"].ref.component == dag2 and \
            st2.inputs["data"].ref.name == list(dag2.inputs.keys())[0]

        # st3
        assert st3.inputs["data"].ref.component == st2 and \
           st3.inputs["data"].ref.name  == "result"

        # dag3
         # 1. parameter
        assert len(dag3.parameters) == 1
        list(dag3.parameters.values())[0].ref == st2.parameters["num"]

        # 2. inputs
        assert len(dag3.inputs) == 1
        list(dag3.inputs.values())[0].ref == st2.outputs["result"]

        # 3. outputs
        assert len(dag3.outputs) == 1
        list(dag3.outputs.values())[0].ref == st4.outputs["result"]

        # st4
        assert st4.parameters["num"].ref.component == dag3 and \
            st4.parameters["num"].ref.name == list(dag3.parameters.keys())[0]

        # st5
        assert st5.inputs["data"].ref.component == dag2 and \
            st5.inputs["data"].ref.name == list(dag2.outputs.keys())[0]

        # st6
        assert list(st6.parameters.values())[0].ref == st5.parameters["num"]
        assert st6.command == "cat {{" + list(st6.parameters.keys())[0] + "}}"

        assert len(st4._dependences) == 0
        assert dag3._dependences == {"dag-show"}
        assert st5._dependences == {"dag2"}

        assert st4.env["a"] == "2" and st6.env["a"] == "1"
 