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

""" unit test for paddleflow.pipeline.dsl.component.Component
"""

import pytest 
from pathlib import Path

from paddleflow.pipeline.dsl.component import Component
from paddleflow.pipeline.dsl import ContainerStep
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


class TestComponent(object):
    """ unit test for Step
    """
    @pytest.mark.outputs
    def test_output(self):
        """ test set_output() and output()
        """
        outputs = {
                    "train_data": Artifact(),
                    "validate_data": Artifact()
                    }

        step = Component(name="pipeline", outputs=outputs)
        assert isinstance(step.outputs, OutputArtifactDict)
        assert step.outputs.keys() == outputs.keys()

        assert step.outputs["train_data"].component == step and step.outputs["train_data"].name == "train_data"
        
        step.outputs["predict_data"] = Artifact()
        assert step.outputs["predict_data"].component == step and step.outputs["predict_data"].name == "predict_data"
        
        # 测试更改 outputs 属性
        with pytest.raises(AttributeError):
            step.outputs = {}
    
        # 设置错误的 artifact
        with pytest.raises(PaddleFlowSDKException):
            step.outputs["predict_data"] = 123

        with pytest.raises(PaddleFlowSDKException):
            step.outputs["predict_data09.//"] = Artifact()

        with pytest.raises(PaddleFlowSDKException):
            step = Component(name="pipeline", outputs={"predict_data": "1234"})
        
        step = Component(name="pipeline")
        assert isinstance(step.outputs, OutputArtifactDict) and len(step.outputs) == 0


    @pytest.mark.inputs
    def test_inputs(self):
        """ test inputs() and _set_inputs()
        """
        outputs = {
                    "train_data": Artifact(),
                    "model": Artifact()
                    }

        upstream_step = Component(name="pipeline", outputs=outputs)
        
        inputs = {
                "data": upstream_step.outputs["train_data"], 
                "model": upstream_step.outputs["model"],
                }

        step = Component(name="train", inputs=inputs)
        assert isinstance(step.inputs, InputArtifactDict) and len(step.inputs) == 2 and \
                step.inputs["data"].ref == upstream_step.outputs["train_data"]

        assert step.inputs["data"].name == "data"

        step.inputs["data2"] = upstream_step.outputs["train_data"]

        with pytest.raises(PaddleFlowSDKException):
            step = Component(name="train", inputs={"data": "data"})

        with pytest.raises(PaddleFlowSDKException):
            step = Component(name="train", inputs={"2524data": upstream_step.outputs["train_data"]})

        with pytest.raises(AttributeError):
            step.inputs = {}

    @pytest.mark.parameters
    def test_parameters(self):
        """ test parameters() and _set_parameters
        """
        parameters1 = {
                "data": "./dede",
                "model": Parameter(type=str, default="./model")
                }
        step1 = Component(name="upstream", parameters=parameters1)
        assert isinstance(step1.parameters, ParameterDict) and len(step1.parameters) == 2 and \
            step1.parameters["data"].ref == "./dede" and step1.parameters["data"].component == step1
    
        parameters2 = {
                "data": "./data/train_data",
                "model": step1.parameters["model"]
                }
        step2 = Component(name="step2", parameters=parameters2)
        assert step2.parameters["model"].ref == step1.parameters["model"] and step2.parameters["model"].component == step2 and \
                step1.parameters["model"].component == step1

        with pytest.raises(AttributeError):
            step2.parameters = {}

        with pytest.raises(PaddleFlowSDKException):
            step2.parameters["1222"] = 1234

        with pytest.raises(PaddleFlowSDKException):
            Component(name="step3", parameters=[1122])

        # 测试连续传递
        step4 = Component(name="step4", parameters={"model": step2.parameters["model"]})
        assert step4.parameters["model"].component == step4 and step4.parameters["model"].ref == step2.parameters["model"] and \
                step4.parameters["model"].ref.component == step2 and step4.parameters["model"].ref.ref == step1.parameters["model"]

    @pytest.mark.after
    def test_after(self):
        """ after
        """
        step1 = Component(name="step1")
        step2 = Component(name="step2")
        step3 = Component(name="step3")

        step2.after(step1)
        step3.after(step1, step2)

        assert step1._dependences == set() and len(step2._dependences) == 1 and "step1" in step2._dependences and \
                len(step3._dependences) == 2 and "step1" in step3._dependences and "step2" in step3._dependences
       
    @pytest.mark.init
    def test_init(self):
        """ test __init__
        """
        # 1. 测试名字合法性
        with pytest.raises(PaddleFlowSDKException):
            Component(name="1233")

        parameters1 = {
                "data": "./dede",
                "pre_model": Parameter(type=str, default="./model")
                }
        outputs = {
                    "train_data": Artifact(),
                    "model": Artifact()
                    }

        step1 = ContainerStep(name="step1", outputs=outputs)
        step2 = ContainerStep(name="step2", inputs={"model": step1.outputs["model"]}, outputs=outputs, parameters=parameters1,
                )

        assert step2.inputs["model"].ref.name == "model" and \
            step2.inputs["model"].ref.component.full_name == "step1"

        assert step1.outputs["model"].ref is None
        
        assert len(step2.outputs) == 2 and "model" in step2.outputs 
        assert step2.parameters["data"].ref == "./dede"

    @pytest.mark.loop_argument
    def test_loop_arugment(self):
        """ test loop_argument
        """
        step1 = Component(name="step1", loop_argument=[1,2,3])
        assert step1.loop_argument.argument == [1,2,3]
        assert str(step1.loop_argument.item) == "{{loop: step1.PF_LOOP_ARGUMENT}}"

        step2 = Component(name="step1")
        step2.loop_argument = [1,2,3]
        assert step2.loop_argument.argument == [1,2,3]
        assert str(step2.loop_argument.item) == "{{loop: step1.PF_LOOP_ARGUMENT}}"

    @pytest.mark.condition
    def test_condition(self):
        """ test condition
        """ 
        with pytest.raises(PaddleFlowSDKException):
            Component(name="step1", condition=1 >2)
        
        parameters1 = {
                "data": "./dede",
                "pre_model": Parameter(type=str, default="./model")
                }
        step1 = Component(name="step1", parameters=parameters1, condition="1>2")

        assert step1.condition == "1>2"

        step2 = Component(name="step1", parameters=parameters1, loop_argument=[1,2,3])
        step2.condition = f"{step2.loop_argument.item} > 2 && {step2.parameters['data']} == './dede'"

        assert step2.condition == "{{loop: step1.PF_LOOP_ARGUMENT}} > 2 && {{parameter: step1.data}} == './dede'"