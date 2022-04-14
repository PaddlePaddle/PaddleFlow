#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.steps.step
"""

import pytest 
from pathlib import Path

from paddleflow.pipeline.dsl.steps.step import Step
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.io_types.dicts import InputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import OutputArtifactDict
from paddleflow.pipeline.dsl.io_types.dicts import ParameterDict
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import STEP_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import VARIBLE_NAME_REGEX
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class TestStep(object):
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

        step = Step(name="pipeline", outputs=outputs)
        assert isinstance(step.outputs, OutputArtifactDict)
        assert step.outputs.keys() == outputs.keys()

        assert step.outputs["train_data"].step == step and step.outputs["train_data"].name == "train_data"
        
        step.outputs["predict_data"] = Artifact()
        assert step.outputs["predict_data"].step == step and step.outputs["predict_data"].name == "predict_data"
        
        # 测试更改 outputs 属性
        with pytest.raises(AttributeError):
            step.outputs = {}
    
        # 设置错误的 artifact
        with pytest.raises(PaddleFlowSDKException):
            step.outputs["predict_data"] = 123

        with pytest.raises(PaddleFlowSDKException):
            step.outputs["predict_data09.//"] = Artifact()

        with pytest.raises(PaddleFlowSDKException):
            step = Step(name="pipeline", outputs={"predict_data": "1234"})
        
        step = Step(name="pipeline")
        assert isinstance(step.outputs, OutputArtifactDict) and len(step.outputs) == 0


    @pytest.mark.inputs
    def test_inputs(self):
        """ test inputs() and _set_inputs()
        """
        outputs = {
                    "train_data": Artifact(),
                    "model": Artifact()
                    }

        upstream_step = Step(name="pipeline", outputs=outputs)
        
        inputs = {
                "data": upstream_step.outputs["train_data"], 
                "model": upstream_step.outputs["model"],
                }

        step = Step(name="train", inputs=inputs)
        assert isinstance(step.inputs, InputArtifactDict) and len(step.inputs) == 2 and \
                step.inputs["data"] == upstream_step.outputs["train_data"]

        assert step.inputs["data"].name == "train_data"

        step.inputs["data2"] = upstream_step.outputs["train_data"]

        with pytest.raises(PaddleFlowSDKException):
            step = Step(name="train", inputs={"data": "data"})

        with pytest.raises(PaddleFlowSDKException):
            step = Step(name="train", inputs={"2524data": upstream_step.outputs["train_data"]})

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
        step1 = Step(name="upstream", parameters=parameters1)
        assert isinstance(step1.parameters, ParameterDict) and len(step1.parameters) == 2 and \
            step1.parameters["data"].ref == "./dede" and step1.parameters["data"].step == step1
    
        parameters2 = {
                "data": "./data/train_data",
                "model": step1.parameters["model"]
                }
        step2 = Step(name="step2", parameters=parameters2)
        assert step2.parameters["model"].ref == step1.parameters["model"] and step2.parameters["model"].step == step2 and \
                step1.parameters["model"].step == step1

        with pytest.raises(AttributeError):
            step2.parameters = {}

        with pytest.raises(PaddleFlowSDKException):
            step2.parameters["1222"] = 1234

        with pytest.raises(PaddleFlowSDKException):
            Step(name="step3", parameters=[1122])

        # 测试连续传递
        step4 = Step(name="step4", parameters={"model": step2.parameters["model"]})
        assert step4.parameters["model"].step == step4 and step4.parameters["model"].ref == step2.parameters["model"] and \
                step4.parameters["model"].ref.step == step2 and step4.parameters["model"].ref.ref == step1.parameters["model"]

    @pytest.mark.after
    def test_after(self):
        """ after
        """
        step1 = Step(name="step1")
        step2 = Step(name="step2")
        step3 = Step(name="step3")

        step2.after(step1)
        step3.after(step1, step2)

        assert step1._dependences == [] and len(step2._dependences) == 1 and step1 in step2._dependences and \
                len(step3._dependences) == 2 and step1 in step3._dependences and step2 in step3._dependences
    
        

    @pytest.mark.dependences
    def test_get_dependences(self):
        """ test get_dependences
        """
        parameters1 = {
                "data": "./dede",
                "pre_model": Parameter(type=str, default="./model")
                }
        outputs = {
                    "train_data": Artifact(),
                    "model": Artifact()
                    }

        step1 = Step(name="step1", parameters=parameters1, outputs=outputs)
        step2 = Step(name="step2", parameters={"data": step1.parameters["data"]})
        step3 = Step(name="step3", inputs={"model": step1.outputs["model"]})

        step4 = Step(name="step4")

        step4.after(step1, step2)

        assert step2.get_dependences() == [step1]
        assert step3.get_dependences() == [step1]
        assert len(step4.get_dependences()) == 2 and step1 in step4.get_dependences() and \
                step2 in step4.get_dependences()

        step3.parameters["data"] = step2.parameters["data"]
        step4.parameters["data"] = step3.parameters["data"]

        assert step2 in step3.get_dependences() and step3 in step4.get_dependences()
        assert len(step3.get_dependences()) == 2 and len(step4.get_dependences()) == 3

        # 测试dependences 是否会重复添加同一个 step
        step3.after(step1)
        assert len(step3.get_dependences()) == 2
       
    @pytest.mark.init
    def test_init(self):
        """ test __init__
        """
        # 1. 测试名字合法性
        with pytest.raises(PaddleFlowSDKException):
            Step(name="1233")

        parameters1 = {
                "data": "./dede",
                "pre_model": Parameter(type=str, default="./model")
                }
        outputs = {
                    "train_data": Artifact(),
                    "model": Artifact()
                    }

        step1 = Step(name="step1", outputs=outputs)
        step2 = Step(name="step2", inputs={"model": step1.outputs["model"]}, outputs=outputs, parameters=parameters1,
                )

        assert step2.inputs["model"] == step1.outputs["model"]
        
        assert len(step2.outputs) == 2 and "model" in step2.outputs 
        assert step2.parameters["data"].ref == "./dede"

    @pytest.mark.io_names
    def test_validate_io_names(self):
        """ test validate_io_names()
        """
        parameters1 = {
                "data": "./dede",
                "pre_model": Parameter(type=str, default="./model")
                }
        outputs = {
                    "train_data": Artifact(),
                    "model": Artifact()
                    }
        step1 = Step(name="step1", outputs=outputs)
        step2 = Step(name="step2", parameters=parameters1, inputs={"train": step1.outputs["train_data"]})
        step2.validate_io_names()

        step2.outputs["train"] = Artifact()
        with pytest.raises(PaddleFlowSDKException):
            step2.validate_io_names()

        step2.outputs.pop("train")

        step2.inputs["pre_model"] = step1.outputs["model"]
        with pytest.raises(PaddleFlowSDKException):
            step2.validate_io_names()

        step2.inputs.pop("pre_model")
        step2.outputs["data"] = Artifact()
        with pytest.raises(PaddleFlowSDKException):
            step2.validate_io_names()
