#!/usr/bin/env python3
""" unit test for paddelfow.pipeline.dsl.io_typesdicts
"""

import pytest

from paddleflow.pipeline import Artifact
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline.dsl.io_types.dicts import  ParameterDict, InputArtifactDict, OutputArtifactDict, EnvDict
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

from .mock_step import Step

class TestParameterDict(object):
    """ test ParameterDict
    """
    @pytest.mark.param
    def test_param(self):
        """ test ParameterDict
        """
        upstream_step = Step(name="paddle")
        upstream_param = Parameter(default="paddle")
        upstream_param.set_base_info(step=upstream_step, name="department")

        step = Step(name="paddleflow")
        param_dict = ParameterDict(step)
        
        param_dict["name"] = "paddleflow"
        param_dict["department"] = upstream_param
        param_dict["age"] = Parameter(type="int", default=1)

        for key, value in param_dict.items():
            # 1. 校验类型
            assert isinstance(value, Parameter)

            # 2. 校验step
            assert value.step == step

        # 3. 校验 name
        assert param_dict["name"].name == "name"

        # 4. 校验 ref
        assert param_dict["department"].ref == upstream_param
        assert param_dict["name"].ref == "paddleflow"
        assert param_dict["age"].ref is None

        assert param_dict["age"].default == 1


class TestInputArtifactDict(object):
    """ test InputArtifactDict
    """
    @pytest.mark.inputs
    def test_input_artifact_dict(self):
        """ test input artifact_dict
        """
        upstream_step = Step(name="paddle")
        step = Step(name="paddleflow")

        inputs = InputArtifactDict(step)
        
        with pytest.raises(PaddleFlowSDKException):
            inputs["abc"] = Artifact()

        art1 = Artifact()
        art1.set_base_info(step=upstream_step, name="department")
        inputs["department"] = art1

        assert inputs["department"] == art1

        with pytest.raises(PaddleFlowSDKException):
            inputs["23department2"] = art1


class TestOutputArtifactDict(object):
    """ test OutputArtifactDict
    """
    @pytest.mark.outputs
    def test_output_artifact_dict(self):
        """ test OutputArtifactDict
        """
        step = Step(name="paddleflow")
        upstream_step = Step(name="paddle")

        art1 = Artifact()
        art2 = Artifact()
        art2.set_base_info(step=upstream_step, name="department")

        outputs = OutputArtifactDict(step)
        outputs["department"] = art1

        with pytest.raises(PaddleFlowSDKException):
            outputs["department2"] = art2

        with pytest.raises(PaddleFlowSDKException):
            outputs["23department"] = art1


class TestEnvDict(object):
    """ test EnvDict
    """
    def test_env(self):
        """ test env dict
        """
        step = ContainerStep(name="step1")

        with pytest.raises(PaddleFlowSDKException):
            step.env["hahah"] = step

        with pytest.raises(PaddleFlowSDKException):
            step.env["123"] = "haha"

        step.env["haha"] = "123"
