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

""" unit test for paddelfow.pipeline.dsl.io_typesdicts
"""

import pytest

from paddleflow.pipeline import Artifact
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import ContainerStep, DAG
from paddleflow.pipeline.dsl.io_types.dicts import  ParameterDict, InputArtifactDict, OutputArtifactDict, EnvDict
from paddleflow.pipeline.dsl.io_types.placeholder import ParameterPlaceholder, ArtifactPlaceholder
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
        upstream_param.set_base_info(component=upstream_step, name="department")

        step = Step(name="paddleflow")
        param_dict = ParameterDict(step)
        
        param_dict["name"] = "paddleflow"
        param_dict["department"] = upstream_param
        param_dict["age"] = Parameter(type="int", default=1)
        param_dict["placeholder"] = ParameterPlaceholder("holder", "abcdef")

        for key, value in param_dict.items():
            # 1. 校验类型
            assert isinstance(value, Parameter)

            # 2. 校验step
            assert value.component == step

        # 3. 校验 name
        assert param_dict["name"].name == "name"

        # 4. 校验 ref
        assert param_dict["department"].ref == upstream_param
        assert param_dict["name"].ref == "paddleflow"
        assert param_dict["age"].ref is None
        param_dict["placeholder"].ref.name == "holder"

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
        art1.set_base_info(component=upstream_step, name="department")
        inputs["department"] = art1
        inputs["holder"] = ArtifactPlaceholder(name="def", component_full_name="abc")

        assert inputs["department"].ref == art1
        assert inputs["holder"].ref.name == "def"

        with pytest.raises(PaddleFlowSDKException):
            inputs["23department2"] = art1


class TestOutputArtifactDict(object):
    """ test OutputArtifactDict
    """
    @pytest.mark.outputs
    def test_output_artifact_dict(self):
        """ test OutputArtifactDict
        """
        from paddleflow.pipeline.dsl.component import Step
        step = Step(name="paddleflow")
        upstream_step = Step(name="paddle")

        art1 = Artifact()
        art2 = Artifact()
        art2.set_base_info(component=upstream_step, name="department")

        outputs = OutputArtifactDict(step)
        outputs["department"] = art1
        with pytest.raises(PaddleFlowSDKException):
            outputs["holder"] = ArtifactPlaceholder(name="def", component_full_name="abc")

        dag = DAG("abc")
        outputs2 = OutputArtifactDict(dag)
        outputs2["holder"] = ArtifactPlaceholder(name="def", component_full_name="abc")

        assert outputs["department"] == art1
        assert outputs2["holder"].ref.name == "def"

        with pytest.raises(PaddleFlowSDKException):
            outputs["department2"] = art2

        with pytest.raises(PaddleFlowSDKException):
            outputs["23department"] = art1


class TestEnvDict(object):
    """ test EnvDict
    """
    @pytest.mark.env
    def test_env(self):
        """ test env dict
        """
        step = ContainerStep(name="step1")

        with pytest.raises(PaddleFlowSDKException):
            step.env["hahah"] = step

        with pytest.raises(PaddleFlowSDKException):
            step.env["123"] = "haha"

        step.env["haha"] = "123"

        p = Parameter(default="paddle")

        with pytest.raises(PaddleFlowSDKException):        
            step.env["hahah"] = p
        
        p.set_base_info("adf", step)
        step.env["hahah"] = p
        
        assert step.env["hahah"] == "{{parameter: step1.adf}}"

        a = Artifact()
        a.set_base_info("adf", step)
        with pytest.raises(PaddleFlowSDKException):
            step.env["haha"] = a