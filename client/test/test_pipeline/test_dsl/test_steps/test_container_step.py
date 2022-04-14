#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.steps.container_step
"""

import pytest

from pathlib import Path
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class TestContainerStep(object):
    """ unit test for ContainerStep
    """
    @pytest.mark.init
    def test_init(self):
        """ test __init__
        """
        parameters1 = {
                "data": "./dede",
                "pre_model": Parameter(type=str, default="./model")
                }
        outputs = {
                    "train_data": Artifact(),
                    "model": Artifact()
                    }
        env = {
                "name": "pipeline",
                "company": "Baidu",
                "age": "2",
                }

        cache = CacheOptions(enable=True, fs_scope="/")
        step1 = ContainerStep(name="step1", outputs=outputs, cache_options=cache)
        step2 = ContainerStep(name="step2", env=env, inputs={"model": step1.outputs["model"]}, outputs=outputs,
                parameters=parameters1, docker_env="python:3.7", command="python3 train.py")

        assert step1.cache_options is cache
        assert step2.docker_env == "python:3.7" and step2.command == "python3 train.py" and \
                step2.env == env and step2.inputs["model"] == step1.outputs["model"]
        
        assert len(step2.outputs) == 2 and "model" in step2.outputs 
        assert step2.parameters["data"].ref == "./dede"

    @pytest.mark.add_env
    def test_add_env(self):
        """ test add_env() and env()
        """
        env = {
                "name": "pipeline",
                "company": "Baidu",
                "age": "2",
                }
        step1 = ContainerStep(name="step1")
        step1.add_env(env)
        
        assert step1.env == env

        
        step1.add_env({"age": "3"})
        assert step1.env["age"] == "3"

        with pytest.raises(PaddleFlowSDKException):
            step1.add_env({"12245": "345"})

        with pytest.raises(PaddleFlowSDKException):
            step1.add_env({"12245": step1})
    
        with pytest.raises(AttributeError):
            step1.env = {}
