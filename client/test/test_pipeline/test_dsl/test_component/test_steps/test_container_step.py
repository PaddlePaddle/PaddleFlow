#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.steps.container_step
"""

import pytest

from pathlib import Path
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.options import FSScope
from paddleflow.pipeline.dsl.options import ExtraFS
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

        fs_scope = FSScope(name="xiaoming", path="def")
        cache = CacheOptions(enable=True, fs_scope=[fs_scope])
        step1 = ContainerStep(name="step1", outputs=outputs, cache_options=cache)

        step2 = ContainerStep(name="step2", env=env, inputs={"model": step1.outputs["model"]}, outputs=outputs,
                parameters=parameters1, docker_env="python:3.7", command="python3 train.py")

        assert step1.cache_options is cache
        assert step2.docker_env == "python:3.7" and step2.command == "python3 train.py" and \
                step2.env == env and step2.inputs["model"].ref == step1.outputs["model"]
        
        assert len(step2.outputs) == 2 and "model" in step2.outputs 
        assert step2.parameters["data"].ref == "./dede"

        with pytest.raises(PaddleFlowSDKException):
            step1 = ContainerStep(name="step1", outputs=outputs, extra_fs="ad")

        extra_fs = ExtraFS(name="xiaoming")
        extra_fs2 = ExtraFS(name="xx")
        step1 = ContainerStep(name="step1", outputs=outputs, extra_fs=extra_fs)
        assert step1.extra_fs == [extra_fs]

        step1.extra_fs.append(extra_fs2)
        assert  step1.extra_fs == [extra_fs, extra_fs2]

    @pytest.mark.add_env
    def test_add_env(self):
        """ test add_env() and env()
        """
        env = {
                "name": "pipeline",
                "company": "Baidu",
                "age": "2",
                }
        step1 = ContainerStep(name="step1", parameters={"def": 123})
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

        step2 = ContainerStep(name="step2")
        step2.add_env({"abc": step1.parameters["def"]})
        assert step2.env["abc"] == "{{parameter: step1.def}}"
