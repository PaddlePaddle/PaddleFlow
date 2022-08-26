#!/usr/bin/env python3

""" unit test for paddleflow.pipeline.dsl.pipeline
"""
import os
import yaml
import copy
import json
import pytest 
from pathlib import Path
from pytest_mock import mocker

from .pipeline_demo import hello, hello_with_io
from .pipeline_demo import dag_example

from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import DAG
from paddleflow.pipeline.dsl.component import Step
from paddleflow.pipeline.dsl.component import ContainerStep
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import PIPELINE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError, ENTRY_POINT_NAME
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class TestPipeline(object):
    """ unit test for Pipeline
    """
    @pytest.mark.add_env
    def test_add_env(self):
        """ test add_env() and env()
        """
        env = {
                "name": "pipeline",
                "company": "Baidu",
                "age": "2",
                }
        pipeline1 = Pipeline(name="pipeline1")
        pipeline1.add_env(env)
        
        assert pipeline1.env == env

        env["age"] = "2"
        assert pipeline1.env == env

        with pytest.raises(PaddleFlowSDKException):
            pipeline1.add_env({"12245": "345"})

        with pytest.raises(PaddleFlowSDKException):
            pipeline1.add_env({"12245": pipeline1})
    
        with pytest.raises(AttributeError):
            pipeline1.env = {}

    @pytest.mark.name
    def test_name(self):
        """ test name
        """
        pipeline = Pipeline(name="ppl")
        assert pipeline.name == "ppl"

        pipeline.name = "ppl1"
        assert pipeline.name == "ppl1"

        with pytest.raises(PaddleFlowSDKException):
            pipeline.name = "01224"

        with pytest.raises(PaddleFlowSDKException):
            pipeline.name = "a" * 53

    @pytest.mark.call
    def test_call(self):
        """ test __call__ and _organize_pipeline
        """
        pipeline = Pipeline(name="hello")(hello)(name="paddleflow", age=1)

        assert len(pipeline._entry_points.entry_points) == 3 and "introduce" in pipeline._entry_points.entry_points
        assert ENTRY_POINT_NAME + ".introduce" in pipeline._entry_points.entry_points["hello"]._dependences

        assert pipeline._entry_points.entry_points["hello"].parameters["name"].ref == "paddleflow" and \
                pipeline._entry_points.entry_points["hello"].parameters["age"].ref == 1 and \
                pipeline._entry_points.entry_points["byebye"].parameters["name"].ref == pipeline._entry_points.entry_points["hello"].parameters["name"]

        pipeline = Pipeline(name="dag_example")(dag_example)()
        assert len(pipeline._entry_points.entry_points) == 4 
        assert "show" in pipeline._entry_points.entry_points and "dag-show" not in pipeline._entry_points.entry_points and \
            "dag-show" in pipeline._entry_points.entry_points["dag2"].entry_points
        
    @pytest.mark.compile
    def test_compile(self):
        """ test compile
        """
        cache = CacheOptions(enable=True, max_expired_time=-1)
        pipeline = Pipeline(name="dag_examle", docker_env="python:3.9", cache_options=cache)(dag_example)()

        ppl_dict = pipeline.compile(save_path="./hello_with_io.yaml")
        with open("./hello_with_io.yaml") as fp:
            assert yaml.safe_load(fp) == ppl_dict 

        os.remove("./hello_with_io.yaml")

    @pytest.mark.run
    def test_run(self, mocker):
        """ test run 
        """
        pf_config = str(Path(__file__).parent / "pf.config")
        pipeline = Pipeline(name="hello", docker_env="python:3.9")(hello_with_io)(name="pipeline", age=17)

        mocker.patch("paddleflow.Client.login", return_value=(True, ""))
        mocker.patch("paddleflow.Client.create_run", return_value=None)
        mocker.patch("paddleflow.Client.pre_check", return_value=None)
        
        pipeline.run(pf_config, "xiaodu", "fs-test", "test", "just for test")

        pipeline._client = None
        mocker.patch("paddleflow.Client.login", return_value=(False, ""))
        with pytest.raises(PaddleFlowSDKException):
            pipeline.run(pf_config, "xiaodu", "fs-test", "test", "just for test")

        with pytest.raises(PaddleFlowSDKException):
            pipeline.run(pf_config, "xiaodu", "fs-test", "test", "just for test", disabled=["ad1223"])
        
        cache = CacheOptions(fs_scope="/")
        pipeline = Pipeline(name="hello", docker_env="python:3.9", cache_options=cache)
        pipeline(hello_with_io)(name="pipeline", age=17)
        
        with pytest.raises(PaddleFlowSDKException):
            pipeline.run(pf_config, "xiaodu")

    @pytest.mark.post
    def test_post_process(self):
        """ test set_post_process and get_post_process
        """
        # 1. set_post_process
        pipeline = Pipeline(name="hello", docker_env="python:3.9")(hello_with_io)(name="pipeline", age=17)

        step1 = ContainerStep(name="post-process")
        pipeline.set_post_process(step1)

        assert  pipeline.get_post_process() == step1

        step2 = ContainerStep(name="post-process2")
        pipeline.set_post_process(step2)

        assert len(pipeline._post_process) == 1 and pipeline.get_post_process() == step2
