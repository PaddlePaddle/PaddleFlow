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


from paddleflow.pipeline import Pipeline
from paddleflow.pipeline.dsl.steps import Step
from paddleflow.pipeline.dsl.steps import ContainerStep
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import PIPELINE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

from .pipeline_demo import hello
from .pipeline_demo import hello_with_io
from .pipeline_demo import hello_with_io_dict

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

    @pytest.mark.add_step
    def test_add_step(self):
        """ test add_step(), _make_step_name_unique_by_adding_index() and step()
        """
        step1 = ContainerStep(name="step1")
        step2 = ContainerStep(name="step-2")
        step3 = ContainerStep(name="step-3")

        pipeline = Pipeline(name="pipeline")
        
        pipeline.add(step1)
        pipeline.add(step2)
        pipeline.add(step3)
        
        assert len(pipeline.steps) == 3 and pipeline._steps["step1"] is step1

        step4 = ContainerStep(name="step1")
        with pytest.raises(PaddleFlowSDKException):
            pipeline.add(step4)

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

    @pytest.mark.get_params
    def test_get_params(self):
        """ test get_params
        """
        step1 = ContainerStep(name="step1")
        step2 = ContainerStep(name="step2", parameters={"name": "xiaodu"})
        step3 = ContainerStep(name="step3", parameters={"name": step2.parameters["name"], "age": "18"})

        pipeline = Pipeline(name="pipeline")
        
        pipeline.add(step1)
        pipeline.add(step2)
        pipeline.add(step3)

        params = pipeline.get_params()
        assert len(params[step1]) == 0
        assert len(params[step2]) == 1 and params[step2]["name"].ref == "xiaodu"
        assert len(params[step3]) == 2 and params[step3]["name"].ref == params[step2]["name"]

    @pytest.mark.topo
    def test_topological_sort(self):
        """ test topological_sort
        """
        step1 = ContainerStep(name="step1")
        step2 = ContainerStep(name="step2", parameters={"name": "xiaodu"})
        step3 = ContainerStep(name="step3", parameters={"name": step2.parameters["name"], "age": "18"})

        pipeline = Pipeline(name="pipeline")
        
        pipeline.add(step1)
        pipeline.add(step2)
        pipeline.add(step3)
        
        step2.after(step1)
        step3.after(step2)
        topos = pipeline.topological_sort()
        assert topos == [step1, step2, step3]

        step1.after(step3)
        with pytest.raises(PaddleFlowSDKException):
            pipeline.topological_sort()

        # 节点依赖于自身
        step1 = ContainerStep(name="step4")
        step1.after(step1)

        pipeline = Pipeline(name="pipeline")
        pipeline.add(step1)
        with pytest.raises(PaddleFlowSDKException):
            pipeline.topological_sort()

        # 部分节点构成环
        step1 = ContainerStep(name="step1")
        step2 = ContainerStep(name="step2", parameters={"name": "xiaodu"})
        step3 = ContainerStep(name="step3", parameters={"name": step2.parameters["name"], "age": "18"})

        pipeline = Pipeline(name="pipeline")
        
        pipeline.add(step1)
        pipeline.add(step2)
        pipeline.add(step3)

        step2.after(step3)
        with pytest.raises(PaddleFlowSDKException):
            pipeline.topological_sort()
    
    @pytest.mark.call
    def test_call(self):
        """ test __call__ and _organize_pipeline
        """
        pipeline = Pipeline(name="hello")
        pipeline(hello)(name="paddleflow", age=1)

        assert len(pipeline._steps) == 3 and "introduce" in pipeline._steps
        assert pipeline._steps["hello"].get_dependences()[0] == pipeline._steps["introduce"]

        assert pipeline._steps["hello"].parameters["name"].ref == "paddleflow" and \
                pipeline._steps["hello"].parameters["age"].ref == 1 and \
                pipeline._steps["byebye"].parameters["name"].ref == pipeline._steps["hello"].parameters["name"]

        assert pipeline.topological_sort() == \
                [pipeline._steps["introduce"], pipeline._steps["hello"], pipeline._steps["byebye"]
                ]

    @pytest.mark.compile
    def test_compile(self):
        """ test compile
        """
        cache = CacheOptions(fs_scope="/", enable=True, max_expired_time=-1)
        pipeline = Pipeline(name="hello", docker_env="python:3.9", cache_options=cache)
        pipeline(hello_with_io)(name="pipeline", age=17)
        ppl_dict = copy.deepcopy(hello_with_io_dict)

        ppl_dict.pop("parallelism")
        assert pipeline.compile() == ppl_dict 

        pipeline.compile(save_path="./hello_with_io.yaml")
        with open("./hello_with_io.yaml") as fp:
            assert yaml.safe_load(fp) == ppl_dict 

        os.remove("./hello_with_io.yaml")

    @pytest.mark.run
    def test_run(self, mocker):
        """ test run 
        """
        pf_config = str(Path(__file__).parent / "pf.config")
        pipeline = Pipeline(name="hello", docker_env="python:3.9")
        pipeline(hello_with_io)(name="pipeline", age=17)

        mocker.patch("paddleflow.Client.login", return_value=(True, ""))
        mocker.patch("paddleflow.Client.create_run", return_value=None)
        mocker.patch("paddleflow.Client.pre_check", return_value=None)
        
        pipeline.run(pf_config, "xiaodu", "fs-test", "test", "just for test")
        
        with pytest.raises(PaddleFlowSDKException):
            pipeline.run(pf_config, "xiaodu", "fs-test", "test", "just for test", entry="abc")

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

    def test_update_and_validate_steps(self):
        """ test _update_and_validate_steps 
        """
        pipeline = Pipeline(name="hello")
        step1 = ContainerStep(name="step1")

        pipeline.add(step1)
        with pytest.raises(PaddleFlowSDKException):
            pipeline._update_and_validate_steps(pipeline.steps)

    @pytest.mark.post
    def test_post_process(self):
        """ test set_post_process and get_post_process
        """
        # 1. set_post_process
        pipeline = Pipeline(name="hello", docker_env="python:3.9")
        pipeline(hello_with_io)(name="pipeline", age=17)

        step1 = ContainerStep(name="post-process")
        pipeline.set_post_process(step1)

        assert  pipeline.get_post_process() == step1

        step2 = ContainerStep(name="post-process2")
        pipeline.set_post_process(step2)

        assert len(pipeline._post_process) == 1 and pipeline.get_post_process() == step2

        topo_sort = pipeline.topological_sort()
        assert len(topo_sort) == 4 and topo_sort[-1] == step2

        # 2. test depeneds on entrypoints
        step3 = ContainerStep(name="post-process2", inputs={"art1": pipeline.steps["hello"].outputs["information"]})
        pipeline.set_post_process(step3)
        with pytest.raises(PaddleFlowSDKException):
            pipeline._update_and_validate_steps(pipeline._post_process)

