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

""" unit test for paddleflow.pipeline.dsl.compiler.compiler
"""
import os
import json
import yaml
import shutil
import pytest
import copy

from paddleflow.pipeline import DAG
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import FailureOptions
from paddleflow.pipeline import FSOptions
from paddleflow.pipeline import FSScope
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline.dsl.compiler import Compiler
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class TestCompiler(object):
    """ test Compiler
    """
    @pytest.mark.write
    def test_write(self):
        """ test _write()
        """
        compiler = Compiler()
        compiler._pipeline_dict = {"name": "pipeline"}

        compiler._write("ppl.json")
        compiler._write("ppl.yaml")
        compiler._write("ppl.yml")

        with open("ppl.json") as fp:
            assert json.load(fp) == {"name": "pipeline"}

        with open("ppl.yml") as fp:
            assert yaml.load(fp) == {"name": "pipeline"}

        with open("ppl.yaml") as fp:
            assert yaml.load(fp) == {"name": "pipeline"}

        compiler._write("./pipeline_write/pipeline1/ppl.yml")
        with open("./pipeline_write/pipeline1/ppl.yml") as fp:
            assert yaml.load(fp) == {"name": "pipeline"}

        with pytest.raises(PaddleFlowSDKException):
            compiler._write("ppl.txt")

        os.remove("ppl.json")
        os.remove("ppl.yaml")
        os.remove("ppl.yml")

        shutil.rmtree("./pipeline_write")

    @pytest.mark.compile
    def test_compile(self):
        """ test compile()
        """
        with DAG("dag") as dag:
            step1 = ContainerStep(
                name="step1",
                parameters={"abc": 123, "def": 456},
                outputs={"out1": Artifact()}
                )

            with DAG("dag1") as dag1:
                step2 = ContainerStep(
                name="step2",
                )
                step3 = ContainerStep(
                name="step3",
                outputs = {"out1": Artifact(), "out2": Artifact()}
                )
            
            step4 = ContainerStep(
                name="step4",
                )
            
        dag1.inputs["in1"] = step1.outputs["out1"]
        dag1.parameters["abc1"] = step1.parameters["abc"]
        dag1.parameters["def"] = step1.parameters["def"]

        step2.parameters["abc"] = dag1.parameters["abc1"]

        step3.parameters["def1"] = dag1.parameters["def"]
        step3.inputs["in"] = dag1.inputs["in1"]

        dag1.outputs["out1"] = step3.outputs["out1"]
        dag1.outputs["out2"] = step3.outputs["out2"]

        step4.inputs["in1"] = dag1.outputs["out1"]
        step4.inputs["in2"] = dag1.outputs["out2"]

        pipeline = Pipeline(name="hello", parallelism=5, docker_env="python:3.9")
        pipeline._entry_points = dag
        
        pipeline_dict = Compiler().compile(pipeline, "hello.yaml")
        assert len(pipeline_dict["entry_points"]) == 3
        assert "step1" in pipeline_dict["entry_points"] and "dag1" in pipeline_dict["entry_points"] and \
            "step4" in pipeline_dict["entry_points"]
        assert pipeline_dict["name"] == "hello"
        assert pipeline_dict["parallelism"] == 5
        assert pipeline_dict["docker_env"] == "python:3.9"

        for key in ["post_process", "cache", "failure_options", "fs_options", "type"]:
            assert key not in pipeline_dict

        fs_scope1 = FSScope(name="abc", path="abc,def")
        fs_scope2 = FSScope(name="abc2", path="abc,def")
        cache = CacheOptions(enable=True, fs_scope=[fs_scope1, fs_scope2])

        extra1 = ExtraFS(name="abc", sub_path="abc")
        extra2 = ExtraFS(name="abc", sub_path="abc2")
        main_fs = MainFS(name="abc", sub_path="main")
        fs_options = FSOptions(main_fs=main_fs, extra_fs=[extra1, extra2])

        st7 = ContainerStep(name="abcd")
        pipeline = Pipeline(name="hello", parallelism=5, docker_env="python:3.9",
            fs_options=fs_options, cache_options=cache, failure_options=FailureOptions("fail_fast"),
            )
        pipeline.set_post_process(st7)
        pipeline._entry_points = dag
        pipeline_dict = Compiler().compile(pipeline, "hello.yaml")

        assert len(pipeline_dict["cache"]["fs_scope"]) == 2
        assert len(pipeline_dict["fs_options"]["extra_fs"]) == 2
        assert pipeline_dict["fs_options"]["main_fs"] == {"name": "abc", "sub_path": "main"}
        assert pipeline_dict["failure_options"] == {'strategy': 'fail_fast'}

        pipeline = Pipeline(name="hello", parallelism=5, docker_env="python:3.9",
            fs_options=fs_options, cache_options=cache, failure_options=FailureOptions("fail_fast"),
            )
        st7 = ContainerStep(name="abcd", condition=f"{step3.outputs['out1']} > 10")
        pipeline.set_post_process(st7)
        pipeline._entry_points = dag
        with pytest.raises(PaddleFlowSDKException):
            Compiler().compile(pipeline, "hello.yaml")
        
        st7 = ContainerStep(name="step1")
        pipeline.set_post_process(st7)
        pipeline._entry_points = dag
        with pytest.raises(PaddleFlowSDKException):
            Compiler().compile(pipeline, "hello.yaml")

        st7 = ContainerStep(name="step3")
        pipeline.set_post_process(st7)
        pipeline._entry_points = dag
        Compiler().compile(pipeline, "hello.yaml")

        pipeline = Pipeline(name="hello", parallelism=5)
        pipeline._entry_points = dag
        
        with pytest.raises(PaddleFlowSDKException):
            pipeline_dict = Compiler().compile(pipeline, "hello.yaml")

        
        pipeline = Pipeline(name="hello", parallelism=5)
        pipeline._entry_points = dag
        step1.docker_env = "python:3.7"
        step2.docker_env = "python:3.7"
        step3.docker_env = "python:3.7"
        step4.docker_env = "python:3.7"