#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.compiler.compiler
"""
import os
import json
import yaml
import shutil
import pytest
import copy

from paddleflow.pipeline import Artifact
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import FailureOptions
from paddleflow.pipeline.dsl.compiler import Compiler
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

from test_pipeline.test_dsl.pipeline_demo import hello_with_io
from test_pipeline.test_dsl.pipeline_demo import hello_with_io_dict

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
        pipeline = Pipeline(name="hello", parallelism=5, docker_env="python:3.9")
        pipeline(hello_with_io)(name="pipeline", age=17)
        
        pipeline_dict = Compiler().compile(pipeline, "hello.yaml")
        ppl_dict = copy.deepcopy(hello_with_io_dict)
        ppl_dict.pop("cache")
        
        assert pipeline_dict == ppl_dict

        with open("hello.yaml") as fp:
            assert yaml.load(fp) == pipeline_dict

        os.remove("hello.yaml")

        cache = CacheOptions(fs_scope="/", enable=True)
        fail = FailureOptions()
        pipeline = Pipeline(name="hello", parallelism=5, docker_env="python:3.9", failure_options=fail, cache_options=cache)
        pipeline(hello_with_io)(name="pipeline", age=17)
        pipeline_dict = Compiler().compile(pipeline)
        ppl_dict = copy.deepcopy(hello_with_io_dict)
        ppl_dict["failure_options"] = {"strategy": "fail_fast"}

        assert pipeline_dict == ppl_dict



