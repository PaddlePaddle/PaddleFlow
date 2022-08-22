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

""" unit test for paddleflow.pipeline.dsl.compiler.step_compiler
"""
import pytest 

from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline.dsl.compiler.step_compiler import StepCompiler
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class TestStepCompiler(object):
    """ unit test for StepCompiler
    """
    def compile_part_step(self, step, part_func):
        """ compile_part_step
        """
        compiler = StepCompiler(step)
        getattr(compiler, part_func)()
        return compiler

    @pytest.mark.baseinfo
    def test_compile_base_info(self):
        """ test _compile_base_info()
        """
        
        # 1、step 没有任何信息
        step = ContainerStep(name="haha")
        compiler = self.compile_part_step(step, "_compile_base_info") 
        assert compiler._dict == {}

        # 2、step 只有部分基础信息
        step = ContainerStep(name="hahaha", command="echo hahaha")
        compiler = self.compile_part_step(step, "_compile_base_info") 

        assert compiler._dict == {"command": "echo hahaha"}

        # 3、step 有全量的信息
        step = ContainerStep(name="hahaha", command="echo hahaha", env={"name": "xiaodu"}, docker_env="python:3.7")
        compiler = self.compile_part_step(step, "_compile_base_info") 

        assert compiler._dict == {"command": "echo hahaha", "docker_env": "python:3.7", "env": {"name": "xiaodu"}}

    @pytest.mark.artifact
    def test_compile_artifacts(self):
        """ test _compile_artifacts(), _compile_input_artifact and _compile_output_artifact
        """
        # 1. 没有 artifact
        step = ContainerStep(name="haha")
        compiler = self.compile_part_step(step, "_compile_artifacts")
        assert compiler._dict == {}

        # 2. 只有输出artifact
        outputs = {
                    "train_data": Artifact(),
                    "validate_data": Artifact(),
                }
        
        step = ContainerStep(name="step", outputs=outputs)
        compiler = self.compile_part_step(step, "_compile_artifacts")
        assert len(compiler._dict["artifacts"]["output"]) == 2 and \
                isinstance(compiler._dict["artifacts"]["output"], list) and \
                "train_data" in compiler._dict["artifacts"]["output"] and \
                "validate_data" in compiler._dict["artifacts"]["output"] and \
                "input" not in compiler._dict["artifacts"]

        # 3. 只有输入 artifact
        step2 = ContainerStep(name="step2", inputs={"data": step.outputs["train_data"]})
        compiler = self.compile_part_step(step2, "_compile_artifacts")
        assert compiler._dict["artifacts"]["input"] == {"data": "{{step.train_data}}"} and \
                "output" not in compiler._dict
       
        # 4. 既有输出 artifact 又有 输入artifact
        step2.outputs["model"] = Artifact()
        compiler = self.compile_part_step(step2, "_compile_artifacts")
        assert compiler._dict["artifacts"] == {
                     "input": {"data": "{{step.train_data}}"},
                     "output": ["model"]
                }

    @pytest.mark.parameters
    def test_compile_params(self):
        """ test compile_parameters
        """
        # 1. 没有parameters
        step = ContainerStep(name="step")
        compiler = self.compile_part_step(step, "_compile_parameters")
        assert compiler._dict == {}

        # 2. parameters: 1) 来自上游， 2) 普通变量， 3) Parameter 实例
        parameters = {
                    "name": "pipeline",
                    "num": Parameter(type=int),
                    "base": Parameter(type=str, default="base info"),
                    "age": Parameter(default=123)
                }
        step = ContainerStep(name="step", parameters=parameters)
        compiler = self.compile_part_step(step, "_compile_parameters")
        assert compiler._dict["parameters"] == {
                    "name": "pipeline",
                    "num": {"type": "int"},
                    "base": {"type": "string", "default": "base info"},
                    "age": {"default": 123}
                }

        step2 = ContainerStep(name="step2", parameters={"name": step.parameters["name"]})
        compiler = self.compile_part_step(step2, "_compile_parameters")
        assert compiler._dict["parameters"] == {
                    "name": "{{step.name}}"
                }

    @pytest.mark.deps
    def test_compile_dependences(self):
        """ test _compile_dependences()
        """
        # 1. 没有dep
        step = ContainerStep(name="step")
        compiler = self.compile_part_step(step, "compile")
        assert "deps" not in compiler._dict

        # 2. 有 dep
        step = ContainerStep(name="step")
        step1 = ContainerStep(name="step1")
        step2 = ContainerStep(name="step2")

        step2.after(step, step1)

        compiler = self.compile_part_step(step2, "compile")
        deps = compiler._dict["deps"].split(",")
        assert len(deps) == 2 and "step" in deps and \
                "step1" in deps 

    @pytest.mark.cache
    def test_compiler_cache_options(self):
        """ test _compiler_cache_options
        """
        # 1. 没有 cache_options 配置
        step = ContainerStep(name="step")
        compiler = self.compile_part_step(step, "_compile_cache_options")
        assert compiler._dict == {}

        # 2. 有 cache_options
        cache = CacheOptions()
        step = ContainerStep(name="step", cache_options=cache)
        compiler = self.compile_part_step(step, "_compile_cache_options")
        assert compiler._dict["cache"] == {
                    "enable": False,
                    "max_expired_time": -1,
                }

    @pytest.mark.compile
    def test_compile(self):
        """ test compile
        """
        parameters = {
                "data": "/data",
                "epoch": Parameter(type=int, default=1),
                }
        outputs = {"model": Artifact()}
        env = {
                "batch_size": "123",
                }

        step = ContainerStep(name="step1", docker_env="python:3.7", env=env, parameters=parameters,
                outputs=outputs, command="echo 1234")
        step_dict = StepCompiler(step).compile()

        assert step_dict == {
                    "docker_env": "python:3.7",
                    "command": "echo 1234",
                    "env": env, 
                    "artifacts": {"output": ["model"]},
                    "parameters": {"data": "/data", "epoch": {"type": "int", "default": 1}},
                    "type": "step"
                    }

        step2 = ContainerStep(name="step2", docker_env="python:3.7", parameters={"data": step.parameters["data"]},
                inputs={"model": step.outputs["model"]}, command="echo 456")
        step2._dependences = {"step1"}

        step_dict = StepCompiler(step2).compile()

        assert step_dict == {
                "docker_env": "python:3.7",
                "command": "echo 456",
                "artifacts": {"input": {"model": "{{step1.model}}"}},
                "parameters": {"data": "{{step1.data}}"},
                "deps": "step1",
                "type": "step"
                }

    @pytest.mark.io_names
    def test_validate_io_names(self):
        """ validate io names
        """
        parameters = {
                "data": "/data",
                "epoch": Parameter(type=int, default=1),
                }
        outputs = {"model": Artifact()}
        env = {
                "batch_size": "123",
                }

        step = ContainerStep(name="step1", docker_env="python:3.7", env=env, parameters=parameters,
                outputs=outputs, command="echo 1234")
        
        step2 = ContainerStep(name="step2", docker_env="python:3.7", parameters={"data": step.parameters["data"]},
                inputs={"model": step.outputs["model"]}, command="echo 456")

        step_dict = StepCompiler(step2).compile()

        step2.parameters["model"] = "123"
        with pytest.raises(PaddleFlowSDKException):
            step_dict = StepCompiler(step2).compile()

        step2.parameters.pop("model")

        step2.outputs["model"] = Artifact()
        with pytest.raises(PaddleFlowSDKException):
            step_dict = StepCompiler(step2).compile()

        step2.outputs.pop("model")
        step2.outputs["data"] = Artifact()
        with pytest.raises(PaddleFlowSDKException):
            step_dict = StepCompiler(step2).compile()

    @pytest.mark.extra_fs
    def test_compile_extra_fs(self):
        """ unit test for _compile_extra_fs
        """
        parameters = {
                "data": "/data",
                "epoch": Parameter(type=int, default=1),
                }
        outputs = {"model": Artifact()}
        env = {
                "batch_size": "123",
                }

        step = ContainerStep(name="step1", docker_env="python:3.7", env=env, parameters=parameters,
                outputs=outputs, command="echo 1234")
        step_dict = StepCompiler(step).compile()

        assert "extra_fs" not in step_dict

        extra = ExtraFS(name="abc", mount_path="/home/work")
        extra2 = ExtraFS(name="abc", mount_path="/home/work2", read_only=True)

        step.extra_fs=[extra, extra2]
        step_dict = StepCompiler(step).compile()

        assert step_dict["extra_fs"] == [
            {"name": "abc", "mount_path": "/home/work", "read_only": False},
            {"name": "abc", "mount_path": "/home/work2", "read_only": True},
        ]