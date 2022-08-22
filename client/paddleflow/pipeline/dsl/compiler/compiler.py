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


import json 
import yaml
from pathlib import Path

from .step_compiler import StepCompiler
from .dag_compiler import DAGCompiler

from paddleflow.pipeline.dsl.inferer import ContainerStepInferer
from paddleflow.pipeline.dsl.inferer import DAGInferer
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


class Compiler(object):
    """ Compiler: trans dsl.Pipeline to static description string
    """
    def compile(
            self,
            pipeline,
            save_path: str=None):
        """ trans dsl.Pipeline to static description string

        Args:
            pipeline (dsl.Pipeline): the Pipeline instances which need to trans to static description string
            save_path: (str): the path of file to save static description string, should be end with one of[".json", ".yaml", ".yml"]

        Returns:
            a dict which description this Pipeline instance

        Raises:
            PaddleFlowSDKException: if compile failed
        """
        # 1、init pipeline_dict
        self._pipeline_dict = {}

        # 2、compile Entypoint
        self._pipeline_dict["entry_points"] = DAGCompiler(pipeline._entry_points).compile()["entry_points"]
        
        # 3、compile post_process
        post_process = pipeline.get_post_process()

        if post_process is not None:
            self._pipeline_dict["post_process"] = {}
            self._pipeline_dict["post_process"][post_process.name] = StepCompiler(post_process).compile()

        # 4、trans pipeline conf
        if pipeline.docker_env:
            self._pipeline_dict["docker_env"] = pipeline.docker_env
        
        self._pipeline_dict["name"] = pipeline.name
        
        if pipeline.cache_options and pipeline.cache_options.compile():
            self._pipeline_dict["cache"] = pipeline.cache_options.compile()

        if pipeline.failure_options and pipeline.failure_options.compile():
            self._pipeline_dict["failure_options"] = pipeline.failure_options.compile()

        if pipeline.parallelism:
            self._pipeline_dict["parallelism"] = pipeline.parallelism

        if pipeline.fs_options:
            self._pipeline_dict["fs_options"] = pipeline.fs_options.compile()

        self._valiedate()
        #4、write to file
        if save_path:
            self._write(save_path)

        return self._pipeline_dict


    def _write(
            self,
            save_path: str=None):
        """ write pipeline_dict to save_path

        Args:
            the path of file to save static description string, should be end with one of[".json", ".yaml", ".yml"]

        Raises:
            PaddleFlowSDKException: if save_path is not end with one of [".json", ".yaml", ".yml"]
        """
        suffix = save_path.split(".")[-1]

        if suffix not in ["json", "yaml", "yml"] or len(save_path.split(".")) < 2:
            raise PaddleFlowSDKException(PipelineDSLError, 
                    "the name of the file to save pipeline static description should be ends with one of" + \
                            '[".json", ".yaml", ".yml"]') 

        Path(save_path).parent.mkdir(exist_ok=True, parents=True)
        with open(save_path, "w") as fp:
            if suffix in ["json"]:
                json.dump(self._pipeline_dict, fp)

            else:
                yaml.dump(self._pipeline_dict, fp)

    def _validate_post_process(self):
        """ validate post_process is illegal or not
        """
        if "post_process" not in self._pipeline_dict:
            return 

        for name, post_process in self._pipeline_dict["post_process"].items():
            for key in ["condition", "entry_points", "loop_arugment", "deps", "cache"]:
                if key in post_process:
                    raise PaddleFlowSDKException(PipelineDSLError, 
                        f"post_process filed only support Step component, and it does not support"  + \
                        "[condition, loop_argument, cache_options], and cannot deps on any other component")
        
            if name in self._pipeline_dict["entry_points"]:
                raise PaddleFlowSDKException(PipelineDSLError, 
                    f"Step name[{name}] in post_process cannot be the same as the component names in entry_points")

    def _valiedate(self):
        """ validate
        """
        self._validate_post_process()
        self._validate_docker_env()

    def _validate_docker_env(self):
        """ validate docker env
        """
        if "docker_env" in self._pipeline_dict:
            return
        
        return self._validate_docker_env_by_dag(self._pipeline_dict["entry_points"])
    
    def _validate_docker_env_by_dag(self, dag_dict):
        """ validate docker env by dag
        """
        for key, cp_dict in dag_dict.items():
            if cp_dict["type"] == "dag":
                self._validate_docker_env_by_dag(cp_dict["entry_points"])
            else:
                if "docker_env" not in cp_dict:
                    raise PaddleFlowSDKException(PipelineDSLError,
                        f"all step should specify docker_env when pipeline does not specify")
