#!/usr/bin/env python3
""" pipeline demo
"""

from paddleflow.pipeline import Pipeline
from paddleflow.pipeline.dsl.steps import step
from paddleflow.pipeline.dsl.steps import Step
from paddleflow.pipeline.dsl.steps import ContainerStep
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import Parameter
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.utils.util import validate_string_by_regex
from paddleflow.pipeline.dsl.utils.consts import PIPELINE_NAME_REGEX
from paddleflow.pipeline.dsl.utils.consts import PipelineDSLError
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException


def hello(name, age):
    """ pipeline demo
    """
    intro = Step(name="introduce")
    hello = Step(name="hello", parameters={"name": name, "age": age})
    bye = Step(name="byebye", parameters={"name": hello.parameters["name"]})

    hello.after(intro)

def hello_with_io(name, age):
    """ full version of hello
    """
    intro = ContainerStep(name="introduce", command="hello, there is paddle flow pipeline", docker_env="python:3.7")
    hello = ContainerStep(name="hello", parameters={"name": name, "age": age}, outputs={"information": Artifact()},
            env={"num": "10"}, command="echo hello, {{name}}")
    bye = ContainerStep(name="byebye", parameters={"name": hello.parameters["name"]}, docker_env="python:3.8",
            inputs={"info": hello.outputs["information"]}, command="echo bye, {{name}}")

    hello.after(intro)

       
# the  pipleline dict for hellow_with_io
# attention: should change the value of name and age
hello_with_io_dict = {
                "name": "hello",
                "parallelism": 5,
                "docker_env": "python:3.9",
                "cache": {"enable": True, "fs_scope": "/", "max_expired_time": -1},
                "entry_points": {
                        "introduce": {
                            "command": "hello, there is paddle flow pipeline",
                            "docker_env": "python:3.7",
                            },
                        "hello": {
                            "command": "echo hello, {{name}}",
                            "env": {"num": "10"},
                            "deps": "introduce",
                            "parameters": {"name": "pipeline", "age": 17},
                            "artifacts": {"output": ["information"]},
                            },
                        "byebye": {
                            "command": "echo bye, {{name}}",
                            "deps": "hello",
                            "docker_env": "python:3.8",
                            "parameters": {"name": "{{hello.name}}"},
                            "artifacts": {"input": {"info": "{{hello.information}}"}}
                            }
                    }
                }


def ring():
    """ pipeline with ring
    """
    step1 = Step(name="step1")
    step2 = Step(name="step2")
    step3 = Step(name="step3")
    
    step1.after(step3)
    step2.after(step1)
    step3.after(step2)
