#!/usr/bin/env python3
""" pipeline demo
"""

from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import DAG
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
    intro = ContainerStep(name="introduce")
    hello = ContainerStep(name="hello", parameters={"name": name, "age": age})
    bye = ContainerStep(name="byebye", parameters={"name": hello.parameters["name"]})

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



def step1(num):
    return ContainerStep(
            name="step1",
            command="echo {{num}} >> result",
            parameters={"num": num},
            outputs={"result": Artifact()},
            )


def show(name, num, num2, art):
    return ContainerStep(
            name=name,
            command="echo {{num}} && echo {{num2}} >>result  && cat {{data}}",
            parameters={"num": num, "num2": num2},
            inputs={"data": art},
            outputs={"result": Artifact()},
            env={"a": "2", "b": "1"}
            )


def dag_example(num=0, num2=1):
    """ dag_example
    """
    st1 = step1(num)

    with DAG(name="dag2") as dag2:
        st2 = show("dag-show", st1.parameters["num"], num2, st1.outputs["result"])
        st3 = show("show", num, num2, st2.outputs["result"])

        with DAG(name="dag3") as dag3:
            st4 = show("show", st2.parameters["num"], num2, st2.outputs["result"])

    st5 = show("show", num, num2, st4.outputs["result"])
    st6 = ContainerStep(name="step6", command=f"cat {st5.parameters['num']}")