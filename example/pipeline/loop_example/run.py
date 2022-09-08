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
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import DAG
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions

def generate_extra_fs(sub_path):
    """ generate ExtraFS
    """
    return ExtraFS(
        name="ppl",
        sub_path="loop_example/" + sub_path,
        mount_path="/" + sub_path,
        read_only=True
    )

def randint(lower, upper, num):
    """ randint step
    """
    extra_fs = generate_extra_fs("randint")
    
    step = ContainerStep(
        name="randint",
        parameters={
            "lower": lower,
            "upper": upper,
            "num": num
        },
        outputs={"random_num": Artifact()},
        extra_fs=[extra_fs],
        command="cd /randint && python3 randint.py {{num}} {{lower}} {{upper}}"
    )
    
    return step

def process(nums):
    step = ContainerStep(
        name="process",
        loop_argument=nums,
        outputs={"result": Artifact()},
    )
    step.command = f"cd /process && python3 process.py {step.loop_argument.item}"
    
    extra_fs = generate_extra_fs("process")
    step.extra_fs = [extra_fs]
    return step
    
    
def sum(nums):
    """ sum
    """
    step = ContainerStep(
        name="sum",
        command="cd /sum && python3 sum.py",
        inputs={
            "nums": nums,
        },
        outputs={
            "result": Artifact()
        },
    )
    
    extra_fs = generate_extra_fs(f"sum")
    step.extra_fs=[extra_fs]
    return step

@Pipeline(name="loop_example", docker_env="python:3.7", parallelism=2)
def loop_example(lower=-10, upper=10, num=10):
    """ pipeline example for artifact
    """
    randint_step = randint(lower, upper, num)
    process_step = process(randint_step.outputs["random_num"])
    sum(process_step.outputs["result"])
    
    
if __name__ == "__main__":
    ppl = loop_example()
    main_fs = MainFS(name="ppl")
    ppl.fs_options = FSOptions(main_fs)
    
    ppl.compile("loop.yaml")
    print(ppl.run())