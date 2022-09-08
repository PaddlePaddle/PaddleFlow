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
        sub_path="dag_example/" + sub_path,
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


def split(nums, threshold):
    """ split data
    """
    step = ContainerStep(
        name="split",
        command="cd /split && python3 split.py {{threshold}}",
        outputs={
            "negetive": Artifact(),
            "positive": Artifact(),
        },
        inputs={
            "nums": nums,
        },
        parameters={
            "threshold": threshold,
        }
    )
    
    extra_fs = generate_extra_fs("split")
    step.extra_fs = [extra_fs]
    return step
    
def process_data(polarity, data):
    """ process data
    """
    step = ContainerStep(
        name=f"process-{polarity}",
        command=f"cd /process_{polarity} && python3 process_{polarity}.py",
        inputs={
            polarity: data
        },
        outputs={"result": Artifact()},
    )
    extra_fs = generate_extra_fs(f"process_{polarity}")
    step.extra_fs=[extra_fs]
    return step
    
def collector(negetive, positive):
    """ collector
    """
    step = ContainerStep(
        name="collector",
        command="cd /collector && python3 collect.py",
        inputs={
            "negetive": negetive,
            "positive": positive,
        },
        outputs={
            "collection": Artifact()
        },
    )
    
    extra_fs = generate_extra_fs(f"collector")
    step.extra_fs=[extra_fs]
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


@Pipeline(name="dag_example", docker_env="python:3.7", parallelism=2)
def dag_example(lower=-10, upper=10, num=10):
    """ pipeline example for artifact
    """
    randint_step = randint(lower, upper, num)
    
    with DAG(name="process"):
        split_step = split(randint_step.outputs["random_num"], 0)
        process_negetive = process_data("negetive", split_step.outputs["negetive"])
        process_positive = process_data("positive", split_step.outputs["positive"])
        collector_step = collector(process_negetive.outputs["result"], process_positive.outputs["result"])
    
    sum(collector_step.outputs["collection"])
    
    
if __name__ == "__main__":
    ppl = dag_example()
    main_fs = MainFS(name="ppl")
    ppl.fs_options = FSOptions(main_fs)
    
    print(ppl.run())