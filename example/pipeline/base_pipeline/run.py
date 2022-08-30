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
from paddleflow.pipeline import PF_RUN_ID


def preprocess():
    """ data preprocess step
    """
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./base_pipeline/data/{PF_RUN_ID}"},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        command="bash base_pipeline/shells/data.sh {{data_path}}"
    )
    return step
    
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash base_pipeline/shells/train.sh {{epoch}} {{train_data}} {{model_path}}",
        parameters={
            "epoch": epoch,
            "model_path": f"./output/{PF_RUN_ID}",
            "train_data": train_data
        }
    )
    return step
    
def validate(model_path):
    """ validate step
    """ 
    step = ContainerStep(
        name="validate",
        command="bash base_pipeline/shells/validate.sh {{model_path}}",
        parameters={"model_path": model_path}
    )    
    return step

@Pipeline(name="base_pipeline", docker_env="nginx:1.7.9", parallelism=1)
def base_pipeline(epoch=5):
    """ base pipeline
    """
    pre_step = preprocess()
    train_step = train(epoch, pre_step.parameters["data_path"])
    validate_step = validate(train_step.parameters["model_path"])
    

if __name__ == "__main__":
    ppl = base_pipeline()
    print(ppl.run(fs_name="ppl"))
