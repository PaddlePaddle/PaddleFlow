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
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import PF_RUN_ID
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions
from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import FSScope

def preprocess():
    """ data preprocess step
    """
    fs_scope=FSScope(name="ppl", path="cache_example/run.yaml")
    cache = CacheOptions(enable=True, max_expired_time=300, fs_scope=[fs_scope])
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./cache_example/data/"},
        outputs={"train_data": Artifact(), "validate_data": Artifact()},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        cache_options=cache,
        command="bash -x cache_example/shells/data_artifact.sh {{data_path}} {{train_data}} {{validate_data}}",
    )
    return step
    
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash -x cache_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}",
        inputs={"train_data": train_data},
        outputs={"train_model": Artifact()},
        parameters={"epoch": epoch}
    )
    return step
    
def validate(model, data):
    """ validate step
    """
    cache = CacheOptions(enable=False, max_expired_time=-1)
    step = ContainerStep(
        name="validate",
        command="bash cache_example/shells/validate.sh {{model}}",
        inputs={"model": model, "data": data},
        cache_options=cache,
    )    
    return step


fs_scope=FSScope(name="ppl", path="cache_example/shells")
cache = CacheOptions(enable=True, max_expired_time=600, fs_scope=[fs_scope])

@Pipeline(name="cache_example", docker_env="nginx:1.7.9", parallelism=1, cache_options=cache)
def cache_example(epoch=15):
    """ pipeline example for artifact
    """
    preprocess_step = preprocess()
    train_step = train(epoch, preprocess_step.outputs["train_data"])
    validate_step = validate(train_step.outputs["train_model"], preprocess_step.outputs["validate_data"])
    

if __name__ == "__main__":
    ppl = cache_example()
    
    print(ppl.run(fs_name="ppl"))