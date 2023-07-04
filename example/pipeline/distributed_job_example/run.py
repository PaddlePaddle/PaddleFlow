"""
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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
from paddleflow.pipeline import PF_RUN_ID
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions
from paddleflow.job import Member
from paddleflow.pipeline import DistributedJob


def preprocess():
    """ data preprocess step
    """
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./distributed_job_example/data/{PF_RUN_ID}"},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        command="bash distributed_job_example/shells/data.sh {{data_path}}"
    )
    return step


def train(epoch, train_data):
    """ distributed job
    """
    dist_jobs = DistributedJob(
        framework="paddle",
        members=[Member(role="pworker", replicas=2, image="paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
                        command="sleep 30; echo worker {{epoch}} {{train_data}} {{model_path}}"),
                 Member(role="pserver", replicas=2, image="paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
                        command="sleep 30; echo ps {{epoch}} {{train_data}} {{model_path}}")]
    )

    """ train step
    """
    step = ContainerStep(
        name="train",
        parameters={
            "epoch": epoch,
            "model_path": f"./output/{PF_RUN_ID}",
            "train_data": train_data
        },
        env={"PS_NUM": "2", "WORKER_NUM": "2"},
        command="",
        distributed_job=dist_jobs,
    )
    return step


@Pipeline(name="distributed_pipeline", docker_env="nginx:1.7.9", parallelism=1)
def distributed_pipeline(epoch=15):
    """ pipeline example for distributed job
    """
    preprocess_step = preprocess()
    train_step = train(epoch, preprocess_step.parameters["data_path"])


if __name__ == "__main__":
    ppl = distributed_pipeline()

    main_fs = MainFS(name="ppl")
    ppl.fs_options = FSOptions(main_fs)
    print(ppl.run())
