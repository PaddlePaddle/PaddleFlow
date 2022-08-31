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
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions

def main_fs():
    return ContainerStep(
        name="main-fs",
        outputs={"main": Artifact()},
        command="cd shells/main_fs && bash run.sh",
        docker_env="centos:centos7",
    )
    
def global_extra_fs():
    return ContainerStep(
        name="global-extra-fs",
        outputs={"global_extra": Artifact()},
        command="cd /home/global && bash run.sh",
        docker_env="centos:centos7"
    )

def step_extra_fs():
    return ContainerStep(
        name="extra-fs",
        outputs={"extra": Artifact()},
        command="cd /home/extra && bash run.sh",
        docker_env="centos:centos7",
        extra_fs=[ExtraFS(name="ppl", sub_path="multiple_fs/shells/extra_fs", mount_path= "/home/extra")],
    )
    

@Pipeline(name="multiple_fs", docker_env="nginx:1.7.9", parallelism=2)
def multiple_fs():
    main_fs()
    global_extra_fs()
    step_extra_fs()

if __name__ == "__main__":
    main = MainFS(
        name="ppl",
        sub_path="multiple_fs",
        mount_path="/home/main"
        )
    extra = ExtraFS(
        name="ppl",
        sub_path="multiple_fs/shells/global_extra_fs",
        mount_path="/home/global"
    )
    ppl = multiple_fs()
    ppl.fs_options = FSOptions(main, [extra])
    print(ppl.run())