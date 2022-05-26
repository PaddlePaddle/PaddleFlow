from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import PF_USER_NAME

def job_info():
    return {
        "PF_JOB_TYPE": "vcjob",
        "PF_JOB_MODE": "Pod",
        "PF_JOB_QUEUE_NAME": "ppl-queue",
        "PF_JOB_FLAVOUR": "flavour1",
    }

def preprocess(data_path):
    return ContainerStep(
        name="preprocess",
        parameters={"data_path": data_path},
        outputs={"train_data": Artifact(), "validate_data": Artifact()},
        docker_env="centos:centos7",
        command="bash -x artifact_example/shells/data_artifact.sh {{data_path}} {{train_data}} {{validate_data}}",
        env={"USER_ABC": f"123_{PF_USER_NAME}"}
    )

def train(epoch, train_data):
    return ContainerStep(
        name="train",
        parameters={
            "epoch": epoch,
        },
        inputs={"train_data": train_data},
        outputs={"train_model": Artifact()},
        command="bash artifact_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}",
    )

def validate(data, model):
    return ContainerStep(
        name="validate",
        inputs={"data":data, "model": model},
        command="bash artifact_example/shells/validate.sh {{model}}", 
    )

@Pipeline(name="artifact_example", docker_env="nginx:1.7.9", env=job_info(), parallelism=1)
def artifact_example(data_path, epoch):
    preprocess_step = preprocess(data_path)

    train_step = train(epoch, preprocess_step.outputs["train_data"])

    validate_step = validate(preprocess_step.outputs["validate_data"], train_step.outputs["train_model"])


if __name__ == "__main__":
    ppl = artifact_example(data_path="./artifact_example/data/", epoch=15)
    result = ppl.run(fsname="your_fs_name")
    print(result)

