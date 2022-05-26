from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import PF_RUN_ID
from paddleflow.pipeline import PF_USER_NAME

def preprocess(data_path):
    return ContainerStep(
        name="preprocess",
        parameters={"data_path": data_path},
        command="bash base_pipeline/shells/data.sh {{data_path}}",
        docker_env="centos:centos7",
        env={
            "USER_ABC": f"123_{PF_USER_NAME}",
            "PF_JOB_TYPE": "vcjob",
            "PF_JOB_QUEUE_NAME": "ppl-queue",
            "PF_JOB_MODE": "Pod",
            "PF_JOB_FLAVOUR": "flavour1",
        },
    )

def train(epoch, mode_path, train_data):
    return ContainerStep(
        name="train",
        parameters={
            "epoch": epoch,
            "model_path": mode_path,
            "train_data": train_data,
        },
        command="bash base_pipeline/shells/train.sh {{epoch}} {{train_data}} {{model_path}}",
        env={
            "PF_JOB_TYPE": "vcjob",
            "PF_JOB_QUEUE_NAME": "ppl-queue",
            "PF_JOB_MODE": "Pod",
            "PF_JOB_FLAVOUR": "flavour1",
        },
    )

def validate(model_path):
    return ContainerStep(
        name="validate",
        parameters={
            "model_path": model_path,
        },
        command="bash base_pipeline/shells/validate.sh {{model_path}}", 
        env={
            "PF_JOB_TYPE": "vcjob",
            "PF_JOB_QUEUE_NAME": "ppl-queue",
            "PF_JOB_MODE": "Pod",
            "PF_JOB_FLAVOUR": "flavour1",
        },
    )

@Pipeline(name="base_pipeline", docker_env="nginx:1.7.9", parallelism=1)
def base_pipeline(data_path, epoch, model_path):
    preprocess_step = preprocess(data_path)

    train_step = train(epoch, model_path, preprocess_step.parameters["data_path"])
    train_step.after(preprocess_step)

    validate_step = validate(train_step.parameters["model_path"])


if __name__ == "__main__":
    ppl = base_pipeline(data_path=f"./base_pipeline/data/{PF_RUN_ID}", epoch=5, model_path=f"./output/{PF_RUN_ID}")
    result = ppl.run(fsname="your_fs_name")
    print(result)
