# DSL 使用说明

# 1、pipeline 示例
开发者除了可以通过编写 [yaml][pipeline yaml] 来定义 pipeline 外，PaddleFlow Pipeline 也提供 Python DSL 供开发者们通过写 Python 代码的方式来完成 pipeline 的定义。下面是一个最基础的使用 Python DSL 编排的 pipeline。

> 该示例中pipeline定义，以及示例相关运行脚本，来自pddleflow项目下example/pipeline/base_pipeline示例。
>
> 示例链接：[base_pipeline][base_pipeline]

```python3
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
        docker_env="registry.baidubce.com/pipeline/kfp_mysql:1.7.0",
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

@Pipeline(name="base_pipeline", docker_env="registry.baidubce.com/pipeline/nginx:1.7.9", parallelism=1)
def base_pipeline(data_path, epoch, model_path):
    preprocess_step = preprocess(data_path)

    train_step = train(epoch, model_path, preprocess_step.parameters["data_path"])
    train_step.after(preprocess_step)

    validate_step = validate(train_step.parameters["model_path"])


if __name__ == "__main__":
    ppl = base_pipeline(data_path=f"./base_pipeline/data/{PF_RUN_ID}", epoch=5, model_path=f"./output/{PF_RUN_ID}")
    result = ppl.run(fsname="your_fs_name")
    print(result)
```
到此时，我们介绍了通过DSL定义pipeline 的的基本形式，那么接下来，将详细的讲解通过DSL定义Pipeline的各个步骤。

> 在阅读本文档前， 请确认已经安装了 PaddleFlow SDK，并完成了相关配置。详情请点击[这里][sdk 安装与配置]

## 2、导入DSL相关模块
与编写任何Python脚本一样，我们首先要导入将会使用到的模块、类、或者函数等。 Python DSL 提供的模块、类、函数等都可以通过 paddleflow.pipeline 模块完成导入，如上面的[示例](#1、pipeline 示例)所示：
```python3
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import PF_RUN_ID
from paddleflow.pipeline import PF_USER_NAME
```

## 3、定义Step
在PaddleFlow Pipeline 中，Step是运行Pipeline时最基本的调度单位，每一个Step都会执行一个指定的任务。在定义Pipeline之前，首先需要完成 Step 的定义，在DSL中，我们实例化 ContainerStep 即可完成Step的定义。如上面

[pipeline yaml]: /docs/zh_cn/reference/pipeline/yaml_definition
[base_pipeline]: /example/pipeline/base_pipeline
[dsl 接口文档]: TODO
[sdk 安装与配置]: TODO
