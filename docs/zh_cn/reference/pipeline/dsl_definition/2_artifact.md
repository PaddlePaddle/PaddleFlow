# 使用 Artifact
在[DSL使用基础中][DSL使用基础]，我们介绍了DSL的基础使用。但是，在某些情况下，Step所执行的任务会生成一些数据，我们需要将这些数据进行科学的管理，方便后期使用、追踪、复现等操作。因此我们引入了Artifact的概念，关于Artifact的详细解释，请点击[这里][Artifact-ref]。本文主要讲解如何在使用DSL定义pipeline时使用Artifact特性，不在对其定义进行赘述。

# 1、Pipeline示例
下面是基于[DSL使用基础中][DSL使用基础]的示例增加了Artifact特性制作而成:
>该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/artifact_pipeline示例
>
>示例链接: [artifact_pipeline][artifact_pipeline]

```python3
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

```

# 2、定义输出Artifact
在调用ContainerStep的实例化函数时，通过给其outputs参数进行赋值即可给ContainerStep定义输出Artifact。outputs的参数值需要是一个Dict，其key将会作为输出Artifact的名字，而其Value则必须是Artifact()。

如在上面的示例中的，通过定如下的代码，给train_step定义了一个名为"train_model"的输出artifact

```python3
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
```

# 3、定义输入Artifact
在调用ContainerStep的实例化函数时，通过给其inputs参数进行赋值即可给ContainerStep定义输入Artifact。inputs的参数值需要是一个Dict，其key将会作为输入Artifact的名字，而其Value则必须是其余节点的输出Artifact的引用。

如在上面的示例中的，通过如下的代码便给train_step定义了一个名为"train_data"的输入artifact，其值为preprocess_step输出artifact["train_data"]的引用。

>在运行train_step时，会将其输入artifact["train_data"]替换为preprocess_step输出artifact["train_data"]的存储路径。关于Artifact替换的更多信息请参考[这里][Artifact-ref]

```python3
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

train_step = train(epoch, preprocess_step.outputs["train_data"])
```


>如果StepA使用了StepB的输出artifact作为自身的输入artifact，则会隐含StepA在流程上依赖于StepB

# 4、更多信息
[在DSL中使用Cache][DSL-Cache]
    
[在DSL中使用PostProcess和FailureOpitons][DSL-PostProcess-And-FailureOpitons]

[DSL接口文档][DSL接口文档]



[DSL使用基础]: /docs/zh_cn/reference/pipeline/dsl_definition/1_pipeline_basic.md

[artifact_pipeline]: /example/pipeline/artifact_example

[Artifact-ref]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#2-artifact详解

[DSL接口文档]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md

[DSL-PostProcess-And-FailureOpitons]: /docs/zh_cn/reference/pipeline/dsl_definition/4_failure_options_and_post_process.md

[DSL-Cache]: /docs/zh_cn/reference/pipeline/dsl_definition/3_cache.md
