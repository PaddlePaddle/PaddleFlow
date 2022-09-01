在某些情况下，Step所执行的任务会生成一些数据，我们需要将这些数据进行科学的管理，方便后期使用、追踪、复现等操作。因此我们引入了Artifact的概念，关于Artifact的详细解释，请点击[这里][Artifact-ref]。本文主要讲解如何在使用DSL定义pipeline时使用Artifact特性，不在对其定义进行赘述。

# 1、pipeline示例
下面是基于[DSL使用基础中][DSL使用基础]的示例增加了Artifact特性制作而成:
>该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/artifact_pipeline示例
>
>示例链接: [artifact_pipeline][artifact_pipeline]

```python3
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import PF_RUN_ID
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions

def preprocess():
    """ data preprocess step
    """
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./artifact_example/data/"},
        outputs={"train_data": Artifact(), "validate_data": Artifact()},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        command="bash -x artifact_example/shells/data_artifact.sh {{data_path}} {{train_data}} {{validate_data}}",
    )
    return step
    
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash artifact_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}",
        inputs={"train_data": train_data},
        outputs={"train_model": Artifact()},
        parameters={"epoch": epoch}
    )
    return step
    
def validate(model, data):
    """ validate step
    """
    step = ContainerStep(
        name="validate",
        command="bash artifact_example/shells/validate.sh {{model}}",
        inputs={"data": data, "model": model}
    )    
    return step

@Pipeline(name="artifact_example", docker_env="nginx:1.7.9", parallelism=1)
def artifact_example(epoch=15):
    """ pipeline example for artifact
    """
    preprocess_step = preprocess()
    train_step = train(epoch, preprocess_step.outputs["train_data"])
    validate_step = validate(train_step.outputs["train_model"], preprocess_step.outputs["validate_data"])
    

if __name__ == "__main__":
    ppl = artifact_example()
    
    main_fs = MainFS(name="ppl")
    ppl.fs_options = FSOptions(main_fs)
    print(ppl.run())
```

# 2、定义输出Artifact
在调用ContainerStep的实例化函数时，通过给其outputs参数进行赋值即可给ContainerStep定义输出Artifact。

outputs的参数值需要是一个Dict，其key将会作为输出Artifact的名字，而其Value则必须是`Artifact()`。

如在上面的示例中的，通过定如下的代码，给train_step定义了一个名为"train_model"的输出artifact

```python3
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash artifact_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}",
        inputs={"train_data": train_data},
        outputs={"train_model": Artifact()},
        parameters={"epoch": epoch}
    )
    return step
```

# 3、定义输入Artifact
在调用ContainerStep的实例化函数时，通过给其inputs参数进行赋值即可给ContainerStep定义输入Artifact。

inputs的参数值需要是一个Dict，其key将会作为输入Artifact的名字，而其Value则必须是其余节点的输出Artifact的引用。

如在上面的示例中的，通过如下的代码便给train_step定义了一个名为"train_data"的输入artifact，其值为preprocess_step输出artifact["train_data"]的引用。

>在运行train_step时，会将其输入artifact["train_data"]替换为preprocess_step输出artifact["train_data"]的存储路径。关于Artifact替换的更多信息请参考[这里][Artifact-ref]

```python3
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash artifact_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}",
        inputs={"train_data": train_data},
        outputs={"train_model": Artifact()},
        parameters={"epoch": epoch}
    )
    return step

train_step = train(epoch, preprocess_step.outputs["train_data"])
```


>如果StepA使用了StepB的输出artifact作为自身的输入artifact，则会隐含StepA在流程上依赖于StepB


[DSL使用基础]: /docs/zh_cn/reference/pipeline/dsl_definition/1_pipeline_basic.md

[artifact_pipeline]: /example/pipeline/artifact_example

[Artifact-ref]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#2-artifact详解

[DSL接口文档]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md

[DSL-PostProcess-And-FailureOpitons]: /docs/zh_cn/reference/pipeline/dsl_definition/4_failure_options_and_post_process.md

[DSL-Cache]: /docs/zh_cn/reference/pipeline/dsl_definition/3_cache.md
