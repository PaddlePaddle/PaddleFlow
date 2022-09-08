本章节主要介绍在DSL中如何指定节点运行的队列信息， 关于在pipeline中配置节点的队列与套餐信息请参考[这里][queue_yaml]


# 1、pipeline 示例
下面的示例是在[base pipeline]的基础上给节点增加了队列和套餐等信息而得来。

> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/queue_example示例。
> 
> 示例链接：[queue_example]

> 注意：用户在使用该pipeline创建run前，请先创建对应的**队列和套餐**, 详情可以参考[命令行手册]或者[SDK手册]
```python3
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
        parameters={"data_path": f"./queue_example/data/{PF_RUN_ID}"},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        command="bash queue_example/shells/data.sh {{data_path}}"
    )
    return step
    
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash queue_example/shells/train.sh {{epoch}} {{train_data}} {{model_path}}",
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
        command="bash queue_example/shells/validate.sh {{model_path}}",
        parameters={"model_path": model_path}
    )    
    return step

@Pipeline(name="queue_example", docker_env="nginx:1.7.9", parallelism=1)
def queue_example(epoch=5):
    """ base pipeline
    """
    pre_step = preprocess()
    train_step = train(epoch, pre_step.parameters["data_path"])
    validate_step = validate(train_step.parameters["model_path"])
    

if __name__ == "__main__":
    ppl = queue_example()
    ppl.add_env({
        "PF_JOB_FLAVOUR": "flavour1",
        "PF_JOB_QUEUE_NAME": "ppl-queue",
        "PF_JOB_TYPE": "single"
    })
    
    print(ppl.run(fs_name="ppl"))
```

# 2、详解
与yaml规范中指定节点队列保持一致，在DSL中，也是通过环境变量来配置节点运行时的队列信息。

与yaml规范不一样的是，DSL中，Pipeline和Step均支持环境变量的配置。具体的配置方式请点击下方连接：
- [Pipeline级别环境变量设置]
- [Step级别环境变量设置]

[queue_yaml]: /docs/zh_cn/reference/pipeline/yaml_definition/4_queue.md
[queue_example]: /example/pipeline/queue_example
[命令行手册]: /docs/zh_cn/reference/client_command_reference.md
[SDK手册]: /docs/zh_cn/reference/sdk_reference/sdk_reference.md
[Pipeline级别环境变量设置]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#1pipeline
[Step级别环境变量设置]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#2containerstep