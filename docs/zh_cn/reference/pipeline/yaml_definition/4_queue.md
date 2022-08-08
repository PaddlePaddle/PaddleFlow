到目前为止，所有的Pipeline Run 都是运行在默认的队列和默认套餐下。

而在实际使用中，不同的节点运行所需的资源（CPU/GPU/MEM）等是不一样的，也即不同的节点可能需要运行在不同的队列或者套餐下。

本文便讲解如何在pipeline中配置节点的队列与套餐信息


# 1 pipeline定义

下面的示例是在[base pipeline]的基础上给节点增加了队列和套餐等信息而得来。

> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/queue_example示例。
> 
> 示例链接：[queue_example]

> 注意：用户在使用该pipeline创建run前，请先创建对应的**队列和套餐**, 详情可以参考[命令行手册]或者[SDK手册]

```yaml
name: queue_example

entry_points:
  preprocess:
    command: bash queue_example/shells/data.sh {{data_path}}
    docker_env: centos:centos7 
    env:
      PF_JOB_FLAVOUR: flavour1
      PF_JOB_QUEUE_NAME: ppl-queue
      PF_JOB_TYPE: single
      USER_ABC: 123_{{PF_USER_NAME}}
    parameters:
      data_path: ./queue_example/data/{{PF_RUN_ID}}

  train:
    command: bash queue_example/shells/train.sh {{epoch}} {{train_data}} {{model_path}}
    deps: preprocess
    env:
      PF_JOB_FLAVOUR: flavour1
      PF_JOB_QUEUE_NAME: ppl-queue
      PF_JOB_TYPE: single
    parameters:
      epoch: 5
      model_path: ./output/{{PF_RUN_ID}}
      train_data: '{{preprocess.data_path}}'

  validate:
    command: bash queue_example/shells/validate.sh {{model_path}}
    deps: train
    env:
      PF_JOB_FLAVOUR: flavour1
      PF_JOB_QUEUE_NAME: ppl-queue
      PF_JOB_TYPE: single
    parameters:
      model_path: '{{train.model_path}}'

docker_env: nginx:1.7.9

parallelism: 1
```

# 2 详解
在Paddleflow pipeline中，指定节点运行时的队列和套餐的方式非常简单，只需在节点中添加相应的环境变量即可。

相关的变量说明如下:
|变量名|含义|
|:---:|:---:|
|PF_JOB_FLAVOUR|节点运行时使用的套餐|
|PF_JOB_QUEUE_NAME|节点运行时使用的队列|
|PF_JOB_TYPE|节点的任务类型，当前只支持 `single` 类型 |


[queue_example]: /example/pipeline/queue_example
[base pipeline]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md
[命令行手册]: /docs/zh_cn/reference/client_command_reference.md
[SDK手册]: /docs/zh_cn/reference/sdk_reference/sdk_reference.md