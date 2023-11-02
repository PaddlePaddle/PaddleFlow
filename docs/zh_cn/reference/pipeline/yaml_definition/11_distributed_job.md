在深度学习场景下，分布式训练任务的应用较为广泛。本文介绍如何在PaddleFlow Pipeline中配置分布式任务。
# 1 pipeline定义
下面是基于 [1_pipeline_basic.md] 示例，增加了distributed_job的yaml格式pipeline定义。
> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/distributed_job_example示例。


```yaml
name: distributed_pipeline

entry_points:
  preprocess:
    command: bash distributed_job_example/shells/data.sh {{data_path}}
    docker_env: centos:centos7
    env:
      USER_ABC: 123_{{PF_USER_NAME}}
    parameters:
      data_path: ./distributed_job_example/data/{{PF_RUN_ID}}

  train:
    deps: preprocess
    env:
      PS_NUM: "2"
      WORKER_NUM: "2"
    parameters:
      epoch: 15
      model_path: ./output/{{PF_RUN_ID}}
      train_data: '{{preprocess.data_path}}'
    distributed_job:
      framework: "paddle"
      members:
        - {"role": "pserver", "image": "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7", "command": "sleep 30; echo ps {{epoch}} {{train_data}} {{model_path}}", "replicas": 2, "flavour": { "name": "flavour1" } }
        - {"role": "pworker", "image": "paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7", "command": "sleep 30; echo worker {{epoch}} {{train_data}} {{model_path}}", "replicas": 2, "flavour": { "name": "flavour1" } }

parallelism: 1

fs_options:
  main_fs: {name: "ppl"}
```

# 2 分布式任务详解
在Paddleflow pipeline中，配置分布式任务的方式非常简单，只需在节点中添加distributed_job，并配置框架(framework)和成员(members)信息即可。
下面基于上述pipeline定义，介绍每个字段的作用。

## 2.1 framework
分布式任务的框架名称

## 2.2 members
分布式任务的成员定义，Members中支持定义的字段如下：

### 2.2.1 role
- 当前member在分布式任务中的角色，例如pserver、pworker等。
- members针对角色进行分组定义，一个role对应members列表中的一个list。

### 2.2.2 replicas
replicas字段需指定当前member的副本数，int类型。

### 2.2.3 image 
member镜像名称，string类型。
如果未定义，取该Step的docker_env中定义的镜像。

### 2.2.4 port 
member的端口号，int类型

### 2.2.5 flavour
member使用的flavour定义，例如： { "name": "flavour1", "cpu": "1", "mem": "1G", "scalar_resources": {"nvidia.com/gpu": "1"}}

### 2.2.6 command
member运行的command，string类型。
- 如果Member中定义了command字段，Member使用自己的command。
- 如果Member中没有定义command字段，Member使用所在Step的command。

### 2.2.7 annotations
member自定义的annotations，map类型。

### 2.2.8 labels
member自定义的labels，map类型。

### 2.2.9 env
member运行的自定义环境变量，map类型。
- 如果Member中没有定义env字段，Member的env将包含所在Step的env。
- 如果Member中定义了环境变量，所在Step的env中不包含Member中定义的env名称，member的env将会在此基础上追加所在Step的env。
- 如果Member中定义了环境变量，且所在Step的env中包含Member中定义的env名称，则Member实际使用的该环境变量值为Member指定的env环境变量值，即Member指定的ENV环境变量优先级高于其所在Step的env环境变量。

### 2.2.10 fs
member运行的自定义fs，map类型。
- 如果Member中定义了fs，Member使用自己的fs
- 如果Member中没有定义fs，Member使用所在Step的fs

### 2.2.11 extra_fs
member运行的自定义extra_fs，列表类型。
- 如果Member中定义了extra_fs，Member使用自己的extra_fs
- 如果Member中没有定义extra_fs，Member使用所在Step的extra_fs

# 3 Pipeline运行过程
当Paddleflow开始调度执行分布式任务的节点时，会检查distributed_job字段是否有值，如果有值则会开始执行如下流程：

1. 解析Members中的配置字段，包括role、replicas、image、port、flavour、command等。

2. 替换Member的command字段中的模板。 如果是artifact模板，则会使用artifact的文件目录来替换相应的模板。

3. 根据Framework和各Member的配置信息创建分布式PaddleFlow Job。

4. 运行多个分布式Pod。