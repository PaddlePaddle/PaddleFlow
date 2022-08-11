上一章节，介绍了最基本的pipeline定义，下面引入新的节点参数类型：artifact。

# 1 pipeline定义

下面是基于 [1_pipeline_basic.md] 示例，增加了artifact变量后的yaml格式pipeline定义。

> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/artifact_example示例。
> 
> 示例链接：[artifact_example]


```yaml
name: artifact_example

entry_points:
  preprocess:
    artifacts:
      output:
      - train_data
      - validate_data
    command: bash -x artifact_example/shells/data_artifact.sh {{data_path}} {{train_data}}
      {{validate_data}}
    docker_env: centos:centos7
    env:
      USER_ABC: 123_{{PF_USER_NAME}}
    parameters:
      data_path: ./artifact_example/data/

  train:
    artifacts:
      input:
        train_data: '{{preprocess.train_data}}'
      output:
      - train_model
    command: bash artifact_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}
    deps: preprocess
    parameters:
      epoch: 15

  validate:
    artifacts:
      input:
        data: '{{preprocess.validate_data}}'
        model: '{{train.train_model}}'
    command: bash artifact_example/shells/validate.sh {{model}}
    deps: preprocess,train

docker_env: nginx:1.7.9

parallelism: 1

fs_options:
  main_fs: {name: "ppl"}
```

> 关于fs_options的详细介绍，请点击[3_multiple_fs.md]

# 2 artifact详解

上述pipeline定义中，基于基础字段上，新增加了artifact字段，下面进行详细介绍

## 2.1 什么是artifact

artifact主要用于定义节点运行的输入输出资源（文件/目录）。

### 2.1.1 artifact vs parameter
Paddleflow Pipeline定义中，存在parameter，artifact两种参数，其差异如下：

- parameter是节点运行的参数变量。它的取值，在pipeline run运行前，是可以确定的。
  - 用户可以在pipeline定义中指定，或者在发起pipeline run时通过parameters参数指定。

- artifact是节点运行的输入输出资源（文件/目录）。它的取值，在pipeline run运行前是未知的。
  - artifact的路径，不能由用户在pipeline定义中指定；只能在每个节点运行前，由平台自动生成。
  - 平台生成的路径格式，可以参考[3.1 artifact存储机制]

> 如 [2.4 artifact 使用方式] 所示，用户可以通过变量模板，或者在运行时通过环境变量获取artifact的路径。

### 2.1.2 artifact的使用场景

正如 [2.1.1 artifact vs parameter] 介绍，artifact主要用于定义输入输出资源。

在下面两种场景下，可以优先考虑使用artifact：

1. 需要定义输入输出资源，并且希望资源路径由平台进行生成和管理。

2. 如果使用cache机制，并且希望节点的输出路径，不影响cache命中。

> cache机制详解，以及artifact路径对cache命中机制的影响，可以参考[5_cache.md]

## 2.2 artifact 定义方式

artifact可用于定义节点运行的输入输出路径（可以是文件&目录）。

artifact包括input artifact，output artifact两种类型：

### 2.2.1 output artifact

定义节点的输出路径。与input artifact不同，output artifact只能定义为数组形式。

- 用户无需指定output artifact路径。每次节点运行前，会由Paddleflow自动为每个output artifact生成对应路径。
    - 生成的路径格式，可参考 [3.1 artifact存储机制]

> 如 [1 pipeline定义] 所示，preprocess节点以数组形式，定义了train_data，和validate_data两个output artifact。

### 2.2.2 input artifact

定义节点的输入路径，只能引用上游节点的output artifact

- 没有上游的节点，不能定义输入artifact（因为无法引用上游节点的输出artifact）。

> 如 [1 pipeline定义] 所示，train节点定义了input artifact[train_data]，并且通过{{ preprocess.train_data }}形式引用 preprocess节点的output artifact[train_data]

## 2.3 artifact 定义约束

1. 引入artifact变量后，同一个节点内，parameter，input artifact，output artfiact变量名字不能相同。
   
- 例如：parameter参数和input artifact中，不能同时定义名字为data的变量
- parameter，input artifact，output artfiact变量名字大小写不敏感，`abc`,`Abc`,`ABC`将会被视为同一个名字

2. 只有在配置了[main_fs]时才能定义artifact
- 节点A的输出artifact可能会被其下游节点B使用，但是不同节点的运行环境是隔离的
- Paddleflow通过把artifact存储在共享存储上来实现artifact在节点间的传递

### 2.4 artifact 使用方式

与parameters类似，定义artifact参数后，有下面两种使用方式：

1. 可以在当前节点的command中通过模板形式被引用

    > 如[1 pipeline定义] 所示， preprocess 节点 command 参数使用 {{train_data}} {{validate_data}} 模板，引用这两个output artifact变量

2. 也可以在节点运行时，通过环境变量使用
- input artifact: 环境变量名为${{PF_INPUT_ARTIFACT_ARTIFACTNAME}}

   > 如[1 pipeline定义] 所示，train 节点运行时，train.sh内可以通过 ${{PF_INPUT_ARTIFACT_TRAIN_DATA}} 获取输入artifact[train_dat]的路径

- output artifact: 环境变量名为${{PF_OUTPUT_ARTIFACT_ARTIFACTNAME}}

  > 如上述例子所示，preprocess 节点运行时，data_artifact.sh内可以通过 ${{PF_OUTPUT_ARTIFACT_VALIDATE_DATA}} 获取输出artifact[validate_data]的路径


# 3 pipeline运行流程

## 3.1 artifact存储机制

如上文所示，input artifact直接引用上游节点的输出artifact路径，所以我们只需要关注每个节点的输出artifact存储机制即可。

目前，Paddleflow Pipeline在每个节点运行前，为该节点每个output artifact拼凑对应路径。

> aritfact路径生成规则如下：
>${{MAIN_FS_SUB_PATH}}/.pipeline/{{PF_RUN_ID}}/{{PPL_NAME}}/{{RUNTIME_NAME}}-{{MD5_FULLNAME}}/{{OUTPUT_ARTIFACT_NAME}}
>
>各变量解释如下：
>|变量名 | 含义 | 备注 |
>|:---:|:---:|:---:|
>| MAIN_FS_SUB_PATH | [main_fs]中指定的sub_path的值 |  |
>| PF_RUN_ID | Pipeline Run 的唯一标识符 | | 
>| PPL_NAME | Pipeline 的名字 | | 
>| RUNTIME_NAME | 由节点名和表征当前为该节点第几次运行的序号组成，序号从0开始计数，节点名与序号通过'-'连接 | 对于设置了[loop_argument]字段的节点，可能会运行多次| 
>| MD5_FULLNAME | 节点FULLNAME的md5值 | 节点的FULLNAME是由其所有[祖先节点]与当前节点的{{RUNTIME_NAME}}组合而成，以'.'连接，最古老的祖先位于最前面 |

得到output artifact路径后，Paddleflow会自动创建该路径的父目录。

- 即：自动创建${{MAIN_FS_SUB_PATH}}/.pipeline/{{PF_RUN_ID}}/{{PPL_NAME}}/{{RUNTIME_NAME}}-{{MD5_FULLNAME} 目录

> 为什么不直接生成artifact路径?
> - 因为不清楚artifact是一个文件，还是目录，所以留给用户自己决定。


### 3.1.1 示例

举个例子，使用[1 pipeline定义]，发起一个Pipeline任务。则Paddleflow为output artifact生成的路径可能如下：

> {{PF_RUN_ID}}的值可能会有出入
```
.pipeline/
└── run-000078                                              # {{PF_RUN_ID}}
    └── artifact_example                                    # {{PPL_NAME}}
        ├── preprocess-0-037175363b6ef656300d85b2821919a1   # {{RUNTIME_NAME}}-{{MD5_FULLNAME}}
        │   ├── train_data                                  # {{OUTPUT_ARTIFACT_NAME}}
        │   │   └── data
        │   └── validate_data
        │       └── data
        └── train-0-2b2bc734db42989f06b47d4a59b3d6d4        # {{RUNTIME_NAME}}-{{MD5_FULLNAME}}
            └── train_model
                └── model
```

如上所示：
- 由于在[main_fs]中没有指定 sub_path, 因此在上例中，.pipeline 存储在 [main_fs] 的根目录下

- preprocess节点定义了train_data，validate_data两个output artifact，因此生成两个对应路径。

- train节点定义了train_model一个output artifact，因此生成了一个对应路径。

> 需要注意的是，Paddleflow只会在节点运行前创建创建以下目录：
>
> - preprocess节点运行前，创建 .pipeline/run-000078/artifact_example/preprocess-0-037175363b6ef656300d85b2821919a1
> - train节点运行前，创建 .pipeline/run-000078/artifact_example/train-0-2b2bc734db42989f06b47d4a59b3d6d4
> 每个output artifact的具体路径，需要用户在代码中自己创建


## 3.2 变量模板与替换

### 3.2.1 变量模板

引入artifact变量后，在节点定义中，parameters，command，env，artifact变量都可以使用变量模板。

下面介绍详细的定义规范和替换流程

### 3.2.2 定义规范和替换流程

下面按照每个节点运行前的变量模板替换顺序，来介绍每个变量支持的模板：

1. parameter 支持以下模板：
- 系统变量
- 上游节点parameter

2. input artifact 替换为上游 output artifact的路径

3. output artifact 替换为平台生成的路径

4. env 替换以下模板：
- 系统变量
- 本 step 内 parameter

5. command 替换以下模板：
- 系统变量
- 本 step 内 parameter
- 本 step 内 input artifact
- 本 step 内 output artifact


[1_pipeline_basic.md]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md
[artifact_example]: /example/pipeline/artifact_example
[5_cache.md]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md
[1 pipeline定义]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#1-pipeline%E5%AE%9A%E4%B9%89
[2.1.1 artifact vs parameter]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#211-artifact-vs-parameter
[2.4 artifact 使用方式]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#24-artifact-%E4%BD%BF%E7%94%A8%E6%96%B9%E5%BC%8F
[3.1 artifact存储机制]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#31-artifact%E5%AD%98%E5%82%A8%E6%9C%BA%E5%88%B6
[3_multiple_fs.md]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md
[main_fs]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md#221-main_fs
[loop_argument]:/docs/zh_cn/reference/pipeline/yaml_definition/9_loop.md
[祖先节点]: /docs/zh_cn/reference/pipeline/yaml_definition/7_dag.md
