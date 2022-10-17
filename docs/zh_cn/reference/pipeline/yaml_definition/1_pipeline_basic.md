下面先介绍最基础的pipeline定义。

# 1 pipeline定义

下面是一个典型的，最基础的yaml格式pipeline定义。

> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/base_pipeline示例。
> 
> 示例链接：[base_pipeline]

```yaml
name: base_pipeline

docker_env: nginx:1.7.9

entry_points:
  preprocess:
    command: bash base_pipeline/shells/data.sh {{data_path}}
    docker_env: centos:centos7 
    env:
      USER_ABC: 123_{{PF_USER_NAME}}
    parameters:
      data_path: ./base_pipeline/data/{{PF_RUN_ID}}

  train:
    command: bash base_pipeline/shells/train.sh {{epoch}} {{train_data}} {{model_path}}
    deps: preprocess
    parameters:
      epoch: 5
      model_path: ./output/{{PF_RUN_ID}}
      train_data: '{{preprocess.data_path}}'

  validate:
    command: bash base_pipeline/shells/validate.sh {{model_path}}
    deps: train
    parameters:
      model_path: '{{train.model_path}}'

parallelism: 1
```

# 2 各字段解析

下面基于上述pipeline定义，介绍每个字段的作用

## 2.1 全局字段

### 2.1.1 name

pipeline名。每次运行默认使用该name作为pipeline run的名称。

pipeline名必须满足: 只能由字母数字下划线组成，且以字母或下划线开头

- 正则表达式: ^[A-Za-z_][A-Za-z0-9_]{0,49}$

### 2.1.2 docker_env

镜像路径。每个Step必须通过容器运行，而该参数用于指定每个Step运行时，所使用的镜像。

- 支持定义**全局级别** / **节点级别**的docker_env参数。
- 使用优先级：节点级别的docker_env参数 > 全局级别的docker_env参数。

### 2.1.3 entry_points

用于定义pipeline结构。

pipeline是由各个节点组成的有向无环图（DAG）结构，因此entry_points定义，也是由多个节点定义以DAG结构的形式组成。

每个节点的定义包括运行相关参数，依赖关系构成。节点的具体定义方式，可以参考[2.2 节点字段]


### 2.1.4 并发度（parallelism）

单次pipeline run，最大节点运行并发度。
- 默认是10，目前最大不能超过20。
- 同一个pipeline，发起的不同run，parallelism相互独立。
    - 即不同的run，都可以同时运行最多10个节点job。
- 实际节点运行并发度，也可能会受底层资源影响。


### 2.2 节点字段

如 [2.1.3 entry_points] 所示，此entry_points定义，是由多个节点定义以dag结构的形式组成。节点定义需要满足：

1. entry_points中，不能存在名称相同的节点。
- 而且节点名称必须满足：只能由字母，数字，中划线组成，且以字母开头
- 对应正则表达式: ^[a-zA-Z][a-zA-Z0-9-]{0,29}$

2. 一个节点内，parameters变量名字不能相同。

- 例如：parameters参数，不能同时定义名字为data的变量


> PaddleFlow 提供了两种类型的节点：Step 和 [DAG]
> 
> 为了简单起见， 此处我们只关注Step节点

下面介绍Step节点定义中的每个字段：

##### 2.2.1 docker_env

镜像路径。与全局的docker_env参数作用一致，用于指定该节点运行时，所使用的镜像。

- 使用优先级：节点级别的docker_env参数 > 全局级别的docker_env参数。

- 该字段必须要指定：即节点和全局docker_env参数至少指定一个。

##### 2.2.2 parameters

节点运行参数。

###### 2.2.2.1 parameters参数名

parameters参数名必须满足: 只能由字母数字下划线组成，且以字母或下划线开头

- 正则表达式: ^[A-Za-z_][A-Za-z0-9_]{0,49}$


###### 2.2.2.2 parameters参数值

1. 在yaml中定义的参数，可以理解为默认值

2. 在每次发起任务时，可以通过接口参数，动态传入parameters的值

> 发起任务相关接口，可参考[CLI发起任务]或[SDK发起任务]

###### 2.2.2.3 parameters使用方式

1. parameters可以以变量模板形式被command，env等参数引用，具体使用逻辑可以参考[3.1.1 变量模板]

##### 2.2.3 env

所有在此处定义的参数，在节点运行时，都可以以环境变量的形式获取。

节点实际运行时，可以访问以下类型的环境变量：

1. 节点定义中的env参数

- env参数名必须满足: 只能由字母数字下划线组成，且以字母下划线开头

- 正则表达式: ^[A-Za-z_][A-Za-z0-9_]{0,49}$

3. 平台系统变量，可参考[3.1.1 变量模板]

##### 2.2.4 command

节点运行命令。节点运行时，会直接运行该命令。

##### 2.2.5 deps

表示当前节点，依赖的上游节点。

- 若有多个上游节点，上游节点名通过逗号分隔
- 如果该字段没有定义，或者为空，表示不依赖任何其他节点

# 3 pipeline运行流程

得到pipeline定义后，可以通过CLI，SDK，或者http请求方式发起pipeline run。

发起pipeline run时，后端服务会做以下操作：
1. 校验pipeline定义是否符合规范
2. 解析节点依赖关系，寻找可运行的节点
3. 为可运行节点中所有参数，替换各类变量模板（parameters，command，env等），并发起节点job

其中，在一个pipeline run中，第2，3步会不断循环，直至pipeline run结束。

下面重点介绍各类变量的定义规范，和节点运行前变量模板的替换逻辑。

### 3.1 变量模板与替换

##### 3.1.1 变量模板

在节点定义中，parameters，command，env变量除了可以定义成字符串常量，还可以通过变量模板，引用其他变量。

目前支持的变量模板有三类：

1. **系统变量**：Paddleflow提供的，系统变量的引用，引用模板包括：

- {{PF_RUN_ID}}：表示当前pipeline run的ID，格式为：run-XXXXXX。例如：run-000032

- {{PF_STEP_NAME}}：当前运行的节点名称

- {{PF_USER_NAME}}：当前运行的用户

> 使用示例：[1 pipeline定义]中：
> - preprocess节点，env中的USER_ABC变量，引用了{{PF_USER_NAME}}变量
> - preprocess节点，parameters中的data_path变量，引用了{{PF_RUN_ID}}变量

1. **同节点其他参数**：用于引用同一节点内的其他参数。

> 使用示例：[1 pipeline定义]中：
> preprocess节点的command变量，引用了同节点parameters中的data_path变量

3. **上游节点参数**：用于引用上游节点内的参数，格式为{{stepName.paramName}}。

> 使用示例：[1 pipeline定义]中：
> train节点parameters中，train_data参数使用 {{preprocess.data_path}} 模板，引用了 preprocess 节点的 data_path 参数

> tips: 模板内可以包含空格，Paddleflow在参数替换前会忽略两边的空格。例如：
> - {{data_path}} 与 {{ data_path }} 等价

> 注意: 如果节点A使用了节点B的参数模板，则在节点A的deps字段中必须包含B

##### 3.1.2 定义规范和替换流程

下面按照每个节点运行前的变量模板替换顺序，来介绍每个变量支持的模板：

1. parameters 支持以下模板：
- 系统变量
- 上游节点parameters

2. env 支持以下模板：
- 系统变量
- 本step内 parameters

3. command 支持以下模板：
- 系统变量
- 本step内 parameters

[base_pipeline]: /example/pipeline/base_pipeline
[CLI发起任务]: /docs/zh_cn/reference/client_command_reference.md
[SDK发起任务]: /docs/zh_cn/reference/sdk_reference/sdk_reference.md
[1 pipeline定义]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#1-pipeline%E5%AE%9A%E4%B9%89
[2.1.3 entry_points]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#213-entry_points
[2.2 节点字段]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#22-%E8%8A%82%E7%82%B9%E5%AD%97%E6%AE%B5
[2.2.2.3 parameters使用方式]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#2223-parameters%E4%BD%BF%E7%94%A8%E6%96%B9%E5%BC%8F
[3.1.1 变量模板]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#311-%E5%8F%98%E9%87%8F%E6%A8%A1%E6%9D%BF
[DAG]: /docs/zh_cn/reference/pipeline/yaml_definition/7_dag.md
