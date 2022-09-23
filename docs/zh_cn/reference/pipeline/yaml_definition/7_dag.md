在之前的章节中，pipeline的所有节点都是一个可以发起Job的Step， 这些pipeline对于一些比较复杂的，或者需要多人协作的场景并不能很好的支持, 因此，Paddleflow提供了另外一种节点类型： DAG。

# 1 使用场景
一个完整的AI开发流程一般包含如下四个环节：

- 数据处理
- 模型训练
- 服务部署
- 统计回流

一般而言，每个环节又都可以细分成多个步骤，以`数据处理`为例，其一般包含如下的几个步骤：

- 数据收集
- 数据预处理
- 数据清洗
- 数据分析

其中的每一个步骤都有可能再次细分成多个子步骤，因此，一个完整AI流程是及其复杂的，可能包含了几十个甚至上百个步骤。

在没有DAG的情况下，用户需要在同一层级完成所有步骤的编排与管理工作，这无疑是个极大的挑战。

而在引入了DAG这一概念后，用户便可以将逻辑上相近的步骤组合成一个DAG，然后再将多个DAG组合成一个完整的Pipeline，类似于模块化管理，具备了模块化的所有优点：
- 结构更清晰，复杂度更低
- 支持模块级别(DAG)的复用
- 提升多人协作效率

# 2 pipeline定义
下面为一个定义了DAG节点的示例Pipeline定义

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/dag_example 示例。
> 
> 示例链接：[dag_example]

```yaml
name: dag_example

docker_env: python:3.7

entry_points:
  randint:
    command: "cd /randint && python3 randint.py {{num}} {{lower}} {{upper}}"
    parameters:
      lower: -10
      upper: 10
      num: 10
    artifacts:
      output:
      - random_num
    extra_fs:
    - {name: ppl, mount_path: /randint, sub_path: dag_example/randint, read_only: true}

  process:
    deps: randint
    artifacts:
      input: 
        data: "{{randint.random_num}}"
      output:
        result: "{{collector.collection}}"
    entry_points:

      split:
        command: "cd /split && python3 split.py {{threshold}}"
        artifacts:
          output:
          - negetive
          - positive
          input:
            nums: "{{PF_PARENT.data}}"
        parameters:
          threshold: 0
        extra_fs:
        - {name: ppl, mount_path: /split, sub_path: dag_example/split, read_only: true}

      process-negetive:
        command: "cd /process_negetive && python3 process_negetive.py"
        deps: split
        artifacts:
          input:
            negetive: "{{split.negetive}}"
          output:
          - result
        extra_fs:
        - {name: ppl, mount_path: /process_negetive, sub_path: dag_example/process_negetive, read_only: true}

      process-positive:
        command: "cd /process_positive && python3 process_positive.py"
        deps: split
        artifacts:
          input:
            positive: "{{split.positive}}"
          output:
          - result
        extra_fs:
        - {name: ppl, mount_path: /process_positive, sub_path: dag_example/process_positive, read_only: true}

      collector:
        deps: process-negetive,process-positive
        command: cd /collector && python3 collect.py
        artifacts:
          input:
            negetive: "{{process-negetive.result}}"
            positive: "{{process-positive.result}}"
          output:
          - collection
        extra_fs:
        - {name: ppl, mount_path: /collector, sub_path: dag_example/collector, read_only: true}

  sum:
    deps: process
    command: "cd /sum && python3 sum.py"
    artifacts:
      input:
        nums: "{{process.result}}"
      output:
        - result
    extra_fs:
    - {name: ppl, mount_path: /sum, sub_path: dag_example/sum, read_only: true}

parallelism: 2

fs_options:
  main_fs: {name: ppl}
```

在上例中，pipeline总共定义了三个节点
- randint: `Step`节点，生成随机数
- process: `DAG`节点，处理randint节点生成的数据
- sum: `Step`节点，将经过process节点处理后的数据进行求和

`randint`和`sum`节点与前面章节中的节点定义方式并无区别，因此这里不在赘述。本文接下来将通过`process`节点来说明如何定义`DAG`节点

# 3 DAG详解
如[2 pipeline定义]所示，在entry_points字段中，即可以定义Step节点，也可以定义DAG节点。接下来将依次依次介绍DAG的各个字段

> [post_process]字段中只能定义Step节点

## 3.1 deps
表示当前节点，依赖的上游节点。

- 若有多个上游节点，上游节点名通过逗号分隔
- 如果该字段没有定义，或者为空，表示不依赖任何其他节点

## 3.2 artifacts
与Step节点一样，DAG节点也可以定义artifact。而且也分成 input artifact和output artifact。

但是DAG节点定义artifact的作用与Step不同：
- input artifact: 将**DAG节点**的上游节点的输出artifact**传递**给**DAG节点的子节点**使用
- output artifact: 将**DAG节点的子节点**生成的输出artifact**传递**给**DAG节点**的下游节点使用

### 3.2.1 输入artifact
DAG节点输入artifact的定义方式与[Step节点定义输入artifact]的方式并无二致，这里不在赘述.

### 3.2.2 输出artifact
与Step节点的输出artifact需要是一个list不同，DAG节点的输出artifact需要是一个字典，其value需要是一个引用其子节点的输出artifact的模板

> DAG节点本身不会创建JOB，因此其本身也不能生成任何的文件。所以DAG节点的输出artifact必须来自于其子节点

模板需要指明其自于哪个**子节点**的哪个**输出artifact**， 因此，其格式为：\{\{SUB_NAME.ARTIFACT_NAME\}\}，其中SUB_NAME指代子节点的名字。

> 如示例[2 pipeline定义]所示，DAG节点process的输出artifact[result]的value为"{{collector.collection}}"
> 
> 即表明该输出artifact来自于子节点collector的输出collection

## 3.3 parameters
DAG节点也支持parameters字段，用于定义运行时的参数，其定义方法与[Step节点定义parameters]保持一致，此处不在赘述。

## 3.4 entry_points
DAG也即有向无环图，一个DAG节点**至少需要有一个子节点**， 这些子节点间可以通过单向依赖，来组成一个有向无环图。

在DAG节点中，其子节点需要定义在DAG的entry_points字段中。如[2 pipeline定义]的process节点的entry_points字段所示.

> 在示例[2 pipeline定义]的DAG节点process中，依次定义了四个子节点：split，process-negetive,process-positive，collector。 这四个节点以split为根构成了一个有向无环图。

> TIPS:
> DAG的子节点既可以是Step节点，也可以是DAG节点

DAG的子节点的定义方式与在pipeline的entry_points中定义节点的方式基本相同。只需注意如下几点约束即可
 
### 3.4.1. 节点名约束
- 同一个DAG的子节点的不能同名

### 3.4.2 deps约束
- DAG的子节点只能依赖**同一个DAG**的子节点，也**只能被同一个DAG**的子节点所依赖。

> 以[2 pipeline定义]为例：
> - `process`的子节点`process-negetive`不能依赖节点`randint`，也不能被节点`sum`依赖
> - 但是可以依赖节点`split`，也可以被节点`collector`依赖
> - 因为节点`randint`与`sum`均不是`process`的子节点，而节点`split`，`collector`与`process-negetive`一样，都是`process`的子节点


### 3.4.3 artifact约束
如果节点A的引用了节点B的artifact，则需要满足如下约束(满足一条即可)：
- A和B是**同一DAG的子节点**，且引用的是B的**输出artifact**
- A为B的子节点，且引用的为B的**输入artifact**
  - 此时的引用模板为："{{PF_PARANT.ARTIFACT_NAME}}"
  - 其中: 'PF_PARANT'为固定值，不可更改，用于指代当前节点的父节点
  - 如[2 pipeline定义]中的节点split的输入artifact[nums]所示, 其value为"{{PF_PARENT.data}}"，表明该输入artifact来自于其父节点的输入artifact[data]


### 3.4.4 parameters约束
如果节点A的parameters通过模板引用了节点B的paramter，则需要满足如下约束(满足一条即可)：
- A和B是同一DAG的子节点
- A为B的子节点
  - 此时对应的模板为"{{PF_PARANT.PARAMETER_NAME}}" 
  - 其中: 'PF_PARANT'为固定值，不可更改，用于指代当前节点的父节点


[2 pipeline定义]: /docs/zh_cn/reference/pipeline/yaml_definition/7_dag.md#2-pipeline%E5%AE%9A%E4%B9%89
[dag_example]: /example/pipeline/dag_example
[post_process]: /docs/zh_cn/reference/pipeline/yaml_definition/6_failure_options_and_post_process.md
[Step节点定义输入artifact]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#22-artifact-%E5%AE%9A%E4%B9%89%E6%96%B9%E5%BC%8F
[Step节点定义parameters]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#222-parameters
