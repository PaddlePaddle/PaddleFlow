在之前的章节中，pipeline的所有节点都是一个可以发起Job的Step， 这些pipeline对于一些比较复杂的，或者需要多人协作的场景并不能很好的支持, 因此，Paddleflow pipeline 还额外提供了DAG节点。

# 1 使用场景
一个完整的AI开发流程一般包含如下四个环节：

- 数据处理
- 模型训练
- 服务部署
- 统计回流

一般而言，每个环节又都可以细分成多个步骤，以`数据处理`为例，其一般包含如下的几个步骤：

- 数据收集
- 数据预处理
- 数据清晰
- 数据分析

其中的每一个步骤都有可能再次细分成多个子步骤，因此，一个完整AI流程是及其复杂的，可能包含了几十个甚至上百个步骤。

在没有DAG的情况下，用户需要在同一层级完成所有步骤的编排与管理工作，这无疑是个极大的挑战。

而在引入了DAG这一概念后，用户便可以将逻辑上相近的步骤组合成一个DAG，然后再将多个DAG组合成一个完整的Pipeline，类似于模块化管理，具备了模块化的所有优点：
- 架构清晰，复杂度较低
- 支持模块级别的复用
- 更利于多人协作开发
- 更低的耦合性


# 2 pipeline定义
下面为一个定义了DAG节点的示例Pipeline定义

> 该示例中pipeline定义，以及示例相关运行脚本，来自pddleflow项目下example/pipeline/dag_example 示例。
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
如[2.1 pipeline定义]所示，在entry_points字段中，即可以定义Step节点，也可以定义DAG节点。接下来将依次依次介绍DAG的各个字段

> [post_process]字段中只能定义Step节点

### 3.1 deps
表示当前节点，依赖的上游节点。

- 若有多个上游节点，上游节点名通过逗号分隔
- 如果该字段没有定义，或者为空，表示不依赖任何其他节点

### 3.2 artifacts
与Step节点一样，DAG节点也可以定义artifact。

不一样的是，Step节点定义的artifact要么是节点本身需要使用到（输入artifact），要么是节点本身生成（输出artifact）给其下游节点使用。

而DAG节点定义的artifact主要是为了artifact的*传递*： 
- 将DAG上游节点的输出artifact*传递*给子节点使用(输入artifact)
- 将子节点生成的artifact传递给DAG的下游节点使用(输出artifact)

##### 3.2.1 输入artifact
DAG节点输入artifact的定义方式与[Step节点定义输入artifact]的方式并无二致，因此，这里不赘述.

##### 3.2.2 输出artifact
与Step节点的输出artifact需要是一个list不同，DAG节点的输出artifact需要是一个字典，其value需要是一个**模板("{{$COMPONENT_NAME.ARTIFACT_NAME}}")**, 指名其来自于哪个**子节点**的哪个**输出artifact**。
> 这里的COMPONENT_NAME用于指代子节点的名字

> 如示例[2 pipeline定义]所示，DAG节点process的输出artifact[result]的value为"{{collector.collection}}"
> 
> 即表明该输出artifact来自于子节点collector的输出collection

> 注意：同一个节点中，不能有同名的artifact

### 3.3 parameters
DAG节点也支持parameters字段，用于定义运行时的参数，其定义方法与[Step节点定义parameters]保持一致，此处不在赘述。

> 注意: 一个节点内，parameters变量名字不能相同

### 3.4 entry_points
DAG也即有向无环图，一个DAG节点**至少需要有一个子节点**， 这些子节点间可以通过单向依赖，来组成一个有向无环图。

在DAG节点中，其子节点需要定义在DAG的entry_points字段中。如[2 pipeline定义]的process节点entry_points字段所示.

> 在示例[2 pipeline定义]的DAG节点process中，依次定义了四个子节点：split，process-negetive,process-positive，collector。 这四个节点以split为根构成了一个有向无环图。

> TIPS:
> DAG的子节点既可以是Step节点，也可以是DAG节点

DAG的子节点的定义方式与在pipeline的entry_points中定义节点的方式基本相同。只是定义时需要注意如下几点约束：
 
##### 3.4.1. 节点名约束
- 同一个DAG内内，子节点的节点名必须保持唯一

- 不同层级的节点可以同名，举个例子：
   - pipeline有一个DAG节点B，则B可以有一个子节点的名字为B
 
- 不同的DAG的子节点可以具有相同的名字，举个例子：
   - pipeline有两个DAG节点A和B，则A和B均可以有一个子节点命名为C


##### 3.4.2 deps约束
- DAG的子节点只能依赖于**同一个DAG内**的节点，也**只能被同一个DAG**内的节点所依赖。

> 以[2 pipeline定义]为例：
> - `process`的子节点`process-negetive`不能依赖与`randint`节点，也不能被`sum`节点依赖
> - 但是可以依赖于`split`节点，也可以被`collector`依赖
> - 因为节点`randint`与`sum`均不是`process`的子节点，而节点`split`，`collector`与`process-negetive`一样，都是`process`的子节点


##### 3.4.3 输入artifact约束
如果DAG的子节点A的引用节点B的artifact，则需要满足如下约束(满足一条即可)：
- A和B是**同一DAG的子节点**，且引用的是B的**输出artifact**
- A为B的子节点，且引用的为B的**输入artifact**
  - 此时的引用模板为："{{PF_PARANT.$ARTIFACT_NAME}}"
  - 其中: 'PF_PARANT'为固定值，不可更改，用于指代当前节点的父节点
  - $ARTIFACT_NAME即为需要引用的parameter的名字
  - 如[2 pipeline定义]中的节点split的输如artifact[nums]所示, 其value为"{{PF_PARENT.data}}"，表明该输入artifact来自于其父节点的输入artifact[data]


##### 3.4.4 parameters约束
如果DAG的子节点A的parameters需要通过模板来引用节点B的paramter，则需要满足如下约束(满足一条即可)：
- A和B是同一DAG的子节点
- A为B的子节点
  - 此时对应的模板为"{{PF_PARANT.$PARAMETER_NAME}}" 
  - 其中: 'PF_PARANT'为固定值，不可更改，用于指代当前节点的父节点
  - $PARAMETER_NAME即为需要引用的parameter的名字



[2 pipeline定义]: TODO
[dag_example]: TODO
[post_process]: TODO
[Step节点定义输入artifact]: TODO
[Step节点定义parameters]: TODO
