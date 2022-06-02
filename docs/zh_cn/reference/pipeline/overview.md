# pipeline概览

由PaddleFlow总览可知，PaddleFlow分成存储，调度，工作流三部分。

工作流，即Pipeline部分，主要功能如下：

1. 制定工作流定义规范。通过yaml，python DSL等规范，支持以有向无环图（DAG）的形式定义多个节点间的运行参数，以及运行关系。

- pipeline yaml定义规范：可以参考 [yaml定义规范]

- pipeline python DSL定义规范：可以参考 [DSL定义规范]

2. 支持工作流，工作流任务管理。通过命令行（CLI），python SDK等形式，支持工作流的增删查改，以及工作流任务的管理，实现工作流定义的复用。

- 命令行使用规范：可以参考 [命令行使用规范]

- python SDK使用规范：可以参考 [SDK使用规范]


### 名词解释

工作流(Pipeline)：项目中运行的静态信息通过工作流来进行描述。

节点(Step)：一个工作流，可以包含多个节点。

运行(Run)： 一个工作流的一次运行称为一个Run。

作业(Job)： 一个节点的一次运行，叫做一个作业。

队列(Queue)：资源分配的最小单元，队列可以授权给用户进行使用。

资源套餐(Flavour)：作业运行的资源单位，套餐中指定了作业运行所需要的cpu/mem/gpu等信息。

共享存储(Fs)：目前pf运行需要指定共享存储，job运行时会直接挂载共享存储到容器home目录。


[yaml定义规范]: /docs/zh_cn/reference/pipeline/yaml_definition
[DSL定义规范]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md
[命令行使用规范]: /docs/zh_cn/reference/client_command_reference.md
[SDK使用规范]: /docs/zh_cn/reference/sdk_reference/sdk_reference.md
