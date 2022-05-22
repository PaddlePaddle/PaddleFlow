# pipeline概览

由PaddleFlow总览可知，PaddleFlow分成存储，调度，工作流三部分。

工作流，即Pipeline部分，主要功能如下：

1. 制定工作流定义规范。通过yaml，python dsl等规范，支持以有向无环图（DAG）的形式定义多个节点见的运行内容，以及运行关系。

- pipeline yaml定义规范：可以参考【XXXX】
- pipeline python dsl定义规范：可以参考【XXXX】

2. 支持工作流，工作流任务管理。通过命令行（cli），python sdk等形式，提供工作流的增删查改，以及工作流任务的管理，实现工作流定义的复用。

- 命令行使用规范：可以参考【XXXX】
- python sdk使用规范：可以参考【XXXX】


### 名词解释

工作流(Pipeline)：项目中运行的静态信息通过工作流来进行描述。

节点(Step)：一个工作流，可以包含多个节点。

运行(Run)： 一个工作流的一次运行称为一个Run。

作业(Job)： 一个节点的一次运行，叫做一个作业。

队列(Queue)：资源分配的最小单元，队列可以授权给用户进行使用。

资源套餐(ResourceFlavour)：作业运行的资源单位，套餐中指定了作业运行所需要的cpu/mem/gpu等信息。

共享存储(Fs)：目前pf运行需要指定共享存储，job运行时会直接挂载共享存储到容器home目录。