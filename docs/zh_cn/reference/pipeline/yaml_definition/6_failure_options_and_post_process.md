本章节，继续介绍pipeline运行相关的其他机制：failure options 和 postprocess。

# 1 使用场景

### 1.1 为什么需要failure options（失败条件）

试想，我们构建了一个5节点的pipeline，其中：
- 分支1: step1->step2->step3串行运行
- 分支2: step4->step5串行运行

但是这两个分支的节点间没有相互流程依赖。

在已有调度逻辑中，无论哪一个分支的节点job运行失败，所有运行中的节点会被终止，pipeline run结束。

然而某个场景下，分支1运行失败，我们希望其他分支（如分支2）能够继续运行。

因此，我们提出failure options机制。

### 1.2 为什么需要postprocess（后处理）

通常，在pipeline run运行结束时，我们可能需要以下操作：

- 无论pipeline run运行成功/失败，都发邮件通知相关人员
- 如果pipeline run失败，则执行清理操作

上述场景，需要支持我们定义一个独立于pipeline 主dag结构的节点，无论pipeline run以何种状态结束，都能运行这个独立的节点，进行后处理操作。

因此我们提出了postprocess机制

# 2 pipeline定义

下面为示例pipeline定义，其中包含failure options，和postprocess相关字段。

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/failure_options_and_post_process_example 示例。
> 
> 示例链接：[failure_options_and_post_process_example]

```yaml
name: failure_options_and_post_process_example

entry_points:
  step1:
    command: echo step1

  step2:
    command: echo step2
    deps: step1

  step3:
    command: echo step3
    deps: step2

  step4:
    command: echo step4; exit  1

  step5:
    command: echo step5
    deps: step4

post_process:
  step6:
    command: echo step6

failure_options:
  strategy: continue

parallelism: 1

docker_env: nginx:1.7.9
```

# 3 配置详解

### 3.1 failure option配置

由上述pipeline定义所示，failure options是一个全局参数。目前子参数只有strategy。取值如下：

* fail_fast（默认值）： 如果有节点运行失败，其余正在运行中的节点将会被kill， 未被调度的节点将永远不会被调度, run的状态将会被设置为failed
  * 在这种情况下，当step4异常退出时， step0将会被终止， 其余的节点将会被取消，不会被调度

* continue： 除了当前节点所在的分支外，其余分支的节点将会被正常调度。等到所有节点运行结束后，run的状态将会被设置为 failed
  * 在这种情况下，当step4异常退出时， 由于step5与step4 处于同一个分支（step5是step4的下游节点），因此step5将会被取消，不会被调度。
  * step1， step2，step3 与 step4 不在同一分支上，因此会被继续调度执行。

### 3.2 postprocess配置

由上述pipeline定义所示，postprocess是一个全局参数。

与entrypoints字段类似，在该字段中，我们可以定义 post_process 阶段执行的step，step的定义方式亦与entrypoints中保持一致。

摘取 [2 pipeline定义] 中postprocess部分，展示如下：

```
post_process:
  step6:
    command: "echo step6"
    env:
      PF_JOB_TYPE: vcjob
      PF_JOB_MODE: Pod
      PF_JOB_QUEUE_NAME: queue1
      PF_JOB_FLAVOUR: flavour1
```

在上例中， post_process 中的节点 step6 将会在 entry_points 中的所有节点都处于终态后，才会执行

##### 3.2.1 postprocess配置约束

- post_process 中，最多只能有一个节点
- post_process中的节点，不支持cache相关配置（我们认为后处理步骤不需要缓存）
- post_process中的节点，不能与entry_points中的节点存在任何依赖关系
- post_process中的节点，不能与entry_points中节点名相同

[failure_options_and_post_process_example]: /example/pipeline/failure_options_and_post_process_example
[2 pipeline定义]: /docs/zh_cn/reference/pipeline/yaml_definition/6_failure_options_and_post_process.md#2-pipeline%E5%AE%9A%E4%B9%89
