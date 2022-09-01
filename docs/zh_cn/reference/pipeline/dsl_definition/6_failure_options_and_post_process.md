在某些情况下，在Pipeline任务结束时，我们需要执行某些操作，如向相关人员发送邮件，向某个服务发请求以做进一步的处理等，此时便需要使用post_process机制。

在Pipeline任务运行时，有一个节点运行失败了，其余的节点需要怎么处理？是快速失败还是继续运行？此时便需要使用failure_options机制。

关于post_process与failure_options机制的详细解释，请点击[这里][Post-Fail-ref]，本文主要讲解如何在使用DSL定义pipeline时如何使用，不在对其定义进行赘述。

## 1、pipeline示例
下面的示例是基于[artifact_pipeline][artifact_pipeline]中的pipeline定义增加post_process与failure_options的特性升级而来
>该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/failure_options_and_post_process_example示例。
>
>示例链接：[failure_options_and_post_process_example][failure_options_and_post_process_example]

```python3
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import FailureOptions
from paddleflow.pipeline import FAIL_CONTINUE

def step(name):
    return ContainerStep(
        name=name,
        command=f"echo {name}",
    )
    
@Pipeline(name="failure_options_and_post_process_example", docker_env="nginx:1.7.9",
          parallelism=1, failure_options=FailureOptions(FAIL_CONTINUE))
def failure_options_and_post_process_example():
    steps = []
    for i in range(5):
        steps.append(step(f"step{i+1}"))
        
    steps[1].after(steps[0])
    steps[2].after(steps[1])
    
    steps[3].command = "echo step4; exit  1"
    steps[4].after(steps[3])
    
ppl = failure_options_and_post_process_example()
ppl.set_post_process(step("step6"))
print(ppl.run())
```

## 2、failure_options
在DSL中配置failure_options的非常简单，只需在创建Pipeline实例时，给参数failure_options赋值即可，其值需要是一个[FailureOptions][FailureOptions]实例。

在上例中，便是通过如下代码完成failure_options的相关配置：

```python3
@Pipeline(name="failure_options_and_post_process_example", docker_env="nginx:1.7.9",
          parallelism=1, failure_options=FailureOptions(FAIL_CONTINUE))
```

## 3、 post_process
通过DSL设置post_process的方式十分简单，调用Pipeline实例的set_post_process()函数即可，该函数的函数签名如下：
```python3
def set_post_process(self, step: Step):
    """ 设置post_process阶段需要运行的节点

    参数:
        step (Step): 将会在post_process阶段需要运行的节点 
    """
```

在上面的[示例](#1pipeline示例)中，便是通过如下的代码来设置post_process：
```python3
ppl.set_post_process(step("step6"))
```

[DSL使用基础]: /docs/zh_cn/reference/pipeline/dsl_definition/1_pipeline_basic.md
[FailureOptions]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#10FailureOptions
[DSL接口文档]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md
[在DSL中使用Cache]: /docs/zh_cn/reference/pipeline/dsl_definition/3_cache.md
[artifact_pipeline]: /docs/zh_cn/reference/pipeline/dsl_definition/2_artifact.md
[Post-Fail-ref]: /docs/zh_cn/reference/pipeline/yaml_definition/6_failure_options_and_post_process.md
[failure_options_and_post_process_example]: /example/pipeline/failure_options_and_post_process_example
[DSL-Cache]: /docs/zh_cn/reference/pipeline/dsl_definition/3_cache.md
