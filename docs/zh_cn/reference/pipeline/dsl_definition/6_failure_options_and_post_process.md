# 使用post_process与failure_options
在[DSL使用基础中][DSL使用基础]，我们介绍了DSL的基础使用。但是，在某些情况下，在Pipeline任务结束时，我们需要执行某些操作，如向相关人员发送邮件，向某个服务发请求以做进一步的处理等，此时便需要使用post_process机制。

在Pipeline任务运行时，有一个节点运行失败了，其余的节点需要怎么处理？是快速失败还是继续运行？此时便需要使用failure_options机制。

关于post_process与failure_options机制的详细解释，请点击[这里][Post-Fail-ref]，本文主要讲解如何在使用DSL定义pipeline时如何使用，不在对其定义进行赘述。

## 1、pipeline示例
下面的示例是基于[artifact_pipeline][artifact_pipeline]中的pipeline定义增加post_process与failure_options的特性升级而来
>该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/failure_options_and_post_process_example示例。
>
>示例链接：[failure_options_and_post_process_example][failure_options_and_post_process_example]

```python3
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import FailureOptions
from paddleflow.pipeline import FAIL_CONTINUE
from paddleflow.pipeline import FAIL_FAST

def job_info():
    return {
        "PF_JOB_TYPE": "vcjob",
        "PF_JOB_MODE": "Pod",
        "PF_JOB_QUEUE_NAME": "ppl-queue",
        "PF_JOB_FLAVOUR": "flavour1",
    }

def echo_step(name, exit_error=False):
    command = f"echo {name}"
    command  = f"{command}; exit  1" if exit_error else command

    return ContainerStep(
        name=name,
        command=command
    )

fail = FailureOptions(FAIL_CONTINUE)

@Pipeline(
        name="failure_options_and_post_process_example",
        docker_env="nginx:1.7.9",
        parallelism=1,
        env=job_info(),
        failure_options = fail
        )
def failure_options_and_post_process_example():
    step1 = echo_step("step1")
    step2 = echo_step("step2")
    step3 = echo_step("step3")
    step2.after(step1)
    step3.after(step2)

    step4 = echo_step("step4", True)
    step5 = echo_step("step5")
    step5.after(step4)

def set_post_process(ppl):
    post_process = echo_step("step6")
    ppl.set_post_process(post_process)


if __name__ == "__main__":
    ppl = failure_options_and_post_process_example()
    set_post_process(ppl)
    result = ppl.run(fsname="your_fs_name")
    print(result)

```

## 2、failure_options
在DSL中配置failure_options的非常简单，只需在调用Pipeline的实例化函数时，给参数failure_options赋值即可，其值需要是一个[FailureOptions][FailureOptions]实例。在上例中，便是通过如下代码完成failure_options的相关配置：
```python3
fail = FailureOptions(FAIL_CONTINUE)

@Pipeline(
        name="failure_options_and_post_process_example",
        docker_env="nginx:1.7.9",
        parallelism=1,
        env=job_info(),
        failure_options = fail
        )
```

## 3、 post_process
通过DSL设置post_process的方式十分简单，直接调用Pipeline实例的set_post_process()函数即可，该函数的函数签名如下：
```python3
def set_post_process(self, step: Step):
    """ 设置post_process阶段需要运行的节点

    参数:
        step (Step): 将会在post_process阶段需要运行的节点 
    """
```

在上面的[示例](#1pipeline示例)中，便是通过如下的代码来设置post_process：
```python3
def set_post_process(ppl):
    post_process = echo_step("step6")
    ppl.set_post_process(post_process)
```

# 4、更多信息
[DSL接口文档][DSL接口文档]

[在DSL中使用Cache][DSL-Cache]

[DSL使用基础]: /docs/zh_cn/reference/pipeline/dsl_definition/1_pipeline_basic.md
[FailureOptions]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#6FailureOptions
[DSL接口文档]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md
[在DSL中使用Cache]: /docs/zh_cn/reference/pipeline/dsl_definition/3_cache.md
[artifact_pipeline]: /docs/zh_cn/reference/pipeline/dsl_definition/2_artifact.md
[Post-Fail-ref]: /docs/zh_cn/reference/pipeline/yaml_definition/4_failure_options_and_post_process.md
[failure_options_and_post_process_example]: /example/pipeline/failure_options_and_post_process_example
[DSL-Cache]: /docs/zh_cn/reference/pipeline/dsl_definition/3_cache.md
