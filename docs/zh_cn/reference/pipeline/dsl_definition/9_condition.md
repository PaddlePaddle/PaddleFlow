本章节主要介绍如何使用DSL来定义condition特性，关于conditiond特性的详情，请点击[这里][condition_yaml]

# 1、pipeline示例
下面为一个使用了condition特性的示例Pipeline定义

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/condition_example示例。
> 
> 示例链接：[condition_example]

```python3
def step1(num):
    """ step with condition
    """
    step = ContainerStep(
        name="step1",
        parameters={"num": num}
        )
    step.command = f"echo {step.parameters['num']}"
    step.condition = f"{step.parameters['num']} > 0"
    return step

def step2(num):
    """ step with condition
    """
    step = ContainerStep(
        name="step2",
        parameters={"num": num}
        )
    step.command = f"echo {step.parameters['num']}"
    step.condition = f"{step.parameters['num']} < 0"
    return step
    

@Pipeline(name="condition_example", docker_env="nginx:1.7.9", parallelism=2)
def condition_example(num=10):
    """ pipeline example for artifact
    """
    step1(num)
    step2(num)
    

if __name__ == "__main__":
    ppl = condition_example()
    ppl.compile("dag.yaml")
    print(ppl.run())
```

# 2、定义condition特性
在DSL中，使用condition特性的方式非常简单，在定义节点(DAG节点和Step节点均可以)时，直接访问其condition属性即可。

如上例中的节点`step1`所示:
```python3
step.condition = f"{step.parameters['num']} > 0"
```

需要注意的是，节点condition属性需要是一个字符串形式的条件表达式。

如果在节点的condition属性中，引用某些节点的parameter，或者artifact，则在调度该节点时，会将其替换成对应的parameter和artifact的真实值，具体的替换逻辑请参考[这里][condition_replace]

[condition_example]: /example/pipeline/condition_example
[condition_yaml]: /docs/zh_cn/reference/pipeline/yaml_definition/10_condition.md
[condition_replace]: /docs/zh_cn/reference/pipeline/yaml_definition/10_condition.md#3-pipeline运行流程