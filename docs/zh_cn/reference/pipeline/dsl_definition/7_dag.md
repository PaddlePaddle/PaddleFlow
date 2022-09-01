在本章节中，将会介绍如何使用DSL来定义DAG。 关于DAG的使用场景等详细介绍，请参考[这里][dag_yaml]

# 1、 pipeline示例
下面为一个定义了DAG节点的示例Pipeline定义

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/dag_example 示例。
> 
> 示例链接：[dag_example]
```python3
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import DAG
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions

def generate_extra_fs(sub_path):
    """ generate ExtraFS
    """
    return ExtraFS(
        name="ppl",
        sub_path="dag_example/" + sub_path,
        mount_path="/" + sub_path,
        read_only=True
    )

def randint(lower, upper, num):
    """ randint step
    """
    extra_fs = generate_extra_fs("randint")
    
    step = ContainerStep(
        name="randint",
        parameters={
            "lower": lower,
            "upper": upper,
            "num": num
        },
        outputs={"random_num": Artifact()},
        extra_fs=[extra_fs],
        command="cd /randint && python3 randint.py {{num}} {{lower}} {{upper}}"
    )
    
    return step


def split(nums, threshold):
    """ split data
    """
    step = ContainerStep(
        name="split",
        command="cd /split && python3 split.py {{threshold}}",
        outputs={
            "negetive": Artifact(),
            "positive": Artifact(),
        },
        inputs={
            "nums": nums,
        },
        parameters={
            "threshold": threshold,
        }
    )
    
    extra_fs = generate_extra_fs("split")
    step.extra_fs = [extra_fs]
    return step
    
def process_data(polarity, data):
    """ process data
    """
    step = ContainerStep(
        name=f"process-{polarity}",
        command=f"cd /process_{polarity} && python3 process_{polarity}.py",
        inputs={
            polarity: data
        },
        outputs={"result": Artifact()},
    )
    extra_fs = generate_extra_fs(f"process_{polarity}")
    step.extra_fs=[extra_fs]
    return step
    
def collector(negetive, positive):
    """ collector
    """
    step = ContainerStep(
        name="collector",
        command="cd /collector && python3 collect.py",
        inputs={
            "negetive": negetive,
            "positive": positive,
        },
        outputs={
            "collection": Artifact()
        },
    )
    
    extra_fs = generate_extra_fs(f"collector")
    step.extra_fs=[extra_fs]
    return step
        
    
def sum(nums):
    """ sum
    """
    step = ContainerStep(
        name="sum",
        command="cd /sum && python3 sum.py",
        inputs={
            "nums": nums,
        },
        outputs={
            "result": Artifact()
        },
    )
    
    extra_fs = generate_extra_fs(f"sum")
    step.extra_fs=[extra_fs]
    return step


@Pipeline(name="dag_example", docker_env="python:3.7", parallelism=2)
def dag_example(lower=-10, upper=10, num=10):
    """ pipeline example for artifact
    """
    randint_step = randint(lower, upper, num)
    
    with DAG(name="process"):
        split_step = split(randint_step.outputs["random_num"], 0)
        process_negetive = process_data("negetive", split_step.outputs["negetive"])
        process_positive = process_data("positive", split_step.outputs["positive"])
        collector_step = collector(process_negetive.outputs["result"], process_positive.outputs["result"])
    
    sum(collector_step.outputs["collection"])
    
    
if __name__ == "__main__":
    ppl = dag_example()
    main_fs = MainFS(name="ppl")
    ppl.fs_options = FSOptions(main_fs)
    
    print(ppl.run())
```

# 2、定义DAG
与将节点组装成Pipeline一样，将多个节点组装成一个DAG也可以分成如下三步：
1. 创建DAG实例
2. 将节点加入DAG中
3. 指定节点之间的依赖关系
   
> PS: 这里的节点，既可以是Step节点，也可以是DAG节点

接下来将依次介绍这三个步骤。

## 2.1 创建DAG实例
DAG实例的创建方式与使用python语言创建其余类型的实例并无二致，直接调用[DAG][DAG]的实例化函数即可，如下所示：
```python3
dag = DAG(name="process")
```

## 2.2 将节点加入DAG中
与将节点加入Pipeline中有些许相似，如果要将某个节点加入DAG中，需要在DAG的with语句的上下文中完成相关节点的实例化。

如在[pipeline示例](#1pipeline示例)中，便通过如下的代码，将节点`split_step`, `process_negetive`, `process_positive`, `collector_step` 加入了名为`process`的DAG节点中：
```python3
    with DAG(name="process"):
        split_step = split(randint_step.outputs["random_num"], 0)
        process_negetive = process_data("negetive", split_step.outputs["negetive"])
        process_positive = process_data("positive", split_step.outputs["positive"])
        collector_step = collector(process_negetive.outputs["result"], process_positive.outputs["result"])
```

在DSL中，支持多个DAG实例的嵌套，示例如下：
```python3
with DAG(name="dag1") as dag1:
    step1 = new_step("step1")
    with DAG(name="dag2") as dag2:
        step2 = new_step("step2")
```
在上面的示例中，DAG节点`dag1`有如下的两个子节点：
- Step类型的节点`step1`
- DAG类型节点`dag2`, 其有一个Step类型子节点`step2`

## 3.3 指定节点之间的依赖关系
对于DAG节点，在讨论节点间的依赖关系时，需要考虑分如下两种情况来进行讨论：
- DAG的子节点间依赖关系
- DAG节点与其兄弟节点间的依赖关系

其中，DAG的子节点间依赖关系定义方式与[pipeline中指定节点依赖关系][pipeline中指定节点依赖关系]的方式并无二致，因此，这里只讨论如何定义DAG节点与其兄弟节点间的依赖关系。

DAG节点与其兄弟节点间的依赖关系, 与[pipeline中指定节点依赖关系][pipeline中指定节点依赖关系]的情况一样，也有**流程依赖**和**参数依赖**两种，接下来将依次讨论这两种关系。

### 3.3.1 流程依赖
定义DAG节点的流程依赖有如下两种方式：
- 调用DAG实例的after()函数
- 通过子节点的流程依赖来进行推断

#### 调用DAG实例的after()函数
该种方式的一个示例如下：
```python3
dag1.after(step1, dag2)
```

通过上面的代码, 便定义了如下的流程依赖：
节点`dag1`在流程上依赖于节点`step1`和`dag2`，即需要等待节点`step1`和`dag2`运行完成后，才会运行节点`dag1`

#### 通过子节点的流程依赖来进行推断
对于此种情况，需要再次细分成如下两种情况来进行讨论：
- 子节点依赖了DAG外的节点
- 子节点被DAG外的节点所依赖

##### 子节点依赖了DAG外的节点
我们通过如下示例进行说明：
```python3
step1 = new_step("step1")

with DAG(name="dag1") as dag1:
    step2 = new_step("step2")
    step2.after(step1)
```

在上面的示例中，通过代码指定`dag1`的子节点`step2`在流程上依赖于`dag1`的兄弟节点`step1`。

在编译时，DSL会将相关流程依赖关系更改为：
- 节点`dag1`在流程上依赖于`step1`

##### 子节点被DAG外的节点所依赖
我们通过如下示例进行说明：
```python3
with DAG(name="dag1") as dag1:
    step1 = new_step("step1")

step2 = new_step("step2")
step2.after(step1)
```

在上面的示例中，通过代码指定节点`step2`在流程上依赖了`dag1`的子节点`step1`。

在编译时，DSL会将依赖关系更改为：
- 节点`step2`在流程上依赖其兄弟节点`dag1`


### 3.3.2 参数依赖
与Step节点不同的是，DAG节点不支持用户显示定义Parameter和输入/输出artifact。

DAG节点的Parameter和输入/输出artifact属性将会由DSL根据其子节点的相关属性去进行推断。 在这里我们分如下四种情况来讨论具体的推断规则：
> 对于推断规则不感兴趣的同学可以略过本章节

#### 子节点的Parameter被DAG外的节点引用
DSL不支持这种方式的引用，如果在编译时发现有这种引用关系，将会直接抛出异常。

示例如下：
```python3
with DAG(name="dag1") as dag1:
    step1 = ContainerStep(name="step1", parameters={"p1": 10}

step2 = ContainerStep(name="step2", parameters={"p2": step1.parameters["p1"]}
)
```
在上面的示例中，由于节点`step2`引用了其兄弟节点`dag1`的子节点`step1`的parameter，因此，当你尝试使用其发起任务时，DSL将会抛出异常

>为什么不支持这种引用方式？
> - 因为当前Parameter只能由父节点传递给子节点，不支持由子节点传递给父节点

#### 子节点引用了DAG外的节点的Parameter
以如下的示例说明：
```
step1 = ContainerStep(name="step1", parameters={"p1": 10}
with DAG(name="dag1") as dag1:
    step2 = ContainerStep(name="step2", parameters={"p2": step1.parameters["p1"]}
)
```
在上面的示例中，代码中指定节点`step2`的parameter[p2]引用其父节点的兄弟节点`step1`的parameter[p1]。

在编译时，DSL会将该Parameter的引用关系更改为：
- 节点`dag1`增加一个引用了`step1`的parameter[p1]的Parameter，其名字为：dsl_param_${rand_code}
  - 其中${rand_code}为6位随机码
- 节点`step2`的parameter[p2]改为引用节点`dag1`新增的Parameter[dsl_param_${rand_code}]

#### 子节点的输出artifact被DAG外的节点引用
以如下的示例说明：
```
with DAG(name="dag1") as dag1:
    step1 = ContainerStep(name="step1", outputs={"art1": Aritfact()}

step2 = ContainerStep(name="step2", inputs={"art2": step1.outputs["art1"]}
)
```
在上面的示例中，通过代码指定节点`step2`的输入artifact["art2"]引用了其兄弟节点`dag1`的子节点`step1`的输出artifact['art1']。

在编译时，DSL会将该artifact的引用关系更改为：
- 节点`dag1`增加一个输出artifact， 其名为dsl_art_${rand_code}, 其value为其子节点`step1`的输出artifact['art1']的引用
- 节点`step2`的输入artifact["art2"]更改为引用节点`dag1`的输出artifact["dsl_art_${rand_code}"]

#### 子节点的输入artifact引用DAG外的节点的输出artifact
以如下的示例说明：
```
step1 = ContainerStep(name="step1", outputs={"art1": Aritfact()}
with DAG(name="dag1") as dag1:
    step2 = ContainerStep(name="step2", inputs={"art2": step1.outputs["art1"]}
)
```
在上面的示例中，通过代码指定节点`step2`的输入artifact["art2"]引用了其父节点`dag1`的兄弟节点`step1`的输出artifact['art1']。

在编译时，DSL会将该artifact的引用关系更改为：
- 节点`dag1`增加一个输入artifact， 其名为dsl_art_${rand_code}, 其value为其子节点`step1`的输出artifact['art1']的引用
- 节点`step2`的输入artifact["art2"]更改为引用节点`dag1`的输入artifact["dsl_art_${rand_code}"]



[DAG]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#3dag
[dag_yaml]: /docs/zh_cn/reference/pipeline/yaml_definition/7_dag.md
[pipeline中指定节点依赖关系]: /docs/zh_cn/reference/pipeline/dsl_definition/1_pipeline_basic.md#43%E6%8C%87%E5%AE%9Astep%E5%AE%9E%E4%BE%8B%E9%97%B4%E7%9A%84%E4%BE%9D%E8%B5%96%E5%85%B3%E7%B3%BB
[dag_example]: /example/pipeline/dag_example