在本章节中，我们将介绍在DSL中如何定义循环结构。关于循环结构的详情请参考[这里][loop_yaml]。

# 1、pipeline示例
下面为一个定义了循环结构的示例Pipeline：

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/loop_example示例。
> 
> 示例链接：[loop_example][loop_example]

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
        sub_path="loop_example/" + sub_path,
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

def process(nums):
    step = ContainerStep(
        name="process",
        loop_argument=nums,
        outputs={"result": Artifact()},
    )
    step.command = f"cd /process && python3 process.py {step.loop_argument.item}"
    
    extra_fs = generate_extra_fs("process")
    step.extra_fs = [extra_fs]
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

@Pipeline(name="loop_example", docker_env="python:3.7", parallelism=2)
def loop_example(lower=-10, upper=10, num=10):
    """ pipeline example for artifact
    """
    randint_step = randint(lower, upper, num)
    process_step = process(randint_step.outputs["random_num"])
    sum(process_step.outputs["result"])
    
    
if __name__ == "__main__":
    ppl = loop_example()
    main_fs = MainFS(name="ppl")
    ppl.fs_options = FSOptions(main_fs)
    
    ppl.compile("loop.yaml")
    print(ppl.run())
```

# 2、定义循环结构
在DSL中，定义循环结构的方式非常简单，只需要在节点时(Step类型节点和DAG类型节点均可)时，给loop_argument进行赋值即可。

如上面示例中的`process`节点所示：
```python3
def process(nums):
    step = ContainerStep(
        name="process",
        loop_argument=nums,
        outputs={"result": Artifact()},
    )
    ... ...
```

在运行时，会对loop_argument进行遍历，对于其中的每一项，都会调度执行一次当前的节点。更多的信息请参考[这里][loop_yaml]。

loop_argument可以是如下的类型：
- list
```python3
step.loop_argument=[1,2,3]
```

- json list
```python3
step.loop_argument=json.dumps([1,2,3])
```

- 本节点，祖先节点，以及祖先节点的兄弟节点的parameter
```python3
step1.loop_argument = step2.parameters["data"] 
```

- 本节点的输入artifact
```python3
step1.loop_argument = step1.inputs["data"]
```

- 其余节点的输出artifact
```python3
step1.loop_argument = step2.outputs["data"]
```

- 父节点的循环参数当次遍历值
```python3
with DAG(name="dag1", loop_argument=[[1,2,3], [1,2,3]]) as dag1:
    step1 = ContainerStep(name="step1", loop_argument=dag1.loop_argument.item)
```

# 3、获取当次遍历的循环参数值
对于定义了循环结构的节点，在运行，其会对循环参数进行遍历，对于其中的每一项，都会调度执行一次当前节点。

那么在该节点执行时，如何获取循环参数在当次运行的值呢？
- 在DSL中，节点的command以及其对应的env中，均可以直接引用其本身loop_arguments属性的item属性
  - 如在上面的示例中，节点`process`的command所示：
```python3
step.command = f"cd /process && python3 process.py {step.loop_argument.item}"
```

- 在节点运行时，其将会被替换为循环参数在当次遍历的值
  - 还是以上面示例中的节点`process`为例，假设loop_argument的值为[1, 2, 3]
  - 则节点`process`将会执行三次，其command分别为：
    - cd /process && python3 process.py 1
    - cd /process && python3 process.py 2
    - cd /process && python3 process.py 3

[loop_yaml]: /docs/zh_cn/reference/pipeline/yaml_definition/9_loop.md
[loop_example]: /example/pipeline/loop_example