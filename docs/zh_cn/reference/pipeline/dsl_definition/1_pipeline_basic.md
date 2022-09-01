本章主要介绍如何使用DSL来定义最基础的Pipeline

# 1、pipeline示例
开发者除了可以通过编写[yaml定义规范][yaml定义规范]来定义pipeline外，Paddleflow Pipeline也提供Python DSL供开发者们通过写Python代码的方式来完成pipeline的定义。

下面是一个最基础的使用Python DSL编排的pipeline。

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/base_pipeline示例。
>
> 示例链接：[base_pipeline][base_pipeline]

```python3
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import PF_RUN_ID


def preprocess():
    """ data preprocess step
    """
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./base_pipeline/data/{PF_RUN_ID}"},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        command="bash base_pipeline/shells/data.sh {{data_path}}"
    )
    return step
    
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash base_pipeline/shells/train.sh {{epoch}} {{train_data}} {{model_path}}",
        parameters={
            "epoch": epoch,
            "model_path": f"./output/{PF_RUN_ID}",
            "train_data": train_data
        }
    )
    return step
    
def validate(model_path):
    """ validate step
    """ 
    step = ContainerStep(
        name="validate",
        command="bash base_pipeline/shells/validate.sh {{model_path}}",
        parameters={"model_path": model_path}
    )    
    return step

@Pipeline(name="base_pipeline", docker_env="nginx:1.7.9", parallelism=1)
def base_pipeline(epoch=5):
    """ base pipeline
    """
    pre_step = preprocess()
    train_step = train(epoch, pre_step.parameters["data_path"])
    validate_step = validate(train_step.parameters["model_path"])
    

if __name__ == "__main__":
    ppl = base_pipeline()
    print(ppl.run(fs_name="ppl"))

```

> 在阅读本文档前，请确认已经安装了 Paddleflow SDK，并完成了相关配置。详情请点击[这里][SDK安装与配置]

# 2、导入DSL相关模块
与编写任何Python脚本一样，我们首先要导入将会使用到的模块、类、或者函数等。Python DSL提供的模块、类、函数等都可以通过paddleflow.pipeline模块完成导入，如上面的[示例](#1pipeline示例)所示：
```python3
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import PF_RUN_ID
```

# 3、定义Step
在Paddleflow Pipeline中，Step是运行Pipeline时最基本的调度单位，每一个Step都会执行一个指定的任务。

在定义Pipeline之前，首先需要完成Step的定义，在DSL中，我们实例化ContainerStep对象即可完成Step的定义。

如上面[示例中](#1pipeline示例)的`preprocess()`,`train()`,`validate()`函数所示, 在这三个函数中，都实例化了一个ContainerStep对象。为了方便，我们将`preprocess()`的函数代码抄录如下：

```python3
def preprocess():
    """ data preprocess step
    """
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./base_pipeline/data/{PF_RUN_ID}"},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        command="bash base_pipeline/shells/data.sh {{data_path}}"
    )
    return step
```

ContainerStep 初始化函数的主要参数说明如下：
|字段名称 | 字段类型 | 字段含义 | 是否必须 |备注 |
|:---:|:---:|:---:|:---:|:---:|
|name| string | Step的名字 | 是 |需要满足如下正则表达式： "^[a-zA-Z][a-zA-Z0-9-]{0,29}$" |
|command| string |Step需要执行的任务 | 否| | 
|docker_env| string |docker镜像地址 | 否 | |
|parameters| dict[str, Union[int, string, float, [Parameter][Parameter]]] | Step运行参数，在创建任务之前便需要确定其参数值 | 否 | |
|env| dict[str, str] | 节点运行任务时的环境变量 | 否 | |

> command, docker_env, parameter, env等字段的详细说明请点击[这里][节点字段]查看
> 
> 与ContainerStep相关的更多说明，可以点击[这里][ContainerStep]查看

# 4、定义Pipeline
在完成所有Step的定以后，便可以开始将这些Step有机的组装成一个pipeline。将Step组装成pipeline，可以分成以下三步：
- 实例化Pipeline对象
- 将Step实例添加至Pipeline实例中
- 指定Step实例间的依赖关系

接下来，我们将依次介绍这三个步骤。

## 4.1、实例化Pipeline对像
在将Step实例添加至Pipeline实例前，我们需要先实例化相关的Pipeline对象。这里需要特别注意的是，Pipeline是一个类装饰器，需要将起作为一个函数的装饰器去进行实例化，如上面的[示例](#1pipeline示例)所示：
```python3
@Pipeline(name="base_pipeline", docker_env="nginx:1.7.9", parallelism=1)
def base_pipeline(epoch=5):
    """ base pipeline
    """
    pre_step = preprocess()
    train_step = train(epoch, pre_step.parameters["data_path"])
    validate_step = validate(train_step.parameters["model_path"])
```

Pipeline实例化函数的主要参数说明如下：
|字段名称 | 字段类型 | 字段含义 |是否必须| 备注 |
|:---:|:---:|:---:|:---:|:---:|
|name| string| pipeline 的名字 |是| 需要满足如下正则表达式："^[A-Za-z_][A-Za-z0-9_]{0,49}$" |
|parallelism| int | pipeline 任务的并发数，即最大可以同时运行的节点任务数量 | 否 | | 
|docker_env| string| 各节点默认的docker镜像地址 | 否 | 如果Pipeline和Step均指定了docker_env, 则Step的docker_env具有更高的优先级 |

> 关于Pipeline实例化函数的详细说明，请点击[这里][Pipeline]

## 4.2、将Step实例添加至Pipeline实例中
在完成了Pipeline对象的实例化后, 接下来便需要将Step实例添加至Pipeline实例中，添加方式很简单：
- 只需在**pipeline函数**中完成Step的实例化即可。

> **pipeline函数**: 指被Pipeline实例装饰的函数。
> 
如在上面的[示例](#1pipeline示例)所示, 在pipeline函数 `base_pipeline()`中，依次调用了 `preprocess()`, `train()`, `validate()` 三个函数，而在这三个函数中，均完成了一个 Step对象的实例化。

因此，此时的Pipeline实例将会包含有三个Step实例，其名字依次为："preprocess"、"train"、"validate"。

## 4.3、指定Step实例间的依赖关系
在一个Pipeline实例中，可能包含多个Step实例，那么这么Step实例之间是否存在有某些关系呢？

答案是肯定的，在Paddleflow Pipeline中，Step实例之间可以存在如下的依赖关系:

- 流程依赖: 如果StepA需要在StepB之后运行，则称StepA在流程上依赖于StepB。
- Parameter依赖: 如果StepA的某个Parameter引用了StepB的某个Parameter，则称StepA在Parameter上依赖于StepB。
 
#### 流程依赖
定义节点间流程依赖的方式很简单，只需要调用Step实例的`after()`函数，便可以建立该Step实例与其余Step实例的流程依赖关系。示例如下：

```python3
stepA.after(stepB)
```

通过该语句，便定义了stepA与stepB之间的流程依赖关系：stepA在流程上依赖于stepB。
> 注意：Pipeline的所有Step实例需要组成一个有向无环图(DAG)结构，不支持存在有环的情况。

#### Parameter 依赖
在某些情况下，StepA 的某个Parameter["P1"]需要使用StepB的Parameter["P2"]的值，此时我们便可以定义Parameter参数依赖，定义方式也很简单，直接给StepA的参数Parameter["P1"]赋值为StepB的Parameter["P2"]的引用即可，示例代码如下：
    
```python3
StepA.parameters["P1"] = StepB.parameters["P2"]
```

在运行StepA时，Parameter["P1"]的值将会被替换为StepB的Parameter["P2"]的值，具体的替换逻辑可以参考[这里][变量模板与替换]
> 如果两个步骤间存在与Parameter依赖，则会隐含这两个步骤存在流程依赖，如上例中，则会隐含有stepA在流程上依赖于stepB
    
> 一些细心的用户应该早已发现，在上面的[pipeline示例](#1pipeline示例)中，存在有如下的参数依赖：
>- train_step的Parameter["train_data"] 依赖于preprocess_step的Parameter["data_path"]
>- validate_step的Parameter["model_path"]依赖于train_step的Parameter["model_path"]

## 5、创建pipeline任务
在完成了Pipeline的定义后，我们便可以使用该pipeline来发起任务了。发起pipeline任务也可以分成两步:
- 调用pipeline函数，得到完成了所有编排逻辑的Pipeline实例
- 调用Pipeline实例的run()函数，发起任务。
    
如在上面的[示例](#1pipeline示例)所示:
```python3
if __name__ == "__main__":
    ppl = base_pipeline()
    print(ppl.run(fs_name="ppl"))   
```


[yaml定义规范]: /docs/zh_cn/reference/pipeline/yaml_definition
[base_pipeline]: /example/pipeline/base_pipeline
[SDK安装与配置]: /docs/zh_cn/deployment/how_to_install_paddleflow.md#1%E5%AE%A2%E6%88%B7%E7%AB%AF
[节点字段]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#22-节点字段
[变量模板与替换]: /docs/zh_cn/reference/pipeline/yaml_definition/1_pipeline_basic.md#31-变量模板与替换
[DSL-Artifact]: /docs/zh_cn/reference/pipeline/dsl_definition/2_artifact.md
[DSL-Cache]: /docs/zh_cn/reference/pipeline/dsl_definition/3_cache.md
[PostProcess-And-FailureOpitons]: /docs/zh_cn/reference/pipeline/dsl_definition/4_failure_options_and_post_process.md
[DSL接口文档]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md
[Parameter]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#5Parameter
[ContainerStep]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#2ContainerStep
[Pipeline]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#1Pipeline
