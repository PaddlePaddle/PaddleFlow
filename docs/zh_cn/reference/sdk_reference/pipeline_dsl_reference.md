# Paddleflow Pipeline Python DSL 接口文档
本文档主要介绍Paddleflow Pipeline Python DSL的相关接口， 开发者可以参考本说明结合自身需求进行使用。关于Paddleflow Pipeline的介绍以及使用请参考[这里][Paddleflow Pipeline Overview]


## 1、Pipeline
### 1.1、Pipeline初始化
```python3
@Pipeline(name="base_pipeline", docker_env="nginx:1.7.9", parallelism=1)
def base_pipeline(data_path, epoch, model_path):
    preprocess_step = preprocess(data_path)

    train_step = train(epoch, model_path, preprocess_step.parameters["data_path"])
    train_step.after(preprocess_step)

    validate_step = validate(train_step.parameters["model_path"])

if __name__ == "__main__":
    ppl = base_pipeline(data_path=f"./base_pipeline/data/{PF_RUN_ID}", epoch=5, model_path=f"./output/{PF_RUN_ID}")
```

> Pipeline是一个类装饰器，开发者需要用其对编排pipeline的函数进行装饰

#### 参数说明

|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
|name| string | pipeline的名字 | 是 | 需要满足如下正则表达式： "^[A-Za-z_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$" |
|parallelism| string | pipeline 任务的并发数，即最大可以同时运行的节点任务数量 | 否 | | 
|docker_env| string | 各节点默认的docker 镜像地址 | 否 | |
|env| dict[str, str] | 各节点运行任务时的环境变量 | 否 | |
|cache_options| [CacheOptions](#5CacheOptions) | Pipeline 级别的Cache配置 | 否 | 关于Cache机制的相关介绍，请点击[这里][Cache] |
|failure_options| [FailureOptions](#6FailureOptions) |failure_options配置 | 否 |关于failure_options的相关介绍，请点击[这里][failure_options]  |

> 注意: 有部分参数，在Pipeline和[Step](#2ContainerStep)中都可以进行设置，在运行时，哪一个参数值才会生效？相关说明如下：
> - docker_env: 如果**Step.docker_env**有值，则使用**Step.docker_env**的值，否则使用**Pipeline.docker_env**的值, 如果**Step.docker_env**和**Pipeline.docker_env**均无值，则会报错
> - cache_opitons: 如果**Step.cache_options**有值，则使用**Step.cache_options**的值，否则使用**Pipeline.cache_options**的值, 如果 **Step.docker_env**，**Pipeline.docker_env**均无值，则默认不使用Cache机制
> - env: 采用合并机制: 在运行时，Step的环境变量即包含了**Step.env**属性中指定环境变量，也包含了**Pipeline.env**中包含的环境变量，如果有同名的环境变量，则使用**Step.env**定义的参数值


#### 返回值说明
Pipeline的一个实例


### 1.2、设置post_process节点
```python3
# 这里的send_mail_step()函数需要返回一个ContainerStep实例
ppl.set_post_process(send_mail_step("paddleflow@pipeline.com"))
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| step | [ContainerStep](#2ContainerStep) | post_process阶段运行的Step | 是 | |

#### 返回值说明
无返回值


### 1.3、获取post_process节点 
```python3
ppl.get_post_process()
```

#### 参数说明：
无参数

#### 返回值说明：
一个在[post_process][post_process]阶段运行的[ContainerStep](#2ContainerStep)实例


### 1.4、发起pipeline任务：
```python3
ppl.run(fsname="your_fs_name")
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
|config| string|配置文件路径| 否 | 配置文件的内容请参考[这里][config_content] |
|fsname| string |共享存储名称 | 否 | 如果有使用共享存储，则必须填写该参数| 
|username| string |指定用户，用于root账号运行特定用户的fs的工作流 | 否 | |
|runname| string |工作流名称| 否 | |
|desc| string 工作流描述| 否 | |
|disabled| List[string]|本次运行需要disabled的步骤| 否 | |


#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response|-| 失败返回失败message，成功返回runid


### 1.5、编译Pipeline
```python3
ppl.compile("run.yaml")
```

#### 参数说明：
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| save_path | string | 保存编译产出的文件路径，文件内容可以参考[这里][yaml_definition] | 否 | 文件后缀需要是 [".yaml", ".json", ".yml"]之一 |

#### 返回值说明：
一个包含编译产出的所有信息的Dict。其内容可以参考[这里][yaml_definition]


### 1.6、获取Pipeline中所有节点的拓扑序
```python3
ppl.topological_sort()
```

#### 参数说明
无参数

#### 返回值说明
一个存储Step实例的List，其中的Step按照拓扑序排列。如果Pipeline有设置post_process，则拓扑序中也会包含有post_process中的Step


### 1.7、获取所有Step的 parameter
```python3
ppl.get_params()
```

#### 参数说明
无参数

#### 返回值说明
一个Dict，其key为Step的名字，value为对应的Step所拥有Parameters信息。一个示例如下：
```python3
{
    "step1": {"param_a": 1},
     "step2": {"param_a": 2},
}
```


### 1.8、获取所有的Step(不包含post_process节点)
```python3
ppl.steps()
```

#### 参数说明
无参数

#### 返回值说明
一个Dict，其key为Step的名字，value为Step实例


### 1.9、获取pipeline名字
```python3
ppl.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表示pipeline的名字


### 1.10、设置pipeline名字
```python3
ppl.name = "exp_ppl"
```

#### 参数说明
无参数

#### 返回值说明
无返回值


### 1.11、获取环境变量
```python3
ppl.env
```

#### 参数说明
无参数

#### 返回值说明
一个dict，包含了所有pipeline级别的环境变量信息


### 1.12、添加环境变量
```python3
ppl.add_env({"env1": "env1"})
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| env | dict[str, str] | 需要新增的环境变量 | 是 | | 

#### 返回值说明
无返回值

## 2、ContainerStep
### 初始化
```python3
step = ContainerStep(
        name="preprocess",
        parameters={"data_path": data_path},
        command="bash base_pipeline/shells/data.sh {{data_path}}",
        docker_env="nginx:1.7.9",
        env={
            "USER_ABC": f"123_{PF_USER_NAME}",
            "PF_JOB_TYPE": "vcjob",
            "PF_JOB_QUEUE_NAME": "ppl-queue",
            "PF_JOB_MODE": "Pod",
            "PF_JOB_FLAVOUR": "flavour1",
        },
        )
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
|name| string | step 的名字 | 是 | 需要满足如下正则表达式： "^[A-Za-z][A-Za-z0-9-]{1,250}[A-Za-z0-9-]$" |
|command|string | 需要执行的命令 | 否 | |
|docker_env| string  | docker 镜像地址 | 否 | |
|inputs|dict[string, [Aritfact](#3Artifact)] | 输入artifact信息 | 否 | key将会作为artifact的名字，value需要是其余的节点输出artifact|
|outputs|dict[string, [Artifact](#3Artifact)] | 输出artifact信息 | 否 | key将会作为artifact的名字, value必须是Artifact()|
|parameters|dict[string, Union[string, int, float, [Parameter](#4Parameter)]] | parameter信息 |  否 | key将作为parameter的名字，value即为parameter的默认值|
|env| dict[str, str] | 节点运行任务时的环境变量 |  否 | |
|cache_options| [CacheOptions](#5CacheOptions) |  Cache 配置 | 否 |关于Cache机制的相关介绍，请点击[这里][Cache] |

> 注意1：inputs, outputs, parameters 中的key不可以同名

> 注意2: 有部分参数，在 Pipeline(#Pipeline) 和[Step]中都可以进行设置，在运行时，哪一个参数值才会生效？相关说明如下：
> -  docker_env: 如果**Step.docker_env**有值，则使用**Step.docker_env**的值，否则使用**Pipeline.docker_env**的值, 如果**Step.docker_env**，**Pipeline.docker_env**均无值，则会报错
> - cache_opitons: 如果**Step.cache_options**有值，则使用**Step.cache_options**的值，否则使用**Pipeline.cache_options**的值, 如果 **Step.docker_env**，**Pipeline.docker_env**均无值，则默认不使用Cache机制
> - env: 采用合并机制: 在运行时，Step的环境变量即包含了**Step.env**属性中指定环境变量，也包含了**Pipeline.env**中包含的环境变量，如果有同名的环境变量，则使用**Step.env**定义的参数值

#### 返回值说明
一个ContainerStep的实例


### 2.1、获取环境变量
```python3
step.env
```

#### 参数说明
无参数

#### 返回值说明
一个dict，包含了所有环境变量信息


### 2.2、添加环境变量
```python3
step.add_env({"env1": "env1"})
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| env | dict[str, str] | 需要新增的环境变量 | 是 |  | 

#### 返回值说明
无返回值


### 2.3、获取step名字
```python3
step.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表示step的名字


### 2.4、设置step名字
```python3
step.name = "step1"
```

#### 参数说明
无参数

#### 返回值说明
无返回值


### 2.5、获取输入artifact信息
```python3
step.inputs
```

#### 参数说明
无参数

#### 返回值说明
一个dict: 其中key为artifact的名字，value为该输入artifact的来源，为其余节点的输出artifact

### 2.6、获取输出artifact信息
```python3
step.outputs
```

#### 参数说明
无参数

#### 返回值说明
一个dict: 其中key为artifact的名字，value为[Artifact](#3Artifact)实例


### 2.7、获取parameter信息
```python3
params = ppl.parameters
```

#### 参数说明
无参数

#### 返回值说明
key 为parameter的名字，value为对应的Parameter实例


### 2.8、添加流程依赖
```python3
# 这里 step1，step2均为ContainerStep实例
step.after(step1, step2)
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| *upstream_steps | 可变参数，每一项均需要是Step实例 | 是 | 当前节点的上游节点，在运行时，会等所有上游节点运行成功才会运行当前节点| |

#### 返回值说明
Step实例本身


### 2.9、获取上游节点
```python3
step.upstream_steps()
```

#### 参数说明
无参数

#### 返回值
一个存储Step实例的List，其中的每一项均为当前节点的上游节点。

## 3、Artifact
### 初始化
```python3
art = Artifact()
```

#### 参数说明
无参数

#### 返回值
一个Artifact实例

### 3.1、获取当前的Artifact实例所属的step
```python3
art.step
```

#### 参数说明
无参数

#### 返回值说明
一个ContainerStep实例，当前的Artifact必然为其输出Artifact


### 3.2、获取输出artifact的名字
```python3
art.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表名该Artifact的名字


## 4、Parameter
### 初始化
```python3
pa = Parameter(10)
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| default | Union[string, int, float] | parameter的默认值 | 否 | |
| type | Enum["int", "float", "string"] | parameter的类型 | 否 | |

> 如果设置了**type**字段，则要求default的值与type指代的类型相同

#### 返回值说明
一个Parameter实例

### 4.1、获取当前的Parameter实例所属的step
```python3
step = pa.step
```

#### 参数说明
无参数

#### 返回值说明
一个ContainerStep实例


### 4.2、获取Parameter的名字
```python3
pa_name = pa.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表名该Parameter的名字

### 4.3、获取默认值
```python3
default_value = pa.default
```

#### 参数说明
无参数

#### 返回值说明
该Parameter的默认值


### 4.4、设置默认值
```python3
pa.default = "10"
```

#### 参数说明
无参数

#### 返回值说明
无返回值


### 4.5、获取Parameter的类型信息
```python3
pa_type = pa.type
```

#### 参数说明
无参数

#### 返回值说明
如果Parameter有设置type字段，则返回一个字符串，用于指代该parameter的类型，否则为None

## 5、CacheOptions
### 5.1、初始化
```python3
cache = CacheOptions(
    enable=True,
    max_expired_time=300,
    fs_scope="cache_example/shells/data_artifact.sh"
    )
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| enable | bool | 是否启用[cache][cache]功能 | 否 | |
| max_expired_time | int | cache缓存的有效期 | 否 | 为-1表示无限期 |
| fs_scope| string | 参与计算fingerprint的文件路径 | 否 | 详情请参考[这里][cache] |


#### 返回值说明
一个CacheOptions实例

## 6、FailureOptions
### 6.1、初始化
```python3
failure_options = FailureOptions("continue")
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| strategy | string | failure_options 策略 | 是 | 详情请参考[这里][failure_options] |

> 当前只支持两种策略：continue和fail_fast

#### 返回值说明
一个FailureOptions实例


## 7、系统变量
DSL也提供一些可以节点运行时获取的系统变量，见下表：

|字段名称 | 字段含义 |
|:---:|:---:|
|PF_RUN_ID|pipeline 任务的唯一标识符|
|PF_FS_ID|共享存储的唯一标识符|
|PF_FS_ID|共享存储的名字|
|PF_STEP_NAME|step的名字|
|PF_USER_NAME|发起本次pipeline任务的用户名|


[Paddleflow Pipeline Overview]: /docs/zh_cn/reference/pipeline/overview.md
[post_process]: /docs/zh_cn/reference/pipeline/yaml_definition/4_failure_options_and_post_process.md
[yaml_definition]: /docs/zh_cn/reference/pipeline/yaml_definition
[config_content]: /docs/zh_cn/reference/client_command_reference.md#配置文件说明
[cache]: /docs/zh_cn/reference/pipeline/yaml_definition/3_cache.md
[failure_options]: /docs/zh_cn/reference/pipeline/yaml_definition/4_failure_options_and_post_process.md
