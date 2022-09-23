本文档主要介绍Paddleflow Pipeline Python DSL的相关接口， 开发者可以参考本说明结合自身需求进行使用。关于Paddleflow Pipeline的介绍以及使用请参考[这里][Paddleflow Pipeline Overview]


# 1、Pipeline
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
|name| string | pipeline的名字 | 是 | 需要满足如下正则表达式： "^[A-Za-z_][A-Za-z0-9_]{0,49}$" |
|parallelism| int | pipeline 任务的并发数，即最大可以同时运行的节点任务数量 | 否 | | 
|docker_env| string | 各节点默认的docker镜像地址 | 否 | |
|env| dict[str, str] | 各节点运行任务时的环境变量 | 否 | |
|cache_options| [CacheOptions](#9CacheOptions) | Pipeline 级别的Cache配置 | 否 | 关于Cache机制的相关介绍，请点击[这里][Cache] |
|failure_options| [FailureOptions](#10FailureOptions) |failure_options配置 | 否 |关于failure_options的相关介绍，请点击[这里][failure_options]  |
|fs_options| [FSOptions](#13FSOptions) | 关于fs_options的详细介绍，请点击[这里][multiple_fs]

> 注意: 在Pipeline和[ContainerStep](#2ContainerStep)中都可以进行设置 *env* 参数，在运行时，将采用合并机制: 
> - 在运行时，ContainerStep的环境变量即包含了**ContainerStep.env**属性中指定环境变量，也包含了**Pipeline.env**中包含的环境变量，如果有同名的环境变量，则使用**ContainerStep.env**定义的参数值


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
post_process = ppl.get_post_process()
```

#### 参数说明：
无参数

#### 返回值说明：
一个在[post_process][post_process]阶段运行的[ContainerStep](#2ContainerStep)实例


### 1.4、发起pipeline任务：
```python3
ppl.run(fs_name="your_fs_name", disabled=["a", "b.c"])
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
|config| string|配置文件路径| 否 | 配置文件的内容请参考[这里][config_content] |
|fs_name| string |共享存储名称 | 否 | 如果有使用共享存储，则必须填写该参数| 
|username| string |指定用户，用于root账号运行特定用户的fs的工作流 | 否 | |
|run_name| string |工作流名称| 否 | |
|desc| string 工作流描述| 否 | |
|disabled|List[string]|本次运行需要disabled的节点的全路径名称| 否 | <br>全路径名称: 由其所有祖先节点的名字以及当前节点的名字组合而成，以"."分割，祖先节点名在前</br><br>在上面的实例中，节点a, 以及节点b的子节点c将会被disable</br> |


#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response|-| 失败返回失败message，成功返回run_id


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


### 1.6、获取所有节点(不包含post_process节点)
```python3
ppl.entry_points
```

#### 参数说明
无参数

#### 返回值说明
一个Dict，其key为节点的名字，value为节点实例


### 1.7、获取pipeline名字
```python3
ppl.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表示pipeline的名字


### 1.8、设置pipeline名字
```python3
ppl.name = "exp_ppl"
```

#### 参数说明
无参数

#### 返回值说明
无返回值


### 1.9、获取环境变量
```python3
ppl.env
```

#### 参数说明
无参数

#### 返回值说明
一个dict，包含了所有pipeline级别的环境变量信息


### 1.10、添加环境变量
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
### 2.1 初始化
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
|name| string | step 的名字 | 是 | 需要满足如下正则表达式： "^[a-zA-Z][a-zA-Z0-9-]{0,29}$" |
|command|string | 需要执行的命令 | 否 | |
|docker_env| string  | docker 镜像地址 | 否 | |
|inputs|dict[string, [Aritfact](#4Artifact)] | 输入artifact信息 | 否 | key将会作为artifact的名字，value需要是其余的节点输出artifact|
|outputs|dict[string, [Artifact](#4Artifact)] | 输出artifact信息 | 否 | key将会作为artifact的名字, value必须是Artifact()|
|parameters|dict[string, Union[string, int, float, [Parameter](#5Parameter)]] | parameter信息 |  否 | key将作为parameter的名字，value即为parameter的默认值|
|env| dict[str, str] | 节点运行任务时的环境变量 |  否 | |
|cache_options| [CacheOptions](#9CacheOptions) |  Cache 配置 | 否 |关于Cache机制的相关介绍，请点击[这里][Cache] |
|contidion|string|用于在Pipeline任务运行时决定是否运行当前步骤的条件判断式|否|关于condition的详细信息，请参考[这里][condition] |
|loop_argument|Union[List, Parameter, Artifact, string, [_LoopItem](#7_LoopItem)] | 循环参数，如果有值，在运行时，会对该字段进行遍历，对于其中的每一项，都会调度执行一次当前节点 | 否 | 更多信息，请参考[loop_argument][loop]
|extra_fs| List[[ExtraFS](#11ExtraFS)] | 节点运行时需要挂载的共享存储的相关信息 | 更多信息，请参考[这里][extra_fs]

> 注意1：inputs, outputs, parameters 中的key不可以同名

> 注意2: 在[Pipeline](#1Pipeline)和ContainerStep中都可以进行设置 *env* 参数，在运行时，将采用合并机制: 
> - 在运行时，Step的环境变量即包含了**ContainerStep.env**属性中指定环境变量，也包含了**Pipeline.env**中包含的环境变量，如果有同名的环境变量，则使用**ContainerStep.env**定义的参数值

#### 返回值说明
一个ContainerStep的实例


### 2.2、获取环境变量
```python3
step.env
```

#### 参数说明
无参数

#### 返回值说明
一个dict，包含了所有环境变量信息


### 2.3、添加环境变量
```python3
step.add_env({"env1": "env1"})
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| env | dict[str, str] | 需要新增的环境变量 | 是 |  | 

#### 返回值说明
无返回值


### 2.4、获取step名字
```python3
step.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表示step的名字


### 2.5、设置step名字
```python3
step.name = "step1"
```

#### 参数说明
无参数

#### 返回值说明
无返回值


### 2.6、获取输入artifact信息
```python3
step.inputs
```

#### 参数说明
无参数

#### 返回值说明
一个dict: 其中key为artifact的名字，value为value为[Artifact](#4Artifact)实例

### 2.7、获取输出artifact信息
```python3
step.outputs
```

#### 参数说明
无参数

#### 返回值说明
一个dict: 其中key为artifact的名字，value为[Artifact](#4Artifact)实例


### 2.8、获取parameter信息
```python3
params = ppl.parameters
```

#### 参数说明
无参数

#### 返回值说明
key 为parameter的名字，value为对应的[Parameter](#5parameter)实例


### 2.9、添加流程依赖
```python3
step.after(step1, step2)
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| *upstream |List[Union[ContainerStep, DAG]] |  可变参数，每一项均需要是一个ContainerStep或者DAG实例 | 是 | 当前节点的上游节点，在运行时，会等所有上游节点运行成功才会运行当前节点| |

#### 返回值说明
ContainerStep实例本身


### 2.10、获取loop_argument
```python3
step.loop_argument
```

#### 参数说明
无参数

#### 返回值
用于表征当前节点的循环参数参数的[_LoopArgument](#6_LoopArgument)的实例

### 2.11、获取condition
```python3
step.condition
```

#### 参数说明
无参数

#### 返回值
一个string，表示当前节点的condition


### 2.12 获取节点全路径名
> 全路径名称: 由其所有祖先节点的名字以及当前节点拼凑而成，以"."分割，祖先节点名在前

```python3
step.full_name
```

#### 参数说明
无参数

#### 返回值
一个string，表示当前节点的全路径名

> 注意：
> - Pipeline中所有节点都会有一个名为 **PF-ENTRY-POINT**的祖先节点

## 3、DAG
### 3.1、初始化
```python3
dag = DAG(name="dag")
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
|name| string | dag 的名字 | 是 | 需要满足如下正则表达式： "^[a-zA-Z][a-zA-Z0-9-]{0,29}$" |
|contidion|string|用于在Pipeline任务运行时决定是否运行当前步骤的条件判断式|否|关于condition的详细信息，请参考[这里][condition] |
|loop_argument|Union[List, Parameter, Artifact, string, [_LoopItem](#7_LoopItem) | 循环参数，如果有值，在运行时，会对该字段进行遍历，对于其中的每一项，都会调度执行一次当前节点 | 否|  更多信息，请参考[loop_argument][loop] | 


#### 返回值
一个DAG实例

### 3.2、获取dag名字
```python3
dag.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表示dag的名字


### 3.2、设置dag名字
```python3
dag.name = "dag"
```

#### 参数说明
无参数

#### 返回值说明
无返回值


### 3.3、添加流程依赖
```python3
dag.after(step1, dag2)
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| *upstream | List[Union[ContainerStep, DAG]]|可变参数，每一项均需要是一个ContainerStep或者DAG实例 | 是 | 当前节点的上游节点，在运行时，会等所有上游节点运行成功才会运行当前节点|

#### 返回值说明
DAG实例本身

### 3.4、获取loop_argument
```python3
dag.loop_argument
```

#### 参数说明
无参数

#### 返回值
用于表征当前节点的循环参数参数的[_LoopArgument](#6_loop_argument)的实例

### 3.5、获取condition
```python3
dag.condition
```

#### 参数说明
无参数

#### 返回值
当前节点的condition字段的值

### 3.6 获取节点全路径名
> 全路径名称: 由其所有祖先节点的名字以及当前节点拼凑而成，以"."分割，祖先节点名在前

```python3
dag.full_name
```

#### 参数说明
无参数

#### 返回值
一个string，表示当前节点的全路径名

> 注意：
> - Pipeline中所有节点都会有一个名为**PF-ENTRY-POINT**的祖先节点


## 4、Artifact
### 4.1、初始化
```python3
art = Artifact()
```

#### 参数说明
无参数

#### 返回值
一个Artifact实例

### 4.2、获取当前的Artifact实例所属的节点
```python3
art.component
```

#### 参数说明
无参数

#### 返回值说明
一个DAG或者ContainerStep实例


### 4.3、获取artifact的名字
```python3
art.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表明该Artifact的名字


## 5、Parameter
### 5.1、初始化
```python3
pa = Parameter(10)
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| default | Union[string, int, float, list, [_LoopItem](#7_LoopItem)] | parameter的默认值 | 否 | |
| type | Enum["int", "float", "string". "list"] | parameter的类型 | 否 | |

> 如果设置了**type**字段，则要求default的值与type指代的类型相同

#### 返回值说明
一个Parameter实例

### 5.2、获取当前的Parameter实例所属的节点
```python3
step = pa.component
```

#### 参数说明
无参数

#### 返回值说明
一个DAG或者ContainerStep实例


### 5.3、获取Parameter的名字
```python3
pa_name = pa.name
```

#### 参数说明
无参数

#### 返回值说明
一个string, 表名该Parameter的名字

### 5.4、获取默认值
```python3
default_value = pa.default
```

#### 参数说明
无参数

#### 返回值说明
该Parameter的默认值

### 5.5、设置默认值
```python3
pa.default = "10"
```

#### 参数说明
无参数

#### 返回值说明
无返回值

### 5.6、获取Parameter的类型信息
```python3
pa_type = pa.type
```

#### 参数说明
无参数

#### 返回值说明
如果Parameter有设置type字段，则返回一个字符串，用于指代该Parameter的类型，否则为None

## 6、_LoopArgument
### 6.1 初始化
用户不应该显示的初始化_LoopArgument实例，而是应该在定义节点时，通过给其loop_argument属性赋值来间接的实例化_LoopArgument对象。

如下所示

```python3
step = ContainerStep(
        name="step",
        loop_argument=[1,2,3],
        command="echo {{PF_LOOP_ARGUMENT}}"
        )
```

此时，step.loop_argument 即为一个_LoopArgument的实例

### 6.2、 获取当次遍历循环参数的值
```python3
step.loop_argument.item
```

#### 参数说明
无参数

#### 返回值说明
一个[_LoopItem](#7_loopitem)实例，用于指代在节点运行时，当次运行所对应的循环参数的值

> 更多信息可以参考[这里][loop]

## 7、_LoopItem
### 7.1、初始化
用户不应该显示初始化_LoopItem实例, 实例化[_LoopArgument](#6_LoopArgument)时，会自动实例化一个_LoopItem实例, 如下所示:
```python3
step = ContainerStep(
        name="step",
        loop_argument=[1,2,3],
        command="echo {{PF_LOOP_ARGUMENT}}"
        )
```
此时step.loop_argument.item即为一个_LoopItem类型的实例

## 8、FSScope
### 8.1、 初始化
```python3
fs_scope = FSScope(name="ppl", path="/143")
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| name | string | [共享存储][共享存储]的名字 | 是 | - |
| path | string | [共享存储][共享存储]上需要参与[cache fingerprint][cache fingerprint]计算的路径 | 否 |<br>多个路径以","隔开</br><br>默认值为"/"</br>|

#### 返回值说明
一个FSScope实例


## 9、CacheOptions
### 9.1、初始化
```python3
fs_scope = FSScope(name="ppl", path="/143")
cache = CacheOptions(
    enable=True,
    max_expired_time=300,
    fs_scope=[fs_scope]
    )
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| enable | bool | 是否启用[cache][cache]功能 | 否 | |
| max_expired_time | int | cache缓存的有效期 | 否 | 为-1表示无限期 |
| fs_scope| list[[FSScope](#8FSScope)] | 参与计算fingerprint的共享存储的信息 | 否 | 详情请参考[这里][cache] |


#### 返回值说明
一个CacheOptions实例

## 10、FailureOptions
### 10.1、初始化
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


## 11、ExtraFS
### 11.1、初始化
```python3
extra_fs = ExtraFS("ppl", sub_path="condition_example")
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| name | string | [共享存储][共享存储]的名字 | 是 |  | 
| mount_path| string | 容器内的挂载路径 | 否 | |
| sub_path | string | [共享存储][共享存储]中需要被挂载的子路径 | 否 |  <br>如果没有配置，将会把整个[共享存储][共享存储]挂载至容器中</br><br>不能以"/"开头</br> |
| read_only | bool | 容器对mount_path的访问权限 | 否 |  |

#### 返回值说明
一个ExtraFS实例

## 12、MainFS
### 12.1、初始化
```python3
main_fs = MainFS("ppl", sub_path="output", mount_path="./test_dsl")
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| name | string | [共享存储][共享存储]的名字 | 是 |  | 
| mount_path| string | 容器内的挂载路径 | 否 | |
| sub_path | string | [共享存储][共享存储]中需要被挂载的子路径 | 否 |  <br>如果没有配置，将会把整个[共享存储][共享存储]挂载至容器中</br><br>不能以"/"开头</br><br><mark>如果该路径存在，则必须为目录</mark></br> |

#### 返回值说明
一个MainFS实例

## 13、FSOptions
### 13.1 初始化
```python3
extra_fs = ExtraFS("ppl", sub_path="output", mount_path="/home/output")
main_fs = MainFS("ppl", sub_path="output", mount_path="./test_dsl")
fs_options = FSOptions(main_fs=main_fs, extra_fs=[extra_fs])
```

#### 参数说明
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
| main_fs | [MainFS](#12mainfs) | 用于存储节点输出artifact的共享存储信息 | 否 | | 
| extra_fs | list[[ExtraFS](#11extrafs)] | 节点运行时需要挂载的共享存储信息 | 否 | |

## 14、系统变量
DSL也提供一些可以节点运行时获取的系统变量，见下表：

|字段名称 | 字段含义 |
|:---:|:---:|
|PF_RUN_ID|pipeline 任务的唯一标识符|
|PF_STEP_NAME|step的名字|
|PF_USER_NAME|发起本次pipeline任务的用户名|
|PF_LOOP_ARGUMENT|指代循环参数在当次运行的值|

[Paddleflow Pipeline Overview]: /docs/zh_cn/reference/pipeline/overview.md
[post_process]: /docs/zh_cn/reference/pipeline/yaml_definition/6_failure_options_and_post_process.md
[yaml_definition]: /docs/zh_cn/reference/pipeline/yaml_definition
[config_content]: /docs/zh_cn/reference/client_command_reference.md#配置文件说明
[cache]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md
[failure_options]: /docs/zh_cn/reference/pipeline/yaml_definition/6_failure_options_and_post_process.md
[multiple_fs]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md
[condition]: /docs/zh_cn/reference/pipeline/yaml_definition/10_condition.md
[loop]: /docs/zh_cn/reference/pipeline/yaml_definition/9_loop.md
[extra_fs]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md#21-%E8%8A%82%E7%82%B9%E7%BA%A7%E5%88%AB%E7%9A%84extra_fs%E5%AD%97%E6%AE%B5
[共享存储]: /docs/zh_cn/reference/filesystem
