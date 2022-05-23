# PaddleFlow Pipeline Python DSL 接口文档
本文档主要介绍 PaddleFlow Pipeline Python DSL 的相关接口， 开发者可以参考本说明结合自身需求进行使用。关于 PaddleFlow Pipeline 的介绍以及使用请参考[这里][PaddleFlow Pipeline Overview]

## Pipeline
### Pipeline 初始化
#### 示例
```python3
@Pipeline(name="base_pipeline", docker_env="registry.baidubce.com/pipeline/nginx:1.7.9", parallelism=1)
def base_pipeline(data_path, epoch, model_path):
    preprocess_step = preprocess(data_path)

    train_step = train(epoch, model_path, preprocess_step.parameters["data_path"])
    train_step.after(preprocess_step)

    validate_step = validate(train_step.parameters["model_path"])

if __name__ == "__main__":
    ppl = base_pipeline(data_path=f"./base_pipeline/data/{PF_RUN_ID}", epoch=5, model_path=f"./output/{PF_RUN_ID}")
```

> Pipeline 是一个类装饰器，开发者需要用其对编排 pipeline 的函数进行装饰

#### 参数说明

|字段名称 | 字段类型 | 字段含义 | 备注 |
|:---:|:---:|:---:|:---:|
|name| string (required)| pipeline 的名字 | 需要满足如下正则表达式： "^[A-Za-z_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$ |
|parallelism| string (optional) | pipeline 任务的并发数，即最大可以同时运行的节点任务数量 | | 
|docker_env| string (optional) | 各节点默认的docker 镜像地址 | 参考下方表格 |
|env| dict[str, str] (optional) | 各节点运行任务时的环境变量 | 参考下方表格 |
|cache_options| [CacheOptions](#CacheOptions) (optional)| Pipeline 级别的 Cache 配置 | 关于Cache机制的相关介绍，请点击[这里](Cache机制) |
|failure_options| [FailureOptions](#FailureOptions) (optional) |failure options 配置 | 关于failure options的相关介绍，请点击[这里](Cache机制)  |

> 注意: 有部分参数，在 Pipeline 和 [Step](#Step) 中都可以进行设置，在运行时，哪一个参数值才会生效？ 相关说明如下：
> -  docker_env : 如果 **Step.docker_env** 有值，则使用 **Step.docker_env** 的值，否则使用 **Pipeline.docker_env** 的值, 如果 **Step.docker_env**， **Pipeline.docker_env** 均无值，则会报错
> - cache_opitons: 如果 **Step.cache_options** 有值，则使用 **Step.cache_options** 的值，否则使用 **Pipeline.cache_options** 的值, 如果 **Step.docker_env**， **Pipeline.docker_env** 均无值，则默认不使用 Cache 机制
> - env: 采用合并机制: 在运行时， Step 的环境变量即包含了 **Step.env** 属性中指定环境变量，也包含了 **Pipeline.env** 中包含的环境变量， 如果有同名的环境变量，则使用 **Step.env** 定义的参数值


#### 返回值说明
Pipeline 的一个实例

### set_post_process 
用于设置 [PostProcess](PostProcess) 节点，
### add_env
用户在完成 Pipeline 的初始化后，给 Pipeline 实例增加新的环境变量，示例如下:
```python3
ppl.add_env({"env1": "env1"})
```

#### 参数值说明
|字段名称 | 字段类型 | 字段含义 | 备注 |
|:---:|:---:|:---:|:---:|
| env | dict[str, str] (required) | 需要新增的环境变量 | | 

#### 返回值
无返回值


## CacheOptions

## FailureOptions


[PaddleFlow Pipeline Overview]: /docs/zh_cn/reference/pipeline/overview.md
[PostProcess]:
