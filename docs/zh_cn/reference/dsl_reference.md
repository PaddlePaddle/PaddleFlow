# PaddleFlow Pipeline Python DSL 接口文档
本文档主要介绍 PaddleFlow Pipeline Python DSL 的相关接口， 开发者可以参考本说明结合自身需求进行使用。关于 PaddleFlow Pipeline 的介绍以及使用请参考[这里](/docs/zh_cn/reference/pipeline/overview.md)

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

#### 参数说明

|字段名称 | 字段类型 | 字段含义 | 备注 |
|:---:|:---:|:---:|:---:|:---:|
|name| string (required)| pipeline 的名字 | 需要满足如下正则表达式： "^[A-Za-z_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$ |
|parallelism| string (optional) | pipeline 任务的并发数，即最大可以同时运行的节点任务数量 | | 
|docker_env| string (optional) | 各节点默认的docker 镜像地址 | |
|env| dict[str, str] (optional) | 各节点运行任务时的环境变量 | 如果节点有设置自身的环境变量，则会将其与 Pipeline 级别的环境变量进行合并，对于同名的环境变量，节点的优先级
|cache_options| Cache | fs server restful端口，默认值为8081
|fs_rpc_port| string (optional) | fs server rpc端口，默认值为8082

#### 返回值


### CacheOptions

