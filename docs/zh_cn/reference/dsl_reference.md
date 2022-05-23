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
|字段名称 | 字段类型 | 字段含义 | 是否必须 | 备注 
|:---:|:---:|:---:|
|paddleflow_server| string (required)| paddleflow server服务地址
|fs_server| string (required) | fs server服务地址
|paddleflow_port| string (optional) | paddleflow server端口，默认值为8080
|fs_http_port| string (optional) | fs server restful端口，默认值为8081
|fs_rpc_port| string (optional) | fs server rpc端口，默认值为8082

#### 返回值

