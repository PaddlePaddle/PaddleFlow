# Paddleflow SDK 使用说明
paddleflow python sdk是基于paddleflow服务封装的sdk python包，对用户、队列、存储、工作流进行统一封装client，开发者可以参考本说明结合自身需求进行使用。

### client 初始化
```python
import paddleflow
client = paddleflow.Client("127.0.0.1", "127.0.0.1", paddleflow_port=8083, fs_http_port=8081, fs_rpc_port=8082) 
##paddleflow_port, fs_http_port, fs_rpc_port字段有默认设置值为8080，8081, 8082。 如果用户没有修改,在client 初始化时不需要进行传入
``` 
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|paddleflow_server| string (required)| paddleflow server服务地址
|fs_server| string (required) | fs server服务地址
|paddleflow_port| string (optional) | paddleflow server端口，默认值为8080
|fs_http_port| string (optional) | fs server restful端口，默认值为8081
|fs_rpc_port| string (optional) | fs server rpc端口，默认值为8082

#### 接口返回说明
无