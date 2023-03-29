# PaddleFlow命令行使用demo

本页面提供一个从存储创建到任务发起的一个使用例子。
点击[命令行操作说明](docs/zh_cn/reference/client_command_reference.md) 获取所有操作命令和示例。
## 环境依赖

* python_requires >= 3.6.1
* click==7.1.2
* tabulate==0.8.9
* setuptools >= 21.0.0

## 步骤

### 环境准备
以下示例将演示一个使用paddle cpu 训练作业的demo，请确保您的集群各个节点机器上已经执行一下命令拉取相应镜像：
```bash
docker pull registry.baidubce.com/paddlepaddle/paddle:2.2.1-gpu-cuda10.2-cudnn7
```

### 创建fs
本例子使用一个sftp 类型的存储，具体的存储类型请根据实际情况确定。使用sftp 类型存储时需要确保集群内每个节点均能访问到该sftp host.

```bash
# 注意，sftp的用户名和密码也就是机器ssh的登陆密码和用户名
# PF 会自动为你在sftp_address 该机器创建subpath目录
paddleflow fs create testfs sftp://<sftp_address>/<subpath>
 -o user=<username> -o password=<password>
```

登录该sftp机器，进入subpath 目录，执行如下命令准备好模型代码：
```bash
cd /<subpath> 
wget https://paddleflow.bj.bcebos.com/test_demo/mnist_model.tar
tar -xf mnist_model.tar
# 该文件夹内放置了一个简单的使用mnist 训练模型的代码
```

同时在/<subpath> 目录下创建run.yaml,文件内容如下：
```yaml

name: myproject_runyaml

docker_env: registry.baidubce.com/paddlepaddle/paddle:2.2.1-gpu-cuda10.2-cudnn7

entry_points:
  
  train:
    parameters:
      model_path: "./mnist_model"
    command: "cd {{model_path}} && python mnist_paddlepaddle.py"
    env:
      PF_JOB_TYPE: single
      PF_JOB_QUEUE_NAME: default-queue
      PF_JOB_MODE: Pod
      PF_JOB_FLAVOUR: flavour1

fs_options:
  main_fs: {name: "testfs"}
  extra_fs:
  - {name: "testfs", mount_path: "/train", read_only: false}
parallelism: 5
```
### 创建作业

本示例作业在运行期间，使用的套餐、队列资源，均为PF 提供的默认资源。即"default-queue" 和"flavour1"

```bash
paddleflow run create -f testfs -yp ./run_mnist.yaml
```
此时PF 会返回一个runID, 例如run-000001
使用如下命令即可看到run的信息
```bash 
paddleflow run show run-000001 
```


