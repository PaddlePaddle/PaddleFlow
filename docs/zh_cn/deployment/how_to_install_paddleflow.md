# PaddleFlow部署
PaddleFlow部署分为客户端和服务端两个部分。
## 1.客户端
### 1.1 环境依赖
- pip3

### 1.2 执行部署/卸载

**安装**PaddleFlow客户端有两种方式：

1. 从[release](https://github.com/PaddlePaddle/PaddleFlow/releases)下载最新版, 执行`pip3 install PaddleFlow-1.4.2-py3-none-any.whl`
2. 通过编译包安装，编译包有两处来源，分别是分支的最新产出、执行编译命令`cd client && python3 setup.py bdist_wheel`。得到编译包后执行`cd dist && pip3 install PaddleFlow-1.4.2-py3-none-any.whl`

**卸载**PaddleFlow

`pip3 uninstall paddleflow`

## 2.服务端
部署组件说明
PaddleFlow运行依赖以下几个组件：</br>

| 组件名称              | 组件用途 |
| --------------------- | -------- |
| paddleflow-server     | paddleflow-server提供的功能包括存储资源管理、作业资源调度和工作流编排。 |
| paddleflow-csi-plugin | paddleflow-csi-plugin提供的功能主要包括存储资源管理。 |
| volcano               | volcano基于开源调度器[volcano](https://volcano.sh/zh/docs/architecture)改造,未来将新增弹性资源队列elastic quota以及映射k8s namespace等能力。paddleflow支持使用原生volcano.  |


### 2.1 环境依赖
- kubernetes 1.16+ 或 k3s 1.16+
- mysql (可选)

### 2.2 快速部署

- [PaddleFlow On Kubernetes](install_paddleflow_on_k8s.md)

- [PaddleFlow On K3s](install_paddleflow_on_k3s.md)


### 2.4 服务端部署包说明
部署包位于installer下, 包结构如下：

```
.
├── paddleflow-deployment.yaml
├── database
│   ├── README.md
│   ├── paddleflow.sql
│   └── execute.sh
├── deploys
│   ├── paddleflow-server
│   ├── paddleflow-csi-plugin
│   └── volcano
└── dockerfile
    ├── README.md
    ├── build_latest.sh
    ├── paddleflow-csi-plugin
    │   ├── Dockerfile
    │   └── Dockerfile_base
    └── paddleflow-server
        ├── Dockerfile
        └── Dockerfile_base
```
- `paddleflow-deployment.yaml`用于部署paddleflow各个组件;
- `database`目录用于执行数据库初始化脚本,创建数据库及相应的数据表,详见[数据库初始化指南](../../../installer/database/README.md)
- `deploys`目录用于存放各组件yaml格式的部署文件,包括了`paddleflow-server`,`paddleflow-csi-plugin`,`volcano`
- `dockerfile`目录包含了各组件的镜像构建文件,使用方式详见[paddleflow镜像构建指南](../../../installer/dockerfile/README.md)