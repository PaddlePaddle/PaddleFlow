# PaddleFlow部署
PaddleFlow部署分为客户端和服务端两个部分。
## 1.客户端
### 1.1 环境依赖
- pip3

### 1.2 执行部署/卸载

**安装**PaddleFlow客户端有两种方式：

1. 从[release](https://github.com/PaddlePaddle/PaddleFlow/releases)下载最新版, 执行`pip3 install PaddleFlow-1.4.2-py3-none-any.whl`
2. 通过编译包安装，编译包有两处来源，分别是分支的最新产出、执行编译命令`cd client && python3 setup.py bdist_wheel`。得到编译包后执行`pip3 install PaddleFlow-1.4.2-py3-none-any.whl`

**卸载**PaddleFlow

`pip3 uninstall paddleflow`

## 2.服务端
部署组件说明
PaddleFlow运行依赖以下几个组件：</br>
* paddleflow-server
* paddleflow-csi-plugin
* volcano

**paddleflow-server**

paddleflow-server提供的功能包括存储资源管理、作业资源调度和工作流编排。这是一个服务入口。详细配置参考文档（TODO）。</br>

**paddleflow-csi-plugin**

paddleflow-csi-plugin提供的功能主要包括存储资源管理。</br>

**volcano**

本模块基于开源调度器[volcano](https://volcano.sh/zh/docs/architecture/)改造,新增弹性资源队列elastic quota以及映射k8s namespace等能力。若用户已使用原生volcano，请勿执行`2.2`的快速部署,而是在自定义部署中单独执行`server`和`csi-plugin`的安装命令.

### 2.1 环境依赖
- kubernetes 1.16+ 或 k3s 1.16+
- mysql (可选)

### 2.2 快速部署

1. 创建一个具有写权限的sqlite数据库文件,默认位于`/mnt/paddleflow.db`

```shell
touch /mnt/paddleflow.db && chmod 666 /mnt/paddleflow.db
```

2. 检查 `kubelet root-dir` 路径

在 Kubernetes 集群中任意一个非 Master 节点上执行以下命令：

```shell
ps -ef | grep kubelet | grep root-dir
```

3. 部署

**如果前面检查命令返回的结果不为空**，则代表 kubelet 的 root-dir 路径不是默认值，因此需要在 CSI Driver 的部署文件中更新 `kubeletDir` 路径并部署：
```shell
# Kubernetes version >= v1.16
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/paddleflow-deployment.yaml | sed 's@/var/lib/kubelet@{{KUBELET_DIR}}@g' | kubectl apply -f -
```

> **注意**: 请将上述命令中 `{{KUBELET_DIR}}` 替换成 kubelet 当前的根目录路径。

**如果前面检查命令返回的结果为空**，无需修改配置，可直接部署：

```shell
# 执行部署
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/paddleflow-deployment.yaml
```
### 2.3 自定义安装
#### 2.3.1 安装paddleflow-server

**快速安装paddleflow-server**
```shell
# 创建一个具有写权限的sqlite数据库文件,默认位于`/mnt/paddleflow.db`
touch /mnt/paddleflow.db && chmod 666 /mnt/paddleflow.db
# 创建基于sqlite的PaddleFlow-server
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml
```

**指定数据库为mysql并安装(推荐)**
```shell
# paddleflow默认使用SQLite配置,如需切换成mysql，需执行命令如下
export DB_DRIVER='mysql'
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=paddleflow
export DB_PW=paddleflow
export DB_DATABASE=paddleflow
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml | \
sed -e 's/sqlite/`${DB_DRIVER}`/g'  -e 's/DB_HOST: 127.0.0.2/DB_HOST=`${DB_HOST}`/g'  -e 's/3306/`${DB_PORT}`/g' -e 's/DB_USER: paddleflow/DB_USER: ${DB_USER}/g'  -e 's/DB_PW=paddleflow/DB_PW=${DB_PW}/g'  -e 's/DB_DATABASE: paddleflow/DB_DATABASE: ${DB_DATABASE}/g' \
| kubectl create -f -
```

#### 2.3.2 安装paddleflow-csi-plugin

1. 检查 `kubelet root-dir` 路径

在 Kubernetes 集群中任意一个非 Master 节点上执行以下命令：

```shell
ps -ef | grep kubelet | grep root-dir
```

2. 部署

**如果前面检查命令返回的结果不为空**，则代表 kubelet 的 root-dir 路径不是默认值，因此需要在 CSI Driver 的部署文件中更新 `kubeletDir` 路径并部署：
```shell
# Kubernetes version >= v1.16
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy.yaml | sed 's@/var/lib/kubelet@{{KUBELET_DIR}}@g' | kubectl apply -f -
```

> **注意**: 请将上述命令中 `{{KUBELET_DIR}}` 替换成 kubelet 当前的根目录路径。

**如果前面检查命令返回的结果为空**，无需修改配置，可直接部署：
```shell
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy.yaml
```

#### 2.3.3 安装volcano
```shell
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/pf-volcano/pf-volcano-deploy.yaml
```

### 2.4 服务端部署包说明
部署包位于installer下, 包结构如下：

```
.
├── README.md
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