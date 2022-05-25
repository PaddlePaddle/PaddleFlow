## paddleFlow部署
PaddleFLow部署分为客户端和服务端两个部分。
## 1.客户端
### 1.1 环境依赖
- pip3

### 1.2 执行部署/卸载

**安装**paddleflow客户端有两种方式：

1. 从仓库安装`pip3 install paddleflow`
2. 通过编译包安装，编译包有两处来源，分别是分支的最新产出、执行编译命令`cd client && python3 setup.py bdist_wheel`。得到编译包后执行`pip3 install PaddleFlow-1.4.1-py3-none-any.whl`

**卸载**paddleflow

`pip3 uninstall paddleflow`

## 2.服务端
### 2.1 服务端部署包说明
部署包位于installer下, 包结构如下：

```
.
├── README.md
├── config.sh
├── execute.sh
├── charts
│   ├── execute.sh
│   ├── crds
│   ├── paddleflow-server
│   ├── pfs-csi-plugin
│   ├── pfs-csi-provisioner
│   ├── volcano-admission
│   ├── volcano-admission-init
│   ├── volcano-controller
│   └── volcano-scheduler
├── database
│   ├── README.md
│   ├── paddleflow.sql
│   └── execute.sh
├── dockerfile
│   ├── README.md
│   ├── build_latest.sh
│   ├── paddleflow_csiplugin
│   │   ├── Dockerfile
│   │   └── Dockerfile_base
│   └── paddleflow_server
│       ├── Dockerfile
│       └── Dockerfile_base
├── helm
│   ├── execute.sh
│   ├── execute_uninstall.sh
│   ├── paddleflow
│   │   ├── execute.sh
│   │   └── uninstall.sh
│   ├── storage
│   │   ├── execute.sh
│   │   └── uninstall.sh
│   └── volcano
│       ├── execute.sh
│       └── uninstall.sh
└── tools
    ├── functions
    └── helm

5 directories, many files
```
部署组件说明
PaddleFlow包括以下几个组件：</br>
* paddleflow-server
* storage
* volcano

可选组件：</br>
* mysql：PaddleFlow内置SQLLite，如需替换成为mysql见(todo)
* spark-operator
* mpi-operator

**paddleflow-server**

paddleflow-server提供的功能包括存储资源管理、作业资源调度和工作流编排。这是一个服务入口。详细配置参考文档（TODO）。</br>

**Storage**

Storage提供的功能主要包括存储资源管理。</br>

**volcano**

基于开源调度器[volcano](https://volcano.sh/zh/docs/architecture/)改造新增弹性资源队列elastic quota以及映射k8s namespace等能力。paddleflow-server用户单独部署volcano，在这种情况下，请在部署服务列表中移除volcano

### 2.2 环境依赖
- kubernetes集群 或 k3s集群
- mysql (可选)

### 2.3 执行部署/卸载

```shell
# 执行部署
bash execute.sh install
# 执行卸载
```
### 2.4 自定义配置
配置文件位于install/config.sh中，下面介绍几个重要配置
#### 2.4.1 指定数据库
```shell
# paddleflow默认使用SQLite配置
export DB_DRIVER='sqlite'
# 如需切换成mysql，需完善配置如下
export DB_DRIVER='mysql'
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=paddleflow
export DB_PW=paddleflow
export DB_DATABASE='paddleflow'
```
#### 2.4.2 指定部署/卸载模块
指定要部署的模块
```
# 要安装的模块,不需要部署的模块请以`#`开头
export INSTALL_MODULE_LIST="
paddleflow
storage
volcano
"
```
指定要卸载的模块
```
# 卸载时会删除的模块,不需要卸载的模块请以`#`开头
export UNINSTALL_MODULE_LIST="
paddleflow
storage
volcano
"
```

#### 2.4.3 指定镜像版本

```
# paddleflow服务镜像
export paddleflowServerImage="paddleflow/paddleflow-server"
export paddleflowServerImageTag="20220520_01"

export pfsCsiPluginImage="paddleflow/csi-driver-registrar"
export pfsCsiPluginImageTag="1.2.0"

export csiStorageDriverImage="paddleflow/pfs-csi-plugin"
export csiStorageDriverImageTag="2022520_01"

export pfsCsiProvisionerImage="paddleflow/csi-provisioner"
export pfsCsiProvisionerImageTag="1.4.0"
```

