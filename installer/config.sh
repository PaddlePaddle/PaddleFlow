#!/bin/bash

work_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# 设置部署目录
export DEPLOY_ROOT_DIR=$work_dir

# 运行环境(k3s or k8s)
export RUN_TIME_ENV="k8s"

# 设置部署默认Namespace
export DEPLOY_NAMESPACE=paddleflow

# 设置helm执行文件路径
export PADDLEFLOW_HELM_BIN="$work_dir/tools/helm"

# kubeconfig文件所在位置，默认为k8s的文件位置，k3s需要更改
export KUBE_CONFIG_FILE="/root/.kube/config"

# 数据库设置, DB_DRIVER=mysql时其它字段有效, DB_DRIVER=sqlite3时直接创建sqlite3且其它字段无效
export DB_DRIVER='sqlite3'
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=paddleflow
export DB_PW=paddleflow
export DB_DATABASE='paddleflow'

# 服务部署选择
export PADDLEFLOW_SERVER=true  # paddleflow服务，默认安装,false则不安装
export PADDLEFLOW_STORAGE=true # 安装存储插件，默认安装
export VOLCANO=true
export DATABASE_SQL=true # 是否要执行数据库相关操作，会删除$DB_DATABASE库并重新创建，false则不执行

# 要安装的模块
export INSTALL_MODULE_LIST="
paddleflow
storage
volcano
"

# 卸载时会删除的模块
export UNINSTALL_MODULE_LIST="
paddleflow
storage
volcano
"

# paddleflow服务镜像
export paddleflowServerImage="paddleflow/paddleflow-server"
export paddleflowServerImageTag="20220520_01"

export pfsCsiPluginImage="paddleflow/csi-driver-registrar"
export pfsCsiPluginImageTag="1.2.0"

export csiStorageDriverImage="paddleflow/pfs-csi-plugin"
export csiStorageDriverImageTag="2022520_01"

export pfsCsiProvisionerImage="paddleflow/csi-provisioner"
export pfsCsiProvisionerImageTag="1.4.0"

# the hostname in cluster
pfsServiceName=`hostname`

# todovolcano服务镜像

# spark-operator服务镜像
export SPARK_OPERATOR_IMG_NAME=spark-operator:v1beta2-1.2.0-3.0.0
# paddleflow-server相关设置
export PADDLEFLOW_SERVER_PORT=8999
export PADDLEFLOW_SERVER_PRINTVERSIONANDEXIT=false
export TOKEN_EXPIRATION_HOUR=-1

# paddleflow-log相关设置

# paddleflow-fs csi plugin 相关设置
export PADDLEFLOW_FS_CSI_LOG=/mnt/log/paddleflow-fs-csi
export PADDLEFLOW_FS_CSI_KUBELET_DATA_PATH=/data/lib/kubelet

# paddleflow-fs-server相关设置
export PADDLEFLOW_FS_IMAGE_NAME=paddleflow-fs
export PADDLEFLOW_FS_IMAGE_TAG=1.2.2.2
export PADDLEFLOW_FS_REST_PORT=8997
export PADDLEFLOW_FS_RPC_PORT=8998

# volcano相关设置
export VOLCANO_NAMESPACE=volcano-system
export VOLCANO_SCHEDULER_NAME=volcano
export SCHEDULER_MANAGE_NAMESPACE=default
export VOLCANO_SCHEDULER_CONFIG_FILE_NAME="volcano-scheduler-pf.conf"
export VOLCANO_PORT=8443
