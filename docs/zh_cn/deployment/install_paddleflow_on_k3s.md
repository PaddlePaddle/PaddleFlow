# 1. PaddleFlow服务端安装
## 1.1 基于k3s快速安装（二选一）
1. 创建一个具有写权限的sqlite数据库文件,默认位于`/mnt/paddleflow.db`. 若需更换路径,请等待后续支持的shell部署脚本

```shell
touch /mnt/paddleflow.db && chmod 666 /mnt/paddleflow.db
```

2. 部署

```shell
# Kubernetes version >= v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/paddleflow-deployment.yaml

# Kubernetes version < v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/paddleflow-deployment-before-v1-18.yaml

# For x86: todo
# For arm64: todo
```

## 1.2 基于k3s自定义安装（二选一）
### 1.2.1 安装paddleflow-server
`paddleflow-server`支持多种数据库(`sqlite`,`mysql`)，其中`sqlite`仅用于快速部署和体验功能，不适合用于生产环境。
- **指定用sqllite安装paddleflow-server**
```shell
# 创建一个具有写权限的sqlite数据库文件,默认位于`/mnt/paddleflow.db`. 若需更换路径,请等待后续支持的shell部署脚本
touch /mnt/paddleflow.db && chmod 666 /mnt/paddleflow.db
# 创建基于sqllite的paddleflow-server
# For x86:
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml
# For arm64: todo
```

- **指定用mysql安装paddleflow-server(推荐)**
```shell
# 指定mysql配置如下
export DB_DRIVER='mysql'
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=paddleflow
export DB_PW=paddleflow
export DB_DATABASE=paddleflow
wget https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/database/paddleflow.sql
bash < <(curl -s https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/database/execute.sh)
# 创建基于mysql的paddleflow-server
# For x86:
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml | \
sed -e "s/sqlite/${DB_DRIVER}/g"  -e "s/host: 127.0.0.1/host: ${DB_HOST}/g"  -e "s/3306/${DB_PORT}/g" -e "s/user: paddleflow/user: ${DB_USER}/g"  -e "s/password: paddleflow/password: ${DB_PW}/g"  -e "s/database: paddleflow/database: ${DB_DATABASE}/g" \
| kubectl apply -f -
# For arm64: todo
```

### 1.2.2 安装paddleflow-csi-plugin

1. 部署

```shell
# Kubernetes version >= v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy.yaml

# Kubernetes version < v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy-before-v1-18.yaml

# For x86_64: todo
# For arm64: todo
```

### 1.2.3 安装volcano
```shell
# For x86_64:
kubectl apply -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/volcano/pf-volcano-deploy.yaml

# For arm64:
todo
```


## 1.3 卸载paddleflow
如需要卸载或重装PaddleFlow,请执行如下步骤
```shell
# 清理环境资源
kubectl delete queue `kubectl get queue | grep -v NAME | awk '{print $1}'`
# 清理PaddleFlow服务以及调度器CRD
kubectl delete -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/paddleflow-deployment.yaml
# 清理数据库
export DB_DRIVER='mysql'
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_USER=paddleflow
export DB_PW=paddleflow
export DB_DATABASE=paddleflow
mysql -u$DB_USER -h$DB_HOST -p$DB_PW -P$DB_PORT $DB_DATABASE -e "drop database if exists $DB_DATABASE;"
```

# 2. PaddleFlow客户端安装和初始化
release1.4.2版本需要手动初始化数据,数据初始化操作可在任意环境完成，**要求为能访问paddleflow server所在的目前集群**，流程如下：
1. 安装并配置客户端
2. 注册一个集群到PaddleFlow
3. 注册一个队列到PaddleFlow
4. 验证集群和队列是否注册成功

### step1. 安装并配置客户端

```shell
pip3 install https://github.com/PaddlePaddle/PaddleFlow/releases/download/v0.14.2/PaddleFlow-1.4.2-py3-none-any.whl
# 如果whl包的下载速度过慢,可尝试执行`curl -O https://mirror.ghproxy.com/https://github.com/PaddlePaddle/PaddleFlow/releases/download/v0.14.2/PaddleFlow-1.4.2-py3-none-any.whl` 下载到本地
# 再执行`pip3 install PaddleFlow-1.4.2-py3-none-any.whl`
```

创建配置文件~/.paddleflow/config ，写入如下内容：

```shell
[user]
name = root
# paddleflow的root密码,默认为paddleflow
password = 
[server]
# paddleflow server 地址，例如 paddleflow_server = mock.paddleflow.net
paddleflow_server = 127.0.0.1
# paddleflow server 端口,默认为8999
paddleflow_port = 8999
```

### step2. 注册集群
**准备集群证书:** k3s证书通常位于机器中的`/etc/rancher/k3s/k3s.yaml`. 将`k3s.yaml`文件复制到安装好PaddleFlow客户端的机器上，例如/tmp/config. 

> 在rancher的k3s部署命令后，k3s.yaml内的`endpoint`通常为`127.0.0.1`，但paddleflow-server默认为容器内部署，无法访问，因此需要修改证书中的`endpoint`为`step1`中的paddleflow_server地址

```shell
# 待注册集群的k8s config文件路径
export k8sconfigpath=/tmp/config
# 集群名称
export clustername=default-cluster
# endpoint 可以在证书文件中找到，格式为ip地址
export endpoint=127.0.0.1
export k8sversion=1.16
paddleflow cluster create ${clustername} ${endpoint} Kubernetes -c ${k8sconfigpath} --version ${k8sversion} --source CCE --status online
```

#### step3. 注册队列
移除队列校验器 (Volcano版本<`1.5`需执行; Volcano版本>=`1.5`请跳过)

由于`PaddleFlow1.4.2`默认使用开源Volcano版本为`1.3`,存在禁止删除open状态的队列的拦截逻辑. 为保证PaddleFlow功能的正常使用,需禁用该拦截器,执行如下命令
```shell
kubectl delete validatingwebhookconfigurations.admissionregistration.k8s.io volcano-admission-service-queues-validate
```

创建队列命令

```shell
paddleflow queue create 队列名称 命名空间 集群名（可直接引用步骤2中的clustername） CPU上限 mem上限 --maxscalar 扩展资源，多个资源逗号分隔 --mincpu 最小CPU资源 --minmem 最小内存资源 --minscalar 最小扩展资源

export QUEUENAME=ppl-queue
export QUEUENAMESPACE=default
export QUEUEMaxCPU=100
export QUEUEMaxMEM=100G
export QUEUEMinCPU=10
export QUEUEMinMEM=10G

# 无GPU示例，注册volcano原生queue
paddleflow queue create ${QUEUENAME} ${QUEUENAMESPACE} ${clustername} ${QUEUEMaxCPU} ${QUEUEMaxMEM}  --quota volcanoCapabilityQuota
# 有GPU示例
paddleflow queue create ${QUEUENAME} ${QUEUENAMESPACE} ${clustername} ${QUEUEMaxCPU} ${QUEUEMaxMEM} --maxscalar  nvidia.com/gpu=1 --mincpu ${QUEUEMinCPU} --minmem ${QUEUEMinMEM} --minscalar nvidia.com/gpu=1 --quota volcanoCapabilityQuota
```

### step4. 验证

```shell
paddleflow cluster list
paddleflow queue list
```
# 3. 快速上手
[快速上手](../quickstart/simple_quickstart.md)
