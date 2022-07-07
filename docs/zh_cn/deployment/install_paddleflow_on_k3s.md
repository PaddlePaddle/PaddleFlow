### 在k3s中安装paddleflow-server
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

### 2.3 自定义安装
#### 2.3.1 安装paddleflow-server
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
wget https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/develop/installer/database/paddleflow.sql
bash < <(curl -s https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/develop/installer/database/execute.sh)
# 创建基于mysql的paddleflow-server
# For x86:
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml | \
sed -e "s/sqlite/${DB_DRIVER}/g"  -e "s/host: 127.0.0.1/host: ${DB_HOST}/g"  -e "s/3306/${DB_PORT}/g" -e "s/user: paddleflow/user: ${DB_USER}/g"  -e "s/password: paddleflow/password: ${DB_PW}/g"  -e "s/database: paddleflow/database: ${DB_DATABASE}/g" \
| kubectl apply -f -
# For arm64: todo
```

#### 2.3.2 安装paddleflow-csi-plugin

1. 部署

```shell
# Kubernetes version >= v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy.yaml
# Kubernetes version < v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy-before-v1-18.yaml
# For x86_64: todo
# For arm64: todo
```

#### 2.3.3 安装volcano
```shell
# For x86_64:
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml

# For arm64:
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development-arm64.yaml
```
