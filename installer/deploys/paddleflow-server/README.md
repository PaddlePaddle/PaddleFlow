# paddleflow-server install guide

## 1. create
**快速安装paddleflow-server**
```shell
# 初始化sqlite
touch /mnt/paddleflow.db && chmod 666 /mnt/paddleflow.db
# 部署
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
wget https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/develop/installer/database/paddleflow.sql
bash < <(curl -s https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/develop/installer/database/execute.sh)
# 创建基于mysql的paddleflow-server
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml | \
sed -e "s/sqlite/${DB_DRIVER}/g"  -e "s/host: 127.0.0.1/host: ${DB_HOST}/g"  -e "s/3306/${DB_PORT}/g" -e "s/user: paddleflow/user: ${DB_USER}/g"  -e "s/password: paddleflow/password: ${DB_PW}/g"  -e "s/database: paddleflow/database: ${DB_DATABASE}/g" \
| kubectl apply -f -
```


## 2. check
```shell
kubectl get pod -n paddleflow | grep paddleflow-server
```
