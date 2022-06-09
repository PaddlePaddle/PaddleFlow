# paddleflow-server install guide

## 1. create
**快速安装paddleflow-server**
```shell
# 快速安装方式使用sqlite
touch /mnt/paddleflow.db && chmod 666 /mnt/paddleflow.db && kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml
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


## 2. check
```shell
kubectl get pod -n paddleflow
```
