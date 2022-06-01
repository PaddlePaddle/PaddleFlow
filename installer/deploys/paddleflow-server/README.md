# paddleflow-server install guide

## 1. create
execute command in cluster
```shell
kubectl create -f installer/deploys/paddleflow-server/paddleflow-server-deploy.yaml
```

## 2. check
```shell
kubectl get pod -n paddleflow
```
