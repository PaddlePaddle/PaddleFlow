# paddleflow-storage install guide

## 1. create
execute command in cluster
```shell
kubectl create -f installer/deploys/pf-storage/paddleflow-storage-deploy.yaml
```

## 2. check
```shell
kubectl get pod -n paddleflow
```
