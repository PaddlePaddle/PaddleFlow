# paddleflow-csi-plugin install guide

## 1. create
execute command in cluster
```shell
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy.yaml
```

## 2. check
```shell
kubectl get pod -n paddleflow
```
