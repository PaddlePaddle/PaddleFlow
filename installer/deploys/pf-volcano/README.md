# pf-volcano install guide

## 1. create
execute command in cluster
```shell
# todo(image, volcano-scheduler-pf.conf)
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/pf-volcano/pf-volcano-deploy.yaml
```
如果想安装开源volcano,请参考开源volcano安装指南


## 2. check
```shell
kubectl get pod -n volcano-system
```
