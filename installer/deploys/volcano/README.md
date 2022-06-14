# pf-volcano install guide

## 1. install
目前PaddleFlow使用开源volcano作为默认调度器
```shell
# todo(使用pf版volcano,替换image, volcano-scheduler-pf.conf)
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/volcano/pf-volcano-deploy.yaml
```



## 2. check
```shell
kubectl get pod -n volcano-system
```
