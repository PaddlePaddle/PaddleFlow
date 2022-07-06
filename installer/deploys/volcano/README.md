# pf-volcano安装指南

## 1. 安装
目前PaddleFlow使用开源volcano作为默认调度器
```shell
# todo(使用pf版volcano,替换image, volcano-scheduler-pf.conf)
# For x86_64:
kubectl apply -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/volcano/pf-volcano-deploy.yaml

# For arm64:
todo
```



## 2. 验证
```shell
kubectl get pod -n volcano-system
```
