# pf-volcano安装指南

## 1. 安装
目前PaddleFlow使用开源volcano作为默认调度器
```shell
# todo(使用pf版volcano,替换image, volcano-scheduler-pf.conf)
# For x86_64:
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development.yaml

# For arm64:
kubectl apply -f https://raw.githubusercontent.com/volcano-sh/volcano/master/installer/volcano-development-arm64.yaml
```



## 2. 验证
```shell
kubectl get pod -n volcano-system
```
