# pf-volcano安装指南

## 1. 安装
目前PaddleFlow使用改造过的volcano作为默认调度器,具体改造内容参考todo(zhongzichao)
```shell
# For x86_64:
kubectl apply -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/develop/installer/release-0.14.6/volcano/crd.yaml -n paddleflow
kubectl apply -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/develop/installer/release-0.14.6/volcano/pf-volcano-deploy.yaml -n paddleflow

# For arm64:
todo
```



## 2. 验证
```shell
kubectl get pod -n volcano-system
```
