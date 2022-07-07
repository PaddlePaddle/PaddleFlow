# paddleflow-csi-plugin安装指南

## 1. 安装
### 1.1 paddleflow-csi-plugin on Kubernetes
step1. 检查 `kubelet root-dir` 路径

在`Kubernetes`集群中任意节点上执行以下命令：

```shell
# 查询kubelet当前的根目录路径
ps -ef | grep kubelet | grep root-dir
```

step2. 部署

**如果前面检查命令返回的结果为空**，无需修改配置，可直接部署：
```shell
# Kubernetes version >= v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy.yaml
# Kubernetes version < v1.18
kubectl create -f https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy-before-v1-18.yaml
```

**如果前面检查命令返回的结果不为空**，则代表 kubelet 的 root-dir 路径不是默认值，因此需要在 CSI Driver 的部署文件中更新 `kubeletDir` 路径并部署：
```shell
# Kubernetes version >= v1.18
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy.yaml | sed 's@/var/lib/kubelet@{{KUBELET_DIR}}@g' | kubectl apply -f -
# Kubernetes version < v1.18
curl -sSL https://raw.githubusercontent.com/PaddlePaddle/PaddleFlow/release-0.14.2/installer/deploys/paddleflow-csi-plugin/paddleflow-csi-plugin-deploy-before-v1-18.yaml | sed 's@/var/lib/kubelet@{{KUBELET_DIR}}@g' | kubectl apply -f -
```


## 2. check
```shell
kubectl get pod -n paddleflow | grep pfs
```
