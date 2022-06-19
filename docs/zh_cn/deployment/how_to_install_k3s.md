# 如何快速在单机安装k3s
当前文档基于大家安装经验总结的快速安装k3s的方式
## 使用国内镜像加速安装
```shell
curl -sfL https://rancher-mirror.rancher.cn/k3s/k3s-install.sh | INSTALL_K3S_MIRROR=cn sh -
```
## 更换k3s的默认端口范围
```shell
# 停止服务
systemctl stop k3s
# 修改默认配置
vi /etc/systemd/system/k3s.service  # 修改配置，增加一行
ExecStart=/usr/local/bin/k3s \

    server \

    --kube-apiserver-arg service-node-port-range=1-65535
# 重新启动
systemctl daemon-reload
systemctl start k3s
```
## 更换国内docker hub代理
```shell
# 通过crictl info查看containerd当前配置信息，看到的应该是默认配置
# 拷贝配置文件，原文件不删除
cp /var/lib/rancher/k3s/agent/etc/containerd/config.toml /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl
# 修改配置文件
在/var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl最后增加以下配置
[plugins.cri.registry.mirrors][plugins.cri.registry.mirrors."docker.io"]
    endpoint =["https://vcw3fe1o.mirror.aliyuncs.com","https://docker.mirrors.ustc.edu.cn"]#镜像加速地址
保存后
# 重新启动
systemctl restart k3s
# 检查配置是否生效
crictl info|grep  -A 5 registry 
```


