# csi 下挂载点自动恢复
paddleflow CSI Driver 1.4.3 开始支持挂载点自动恢复
## 使用方法
业务应用需要在 pod 的 MountVolume 中设置 mountPropagation 为 HostToContainer 或 Bidirectional（Bidirectional需要设置 pod 为特权 pod），从而将 host 的挂载信息传送给 pod。配置如下：
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: mount-test
spec:
  volumes:
    - name: task-pv-storage
      persistentVolumeClaim:
        claimName: pfs-fs-root-mounttest-pvc
  containers:
    - name: task-pv-container
      image: nginx
      command:
        - /bin/sh
        - -c
        - sleep 24h
      volumeMounts:
        - mountPath: "/home/paddleflow/mnt"
          name: task-pv-storage
          mountPropagation: HostToContainer
```
## 挂载点恢复测试

pod 挂载后，可以看到 mount pod 如下：

```shell
$ kubectl get po -A
NAMESPACE     NAME                                            READY   STATUS    RESTARTS   AGE
default       pod-mount-test                                  1/1     Running     0          93m
paddleflow    pfs-NODENAME-pfs-fs-root-mounttest-default-pv   1/1     Running     1          93m
```
挂载进程如下:
```shell
ps -ax | grep mounttest
30821 ?        Ssl    0:00 /home/paddleflow/pfs-fuse mount --mount-point=/home/paddleflow/mnt/storage --fs-id=fs-root-mounttest --fs-info=...
````
为了做测试，我们将挂载进程kill，然后观察 pod 的恢复情况. 挂载 pod 状态先变成了```Error```, 而后自动恢复为了running.

```shell
$ kubectl get po -n paddleflow
NAME                                            READY   STATUS    RESTARTS   AGE
...
pfs-NODENAME-pfs-fs-root-mounttest-default-pv   0/1     Error     0          2m25s
...

$ kubectl get po -n paddleflow
NAME                                            READY   STATUS    RESTARTS   AGE
...
pfs-NODENAME-pfs-fs-root-mounttest-default-pv   1/1     Running   1          2m28s
...
```

通过 watch 的结果，可以看到 mount pod 在被删除之后，又被新建出来。接着在业务容器中检查挂载点信息：

```shell
$ kubectl exec -it pod-mount-test sh
/home/paddleflow # ls mnt
image.tar            run.yaml        nofs.yaml            paddleflow.db        runDag.yaml          server.log
```