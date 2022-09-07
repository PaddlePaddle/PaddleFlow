# 数据缓存亲和性调度
paddleflow CSI Driver 1.4.4 开始支持数据缓存结点亲和性调度
## 使用方法
使用结点亲和性需要两个步骤: 1.为k8s环境下的结点打上自定义的标签; 2.发请求设置 fs_cache_config, 设置此fs缓存配置的结点亲和性策略
### 步骤一: 使用标签标识结点
1. 查看全部结点
```shell
$ kubectl get nodes
NAME                  STATUS   ROLES         AGE    VERSION
paddleflow-qa-test2   Ready    master,node   43d    v1.16.15
paddleflow-qa-test3   Ready    master,node   106d   v1.16.15
paddleflow-qa-test4   Ready    master,node   106d   v1.16.15
```
2. 使用标签标识结点
```shell
$ kubectl label nodes paddleflow-qa-test2 cache=true
node/paddleflow-qa-test2 labeled
```
3. 再次查看结点
```shell
$ kubectl get node -L cache
NAME                  STATUS   ROLES         AGE    VERSION    CACHE
paddleflow-qa-test2   Ready    master,node   43d    v1.16.15   true
paddleflow-qa-test3   Ready    master,node   106d   v1.16.15
paddleflow-qa-test4   Ready    master,node   106d   v1.16.15
```
目前，在全部3个结点中，仅有一个结点添加了cache=true的标签，接下来，我们希望数据缓存仅会被放置在该结点之上

## 步骤二: 设置fs缓存配置的结点亲和性策略

假设已存在存储```elsies3```, 发REST请求设置其缓存配置如下. 可以设置亲和性为必须(required)或优先级(preferred).
注: ```nodeAffinity``` 字段的结构完全遵循k8s此结构体的设置, 此结构体的字段和用法的详细说明可参考Kubernetes官方文档.

```json
{
  "fsName": "elsies3",
  "metaDriver": "badgerdb",
  "blockSize": 4096,
  "cacheDir": "/data/elsie/csi/cache_dir",
  "cleanCache": true,
  "resource":{
    "cpuLimit":"3",
    "memoryLimit":"2Gi"
  },
  "nodeAffinity":{
    "requiredDuringSchedulingIgnoredDuringExecution" :{
      "nodeSelectorTerms":[{"matchExpressions":[{
        "key": "cache",
        "operator":"In",
        "values":["true"]
      }]}]
    },
    "preferredDuringSchedulingIgnoredDuringExecution":[{
      "weight" : 100,
      "preference":{
        "matchExpressions":[{
          "key": "cache",
          "operator":"In",
          "values":["woof", "meow"]
        }]
      }
    }]
  }
}
```
## 环境验证
通过以上两个步骤的设置, ```elsies3```这个存储已经设置上了必须调度到标签为```cache=ture```的结点上的亲和性, 和一些环境不存在的标签的优先级亲和性(即只影响调度优先级, 不是必须).
此时发一个任务挂载使用此fs, 即可在环境中查看其作业pod所在结点信息.
```shell
$ kubectl get po -owide
NAME                         READY   STATUS      RESTARTS   AGE   IP              NODE               NOMINATED NODE   READINESS GATES
job-test-fs-cache=affinity   1/1     Running     0         49s   xx.xx.xx.xx   paddleflow-qa-test2   <none>           <none>
````