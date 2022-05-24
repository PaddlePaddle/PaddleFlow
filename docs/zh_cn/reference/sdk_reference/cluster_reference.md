### 集群创建
```python
ret, response = client.create_cluster()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|clustername| string (required)|自定义集群名称
|endpoint| string (required)|集群节点
|clustertype| string (required)|集群类型，比如kubernetes-v1.16
|credential| bytes (optional)|base64编码后的字符串，用于存储集群的凭证信息，比如k8s的kube_config配置
|description| string (optional)| 集群描述
|clusterid| string (optional)| 	集群id，支持外部传入（该值可以是百度云的resource_id，通过resource_id可以获取百度云的floating ip），如果不传则由PF生成
|source| string (optional)| 来源，比如AWS、CCE等	
|setting| string (optional)| 额外配置信息
|status| string (optional)| 集群状态, 比如”online”, “offline”，默认值：online
|namespacelist| string[] (optional)| 命名空间列表，比如[“ns1”, “ns2”]


#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回cluster id

### 集群列表显示
```python
ret, response = client.list_cluster()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|clusterstatus| string (optional)|根据集群状态筛选
|clustername| string (optional)|根据集群名称筛选
|maxsize| int (optional,default=50)| 展示列表数量上限，默认值为100
|marker| string (optional)| 下一页的起始位置，传入展示下一页


#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回run信息，类型为ClusterInfo，可以参考下面ClusterInfo类的定义获取对应的成员变量。
|marker| string| 存在返回下一页的起始string，否则返回null

返回信息response中的列表元素结构如下：
```python
class ClusterInfo(object):
    """the class of cluster info"""
    def __init__(self, clusterid, clustername, description, endpoint, source, clustertype, status, 
    credential, setting, namespacelist, createtime, updatetime):
        """init """
        self.clusterid = clusterid
        self.clustername = clustername
        self.description = description
        self.endpoint = endpoint
        self.source = source
        self.clustertype = clustertype
        self.status = status
        self.credential = credential
        self.setting = setting
        self.namespacelist = namespacelist
        self.createtime = createtime
        self.updatetime = updatetime
```

### 集群详情显示
```python
ret, response = client.show_cluster("clustername")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|clustername| string (required)|集群名称



#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回cluster对象，参考上面的ClusterInfo结构

### 集群删除
```python
ret, response = client.delete_cluster("clustername")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|clustername| string (required)|集群名称



#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功none

### 集群更新
```python
ret, response = client.update_cluster("clustername")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|clustername| string (required)|集群名称
|endpoint| string (optional)|集群节点
|clustertype| string (optional)|集群类型，比如kubernetes-v1.16
|credential| bytes (optional)|base64编码后的字符串，用于存储集群的凭证信息，比如k8s的kube_config配置
|description| string (optional)| 集群描述
|clusterid| string (optional)| 	集群id，支持外部传入（该值可以是百度云的resource_id，通过resource_id可以获取百度云的floating ip），如果不传则由PF生成
|source| string (optional)| 来源，比如AWS、CCE等	
|setting| string (optional)| 额外配置信息
|status| string (optional)| 集群状态, 比如”online”, “offline”，默认值：online
|namespacelist| string[] (optional)| 命名空间列表，比如[“ns1”, “ns2”]



#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功集群id

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功none

### 获取集群资源剩余
```python
ret, response = client.list_cluster_resource()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|clustername| string (optional)|集群名称

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功集群剩余资源字典