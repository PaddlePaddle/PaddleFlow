### 队列授权
```python
ret, response = client.grant_queue('username', 'queuename')
```
#### 接口入口参数
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|username|string (required)|被授权用户用户名
|queuename|string (required)|被授权队列名

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 队列取消授权
```python
ret, response = client.ungrant_queue('username', 'queuename')
```
#### 接口入口参数
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|username|string (required)|被取消授权用户用户名
|queuename|string (required)|被取消授权队列名

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 队列授权列表展示
```python
ret, response = client.show_queue_grant(username, maxsize=100)
```

#### 接口入口参数
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|username|string (optional)|指定用户，用于过滤指定用户对应的授权信息
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回队列列表grantList(list)，每个元素为GrantInfo对象，可以参考下面GrantInfo类的定义获取对应的成员变量。

授权信息GrantInfo类定义
```python
class GrantInfo(object):
    """the class of grant info"""

    def __init__(self, username, resourceName):
        """ init """
        self.username = username
        self.resourceName = resourceName   
```

### 队列列表展示
```python
ret, response = client.list_queue(maxsize=100)
```
#### 接口入口参数
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100
|marker| int (optional)| 展示下一页的数据

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回队列列表queueList(list)，每个元素为QueueInfo对象，可以参考下面QueueInfo类的定义获取对应的成员变量。

队列queue类定义
```python
class QueueInfo(object):
    """the class of queue info"""   

    def __init__(self, name, status, namespace, mem, cpu, clusterName,  createTime, updateTime):
        """init """
        self.name = name
        self.status = status
        self.namespace = namespace
        self.mem = mem
        self.cpu = cpu
        self.clusterName = clusterName
        self.createTime = createTime
        self.updateTime = updateTime
```

### 队列详情展示
```python
ret, response = client.show_queue("queuename")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|queuename| string (required)| 队列名称

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回队列详情queue，类型为QueueInfo，可以参考上面QueueInfo类的定义获取对应的成员变量。

### 队列创建
```python
ret, response = client.create_queue("name","namespace","cpu","men","clustername")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|name| string (required)| 自定义队列名称
|namespace| string (required)| 命名空间
|cpu| string (required)| 最大cpu
|men| string (required)| 最大内存
|clustername| string (required)| 集群名称

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回none

### 队列停止
```python
ret, response = client.stop_queue("queuename")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|queuename| string (required)| 队列名称

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回none

### 队列删除
```python
ret, response = client.del_queue("queuename")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|queuename| string (required)| 队列名称

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回none