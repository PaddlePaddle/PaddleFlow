# Paddleflow SDK 使用说明
paddleflow python sdk是基于paddleflow服务封装的sdk python包，对用户、队列、存储、工作流进行统一封装client，开发者可以参考本说明结合自身需求进行使用。

### client 初始化
```python
import paddleflow
client = paddleflow.Client("127.0.0.1", "your-username", "your-password", "8999") 
##paddleflow_server_port字段有默认设置值为8999。 如果用户没有修改,在client 初始化时不需要进行传入
``` 
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|paddleflow_server_host| string (required)| paddleflow server服务地址
|paddleflow_server_port| string (optional) | paddleflow server端口，默认值为8999

#### 接口返回说明
无

### 用户登录
```python
ret, response = client.login('username', 'password') 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称
|password| string (required) | 用户密码

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户增加
```python
ret, response = client.add_user('username', 'password') 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称
|password| string (required) | 用户密码

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户删除
```python
ret, response = client.del_user('username') 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户密码更新
```python
ret, response = client.update_password(user_name, password) 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称
|password| string (required)| 用户密码

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户列表展示
```python
ret, response = client.list_user(maxsize=100) 
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message(string)，成功返回用户列表userList(list)，每个元素为UserInfo对象，可以参考下面UserInfo类的定义获取对应的成员变量。

用户user类定义
```python
class UserInfo(object):
    """the class of user info"""

    def __init__(self, name, create_time):
        """init """
        self.name = name
        self.create_time = create_time
```

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

### flavour列表显示
```python
ret, response = client.list_flavour()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回一个列表


### 创建存储
```python
ret, response = client.add_fs("fsname", "url")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|新建存储系统名称
|url| string (required)|访问地址如：sftp://192.168.1.2:9000/myfs
|username| string (optional)|指定用户，用于root用户为其他用户创建fs
|properties|dict (optional)| 后端存储的访问配置项，通过key:value键值对提供。如：S3校验{"accessKey":"test","secretKey":"test"}

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 存储详情
```python
ret, response = client.show_fs("fsname")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|存储系统名称
|username| string (optional)|指定用户，用于root用户展示特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回存储系统详情，类型为FSInfo，可以参考下面FSInfo类的定义获取对应的成员变量。

```python
class FSInfo(object):
    """the class of fs info"""
    def __init__(self, name, owner, fstype, server_address, subpath, properties):
        """init """
        self.name = name
        self.owner = owner
        self.fstype = fstype
        self.server_adddress = server_address
        self.subpath = subpath
        self.properties = properties
```

### 存储列表
```python
ret, response = client.list_fs()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100
|username| string (optional)|指定用户，用于root用户列出特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回存储系统详情List，每个元素的类型为FSInfo，，可以参考上面FSInfo类的定义获取对应的成员变量。

### 删除存储
```python
ret, response = client.delete_fs("fsname")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|存储系统名称
|username| string (optional)|指定用户，用于admin管理员删除特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 存储挂载
```python
ret, response = client.mount("fsname", "mount_path")
```

#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|存储系统名称
|path| string (required)|挂载点名称
|username| string (optional)|指定用户，用于root用户挂载特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 创建link
```python
ret, response = client.add_link("fsname", "fspath", "url")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|fspath| string (required)|需要link到文件系统的目录
|url| string (required)|外部存储的访问地址如：hdfs://192.168.1.2:9000,192.168.1.3:9000/linkpath
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link
|properties| dict (optional)|外部存储的访问配置项，通过key:value键值对提供。比如HDFS支持透传HDFS的配置项

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 删除link
```python
ret, response = client.delete_link("fsname", "fspath")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|fspath| string (required)|需要link到文件系统的目录
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### link详情
```python
ret, response = client.show_link("fsname", "fspath")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|fspath| string (required)|需要link到文件系统的目录
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回link详情，类型为LinkInfo，可以参考下面LinkInfo类的定义获取对应的成员变量。

```python
class LinkInfo(object):
    """the class of link info"""
    def __init__(self, name, owner, fstype, fspath, server_address, subpath, properties):
        """init """
        self.name = name
        self.owner = owner
        self.fstype = fstype
        self.fspath = fspath
        self.server_adddress = server_address
        self.subpath = subpath
        self.properties = properties
```

### link列表
```python
ret, response = client.show_link("fsname")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回LinkList(LinkInfo)，每个元素的类型为LinkInfo，可以参考上面LinkInfo类的定义获取对应的成员变量。

### 工作流创建
```python
ret, response = client.create_run(fsname="fsname", runyamlpath="./run.yaml")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (optional)|共享存储名称
|username| string (optional)|指定用户，用于root账号运行特定用户的fs的工作流
|runname| string (optional)|工作流名称
|desc| string (optional)|工作流描述
|runyamlpath| string (optional)|指定的yaml 文件路径，发起任务方式之一
|runyamlraw| string (optional)|本地yaml 文件路径，发起任务方式之一
|pipelineid| string (optional)|pipeline模板的ID，发起任务方式之一
|param| dict (optional)|工作流运行参数 如{"epoch":100}
|disabled| string (optional) |不需要运行的多个步骤，用逗号分割节点名称，如"step1,step2"
|dockerenv| string (optional) |镜像的url或镜像tar包在fs的路径

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回runid

### 工作流列表
```python
ret, response = client.list_run()
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (optional)|共享存储名称，传入只会list出对应fsname的run 
|username| string (optional)|用户名称，传入只会list出指定用户的run 
|runid| string (optional)|runid，传入只会list出指定的run
|runname| string (optional) |run的名称，传入只会list出拥有对应名称的run
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100
|marker| string (optional)| 下一页的起始位置，传入展示下一页，

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回run_list列表，每个元素的类型为PipelineInfo，可以参考下面PipelineInfo类的定义获取对应的成员变量。
|marker| string| 存在返回下一页的起始string，否则返回null

```python
class RunInfo(object):
    """the class of RunInfo info"""   

    def __init__(self, runId, fsname, username, status, name, desc, entry, param,
                 runYaml, runtime, postProcess, dockerEnv, updateTime, source, runMsg, createTime,
                 activateTime):
        """init """
        self.runId = runId
        self.fsname = fsname
        self.username = username
        self.status = status
        self.name = name
        self.desc = desc
        self.entry = entry
        self.param = param
        self.runYaml = runYaml
        self.runtime = runtime
        self.postProcess = postProcess
        self.dockerEnv = dockerEnv
        self.updateTime = updateTime
        self.source = source
        self.runMsg = runMsg
        self.createTime = createTime
        self.activateTime = activateTime
```
其中```job_info```字段在```list```接口下不会展现，会在详情```status```接口中进行赋值。
### 工作流详情
```python
ret, response = client.status_run("runid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|runid| string (required)|需要列出详情的runid

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回run信息，类型为RunInfo，可以参考上面RunInfo类的定义获取对应的成员变量。

返回的run信息中的job_info结构如下：
```python
class JobInfo(object):
    """ the class of job info"""
    def __init__(self, name, deps, parameters, command, env, status, start_time, end_time, dockerEnv, jobid):
        self.name = name
        self.deps = deps
        self.parameters = parameters
        self.command = command
        self.env = env
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.dockerEnv = dockerEnv
        self.jobId = jobid
```
### 工作流停止
```python
ret, response = client.stop_run("runid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|runid| string (required)|需要停止的runid
|force| bool (optional)|是否停止postProcess

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 工作流删除
```python
ret, response = client.delete_run("runid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|runid| string (required)|需要删除的runid

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 工作流重试
```python
ret, response = client.retry_run("runid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|runid| string (required)|需要重试的runid

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 工作流缓存列表显示
```python
ret, response = client.list_cache()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|userfilter| string (optional)|根据用户筛选工作流缓存，列表显示
|fsfilter| string (optional)|根据储存筛选工作流缓存，列表显示
|runfilter| string (optional)|根据run名称筛选工作流缓存，列表显示
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100
|marker| string (optional)| 下一页的起始位置，传入展示下一页，

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回run信息，类型为RunCacheInfo，可以参考下面RunCacheInfo类的定义获取对应的成员变量。
|marker| string| 存在返回下一页的起始string，否则返回null

返回信息response中的列表元素结构如下：
```python
class RunCacheInfo(object):
    """ the class of runcache info"""

    def __init__(self, cacheid, firstfp, secondfp, runid, source, step, fsname, username, expiredtime, strategy, custom, 
                createtime, updatetime):
        self.cacheid = cacheid
        self.firstfp = firstfp
        self.secondfp = secondfp
        self.runid = runid
        self.source = source
        self.step = step
        self.fsname = fsname
        self.username = username
        self.expiredtime = expiredtime
        self.strategy = strategy
        self.custom = custom
        self.createtime = createtime
        self.updatetime = updatetime
```

### 工作流缓存详情显示
```python
ret, response = client.show_cache("cacheid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|cacheid| string (required)|需要显示的cacheid详情

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None


### 工作流缓存删除
```python
ret, response = client.delete_cache("cacheid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|cacheid| string (required)|需要删除的cacheid详情

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 工作流运行产出列表展示
```python
ret, response = client.artifact()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|userfilter| string (optional)|根据用户筛选产出，列表显示
|fsfilter| string (optional)|根据储存筛选产出，列表显示
|runfilter| string (optional)|根据名称筛选产出，列表显示
|typefilter| string (optional)|根据类型名称筛选产出，列表显示
|pathfilter| string (optional)|根据路径名称筛选产出，列表显示
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100
|marker| string (optional)| 下一页的起始位置，传入展示下一页，

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 工作流模板创建
```python
ret, response = client.create_pipeline()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|共享存储名称
|yamlpath| string (required)|yaml 文件所在路径
|name| string (optional)|自定义工作流模板名称
|username| string (optional)|模板所属用户名称

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回pipeline名称
|id| string| 失败返回none，成功返回pipeline id

### 工作流模板列表显示
```python
ret, response = client.list_pipeline()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|userfilter| string (optional)|根据用户筛选工作流模板，列表显示
|fsfilter| string (optional)|根据储存筛选工作流模板，列表显示
|namefilter| string (optional)|根据pipeline名称筛选工作流模板，列表显示
|maxsize| int (optional,default=50)| 展示列表数量上限，默认值为100
|marker| string (optional)| 下一页的起始位置，传入展示下一页

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回run信息，类型为PipelineInfo，可以参考下面PipelineInfo类的定义获取对应的成员变量。
|marker| string| 存在返回下一页的起始string，否则返回null

返回信息response中的列表元素结构如下：
```python
class PipelineInfo(object):
    """the class of cluster info"""
    def __init__(self, pipelineid, name, fsname, username, pipelineyaml, pipelinemd5, 
                createtime, updatetime):
        """init """
        self.pipelineid = pipelineid
        self.name = name
        self.fsname = fsname
        self.username = username
        self.pipelineyaml = pipelineyaml
        self.pipelinemd5 = pipelinemd5
        self.createtime = createtime
        self.updatetime = updatetime
```

### 工作流模板详情显示
```python
ret, response = client.show_pipeline("pipelineid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|pipelineid| string (required)|工作流模板id，


#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回工作流模板对象，参考上面的PipelineInfo结构

### 工作流模板删除
```python
ret, response = client.delete_pipeline("pipelineid")
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|pipelineid| string (required)|工作流模板id，


#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回none

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

### 获取run任务下的运行日志
```python
ret, response = client.show_log("runid")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|runid| string (required)|需要展示运行日志的runid
|jobid| string (optional)|需要展示run下指定job的jobid
|pagesize| int (optional,default=100)|返回的日志内容的每页行数,默认为100
|pageno| int (optional,default=1)|返回的日志内容的页数,默认为1
|logfileposition| string (optional,default=end)|读取日志的顺序,从最开始位置读取为begin,从末尾位置读取为end,默认从尾部开始读取

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回LogInfo的List

response中具体LogInfo结构如下：
```python
class LogInfo(object):

    """the class of log info"""

    def __init__(self, runid, jobid, taskid, has_next_page, truncated, pagesize, pageno, log_content):
        """init """
        # 作业run的id
        self.runid = runid
        # run下子job的id
        self.jobid = jobid
        # job下子task的id
        self.taskid = taskid
        # 日志内容是否还有下一页，为true时则有下一页，否则为最后一页
        self.has_next_page = has_next_page
        # 日志内容是否被截断，为true时则被截断，否则未截断
        self.truncated = truncated
        # 每页日志内容的行数
        self.pagesize = pagesize
        # 日志内容的页数
        self.pageno = pageno
        # 具体的日志内容
        self.log_content = log_content
```
