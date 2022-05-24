### 工作流创建
```python
ret, response = client.create_run("fsname")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|存储系统名称
|username| string (optional)|指定用户，用于root账号运行特定用户的fs的工作流
|runname| string (optional)|工作流名称
|desc| string (optional)|工作流描述
|entry| string (optional)|工作流运行入口
|runyamlpath| string (optional)|指定的yaml 文件路径
|runyamlraw| string (optional)|本地yaml 文件路径
|param| dict (optional)|工作流运行参数 如{"epoch":100}

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
|fsname| string (optional)|存储系统名称，传入只会list出对应fsname的run 
|username| string (optional)|用户名称，传入只会list出指定用户的run 
|run_id| string (optional)|run_id，传入只会list出指定的run
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
                 run_yaml, runtime, imageUrl, update_time, source, runMsg, createTime,
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
        self.run_yaml = run_yaml
        self.runtime = runtime
        self.imageUrl = imageUrl
        self.update_time = update_time
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
    def __init__(self, name, deps, parameters, command, env, status, start_time, end_time, image, jobid):
        self.name = name
        self.deps = deps
        self.parameters = parameters
        self.command = command
        self.env = env
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.image = image
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
ret, response = client.show_cache("cacheid")
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
|fsname| string (required)|存储系统名称，传入只会list出对应fsname的run 
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
