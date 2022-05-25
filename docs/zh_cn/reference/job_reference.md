# paddleflow job 命令参考

paddleflow基本的操作命令可以帮助您更好的上手使用，本页面提供所有的job相关命令的详细参考

## 单作业任务管理

`paddleflow job` 提供了`create`,`show`, `list`, `delete`, `stop`五种不同的方法。 五种不同操作的示例如下：

```bash
paddleflow job list -s(--status) status -t(--timestamp) timestamp  -st(--starttime) starttime -q(--queue) queue -l(--labels) k=v -m(--maxkeys) maxkeys -mk(--marker) marker -fl(--fieldlist) f1,f2 //列出所有的作业 （通过status 列出指定状态的作业;通过timestamp 列出该时间戳后有更新的作业；通过starttime 列出该启动时间后的作业；通过queue 列出该队列下的作业；通过labels 列出具有该标签的作业；通过maxkeys列出指定数量的作业；从marker列出作业；通过fieldlist 列出作业的指定列信息）
paddleflow job show jobid -fl(--fieldlist) f1,f2 // 展示一个作业的详细信息(通过fieldlist 列出作业的指定列信息)
paddleflow job delete jobid  //删除一个作业
paddleflow job create jobtype:required（必须）作业类型(single, distributed, workflow) jsonpath:required(必须) 提交作业的配置文件 // 创建作业
paddleflow job stop jobid  // 停止一个作业
```
### 相关参数说明

list方法
```bash
status参数支持筛选指定状态的作业，其中具体的状态包括（init， pending， running， failed， succeeded， terminating， terminated， cancelled， skipped）
timestamp参数传入具体的时间戳，支持筛选指定时间戳后有更新的作业
starttime参数传入时间字符串参数（"2006-01-02 15:04:05"），支持筛选指定启动时间后的作业
queue参数传入指定队列下的作业
labels参数支持筛选指定labels的作业
maxkeys参数列出指定数量的作业
marker参数指从marker列出作业
fieldlist参数展示指定列的作业信息，具体的列名包括（"id"[作业id];"name"[作业名称];"queue"[队列名称];"status"[作业状态];"acceptTime"[作业接收时间];"startTime"[作业启动时间];"finishTime"[作业结束时间];"user"[作业创建者];"runtime"[单机作业运行详情];"distributedRuntime"[分布式作业运行详情];"workflowRuntime"[工作流作业运行详情];"message"[作业状态说明信息];"labels"[作业标签];"annotations"[作业注释];"priority"[作业优先级];"flavour"[作业套餐];"fileSystem"[作业存储资源];"extraFileSystems"[作业数据存储资源];"image"[作业镜像];"env"[作业环境变量];"command"[作业启动命令];"args"[启动参数];"port"[端口];"extensionTemplate"[作业使用的k8s对象模版];"framework"[作业框架];"members"[作业成员信息]）

```

show方法

```bash
fieldlist参数展示指定列的作业信息，具体的列名包括（"id"[作业id];"name"[作业名称];"queue"[队列名称];"status"[作业状态];"acceptTime"[作业接收时间];"startTime"[作业启动时间];"finishTime"[作业结束时间];"user"[作业创建者];"runtime"[单机作业运行详情];"distributedRuntime"[分布式作业运行详情];"workflowRuntime"[工作流作业运行详情];"message"[作业状态说明信息];"labels"[作业标签];"annotations"[作业注释];"priority"[作业优先级];"flavour"[作业套餐];"fileSystem"[作业存储资源];"extraFileSystems"[作业数据存储资源];"image"[作业镜像];"env"[作业环境变量];"command"[作业启动命令];"args"[启动参数];"port"[端口];"extensionTemplate"[作业使用的k8s对象模版];"framework"[作业框架];"members"[作业成员信息]）

```

create方法

```bash
jobtype参数指创建作业的类型，目前支持single（单机作业），distributed（分布式作业），workflow（工作流作业）
jsonpath参数指定作业json配置文件的路径，其中配置文件中各参数说明如下JobSpec各字段所示

```
JobSpec

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|id| string (optional)|作业id
|name| string (optional)|作业名称
|labels|  Map[string]string(optional)|作业标签
|annotations| Map[string]string(optional)|作业注释
|schedulingPolicy| SchedulingPolicy(required)|作业调度策略
|flavour| Flavour(optional)|作业资源套餐
|fileSystem| FileSystem(optional)|作业存储资源
|extraFileSystems| List<FileSystem>(optional)|作业数据存储资源
|image| string(required)|作业存储资源
|env| Map[string]string(optional)|作业存储资源
|command| string(optional)|作业启动命令
|args| List<string>(optional)|作业启动参数
|port| int(optional)|作业启动端口
|extensionTemplate| Map[string]string(optional)|作业使用的k8s对象模版完整的JSON对象
|framework| string(optional)|作业框架（分布式作业填写）
|members| List <MemberSpec>(optional)|分布式作业成员信息

SchedulingPolicy

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|queue| string (required)|作业所在队列
|priority| string (optional)|作业优先级（High、Normal、Low)）默认为Normal


MemberSpec

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|replicas| int (required)|作业的副本数
|role| string (required)|作业的角色，pserver、pworker、worker(Collective模式)


Flavour

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|name| string (optional)|套餐实例名称，可以是注册的套餐实例名称，也可以是自定义套餐实例（customFlavour或不填）
|cpu| string (optional)|cpu个数，当套餐实例名称为自定义时，有效
|mem| string (optional)|mem大小，当套餐实例名称为自定义时，有效
|scalarResources| Map[string]string (optional)|套餐实例的可扩展资源，例如nvidia.com/gpu、baidu.com/v100_cgpu、baidu.com/v100_cgpu_core等，当套餐名称为自定义时，有效


FileSystem

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|name| string (optional)|存储名称，可以是PaddleFlow的 fs name
|mountPath| string (optional)|Pod内的挂载路径，默认路径是 /home/paddleflow/storage/mnt
|subPath| string (optional)|需要挂在存储的子路径
|readOnly| bool (optional)|挂载之后的存储权限





### 示例

作业任务创建：用户输入```paddleflow job create jobtype jsonpath```，界面上显示

```bash
job create success, id[job-id]

```



作业任务列表显示：用户输入```paddleflow job list```，界面上显示

```bash
+------------------------------+-----------------------+----------+------------+---------------------+---------------------+---------------------+
| job id                       | job name              | queue    | status     | accept time         | start time          | finish time         |
+==============================+=======================+==========+============+=====================+=====================+=====================+
| job-2d7655af86c94de4         | zzc-test              |          | terminated | 2022-04-28 15:53:27 |                     | 2022-05-07 16:19:07 |
+------------------------------+-----------------------+----------+------------+---------------------+---------------------+---------------------+
| job-72b87012e7c7479c         |                       |          | succeeded  | 2022-04-28 16:01:56 | 2022-04-28 16:02:39 | 2022-04-28 16:12:39 |
+------------------------------+-----------------------+----------+------------+---------------------+---------------------+---------------------+
| job-86ad5a13455d4b77         |                       |          | succeeded  | 2022-04-28 16:23:01 | 2022-04-28 16:23:46 | 2022-04-28 16:41:09 |
+------------------------------+-----------------------+----------+------------+---------------------+---------------------+---------------------+
| job-c50cb1876f6848ba         |                       |          | succeeded  | 2022-04-28 16:23:31 | 2022-04-28 16:24:16 | 2022-04-28 16:41:09 |
+------------------------------+-----------------------+----------+------------+---------------------+---------------------+---------------------+
| job-f07cb6b1d18d4478         |                       |          | succeeded  | 2022-04-28 16:25:39 | 2022-04-28 16:26:20 | 2022-04-28 16:41:09 |
+------------------------------+-----------------------+----------+------------+---------------------+---------------------+---------------------+
| job-7b459496f9a0431c         |                       | dzz      | succeeded  | 2022-04-28 18:46:24 | 2022-04-28 18:47:08 | 2022-04-28 18:57:08 |
```


作业任务详情显示：用户输入```paddleflow job show jobid```，界面上显示
```bash
+----------------------+------------+---------+----------+---------------------+--------------+---------------------+
| job id               | job name   | queue   | status   | accept time         | start time   | finish time         |
+======================+============+=========+==========+=====================+==============+=====================+
| job-050093a1b8e54886 | zzc-test   | dzz     | failed   | 2022-05-18 11:45:30 |              | 2022-05-18 11:45:38 |
+----------------------+------------+---------+----------+---------------------+--------------+---------------------+
```

作业任务删除：用户输入```paddleflow job delete jobid```，界面上显示

```bash
job[job-id] delete success
```


作业任务停止：用户输入```paddleflow job stop jobid```，界面上显示

```bash
job[job-id] stop success
```