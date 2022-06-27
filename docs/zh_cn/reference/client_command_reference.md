# PaddleFlow命令参考

PaddleFlow基本的操作命令可以帮助您更好的上手使用，本页面提供所有的命令的详细参考
PaddleFlow-cli 是基于PaddleFlow python-sdk 上层封装的命令行工具。支持用户管理(`user`)、队列管理(`queue`)、 存储管理(`fs`)以及工作流(`run`)的命令行操作。

## 环境依赖

* python_requires >= 3.6.1
* click==7.1.2
* tabulate==0.8.9
* setuptools >= 21.0.0

## 概览

在安装PaddleFlow之后，在终端输入`paddleflow`并执行，就能看到所有可用的命令，同时，每个命令后面添加`--help`能获得该命令的详细帮助信息

```bash
$ paddleflow --help

Usage: paddleflow [OPTIONS] COMMAND [ARGS]...

  paddleflow is the command line interface to paddleflow service.

  provide `user`, `queue`, `fs`, `run`, `pipeline`, `cluster` operation
  commands

Options:
  --pf_config TEXT       the path of default config.
  --output [table|json]  The formatting style for command output.  [default:
                         table]

  --help                 Show this message and exit.

Commands:
  cluster   manage cluster resources
  fs        manage fs resources
  log       manage log resources
  pipeline  manage pipeline resources
  queue     manage queue resources
  run       manage run resources
  user      manage user resources
```

## 命令行规范

命令行使用多层命令结构，命令行以 `paddleflow` 为开头，其中 `<>` 表示必填，`[]` 表示可选；
`[options]` 表示支持的选项参数，`<submodule>` 表示支持的模块子命令，例如`fs`、`queue`等，
`<verb>` 表示执行的操作动作，`<parameters>` 表示必填参数。

```
paddleflow [options] [<submodule> <verb> <parameters> [options]]
```

## 如何使用

安装paddleflow python sdk的过程中会同步完成paddleflow cli 的安装流程。用户在安装完成后只需要完成配置文件的配置，即可开始paddleflow cli的使用之旅。

### 配置文件说明

```bash
[user]
name = 账户名
password = 账户密码
[server]
# paddleflow server 主机地址
paddleflow_server_host = 127.0.0.1
# paddleflow server 端口
paddleflow_server_port = 8999         
```

其中，`paddleflow_server_port`,不是必须填写选择，如果用户在使用过程中没有调整过`paddleflow server`服务的端口，则不需要进行填写。 `paddleflow cli` 会使用默认端口进行初始化操作。

### 配置文件地址

配置文件的地址建议放在`${HOME}/.paddleflow/`目录下，文件名即为`paddleflow.ini`。例如：`work`账号即放置在`/home/work/.paddleflow/paddleflow.ini`
。用户也可自行选择路径，后续的使用过程中则需要通过`--pf_config`指定config文件的地址。

## 用户管理

`user` 提供了`add`,`delete`, `list`, `set`四种不同的方法。 四种不同操作的示例如下：

```bash
paddleflow user add name password  //新增用户 仅root账号可以使用
paddleflow user delete name //删除用户 仅root账号可以使用
paddleflow user set name password // 用户密码更新
paddleflow user list // 用户列表展示 仅root账号可以使用
```

### 示例

新增用户：```paddleflow user add test  pass****```。成功添加后界面上显示:

```user[test] add success```

删除用户： root账号输入```paddleflow user delete ****```。成功删除后界面上显示

```user[****] delete success```

用户密码更新： 输入```paddleflow user set test pass***```。成功添加后界面上显示:

```
user[test] update success
```

用户列表展示： root账号输入 ```paddleflow user list``` 可以在界面上看到当前系统中用户的信息

```
+------------+---------------------------+
| name       | create time               |
+============+===========================+
| aaa        | 2021-09-01T22:51:33+08:00 |
+------------+---------------------------+
| aaa1       | 2021-09-01T23:11:06+08:00 |
+------------+---------------------------+
| aaa3       | 2021-09-01T23:11:25+08:00 |
+------------+---------------------------+
```

## 队列管理

`queue` 提供了`create`,`delete`, `list`, `show`,`stop`,`grantl`,`ungrant`,`grantlist`八种不同的方法。 八种不同操作的示例如下：

```bash
paddleflow queue list // 队列展示 
paddleflow queue show queue-name // 队列详情展示
paddleflow queue stop queue-name // 队列停止
paddleflow queue delete queue-name // 队列删除
paddleflow queue create name namespace cpu men clustername// 队列创建(自定义队列名称；命名空间；最大cpu；最大内存；集群名称)
paddleflow queue grant username queue-name // 队列授权 仅root账号可以使用
paddleflow queue ungrant username queue-name // 队列取消授权 仅root账号可以使用
paddleflow queue grantlist // 队列授权展示 仅root账号可以使用
```

### 示例

队列列表： 用户输入 ```paddleflow queue list ``` 可以在界面上看到当前用户有权限看到的队列集合

```
+--------+----------+-------------------------------+-------------------------------+
| name   | status   | create time                   | update time                   |
+========+==========+===============================+===============================+
| aa     | closed   | 2021-09-01T23:12:14.824+08:00 | 2021-09-01T23:12:14.824+08:00 |
+--------+----------+-------------------------------+-------------------------------+
| aa1    | open     | 2021-09-02T21:14:14.509+08:00 | 2021-09-02T21:14:14.509+08:00 |
+--------+----------+-------------------------------+-------------------------------+
marker: none
```

队列详情： 用户输入 ```paddleflow queue show aa ``` 可以在界面上看到队列`aa`的详细信息

```
+--------+----------+-------------+-------+-------+--------------------+-------------------------------+-------------------------------+
| name   | status   | namespace   | mem   |   cpu | scalar resources   | create time                   | update time                   |
+========+==========+=============+=======+=======+====================+===============================+===============================+
| aa     | closed   | default     | 1Mi   |     1 | none               | 2021-09-01T23:12:14.824+08:00 | 2021-09-01T23:12:14.824+08:00 |
+--------+----------+-------------+-------+-------+--------------------+-------------------------------+-------------------------------+
```

队列创建：用户输入 ```paddleflow queue create name namespace cpu men cluster```，创建成功后可以在界面上看到

```queue[queuename] create  success```

队列停止：用户输入 ```paddleflow queue stop queuename```，停止成功后可以在界面上看到

```queue[queuename] stop  success```


队列删除：用户输入 ```paddleflow queue delete queuename```，删除成功后可以在界面上看到（只能在队列stop之后或状态为closed情况下使用）

```queue[queuename] delete  success```



队列授权： root账号输入 ```paddleflow queue grant username queuename```。授权成功后可以在界面上看到

```queue[queuename] add username[username] success```

队列取消授权：root账号输入 ```paddleflow queue ungrant username queuename```。授权成功后可以在界面上看到

```queue[queuename] delete username[username] success```

队列授权信息展示：root账号输入```paddleflow  queue grantlist```。可以在界面上看到当前系统中授权信息列表

```
+-------------+--------------+
| user name   | queue name   |
+=============+==============+
| aaa3        | aa           |
+-------------+--------------+
| binbin      | aa           |
+-------------+--------------+
| binbin      | aa1          |
```


## flavour管理

`flavour` 提供了 `list`一种不同的方法。 操作的示例如下：

```bash
paddleflow flavour list //列表显示flavour
```

flavour列表显示：用户输入```paddleflow grant flavour```，界面上显示

```bash
+=======+=======+==========+=================================================================+
|     1 | 1Gi   | flavour1 |                                                                 |
+-------+-------+----------+-----------------------------------------------------------------+
|     4 | 8Gi   | flavour2 | {'nvidia.com/gpu': '1'}                                         |
+-------+-------+----------+-----------------------------------------------------------------+
|     4 | 8Gi   | flavour3 | {'nvidia.com/gpu': '2'}                                         |
+-------+-------+----------+-----------------------------------------------------------------+

```


## 存储管理

`fs` 提供了`create`,`show`, `delete`, `list`, `mount`，`link`,`unlink`, `listlink`, `showlink`九种不同的方法。 九种不同操作的示例如下：

```bash
paddleflow fs list -u username// 展示fs列表 -u 表示特定用户的fs
paddleflow fs show fsname -u username// 显示某个fs详情 -u 表示特定用户的fs
paddleflow fs delete fsname -u username // 删除某个fs -u 表示特定用户的fs 
paddleflow fs create fsname url -o ak=xxx -o sk=xxx //创建fs 
paddleflow fs mount fsname /home/mountpath -u username //把某个文件系统挂载到本地 
paddleflow fs link fsname fspath url -o ak=xxx -o sk=xxx -u username //创建link -u 表示特定用户的fs
paddleflow fs unlink fsname fspath -u username // 删除某个特定用户特点文件系统下的link -u 表示特定用户的fs
paddleflow fs listlink fsname -u username// 展示某个文件系统下面的link列表 -u 表示特定用户的fs
paddleflow fs showlink fsname fspath -u username// 显示某个link详情 -u 表示特定用户的fs
```

### 示例

存储列表：用户输入```paddleflow fs list```

```bash
+----------+---------+--------+-----------------------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| name     | owner   | type   | server address                    | sub path           | properties                                                                                                                                                                                                                                  |
+==========+=========+========+===================================+====================+=============================================================================================================================================================================================================================================+
| sftp1236 | root    | sftp   | localhost:8001                 | /data2             | {'password': 'xxx', 'user': 'xxx'}                                                                                                                                                                             |
+----------+---------+--------+-----------------------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| elsiefs1 | root    | hdfs   | 192.168.1.2:9000,192.168.1.3:9000 | /elsiefs           | {'dfs.namenode.address': '192.168.1.2:9000,192.168.1.3:9000', 'group': 'test', 'user': 'test'}
```

存储详情：用户输入```paddleflow fs show {fs_name}```，界面上显示

```bash
+--------+---------+--------+------------------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| name   | owner   | type   | server address   | sub path   | properties                                                                                                                                                                                                                                  |
+========+=========+========+==================+============+=============================================================================================================================================================================================================================================+
| s3-1   | root    | s3     | s3.com | /myfs1     | {'accessKey': 'xxx', 'bucket': 'test', 'endpoint': 's3.com', 'region': 'bj', 'secretKey': 'xxx'} |
+--------+---------+--------+------------------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

创建存储：用户输入```paddleflow fs create {fs_name} {fs_url} -o {}```，可以通过```paddleflow fs create --help```查看具体创建选项,界面上显示
```fs[{fs_name}] create success```

删除存储：用户输入```paddleflow fs delete {fs_name}```，界面上显示
```fs[{fs_name}] delete success```

mount命令：用户输入```paddleflow fs mount {fs_name} {mountpath}```，界面上显示
```mount success```

某个文件系统下的Link列表：用户输入```paddleflow fs listlink {fsname}```

```bash
+----------+---------+--------+------------+------------------+--------------------+-------------------+
| name     | owner   | type   | fs path    | server address   | sub path           | properties        |
+==========+=========+========+============+==================+====================+===================+
| local12  | root    | local  | /linktest1 |                  | /home/yoursubpath/1| {'debug': 'true'} |
+----------+---------+--------+------------+------------------+--------------------+-------------------+
| local12  | root    | local  | /linktest2 |                  | /home/yoursubpath/1| {'debug': 'true'} |
+----------+---------+--------+------------+------------------+--------------------+-------------------+
```

某个Link详情：用户输入```paddleflow fs showlink {fsname} {fspath}```，界面上显示

```bash
+----------+---------+--------+------------+------------------+--------------------+-------------------+
| name     | owner   | type   | fs path    | server address   | sub path           | properties        |
+==========+=========+========+============+==================+====================+===================+
| local12  | root    | local  | /linktest1 |                  | /home/yoursubpath/7| {'debug': 'true'} |
+----------+---------+--------+------------+------------------+--------------------+-------------------+
```

创建link：用户输入```paddleflow fs link {fs_name} {fspath} {fs_url} -o {}```，可以通过```paddleflow fs link --help```查看具体创建选项,界面上显示
```fs[{fs_name}] create link success```

删除link：用户输入```paddleflow fs unlink {fs_name} {fspath}```，界面上显示
```fs[{fs_name}] delete link success```

### 工作流运行管理

`run` 提供了`create`, `list`, `status`, `stop`, `retry`, `delete`, `listcache`, `showcache`, `delcache`, `artifact`十种不同的方法。 十种不同操作的示例如下：

```bash
paddleflow run create -f(--fsname) fs_name -n(--name) run_name  -d(--desc) xxx -u(--username) username -p(--param) data_file=xxx -p regularization=*** -yp(--runyamlpath) ./run.yaml -pplid(--pipelineid) ppl-000666 -yr(runyamlraw) xxx --disabled some_step_names -de(--dockerenv) docker_env // 创建pipeline作业，-yp、-pplid、yr为3中发起任务的方式，每次只能使用其中一种
paddleflow run list -f(--fsname) fsname -u(--username) username -r(--runid) runid -n(--name) name -m(--maxsize) 10 -mk(--marker) xxx // 列出所有运行的pipeline （通过fsname 列出特定fs下面的pipeline；通过username 列出特定用户的pipeline（限root用户）;通过runid列出特定runid的pipeline; 通过name列出特定name的pipeline）
paddleflow run status runid // 展示一个pipeline下面的详细信息，包括job信息列表
paddleflow run stop runid -f(--force) // 停止一个pipeline
paddleflow run retry runid // 重跑一个pipeline
paddleflow run delete runid // 删除一个运行的工作流
paddleflow run listcache -u(--userfilter) username -f(--fsfilter) fsname -r(--runfilter) run-000666 -m(--maxsize) 10 -mk(--marker) xxx // 列出搜有的工作流缓存
paddleflow run showcache cacheid // 显示工作流缓存详情
paddleflow run delcahce cacheid // 删除指定工作流缓存
paddleflow run artifact -u(--userfilter) username -f(--fsfilter) fsname -r(runfilter) run-000666 -t(--typefilter) type -p(--pathfilter) path -m(--maxsize) 10 -mk(--marker) xxx // 列出所有工作流产出
```

### 示例

创建工作流：用户输入```paddleflow run create -f {fs_name} -n {run_name} -d {main} -yp {yaml_path}```发起一次pipeline任务，界面上能够返回对应的```runid```信息。

```bash
run[{run_name}] create success with runid[{runid}]
```

> 由于创建工作流功能较为复杂，下面对该功能展开讲解

参数介绍：

|参数名称 | 是否必填 | 参数含义
|:---:|:---:|:---|
|-f --fsname | optional | 存储名称
|-n --name | optional | 任务名称
|-d --desc | optional | 任务描述
|-u --username | optional | 用户名，仅当登录用户为root时可以填写
|-p --param | optional | 用于进行参数替换
|--disabled | opitonal | 用于指定不需要运行的节点
|-de --dockerenv| optional | 用于指定全局DockerEnv，可以为镜像的url或镜像tar包在fs的路径
|-yp --runyamlpath | optional | 任务发起方式之一，fs下yaml文件的路径
|-yr --runyamlraw | optional | 任务发起方式之一，base64编码的yaml文件内容
|-pplid --pipelineid | opitonal | 任务发起方式之一，工作流模板的ID，如何创建工作流模板请查看后文工作流模板的相关内容

创建工作流至少需要提供1个参数，且必须是 -yp/-yr/-pplid 中的一个，这个参数用来指定创建工作刘的方法，而对于-yp对应的方法，还必须再指定 -f 参数。其他参数则均为选填。

创建工作流的三种方法示例：

1. runyamlpath：

```bash
paddleflow run create -f testfs -yp ./run.yaml
```

上面的命令中，-f 和 -yp 都是必须要填写的参数。

下面给出一个run.yaml的内容示例：

> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/base_pipeline示例。
> 
> 示例链接：[base_pipeline][base_pipeline]

```yaml
name: base_pipeline

entry_points:
  preprocess:
    command: bash base_pipeline/shells/data.sh {{data_path}}
    docker_env: kfp_mysql:1.7.0
    env:
      PF_JOB_FLAVOUR: flavour1
      PF_JOB_MODE: Pod
      PF_JOB_QUEUE_NAME: ppl-queue
      PF_JOB_TYPE: vcjob
      USER_ABC: 123_{{PF_USER_NAME}}
    parameters:
      data_path: ./base_pipeline/data/{{PF_RUN_ID}}

  train:
    command: bash base_pipeline/shells/train.sh {{epoch}} {{train_data}} {{model_path}}
    deps: preprocess
    env:
      PF_JOB_FLAVOUR: flavour1
      PF_JOB_MODE: Pod
      PF_JOB_QUEUE_NAME: ppl-queue
      PF_JOB_TYPE: vcjob
    parameters:
      epoch: 5
      model_path: ./output/{{PF_RUN_ID}}
      train_data: '{{preprocess.data_path}}'

  validate:
    command: bash base_pipeline/shells/validate.sh {{model_path}}
    deps: train
    env:
      PF_JOB_FLAVOUR: flavour1
      PF_JOB_MODE: Pod
      PF_JOB_QUEUE_NAME: ppl-queue
      PF_JOB_TYPE: vcjob
    parameters:
      model_path: '{{train.model_path}}'

parallelism: 1

docker_env: nginx:1.7.9
```

2. runyamlraw:

用户可以自行将yaml文件的内容进行base64转码，然后通过如下命令发起任务：

```bash
paddleflow run create -yr {{base64yaml}}
```

其中 {{base64yaml}} 为将yaml文件的内容进行base64转码后的结果。

> 需要注意的是，如果使用这种方法，或者下面马上要介绍的pipelineid方法，-f 就不是必须的了，但依赖fs的功能则无法使用，如 Artifact。

3. pipelineid:

用户可以先创建工作流模板，具体方法见下文的[工作流模板管理](#工作流模板管理)相关内容，然后通过工作流模板的ID，来发起任务，具体如下：

```bash
paddleflow run create -pplid ppl-000666
```

工作流列表：用户输入```paddleflow run list```，界面上能够显示出所有工作流列表信息,marker下一页的起始位，-mk --marker 参数使用

```bash
+------------+-----------+------------+----------+--------+
| run id     | fs name   | username   | status   | name   |
+============+===========+============+==========+========+
| run-000001 | sftp1235  | root       | failed   |        |
+------------+-----------+------------+----------+--------+
| run-000002 | sparkpi1  | root       | failed   |        |
+------------+-----------+------------+----------+--------+
| run-000003 | sftp1235  | root       | failed   |        |
+------------+-----------+------------+----------+--------+
marker: None
```

工作流详情：用户输入```paddleflow run status run-cb96cf93(run id)```，界面上能够显示出对应工作流的详细信息

```bash
+------------+----------+--------+--------+---------+---------+---------------------+---------------------+
| run id     | status   | name   | desc   | entry   | param   | start time          | update time         |
+============+==========+========+========+=========+=========+=====================+=====================+
| run-000002 | failed   |        |        |         |         | 2021-09-13 15:39:40 | 2021-09-13 15:39:40 |
+------------+----------+--------+--------+---------+---------+---------------------+---------------------+
+----------------------------------------------------------------------------------------------+
| run yaml detail                                                                              |
+==============================================================================================+
| name: spark_pi                                                                               |
|                                                                                              |
| docker_env: xxx                                                                              |
|                                                                                              |
| entry_points:                                                                                |
|   main:                                                                                      |
|     env:                                                                                     |
|       Omitted here                                                                           |
+----------------------------------------------------------------------------------------------+
Job Details
-------------
+----------+--------+----------+--------+--------------+------------+-------------+
| job id   | name   | status   | deps   | start time   | end time   | image       |
+==========+========+==========+========+==============+============+=============+
|          | main   | failed   |        |              |            | nginx:1.7.9 |
+----------+--------+----------+--------+--------------+------------+-------------+

```

作业停止：用户输入```paddleflow run  stop {runid}```，界面上显示

```bash
run[runid] stop success
```

作业重试：用户输入```paddleflow run  retry {runid}```，界面上显示

```bash
run[runid] retry success
```

作业删除：用户输入```paddleflow run  delete {runid}```，界面上显示

```bash
run[runid] delete success
```

作业缓存列表显示：用户输入```paddleflow run  listcache```，界面上显示

```bash
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cache id   | run id     | step       | fsname   | username   |   expired time | create time         | update time         |
+============+============+============+==========+============+================+=====================+=====================+
| cch-000001 | run-000064 | preprocess | mxy      | root       |            400 | 2021-12-10 09:56:42 | 2021-12-10 09:56:42 |
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cch-000002 | run-000064 | train      | mxy      | root       |            400 | 2021-12-10 09:57:00 | 2021-12-10 09:57:00 |
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cch-000003 | run-000064 | validate   | mxy      | root       |            400 | 2021-12-10 09:57:12 | 2021-12-10 09:57:12 |
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cch-000005 | run-000076 | train      | zzc      | root       |            400 | 2021-12-10 11:51:04 | 2021-12-10 11:51:04 |
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
marker: 075edd00394b91e6af9d3e02acecc722

```

作业缓存详情显示：用户输入```paddleflow run  showcache {cacheid}```，界面上显示

```bash
+------------+------------+------------+----------+------------+----------------+--------------+----------+---------------------+---------------------+
| cache id   | run id     | step       | fsname   | username   |   expired time | strategy     | custom   | create time         | update time         |
+============+============+============+==========+============+================+==============+==========+=====================+=====================+
| cch-000001 | run-000064 | preprocess | mxy      | root       |            400 | conservative |          | 2021-12-10 09:56:42 | 2021-12-10 09:56:42 |
+------------+------------+------------+----------+------------+----------------+--------------+----------+---------------------+---------------------+
+------------------------------------------------------------------+------------------------------------------------------------------+
| first fp                                                         | second fp                                                        |
+==================================================================+==================================================================+
| xxx                                                              | xxx                                                              |
+------------------------------------------------------------------+------------------------------------------------------------------+

```

作业缓存删除：用户输入```paddleflow run  delcache {cacheid}```，界面上显示

```bash
runcache[cacheid] delete success
```

工作流产出显示：用户输入```paddleflow run  artifact```，界面上显示

```bash
+------------+----------+------------+-------------------+------------+--------+-----------------+--------+---------------------+---------------------+
| run id     | fsname   | username   | artifact path     | type       | step   | artifact name   | meta   | create time         | update time         |
+============+==========+============+===================+============+========+=================+========+=====================+=====================+
| run-000043 | xiangy2  | root       | ./data/run-000043 | preprocess | input  |                 |        | 2021-12-10 07:55:28 | 2021-12-10 07:55:28 |
+------------+----------+------------+-------------------+------------+--------+-----------------+--------+---------------------+---------------------+
| run-000043 | xiangy2  | root       | ./data/run-000043 | preprocess | input  |                 |        | 2021-12-10 07:55:28 | 2021-12-10 07:55:28 |
+------------+----------+------------+-------------------+------------+--------+-----------------+--------+---------------------+---------------------+
| run-000265 | xiangy2  | root       | ./data/           | preprocess | input  |                 |        | 2021-12-10 08:08:08 | 2021-12-13 11:28:09 |
+------------+----------+------------+-------------------+------------+--------+-----------------+--------+---------------------+---------------------+
| run-000047 | xiangy2  | root       | ./data/run-000047 | preprocess | input  |                 |        | 2021-12-10 08:11:52 | 2021-12-10 08:18:14 |
+------------+----------+------------+-------------------+------------+--------+-----------------+--------+---------------------+---------------------+
marker: f990bc858cbd2a8d5eae9243970a2d8c

```


### 工作流模板管理

`pipeline` 提供了`create`,`show`, `list`, `delete`四种不同的方法。 四种不同操作的示例如下：

```bash
paddleflow pipeline create  fsname:required（必须） yamlpath:required(必须)  -n(--name)  pipeline_name -u(--username) username // 创建pipeline模板(指定创建的pipeline模板名称；指定模板的用户)
paddleflow pipeline list -u(--userfilter) user -f(--fsfilter) fsname -n(--namefilter) pipeline_name -m(--maxkeys) int -mk(--marker) xxx // 列出所有的pipeline模板 （通过username 列出特定用户的pipeline模板（限root用户）;通过fsname 列出特定fs下面的pipeline模板；通过pipelinename列出特定的pipeline模板；列出指定数量的pipeline模板；从marker列出pipeline模板）
paddleflow pipeline show pipelineid // 展示一个pipeline模板下面的详细信息，包括yaml信息
paddleflow pipeline delete  pipelineid // 删除一个pipeline模板 

```


工作流模板创建：用户输入```paddleflow pipeline create fsname yamlpyth```，界面上显示

```bash
pipeline[pipeline name] create  success, id[pipeline id]

```

工作流模板列表显示：用户输入```paddleflow pipeline list```，界面上显示

```bash
+---------------+---------+----------+------------+----------------------------------+---------------------+---------------------+
| pipeline id   | name    | fsname   | username   | pipeline md5                     | create time         | update time         |
+===============+=========+==========+============+==================================+=====================+=====================+
| ppl-000015    | pip1111 | mxy      | root       | 658cb27d7010bfd844               | 2021-12-24 11:31:38 | 2021-12-24 11:31:38 |
+---------------+---------+----------+------------+----------------------------------+---------------------+---------------------+
marker: None

```

工作流模板详情显示：用户输入```paddleflow pipeline show {pipelineid}```，界面上显示

```bash
+---------------+---------+----------+------------+----------------------------------+---------------+---------------+
| pipeline id   | name    | fsname   | username   | pipeline md5                     | create time   | update time   |
+===============+=========+==========+============+==================================+===============+===============+
| ppl-000015    | pip1111 | mxy      | root       | 658cb27d7010bfd844b8e3cd37792cbe |               |               |
+---------------+---------+----------+------------+----------------------------------+---------------+---------------+
+-------------------------------------------------------------------------------------------------+
| pipeline yaml                                                                                   |
+=================================================================================================+
| name: myproject                                                                                 |
|                                                                                                 |
| docker_env: mock host                                                                           |
|                                                                                                 |
| entry_points:                                                                                   |
|                                                                                                 |
|   preprocess:                                                                                   |
|     parameters:                                                                                 |
|       data_path: "./data/"                                                                      |
|Omitted here                                                                                     |
+-------------------------------------------------------------------------------------------------+

```

工作流模板删除：用户输入```paddleflow pipeline delete pipelineid```，界面上显示

```bash
pipelineid[pipelineid] delete success

```


### 集群管理(仅限root用户使用)

`cluster` 提供了`create`,`show`, `list`, `delete`, `update`, `resource`六种不同的方法。 六种不同操作的示例如下：

```bash
paddleflow cluster list -cn(--clustername) cluster_name -cs(--clusterstatus) cluster_status  -m(--maxkeys) int -mk(--marker) xxx//列出所有的集群 （通过cluster_name 列出指定名称的集群;通过cluster_status 列出指定状态的集群；列出指定数量的集群；从marker列出集群）
paddleflow cluster show clustername // 展示一个集群的详细信息，包括credential凭证信息
paddleflow cluster delete  clustername  //删除一个集群
paddleflow cluster create  clustername:required（必须）集群名称 endpoint:required(必须) 节点 clustertype:required(必须) 集群类型 -c(--credential)  凭证文件绝对路径 -id(--clusterid) clusterid -d(--description) 描述 --source Source --setting setting --status status -ns(--namespacelist) namespacelist// 创建集群（自定义集群名称；集群的节点；集群的类型；集群认证的凭证信息，本地文件路径；自定义集群id;集群描述；集群源[AWS, CCE, etc];集群配置信息；集群状态；namespace列表，比如['NS1','NS2']，传入中括号的内容）
paddleflow cluster update  clustername:required（必须）集群名称 -e(--endpoint) 节点 -t(--clustertype) 集群类型 -c(--credential)  凭证文件绝对路径 -id(--clusterid) clusterid -d(--description) 描述 --source Source --setting setting --status status -ns(--namespacelist) namespacelist// 更新集群（需要更新的集群名称；集群的节点；集群的类型；集群认证的凭证信息，本地文件路径；自定义集群id;集群描述；集群源[AWS, CCE, etc];集群配置信息；集群状态；namespace列表，比如['NS1','NS2']，传入中括号的内容）
paddleflow cluster resource  -cn(--clustername)  cluster_name    // 列表显示所有集群剩余资源（显示指定集群的剩余资源）
```


集群创建：用户输入```paddleflow cluster create clustername, endpoint, clustertype```，界面上显示

```bash
cluster[cluster name] create  success, id[cluster id]

```

集群创建：用户输入```paddleflow cluster list```，界面上显示

```bash
+------------------+----------------+----------------------+------------------+----------+---------------------------+---------------------------+
| cluster id       | cluster name   | description          | cluster type     | status   | create time               | update time               |
+==================+================+======================+==================+==========+===========================+===========================+
| 由PF生成            | incididunt     | culpa ipsum pariatur | Duis             | online   | 2021-12-14T16:23:31+08:00 | 2021-12-17T12:59:55+08:00 |
+------------------+----------------+----------------------+------------------+----------+---------------------------+---------------------------+
| cluster-81cbff32 | test_zzc       | culpa ipsum pariatur | kubernetes-v1.16 | online   | 2021-12-14T16:32:03+08:00 | 2021-12-15T17:51:32+08:00 |
+------------------+----------------+----------------------+------------------+----------+---------------------------+---------------------------+
| cluster-461ac0b9 | test           | test                 | k8s1.16          | online   | 2021-12-15T17:53:32+08:00 | 2021-12-15T17:53:32+08:00 |
+------------------+----------------+----------------------+------------------+----------+---------------------------+---------------------------+
| 由PF生产            | test_whq       | culpa ipsum pariatur | kubernetes-v1.16 | offline  | 2021-12-20T14:24:40+08:00 | 2021-12-20T14:24:40+08:00 |
+------------------+----------------+----------------------+------------------+----------+---------------------------+---------------------------+
marker: 879629d9a18721b9a4d1ea6e875e6eaf

```

集群详情显示：用户输入```paddleflow cluster show clustername```，界面上显示

```bash
+----------------------------------+----------------+---------------+----------------+-----------+----------------+----------+-----------+------------------+---------------------------+---------------------------+
| cluster id                       | cluster name   | description   | endpoint       | source    | cluster type   | status   | setting   | namespace list   | create time               | update time               |
+==================================+================+===============+================+===========+================+==========+===========+==================+===========================+===========================+
| cluster-13797361fc624d7cb17c7635 | yyjtest        |               | http://0.0.0.0 | OnPremise | kusba          | online   |           |                  | 2021-12-29T10:15:41+08:00 | 2021-12-29T10:35:47+08:00 |
+----------------------------------+----------------+---------------+----------------+-----------+----------------+----------+-----------+------------------+---------------------------+---------------------------+
credential value:
------------------------
apiVersion: v1
clusters:
.......

```

集群删除：用户输入```paddleflow cluster delete clustername```，界面上显示

```bash
cluster[clustername] delete success

```

集群更新：用户输入```paddleflow cluster update clustername```，界面上显示

```bash
cluster[clustername] update success

```

获取集群资源列表显示：用户输入```paddleflow cluster resource```，界面上显示

```bash
+----------------+-------------------------------------------------------------+
| cluster name   | cluster info                                                |
+================+=============================================================+
| test_zzc       | {                                                           |
|                |     "nodeList": [                                           |
|                |         {                                                   |
|                |             "nodeName": "xxx.com",                          |
|                |             "schedulable": true,                            |
|                |             "total": {                                      |
|                |                 "cpu": ,                                    |
|                |                 "memory": "",                               |
|                |                 "storage": ""                               |
|                |             },                                              |
|                |             "idle": {                                       |
|                |                 "cpu": ,                                    |
|                |                 "memory": "",                               |
|                |                 "storage": ""                               |
|                |             }                                               |
|                |         },                                                  |
|                |         {                                                   |
|                |             "nodeName": "c-2l7plfgf-tpawy7pj",              |
|                |             "schedulable": true,                            |
|                |             "total": {                                      |
|                |                 "cpu": ,                                    |
|                |                 "memory": "",                               |
|                |                 "storage": "",                              |
|                |                 "scalarResources": {                        |
|                |                     "nvidia.com/gpu": ""                    |
|                |                 }                                           |
|                |             },                                              |
|                |             "idle": {                                       |
|                |                 "cpu": ,                                    |
|                |                 "memory": "",                               |
|                |                 "storage": "",                              |
|                |                 "scalarResources": {                        |
|                |                     "nvidia.com/gpu": ""                    |
|                |                 }                                           |
|                |             }                                               |
|                |         }                                                   |
|                |     ],                                                      |
|                |     "summary": {                                            |
|                |         "total": {                                          |
|                |             "cpu": ,                                        |
|                |             "memory": "",                                   |
|                |             "storage": "",                                  |
|                |             "scalarResources": {                            |
|                |                 "nvidia.com/gpu": ""                        |
|                |             }                                               |
|                |         },                                                  |
|                |         "idle": {                                           |
|                |             "cpu": ,                                        |
|                |             "memory": "",                                   |
|                |             "storage": "",                                  |
|                |             "scalarResources": {                            |
|                |                 "nvidia.com/gpu": ""                        |
|                |             }                                               |
|                |         }                                                   |
|                |     },                                                      |
|                |     "errMsg": ""                                            |
|                | }                                                           |
+----------------+-------------------------------------------------------------+

```

## 作业运行日志查询

`log` 提供了`show`的方法，方便用户能够查询正在运行的作业日志，具体的操作示例如下：

```bash
paddleflow log show runid -j(--jobid) jobid -ps(--pagesize) pagesize -pn(--pageno) pageno -fp(--logfileposition) logfileposition 
// (required)runid为要查询的run任务的日志;
// (optional)jobid为run任务下指定某个job的id,默认返回所有;
// (optional)pagesize为返回的日志内容的每页行数,默认为100;
// (optional)pageno为返回的日志内容的页数,默认为1;
// (optional)logfileposition为读取日志的顺序,从最开始位置读取为begin,从末尾位置读取为end,默认从尾部开始读取
```

### 示例
查询某个run任务下的运行日志：用户输入```paddleflow log show run-000419```，界面上能够返回对应的```runid```的运行日志信息。

```bash
+------------+------------------------------+------------------------------------------------+-----------------+-------------+-----------+-------------+-------------------------------------------------------------------------------------------------------------+
| run id     | job id                       | task id                                        | has next page   | truncated   |   page no |   page size | log content                                                                                                 |
+============+==============================+================================================+=================+=============+===========+=============+=============================================================================================================+
| run-000419 | job-run-000419-main-219bc706 | 627e0603-96fd-4fad-91dc-7d0d5fffaf12_container | False           | False       |         1 |         100 | 2022-02-23T09:10:13.673403692Z bash: line 0: cd: /home/work/model-factory/model_: No such file or directory |
|            |                              |                                                |                 |             |           |             | 2022-02-23T09:10:13.693219789Z sh: test_train.sh: No such file or directory                                 |
+------------+------------------------------+------------------------------------------------+-----------------+-------------+-----------+-------------+-------------------------------------------------------------------------------------------------------------+

```

[base_pipeline]: /example/pipeline/base_pipeline
