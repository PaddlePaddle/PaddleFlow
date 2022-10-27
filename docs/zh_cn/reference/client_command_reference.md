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

  provide `user`, `queue`, `fs`, `run`, `pipeline`, `cluster`, `flavour`
  operation commands

Options:
  --pf_config TEXT            the path of default config.
  --output [table|json|text]  The formatting style for command output.
                              [default: table]

  --help                      Show this message and exit.

Commands:
  cluster     manage cluster resources
  flavour     manage flavour resources
  fs          manage fs resources
  job         manage job resources
  log         manage log resources
  pipeline    manage pipeline resources
  queue       manage queue resources
  run         manage run resources
  schedule    manage schedule resources
  statistics  show resources statistics
  user        manage user resources
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

`queue` 提供了`create`, `delete`, `list`, `show`, `update`, `grant`,`ungrant`,`grantlist`八种不同的方法。 八种不同操作的示例如下：

```bash
$ paddleflow queue --help
Usage: paddleflow queue [OPTIONS] COMMAND [ARGS]...

  manage queue resources

Options:
  --help  Show this message and exit.

Commands:
  create     create queue.
  delete     delete queue.
  grant      add grant.
  grantlist  list grant
  list       list queue.
  show       show queue info.
  ungrant    delete grant.
  update     update queue.
```

### 示例

队列列表： 用户输入 ```paddleflow queue list ``` 可以在界面上看到当前用户有权限看到的队列集合

```
+---------------+-------------+----------+-----------------+---------------------+---------------------+
| name          | namespace   | status   | cluster name    | create time         | update time         |
+===============+=============+==========+=================+=====================+=====================+
| default-queue | default     | open     | default-cluster | 2022-08-03 20:18:31 | 2022-08-18 15:45:57 |
+---------------+-------------+----------+-----------------+---------------------+---------------------+
| default       | default     | open     | default-cluster | 2022-08-03 20:18:31 | 2022-08-03 20:18:31 |
+---------------+-------------+----------+-----------------+---------------------+---------------------+
| pf-queue      | default     | open     | default-cluster | 2022-08-03 20:18:31 | 2022-08-03 20:18:31 |
+---------------+-------------+----------+-----------------+---------------------+---------------------+
| root          | default     | open     | default-cluster | 2022-08-03 20:18:31 | 2022-08-03 20:18:31 |
+---------------+-------------+----------+-----------------+---------------------+---------------------+
marker: None
```

队列详情： 用户输入 ```paddleflow queue show default-queue``` 可以在界面上看到队列`aa`的详细信息

```
+---------------+-------------+----------+------------------------+-----------------+---------------------+---------------------+
| name          | namespace   | status   | quota type             | cluster name    | create time         | update time         |
+===============+=============+==========+========================+=================+=====================+=====================+
| default-queue | default     | open     | volcanoCapabilityQuota | default-cluster | 2022-08-03 20:18:31 | 2022-08-18 15:45:57 |
+---------------+-------------+----------+------------------------+-----------------+---------------------+---------------------+
queue info:
[
    {
        "max resources": {
            "cpu": "20",
            "mem": "20Gi"
        },
        "min resources": {
            "cpu": "0",
            "mem": "0"
        },
        "used resources": {
            "cpu": "0",
            "mem": "0"
        },
        "idle resources": {
            "cpu": "20",
            "mem": "20Gi"
        }
    }
]
```

队列创建：用户输入 ```paddleflow queue create queuename default 10 20Gi --clustername default-cluster```，创建成功后可以在界面上看到

```queue[queuename] create  success```

队列更新：用户输入 ```paddleflow queue update queuename --maxcpu 20```，更新成功后可以在界面上看到

```queue[queuename] update  success```


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

`flavour` 提供了 `create`, `delete`, `list`, `show`, `update` 五种不同的方法。 操作的示例如下：

```bash
$ paddleflow flavour --help
Usage: paddleflow flavour [OPTIONS] COMMAND [ARGS]...

  manage flavour resources

Options:
  --help  Show this message and exit.

Commands:
  create  create flavour.
  delete  delete flavour.
  list    list flavour.
  show    show flavour info.
  update  update info from flavourname.
```

### 示例
flavour列表显示：用户输入```paddleflow flavour list```，界面上显示

```bash
+----------+-------+-------+-------------------------+
| name     |   cpu | mem   | scalarResources         |
+==========+=======+=======+=========================+
| flavour1 |     1 | 1Gi   | null                    |
+----------+-------+-------+-------------------------+
| flavour2 |     4 | 8Gi   | {"nvidia.com/gpu": "1"} |
+----------+-------+-------+-------------------------+
| flavour3 |     4 | 8Gi   | {"nvidia.com/gpu": "2"} |
+----------+-------+-------+-------------------------+
```
套餐详情： 用户输入 ```paddleflow flavour show flavour1``` 可以在界面上看到套餐`flavour1`的详细信息

```
+-------------+-------+-------+-------------------------+
| name        |   cpu | mem   | scalarResources         |
+=============+=======+=======+=========================+
| flavour1 |     4 | 8G    | {"nvidia.com/gpu": "8"} |
+-------------+-------+-------+-------------------------+
```

套餐创建：用户输入 ```paddleflow flavour create flavour_gpu -c 4 -m 8G -s nvidia.com/gpu=8```，创建成功后可以在界面上看到

```flavour[flavour_gpu] create success```

套餐更新：用户输入 ```paddleflow flavour update flavour_gpu -m 8```，更新成功后可以在界面上看到

```update [flavour_gpu] success```


套餐删除：用户输入 ```paddleflow flavour delete flavour_gpu```，删除成功后可以在界面上看到

```flavour[flavour_gpu] delete success```



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

```mount success```，还可以通过paddleflow fs mount --help查看挂载其他详细参数，如下
```azure
Usage: paddleflow fs mount [OPTIONS] FSNAME PATH

  mount fs
FSNAME: mount fs name which contain mount fs and mount path
PATH: your local mount path

Options:
  -u, --username TEXT  Mount the specified fs by username, only useful for root.
  -o, --o TEXT         mount options:
                         -o block-size: data cache block size (default: 20971520), if block-size equals to 0, it means that no data cache is used
                         -o data-cache-path: directory path of local data cache (default:"/var/cache/pfs-cache-dir/data-cache")
                         -o data-cache-expire: file data cache timeout (default 0s)
                         -o meta-cache-path: directory path of meta cache (default:"/var/cache/pfs-cache-dir/meta-cache")
                         -o meta-cache-driver: meta driver type (e.g. mem, disk)",
                         -o meta-cache-expire: file meta cache timeout (default 5s)
                         -o entry-cache-expire: file entry cache timeout (default 5s)
```

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
paddleflow run create -f(--fsname) fs_name -n(--name) run_name  -d(--desc) xxx -u(--username) username -p(--param) data_file=xxx -p regularization=*** -yp(--runyamlpath) ./run.yaml -pplid(--pipelineid) ppl-000666 -pplver(--pplversionid) 1 -yr(runyamlraw) xxx --disabled some_step_names -de(--dockerenv) docker_env
// 创建pipeline作业，-yp、-pplid、yr为3中发起任务的方式，每次只能使用其中一种

paddleflow run list -f(--fsname) fsname -u(--username) username -r(--runid) runid -n(--name) name -s(--status) runinng -m(--maxsize) 10 -mk(--marker) xxx
// 列出所有运行的pipeline （通过fsname 列出特定fs下面的pipeline；通过username 列出特定用户的pipeline（限root用户）;通过runid列出特定runid的pipeline; 通过name列出特定name的pipeline; 通过status列出特定状态的pipeline）

paddleflow run show runid // 展示一个pipeline下面的详细信息，包括job信息列表

paddleflow run stop runid -f(--force) // 停止一个pipeline

paddleflow run retry runid // 重跑一个pipeline

paddleflow run delete runid -not-cc(-notcheckcache) // 删除一个运行的工作流

paddleflow run listcache -u(--userfilter) username -f(--fsfilter) fsname -r(--runfilter) run-000666 -m(--maxsize) 10 -mk(--marker) xxx // 列出搜有的工作流缓存

paddleflow run showcache cacheid // 显示工作流缓存详情

paddleflow run deletecahce cacheid // 删除指定工作流缓存

paddleflow run listartifact -u(--userfilter) username -f(--fsfilter) fsname -r(runfilter) run-000666 -t(--typefilter) type -p(--pathfilter) path -m(--maxsize) 10 -mk(--marker) xxx // 列出所有工作流产出
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
paddleflow run create -pplid ppl-000666 -pplver 1
```

工作流列表：用户输入```paddleflow run list```，界面上能够显示出所有工作流列表信息，marker为下一页的起始位，供 -mk --marker 参数使用

```bash
+------------+-----------+------------+------------+---------------+---------------+--------------------------------------------+----------------------------------+-----------------+---------------------+---------------------+---------------------+---------------------+
| run id     | fs name   | username   | status     | name          | description   | run msg                                    | source                           | schedule id     | scheduled time      | create time         | activate time       | update time         |
+============+===========+============+============+===============+===============+============================================+==================================+=================+=====================+=====================+=====================+=====================+
| run-000001 | fs        | root       | succeeded  | myproject     |               | begin to running, update status to running | runDag.yaml                      | schedule-000003 | 2022-08-15 16:47:00 | 2022-08-04 11:45:25 | 2022-08-04 11:45:25 | 2022-08-04 11:48:48 |
+------------+-----------+------------+------------+---------------+---------------+--------------------------------------------+----------------------------------+-----------------+---------------------+---------------------+---------------------+---------------------+
| run-000002 | fs        | root       | succeeded  | myproject     |               | begin to running, update status to running | runDag.yaml                      |                 |                     | 2022-08-04 11:45:29 | 2022-08-04 11:45:29 | 2022-08-04 11:48:59 |
+------------+-----------+------------+------------+---------------+---------------+--------------------------------------------+----------------------------------+-----------------+---------------------+---------------------+---------------------+---------------------+
| run-000003 |           | root       | succeeded  | test          |               | begin to running, update status to running | 0daaa6deade1ff7f10531da958e0c298 |                 |                     | 2022-08-04 11:45:52 | 2022-08-04 11:45:53 | 2022-08-04 11:46:11 |
+------------+-----------+------------+------------+---------------+---------------+--------------------------------------------+----------------------------------+-----------------+---------------------+---------------------+---------------------+---------------------+
```

工作流详情：用户输入```paddleflow run show run-cb96cf93(run id)```，界面上能够显示出对应工作流的详细信息

```bash
+------------+----------+------------+--------+---------+-----------------+--------------------------------------------+---------------------+---------------------+---------------------+
| run id     | status   | name       | desc   | param   | source          | run msg                                    | create time         | update time         | activate time       |
+============+==========+============+========+=========+=================+============================================+=====================+=====================+=====================+
| run-000060 | running  | dagProject |        |         | runDag.yaml     | begin to running, update status to running | 2022-08-18 19:11:33 | 2022-08-18 19:12:51 | 2022-08-18 19:12:51 |
+------------+----------+------------+--------+---------+-----------------+--------------------------------------------+---------------------+---------------------+---------------------+
+-----------+------------+------------+---------------+----------------------------------------------------------------------------------------------+---------------------------+------------+------------------+
| fs name   | username   | docker env | schedule id   | fs options(json)                                                                             | failure options(json)     | disabled   | run cached ids   |
+===========+============+============+===============+==============================================================================================+===========================+============+==================+
| cyang14   | root       | docekrenv  |               | {'mainFS': {'id': '', 'name': 'abcdefg', 'mountPath': '', 'subPath': '', 'readOnly': False}} | {'strategy': 'fail_fast'} |            |                  |
+-----------+------------+------------+---------------+----------------------------------------------------------------------------------------------+---------------------------+------------+------------------+
+-------------------------------------------------------------------------------------+
| run yaml detail                                                                     |
+=====================================================================================+
| name: dagProject                                                                    |
| ......                                                                              |
+-------------------------------------------------------------------------------------+
+-----------------------------------------------------------------------------------------------------+
| runtime in json                                                                                     |
+=====================================================================================================+
| {                                                                                                   |
|   "disDag": [                                                                                       |
|     {                                                                                               |
|       "dagID": "dag-run-000059-8c6fb65ec1d2d3d1",                                                   |
|       "name": "dag-run-000059-disDag",                                                              |
|       Omitted here ......                                                                           |
+-----------------------------------------------------------------------------------------------------+
+---------------------------------------------------------------------------------------------+
| postProcess in json                                                                         |
+=============================================================================================+
| {                                                                                           |
|   "last": {                                                                                 |
|     "artifacts": {                                                                          |
|       "input": {},                                                                          |
|       "output": {}                                                                          |
|     },                                                                                      |
|     Omitted here ......                                                                     |
+---------------------------------------------------------------------------------------------+
```

作业停止：用户输入```paddleflow run  stop {runid}```，界面上显示

```bash
run[runid] stop success
```

作业重试：用户输入```paddleflow run  retry {runid}```，界面上显示

```bash
run[runid] retry success, new run id is [run-newid]
```

作业删除：用户输入```paddleflow run  delete {runid}```，界面上显示

```bash
run[runid] delete success
```

作业缓存列表显示：用户输入```paddleflow run  listcache```，界面上显示

```bash
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cache id   | run id     | jobid      | fsname   | username   |   expired time | create time         | update time         |
+============+============+============+==========+============+================+=====================+=====================+
| cch-000001 | run-000064 | job-xxxxxx | mxy      | root       |            400 | 2021-12-10 09:56:42 | 2021-12-10 09:56:42 |
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cch-000002 | run-000064 | job-xxxxxx | mxy      | root       |            400 | 2021-12-10 09:57:00 | 2021-12-10 09:57:00 |
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cch-000003 | run-000064 | job-xxxxxx | mxy      | root       |            400 | 2021-12-10 09:57:12 | 2021-12-10 09:57:12 |
+------------+------------+------------+----------+------------+----------------+---------------------+---------------------+
| cch-000005 | run-000076 | job-xxxxxx | zzc      | root       |            400 | 2021-12-10 11:51:04 | 2021-12-10 11:51:04 |
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

作业缓存删除：用户输入```paddleflow run  deletecache {cacheid}```，界面上显示

```bash
runcache[cacheid] delete success
```

工作流产出显示：用户输入```paddleflow run listartifact```，界面上显示

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

`pipeline` 提供了`create`,`show`, `list`, `delete`, `update`, `showverion`, `deleteverion` 7种不同的方法。 7种不同操作的示例如下：

```bash
paddleflow pipeline create  fsname:required（必须） -yp (--yamlpath) path  -n(--name)  pipeline_name -u(--username) username // 创建pipeline模板(指定创建的pipeline模板名称；指定模板的用户)
paddleflow pipeline list -u(--userfilter) user -n(--namefilter) pipeline_name -m(--maxkeys) int -mk(--marker) xxx // 列出所有的pipeline模板 （通过username 列出特定用户的pipeline模板（限root用户）;通过fsname 列出特定fs下面的pipeline模板；通过pipelinename列出特定的pipeline模板；列出指定数量的pipeline模板；从marker列出pipeline模板）
paddleflow pipeline show pipelineid // 展示一个pipeline模板下面的详细信息，包括yaml信息
paddleflow pipeline delete pipelineid // 删除一个pipeline模板 

paddleflow pipeline update piplineid fsname yamlpath  -n(--name) pipeline_name -u(--username) username // 更新Pipeline模板（创建pipeline模板版本）
paddleflow pipeline showversion pipelineid pipelineversionid // 查看一个pipeline模板版本
paddleflow pipeline deleteversion pipelineid pipelineversionid // 删除一个pipeline模板版本
```


工作流模板创建：用户输入```paddleflow pipeline create fsname -yp yamlpyth```，界面上显示

```bash
pipeline[dagProject] create  success, id[ppl-000009], versionID[1]
```

工作流模板列表显示：用户输入```paddleflow pipeline list```，界面上显示

```bash
+---------------+-----------------+------------+--------+---------------------+---------------------+
| pipeline id   | name            | username   | desc   | create time         | update time         |
+===============+=================+============+========+=====================+=====================+
| ppl-000004    | myproject       | root       |        | 2022-08-11 12:03:39 | 2022-08-11 15:01:54 |
+---------------+-----------------+------------+--------+---------------------+---------------------+
| ppl-000006    | dagProject_test | root       |        | 2022-08-17 15:31:29 | 2022-08-17 15:31:29 |
+---------------+-----------------+------------+--------+---------------------+---------------------+
| ppl-000008    | dagProject      | root       |        | 2022-08-18 19:02:42 | 2022-08-18 19:04:23 |
+---------------+-----------------+------------+--------+---------------------+---------------------+
marker: None

```

工作流模板详情显示：用户输入```paddleflow pipeline show {pipelineid}```，界面上显示

```bash
+---------------+------------+------------+-----------------+---------------------+---------------------+
| pipeline id   | name       | username   | pipeline desc   | create time         | update time         |
+===============+============+============+=================+=====================+=====================+
| ppl-000009    | dagProject | root       |                 | 2022-08-19 16:04:38 | 2022-08-19 16:04:38 |
+---------------+------------+------------+-----------------+---------------------+---------------------+
+--------------+-----------+-----------------+------------+---------------------+---------------------+
|   ppl ver id | fs name   | yaml path       | username   | create time         | update time         |
+==============+===========+=================+============+=====================+=====================+
|            1 | abcdefg   | runDag.yaml     | root       | 2022-08-19 16:04:38 | 2022-08-19 16:04:38 |
+--------------+-----------+-----------------+------------+---------------------+---------------------+

```

工作流模板删除：用户输入```paddleflow pipeline delete pipelineid```，界面上显示

```bash
pipelineid[pipelineid] delete success

```

工作流模板更新：用户输入```paddleflow pipeline update ppl-000001 fsName yamlPath```，界面上显示

```bash
pipeline[ppl-000009] update success, new version id [2]
```

工作流模板版本详情查看：用户输入```paddleflow pipeline showversion ppl-000001 1```，界面上显示

```bash
+---------------+------------+------------+-----------------+---------------------+---------------------+
| pipeline id   | name       | username   | pipeline desc   | create time         | update time         |
+===============+============+============+=================+=====================+=====================+
| ppl-000001    | dagProject | root       |                 | 2022-08-19 16:04:38 | 2022-08-19 16:04:38 |
+---------------+------------+------------+-----------------+---------------------+---------------------+
+--------------+-----------+-----------------+------------+---------------------+---------------------+
|   ppl ver id | fs name   | yaml path       | username   | create time         | update time         |
+==============+===========+=================+============+=====================+=====================+
|            1 | fs_name   | runDag.yaml     | root       | 2022-08-19 16:04:38 | 2022-08-19 16:04:38 |
+--------------+-----------+-----------------+------------+---------------------+---------------------+
+--------------+-------------------------------------------------------------------------------------+
|   ppl ver id | pipeline yaml                                                                       |
+==============+=====================================================================================+
|            1 | name: dagProject                                                                    |
|              |                                                                                     |
|              | fs_options:                                                                         |
|              |   main_fs: {name: lalala}                                                           |
|              |    omitted here ......                                                              |
+--------------+-------------------------------------------------------------------------------------+
```

工作流模板版本删除：用户输入```paddleflow pipeline deleteversion ppl-000001 1```，界面上显示

```bash
pipeline version [ppl-000001] of pipeline [1] delete success
```

### 周期调度管理
周期调度(`schedule`)提供了5种不同的方法，示例如下：
```bash
paddleflow schedule create name pplid pplverid crontab -d(--desc) xxx -s(starttime) xxx -e(--endtime) xxx -c(--concurrency) xxx -cp(--concurrencypolicy) xxx -ei(--expireinterval) xxx (--catchup) xxx -u(--username) xxx
// 创建周期调度

paddleflow schedule list -u(--userfilter) user -n(--namefilter) name -p(--pplfilter) xxx -pv(--pplverfilter) xx -s(--statusfilter) xx -m(--maxkeys) 50 -mk(--marker) xxx 
// 查看周期调度列表

paddleflow schedule show scheduleid -r(--runfilter) xxx -s(--statusfilter) xxx -m(--maxkeys) 50 -mk(--marker) xxx 
// 查看周期调度详情，同时查看该周期调度已发起的Run

paddleflow schedule stop scheduleid
// 暂停周期调度

paddleflow schedule delete scheduleid
// 删除周期调度
```

创建周期调度：用户输入```paddleflow schedule create NAME ppl-000001 1 '*/10 * * * *'```

> 注意这里的crontab需要用引号包起来，如 '*/10 * * * *'

```bash
schedule [NAME] create success with schedule id[schedule-000001]
```

停止周期调度：用户输入```paddleflow schedule stop  schedule-000001```，界面显示

```bash
schedule with id [schedule-000001] stop success
```

查看周期调度详情：用户输入```paddleflow schedule show schedule-000001```，界面显示

```bash
+-----------------+--------+--------+---------------+-----------------------+-------------+------------+------------------+-------------------------------------------------------------------------------------------+-----------+------------+
| schedule id     | name   | desc   | pipeline id   |   pipeline version id | crontab     | username   | fs config        | options                                                                                   | message   | status     |
+=================+========+========+===============+=======================+=============+============+==================+===========================================================================================+===========+============+
| schedule-000006 | test3  |        | ppl-000005    |                     1 | */5 * * * * | root       | {'username': ''} | {'catchup': False, 'expireInterval': 0, 'concurrency': 0, 'concurrencyPolicy': 'suspend'} |           | terminated |
+-----------------+--------+--------+---------------+-----------------------+-------------+------------+------------------+-------------------------------------------------------------------------------------------+-----------+------------+
+-----------------+--------------+------------+---------------------+---------------------+---------------------+
| schedule id     | start time   | end time   | create time         | update time         | next run time       |
+=================+==============+============+=====================+=====================+=====================+
| schedule-000006 |              |            | 2022-08-18 16:36:07 | 2022-08-18 17:16:29 | 2022-08-18 17:20:00 |
+-----------------+--------------+------------+---------------------+---------------------+---------------------+
+------------+-----------+------------+-----------+--------+---------------+--------------------------------------------+--------------+-----------------+---------------------+---------------------+---------------------+---------------------+
| run id     | fs name   | username   | status    | name   | description   | run msg                                    | source       | schedule id     | scheduled time      | create time         | activate time       | update time         |
+============+===========+============+===========+========+===============+============================================+==============+=================+=====================+=====================+=====================+=====================+
| run-000048 | fs_name   | root       | succeeded | test3  |               | begin to running, update status to running | ppl-000005-1 | schedule-000006 | 2022-08-18 16:40:00 | 2022-08-18 16:40:00 | 2022-08-18 16:40:00 | 2022-08-18 16:42:13 |
+------------+-----------+------------+-----------+--------+---------------+--------------------------------------------+--------------+-----------------+---------------------+---------------------+---------------------+---------------------+
| run-000049 | fs_name   | root       | succeeded | test3  |               | begin to running, update status to running | ppl-000005-1 | schedule-000006 | 2022-08-18 16:45:00 | 2022-08-18 16:45:00 | 2022-08-18 16:45:00 | 2022-08-18 16:47:14 |
+------------+-----------+------------+-----------+--------+---------------+--------------------------------------------+--------------+-----------------+---------------------+---------------------+---------------------+---------------------+
| run-000050 | fs_name   | root       | succeeded | test3  |               | begin to running, update status to running | ppl-000005-1 | schedule-000006 | 2022-08-18 16:50:00 | 2022-08-18 16:50:00 | 2022-08-18 16:50:00 | 2022-08-18 16:52:19 |
+------------+-----------+------------+-----------+--------+---------------+--------------------------------------------+--------------+-----------------+---------------------+---------------------+---------------------+---------------------+
```

查看周期调度列表：用户输入```paddleflow schedule list```，界面显示

```bash
+-----------------+---------+--------+---------------+-----------------------+--------------+------------+------------------+-------------------------------------------------------------------------------------------+-----------+------------+
| schedule id     | name    | desc   | pipeline id   |   pipeline version id | crontab      | username   | fs config        | options                                                                                   | message   | status     |
+=================+=========+========+===============+=======================+==============+============+==================+===========================================================================================+===========+============+
| schedule-000002 | test    |        | ppl-000003    |                     2 | * */10 * * * | root       | {'username': ''} | {'catchup': True, 'expireInterval': 0, 'concurrency': 0, 'concurrencyPolicy': 'suspend'}  |           | terminated |
+-----------------+---------+--------+---------------+-----------------------+--------------+------------+------------------+-------------------------------------------------------------------------------------------+-----------+------------+
| schedule-000005 | test2   |        | ppl-000005    |                     1 | */5 * * * *  | root       | {'username': ''} | {'catchup': False, 'expireInterval': 0, 'concurrency': 0, 'concurrencyPolicy': 'suspend'} |           | terminated |
+-----------------+---------+--------+---------------+-----------------------+--------------+------------+------------------+-------------------------------------------------------------------------------------------+-----------+------------+
+-----------------+--------------+------------+---------------------+---------------------+---------------------+
| schedule id     | start time   | end time   | create time         | update time         | next run time       |
+=================+==============+============+=====================+=====================+=====================+
| schedule-000002 |              |            | 2022-08-10 20:14:41 | 2022-08-11 11:14:10 | 2022-08-11 20:00:00 |
+-----------------+--------------+------------+---------------------+---------------------+---------------------+
| schedule-000005 |              |            | 2022-08-18 16:30:00 | 2022-08-18 16:33:47 | 2022-08-18 16:35:00 |
+-----------------+--------------+------------+---------------------+---------------------+---------------------+
```

删除周期调度：用户输入```paddleflow schedule delete schedule-000001```，界面显示

```bash
schedule with id [schedule-000001] delete success
```


### 集群管理(仅限root用户使用)

`cluster` 提供了`create`, `show`, `list`, `delete`, `update`, `resource`六种不同的方法。 六种不同操作的示例如下：

```bash
$ paddleflow cluster --help
Usage: paddleflow cluster [OPTIONS] COMMAND [ARGS]...

  manage cluster resources

Options:
  --help  Show this message and exit.

Commands:
  create    create cluster.
  delete    delete cluster.
  list      list cluster.
  resource  Get the remaining resource information of the cluster.
  show      show cluster info.
  update    update info from clustername.

$
paddleflow cluster list -cn(--clustername) cluster_name -cs(--clusterstatus) cluster_status  -m(--maxkeys) int -mk(--marker) xxx//列出所有的集群 （通过cluster_name 列出指定名称的集群;通过cluster_status 列出指定状态的集群；列出指定数量的集群；从marker列出集群）
paddleflow cluster show clustername // 展示一个集群的详细信息，包括credential凭证信息
paddleflow cluster delete  clustername  //删除一个集群
paddleflow cluster create  clustername:required（必须）集群名称 endpoint:required(必须) 节点 clustertype:required(必须) 集群类型 -c(--credential)  凭证文件绝对路径 -id(--clusterid) clusterid -d(--description) 描述 --source Source --setting setting --status status -ns(--namespacelist) namespacelist// 创建集群（自定义集群名称；集群的节点；集群的类型；集群认证的凭证信息，本地文件路径；自定义集群id;集群描述；集群源[AWS, CCE, etc];集群配置信息；集群状态；namespace列表，比如['NS1','NS2']，传入中括号的内容）
paddleflow cluster update  clustername:required（必须）集群名称 -e(--endpoint) 节点 -t(--clustertype) 集群类型 -c(--credential)  凭证文件绝对路径 -id(--clusterid) clusterid -d(--description) 描述 --source Source --setting setting --status status -ns(--namespacelist) namespacelist// 更新集群（需要更新的集群名称；集群的节点；集群的类型；集群认证的凭证信息，本地文件路径；自定义集群id;集群描述；集群源[AWS, CCE, etc];集群配置信息；集群状态；namespace列表，比如['NS1','NS2']，传入
中括号的内容）
paddleflow cluster resource  -cn(--clustername)  cluster_name    // 列表显示所有集群剩余资源（显示指定集群的剩余资源）
```

### 示例

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

## 统计信息查询
`statistics` 提供了 `job` 的方法，方便用户能够查询某个任务的统计信息，具体的操作示例如下：

```bash
paddleflow statistics job jobid -d(--detail) -s(--start) start -e(--end) end -st(--step) step
// (required)jobid为要查询统计信息的job的id;
// (optional)添加detail可以查询更详细的统计信息, 默认不添加;
// (optional)start可以查询指定起始时间的统计信息, 默认为空, 必须和 -d(--detail)参数一起使用;
// (optional)end可以查询指定结束时间的统计信息, 默认为空, 必须和 -d(--detail) 与 -s(--start)参数一起使用, 且必须大于start;
// (optional)step可以查询指定时间范围内的统计信息, 默认为空, 必须和 -d(--detail)参数一起使用;
```

### 示例
查询某个job的统计信息：用户输入```paddleflow statistics job {jobid}```，界面上能够返回对应的```jobid```的统计信息。

```bash
+------------------+------------------+------------------+-------------------+--------------------+-------------------+------------+-------------------+---------------------+---------------------+------------------+
| cpu usage rate   | disk read rate   | disk usage       | disk write rate   | gpu memory usage   | gpu memory util   | gpu util   | memory usage      | memory usage rate   | net receive bytes   | net send bytes   |
+==================+==================+==================+===================+====================+===================+============+===================+=====================+=====================+==================+
| 96.49%           | 0.00(B/s)        | 265420.80(Bytes) | 0.00(B/s)         | 0.00(Bytes)        | 0.00%             | 0.00%      | 4067737.60(bytes) | 0.38%               | 1.71(B/s)           | 0.00(B/s)        |
+------------------+------------------+------------------+-------------------+--------------------+-------------------+------------+-------------------+---------------------+---------------------+------------------+
```

查询某个job的详细统计信息：用户输入```paddleflow statistics job {jobid} -d(--detail)```，界面上能够返回对应的```jobid```的统计信息。

```bash
+-------------+------------------+----------------+--------------+---------------------+------------------+------------+-------------------+
|   timestamp |   cpu usage rate |   memory usage |   disk usage |   net receive bytes |   net send bytes |   gpu util |   gpu memory util |
+=============+==================+================+==============+=====================+==================+============+===================+
|  1657807709 |         0.265535 |       0.245772 |      1961984 |                   0 |                0 |          0 |          0.082387 |
+-------------+------------------+----------------+--------------+---------------------+------------------+------------+-------------------+
|  1657807769 |         0.264947 |       0.245773 |      1982464 |                   0 |                0 |          0 |          0.082387 |
+-------------+------------------+----------------+--------------+---------------------+------------------+------------+-------------------+
|  1657807829 |         0.264354 |       0.245773 |      2007040 |                   0 |                0 |          0 |          0.082387 |
+-------------+------------------+----------------+--------------+---------------------+------------------+------------+-------------------+
# 如果返回的消息过长，则会被服务器截断
results has been truncated due to server side limitation
```