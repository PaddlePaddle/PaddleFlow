在上一章节，我们介绍了如何在pipeline中使用artifact，同时也提到了paddleflow会把artifact存放在共享存储上。本文便介绍如何在pipeline中使用共享存储

>关于共享存储的详细介绍，可以参考[filesystem]

# 1 使用场景
- 一个pipeline都会有多个节点，如果节点之间有文件（如 artifact）需要传递，便需要使用共享存储
- 对于一份数据，一般会被多次使用，此时使用共享存储既能降低在运行任务时的数据准备时间，也能降低数据副本数，节省存储空间

# 2 pipeline定义

下面是一个在pipeline中使用共享存储的示例。

> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/multiple_fs示例。
> 
> 示例链接：[multiple_fs]


```yaml
name: multiple_fs 

entry_points:
  main-fs:
    artifacts:
      output:
      - main
    command: "cd shells/main_fs && bash run.sh"
    docker_env: centos:centos7

  global-extra-fs:
    artifacts:
      output:
      - global_extra
    command: "cd /home/global && bash run.sh"
    docker_env: centos:centos7

  extra-fs:
    artifacts:
      output:
      - extra
    command: "cd /home/extra && bash run.sh"
    docker_env: centos:centos7
    extra_fs:
    - {name: "ppl", sub_path: "multiple_fs/shells/extra_fs", mount_path: "/home/extra"}

docker_env: nginx:1.7.9

parallelism: 2

fs_options:
  main_fs: {name: "ppl", sub_path: "multiple_fs", mount_path: "/home/main"}
  extra_fs:
  - {name: "ppl", sub_path: "multiple_fs/shells/global_extra_fs", mount_path: "/home/global"}
```

# 2 详解

上述pipeline定义中，基于基础字段上，在节点级别，增加了extra_fs字段，在全局级别，增加了fs_options字段，下面将详细这几个字段

## 2.1 节点级别的extra_fs字段
一般情况下，一个pipeline中会包含有多个节点，而每个节点都有自己所需要的数据，代码等。

如果你已经将数据或者代码存放到了共享存储上，便可以在定义节点时，通过extra_fs字段指定共享存储的挂载信息。

Paddleflow在运行节点时，会根据extra_fs字段将共享存储挂载至容器中。

extra_fs字段是一个list，也即一个节点可以挂载多个共享存储

extra_fs支持的子字段及含义如下表：

| 字段名 | 类型 | 是否必须| 含义 | 示例 | 默认值 | 备注 |
| :---:| :---:|:---:|:---:|:---:|:---:|:---|
| name | string | 是 | 共享存储的名字 | "xiaoming" | - | |
| mount_path| string | 否 | 容器内的挂载路径 | "/home/fs" | /home/paddleflow/storage/mnt/${fs_id} | |
| sub_path | string | 否 | 共享存储中需要被挂载的子路径 | "multiple_fs" | - | <br>如果没有配置，将会把整个共享存储挂载至容器中</br><br>不能以"/"开头</br> |
| read_only | bool | 否 | 容器对mount_path的访问权限 | true | false |

>举个例子:
>
>在上面的示例中，节点 `extra-fs` 配置了如下的 *extra_fs*信息：
```yaml
    extra_fs:
      - {name: "ppl", sub_path: "multiple_fs/shells/global_extra_fs", mount_path: "/home/global"}
 ```
> 则在节点 `extra_fs` 运行时，Paddleflow会将共享存储 `ppl`的子路径`multiple_fs/shells/global_extra_fs`, 挂载到容器的 `/home/global` 路径下，且容器对路径`/home/global`具有读写权限


## 2.2 全局级别的fs_options字段
当前fs_options字段支持如下两个子字段：
- main_fs
- extra_fs

接下来将依次讲解这两个字段

### 2.2.1 main_fs
正如在[artifact定义约束]所述：Paddleflow需要将节点输出artifact存储在共享存储上，并据此完成artifact在不同节点间的传递。

而artifact具体存放在哪个共享存储上？以及存放在共享存储的哪个路径下？则是由main_fs字段所决定。

>关于artifact的存储路径，可以查看[artifact存储机制]

main_fs支持的子字段如下所示：
| 字段名 | 类型 | 是否必须| 含义 | 示例 | 默认值 | 备注 |
| :---:| :---:|:---:|:---:|:---:|:---:|:---|
| name | string | 是 | 共享存储的名字 | "xiaoming" | - | |
| mount_path| string | 否 | 容器内的挂载路径 | "/home/fs" | /home/paddleflow/storage/mnt/${fs_id} | |
| sub_path | string | 否 | 共享存储中需要被挂载的子路径 | "multiple_fs" | - | <br>如果没有配置，将会把整个共享存储挂载至容器中</br><br>不能以"/"开头</br><br><mark>如果该路径存在，则必须为目录</mark></br> |

>举个例子：
>使用[2 pipeline定义]，发起一个Run, 则其生成的artifact都将会存放在共享存储`ppl`的`"multiple_fs"`路径下

### 2.2.2 extra_fs
如果pipeline的所有节点都要挂相同的共享存储，此时，只需在fs_options.extra_fs字段中填写这部分的共享存储的挂载信息即可，无需在每个节点中都填写一遍。

fs_options的子字段extra_fs的规范与[节点级别的extra_fs字段]相同，此处不在进行赘述

# 3 pipeline运行流程

## 3.1 fs_options.extra_fs的下沉
在节点运行前，fs_options中的extra_fs的信息将会被**追加**至节点的extra_fs字段中。

> 举个例子
> 使用[2 pipeline定义]，发起一个Pipeline任务, 节点`main-fs`和`global-extra-fs`的`extra_fs`字段将会被追加成如下形式：
```yaml
extra_fs:
  - {name: "ppl", sub_path: "multiple_fs/shells/global_extra_fs", mount_path: "/home/global"}
```

>节点`extra-fs`的`extra_fs`字段将会被追加成如下形式：
```yaml
extra_fs:
  - {name: "ppl", sub_path: "multiple_fs/shells/extra_fs", mount_path: "/home/extra"}
  - {name: "ppl", sub_path: "multiple_fs/shells/global_extra_fs", mount_path: "/home/global"}
```

## 3.2 节点目录跳转
如果使用了共享存储，在节点运行时，Paddleflow会默认在容器的command前添加目录跳转命令，具体规则如下：

- 如果指定了main_fs, 则会默认跳转至main_fs的mount_path下
- 如果没有指定main_fs, 在完成了[3.1 fs_options.extra_fs的下沉]操作后，如果节点的extra_fs不为空，则会跳转到extra_fs中第一项所指定mount_path下
    > - fs_options.extra_fs的配置项会位于节点extra_fs配置项之后

>如果不需要Paddleflow添加目录跳转命令，则可以在节点中添加如下的环境变量：
```yaml
env:
  PF_MOUNT_PATH: NONE
```

[multiple_fs]: /example/pipeline/multiple_fs
[artifact定义约束]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#23-artifact-定义约束
[artifact存储机制]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md#31-artifact%E5%AD%98%E5%82%A8%E6%9C%BA%E5%88%B6
[2 pipeline定义]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md#2-pipeline%E5%AE%9A%E4%B9%89
[节点级别的extra_fs字段]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md#21-%E8%8A%82%E7%82%B9%E7%BA%A7%E5%88%AB%E7%9A%84extra_fs%E5%AD%97%E6%AE%B5
[3.1 fs_options.extra_fs的下沉]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md#31-fs_optionsextra_fs%E7%9A%84%E4%B8%8B%E6%B2%89
[filesystem]: /docs/zh_cn/reference/filesystem