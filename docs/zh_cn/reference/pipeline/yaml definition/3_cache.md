前面两个章节，介绍了pipeline定义中常用的定义方式。从这章开始，将逐渐介绍更场景化的需求。

本章节，主要考虑节点缓存(cache)的功能。

# 1 为什么要节点缓存(cache)

试想某个场景，当我们构建了一个多节点的pipeline，并发起pipeline run后，某个中间节点运行失败了。

很显然，该节点的代码存在bug。然而我们升级代码后，需要再次运行时，只能将整个pipeline的全部节点重新运行一遍。

对于节点数多，或者节点运行耗时的场景，重复运行之前正确运行的节点，将会耗费大量调试时间。

因此，我们提出节点缓存（cache）机制。拥有相同代码，相同参数的节点，再次运行时，可以选择直接跳过。从而实现断点续跑功能，加快调试效率。

# 2 pipeline定义

下面是基于【2_artifact.mdXXXXXX】示例，增加了cache相关参数后的pipeline定义。

```
name: artifact_example

docker_env: nginx:1.7.9

entry_points:

  preprocess:
    parameters:
      data_path: "./data/"
    command: "bash -x shell/data_artifact.sh {{data_path}} {{train_data}} {{validate_data}}"
    env:
      PF_JOB_TYPE: vcjob
      PF_JOB_MODE: Pod
      PF_JOB_QUEUE_NAME: queue1
      PF_JOB_FLAVOUR: flavour1
    artifacts:
      output:
        - train_data
        - validate_data
    cache:
      fs_scope: "shell/data_artifact.sh"
      max_expired_time: 300

  train:
    deps: preprocess
    parameters:
      epoch: 15
      model_path: "./output"
    command: "bash shell/train.sh {{epoch}} {{preprocess.data_path}} {{model_path}} "
    env:
      USER_ABC: 123
      PF_JOB_TYPE: vcjob
      PF_JOB_QUEUE_NAME: qdh
      PF_JOB_MODE: Pod
      PF_JOB_FLAVOUR: flavour1
    artifacts:
      input:
        train_data: "{{ preprocess.train_data }}"
      output:
        - train_model
    cache:
      enable: false

  validate:
    deps: train,preprocess
    parameters:
      model_path: "{{train.model_path}}"
    command: "bash shell/validate.sh {{ model_path }}"
    env:
      PF_JOB_TYPE: vcjob
      PF_JOB_QUEUE_NAME: qdh
      PF_JOB_MODE: Pod
      PF_JOB_FLAVOUR: flavour1
    artifacts:
      input:
        data: "{{ preprocess.validate_data }}"
        model: "{{ train.train_model }}"

cache:
  enable: true
  fs_scope: "./shell/train.sh,./shell/validate.sh,./shell/data_artifact.sh"
  max_expired_time: 600
  
parallelism: 5
```

# 2 cache参数详解

### 2.1 cache参数

节点缓存，是一个节点级别的功能，主要用于在每个几点运行前，判断是否需要直接复用过去已经运行结束的节点job。

由【2 pipeline定义】所示，目前paddlwflow pipeline支持全局级别，以及节点级别的cache参数。

参数字段包括以下三种：

##### 2.1.1 enable

- enable=true，表示开启节点缓存功能。

- 默认为false。

##### 2.1.2 fsscope

由【3.2.1 cache fingerprint计算机制】可知，计算节点是否重复运行，主要判断节点参数，以及节点代码文件是否改动过。

- fsscope用于指定需要检查是否被改动过的文件/目录路径。
    - 用于保证节点运行代码被修改后，不再复用以前的运行结果cache。

- 默认为空字符串，表示不检查任何文件/目录路径。

##### 2.1.3 max_expired_time

- 表示这次节点job运行后，生成的cache的有效期。单位为秒。

- 默认-1，表示无限时间。

### 2.2 配置优先级

- 节点级别的cache参数  >  全局级别的cache参数  >  cache参数默认值。

> 例子：如【2 pipeline定义】所示：

> * preprocess节点：定义了enable，fs_scope，max_expired_time 三个参数，所以只需直接使用节点中的定义。
> * train节点：没有定义任何cache参数，所以三个参数直接使用全局配置。
> * validate节点：定义了enable=false，max_expired_time=-1。但是fs_scope没有定义，因此只需从全局配置中获取fs_scope的值即可。

- 如果全局cache参数，节点内cache参数都没有定义，则直接采用默认值。


# 3 cache运行机制

下面，将介绍节点运行前，命中cache的流程，以及影响cache命中的参数配置

### 3.1 cache运行流程

如果pipeline定义中，某节点开启了cache机制，则在每个节点运行时：

1. 在运行前，根据参数替换后的参数，计算cache fingerprint
> 计算cache fingerprint前参数替换方法与运行节点前的替换方法不同
>
> 计算cache fingerprint前参数替换方法，可参考【3.2 cache 命中机制】

2. 根据第一步得到的cache fingerprint，寻找是否有fingerprint相同，同时满足其他条件的历史节点任务

a. 有：则判断cache的任务状态是否为终止态

- 是，则判断cache的任务状态是否为自然终止态（成功，失败）：
    - 是，则更新当前job状态为cache任务状态，跳过运行
    - 否（处于cancelled，terminated状态），则cache的job被无视，当前要运行的job还会继续运行

- 否，则不断轮询，直至cache的任务状态跳转为终止态

b. 没有：则为当前要运行的job，将计算的 cache fingerprint 更新到数据库中

3. 节点job运行，直至结束。

如果pipeline定义中，节点没有开启cache机制，则直接执行上述步骤中的第3步。
- 既不会计算cache fingerprint，查找匹配的cache记录
- 也不会在节点运行前插入cache记录


### 3.2 cache 命中机制

目前PaddleFlow Pipeline在开启Cache功能下，运行工作流命中Cache需要满足以下三个条件：

* 同一个User、FS、Yaml文件发起

* 相同的Cache Fingerprint

* Cache未失效（max_expired_time参数控制）

其中，cache fingerprint计算机制如下：

##### 3.2.1 cache fingerprint计算机制

为节点job计算cache fingerprint，主要关注的内容有两部分：

- 参数值：包括command，parameters，input artifacts，output artifacts，env等

- 运行脚本：通过fsscope参数指定需要监控的脚本/目录路径

基于上述两部分关注点，在目前策略下，paddleflow会为每个开启cache的节点job，计算两层Fingerprint。每层fingerprint的计算，使用的参数名/参数值 包括：

第一层Fingerprint
* docker_env (参数名 & 值)
* step_name
* paramters (参数名 & 值)
* command (参数名 & 值)
* input artifact（参数名 & 参数）
* output artifact（only 参数名）
* env (参数名 & 值)


第二层Fingerprint
* input artifact（内容）
* fs_scope中指定的路径（内容）

> 其中，需要注意的点有：
> - 判断当前job等input artifact, fsscope内容是否与cache job记录所使用内容一致，可以有两种办法： 
>   - 读取文件/目录下所有内容，计算对应hash值。
>   - 或者取文件/目录的stat modify time。
> - 为了方便目前采取第二种方式获取。
 
> - 计算节点Fingerprint时，无需再关注当前节点的output artifact路径。因此：
>   - 第一层Fingerprint计算时，output artifact只使用名称，用于计算fingerprint。
>   - command在计算Fingerprint时，不需要展开output artifact的变量
>     - 但是在实际运行前，依然会利用output artifact的实际路径对command中的变量进行替换。
