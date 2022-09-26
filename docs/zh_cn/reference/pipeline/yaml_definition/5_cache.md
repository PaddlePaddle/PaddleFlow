前面几个章节，介绍了pipeline定义中常用的定义方式。从这章开始，将逐渐介绍更场景化的需求。

本章节，主要考虑节点缓存(cache)的功能。

# 1 为什么要节点缓存(cache)

试想某个场景，当我们构建了一个多节点的pipeline，并发起pipeline run后，某个中间节点运行失败了。

很显然，该节点的代码存在bug。然而我们升级代码后，需要再次运行时，只能将整个pipeline的全部节点重新运行一遍。

对于节点数多，或者节点运行耗时的场景，重复运行之前正确运行的节点，将会耗费大量调试时间。

因此，我们提出节点缓存（cache）机制。拥有相同代码，相同参数的节点，再次运行时，可以选择直接跳过。从而实现断点续跑功能，加快调试效率。

# 2 pipeline定义

下面是基于 [2_artifact.md] 示例，增加了cache相关参数后的pipeline定义。

> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/cache_example示例。
> 
> 示例链接：[cache_example]

```yaml
name: cache_example

entry_points:
  preprocess:
    artifacts:
      output:
      - train_data
      - validate_data
    cache:
      enable: true
      max_expired_time: 300
      fs_scope: 
      - {name: "ppl", path: cache_example/run.yaml}
    command: bash -x cache_example/shells/data_artifact.sh {{data_path}} {{train_data}}
      {{validate_data}}
    docker_env: centos:centos7
    env:
      USER_ABC: 123_{{PF_USER_NAME}}
    parameters:
      data_path: ./cache_example/data/

  train:
    artifacts:
      input:
        train_data: '{{preprocess.train_data}}'
      output:
      - train_model
    command: bash -x cache_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}
    deps: preprocess
    parameters:
      epoch: 15

  validate:
    artifacts:
      input:
        data: '{{preprocess.validate_data}}'
        model: '{{train.train_model}}'
    cache:
      enable: false
      max_expired_time: -1
    command: bash cache_example/shells/validate.sh {{model}}
    deps: preprocess,train

parallelism: 1

cache:
  enable: true
  max_expired_time: 600
  fs_scope: 
  - {name: "ppl", path: "cache_example/shells"}

docker_env: nginx:1.7.9
```

# 3 cache参数详解

## 3.1 cache参数

节点缓存，是一个节点级别的功能，主要用于在每个节点运行前，判断是否需要直接复用过去已经运行结束的节点job。

由[2 pipeline定义]所示，目前Paddleflow pipeline支持全局级别，以及节点级别的cache参数。

参数字段包括以下三种：

### 3.1.1 enable

- enable=true，表示开启节点缓存功能。

- 默认为false。

### 3.1.2 fs_scope

由[4.2.1 cache fingerprint计算机制]可知，计算节点是否重复运行，主要判断节点参数，以及节点代码文件是否改动过。

- fs_scope用于指定需要检查是否被改动过的文件/目录路径。
    - 用于保证节点运行代码被修改后，不再复用以前的运行结果cache。

fs_scope字段为一个list，其中的每一项所支持的字段如下表所示：

| 字段名 | 类型 | 是否必须| 含义 | 示例 | 默认值 | 备注 |
| :---:| :---:|:---:|:---:|:---:|:---:|:---|
| name | string | 是 | 共享存储的名字 | "xiaoming" | - | |
| path | string | 否 | 共享存储上需要检查是否发生过改动的文件/目录路径 | "cache_example/shells" | "/" | 如果有多个文件/目录需要检查，路径与路径之间以','分隔 |  

### 3.1.3 max_expired_time

- 表示这次节点job运行后，生成的cache的有效期。单位为秒。

- 默认-1，表示无限时间。

## 3.2 配置优先级

- enable && max_expired_time : 节点级别 > 全局级别 > 默认值。
- fs_scope: 全局级别的fs_scope配置将会被**追加**至所有节点级别的fs_scope配置中

> 例子：如[2 pipeline定义] 所示：

> * preprocess节点：
>   - 其cache字段定义了eable，max_expired_time，fs_scope 三个参数, 
>   - 所以在该节点运行时，eable，max_expired_time两个参数将会使用节点级别的配置，fs_scope将会在节点的基础上追加全局级别的配置，如下所示：
```yaml
    cache:
      enable: true
      max_expired_time: 300
      fs_scope: 
      - {name: "ppl", path: cache_example/run.yaml}
      - {name: "ppl", path: "cache_example/shells"}
```

> * train节点：
>    - 没有定义任何cache参数，所以三个参数直接使用全局配置
>    - 所以在该节点运行时，使用的cache配置如下：
```yaml
cache:
  enable: true
  max_expired_time: 600
  fs_scope: 
  - {name: "ppl", path: "cache_example/shells"}
```

> * validate节点：
>   - 定义了enable=false，max_expired_time=-1。但是fs_scope没有定义，
>   - 因此只需从全局配置中获取fs_scope的值即可，如下所示：
```yaml
    cache:
      enable: false
      max_expired_time: -1
      fs_scope: 
      - {name: "ppl", path: "cache_example/shells"}
```

- 如果全局cache参数，节点内cache参数都没有定义，则直接采用默认值。
  - fs_scope的默认值为一个空的list

# 4 cache运行机制

下面，将介绍节点运行前，命中cache的流程，以及影响cache命中的参数配置


## 4.1 cache运行流程

如果pipeline定义中，某节点开启了cache机制，则在该节点运行时：

1. 在运行前，根据参数替换后的参数，计算cache fingerprint
> 计算cache fingerprint前参数替换方法与运行节点前的替换方法不同
>
> 计算cache fingerprint前参数替换方法，可参考[4.2 cache 命中机制]

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


## 4.2 cache 命中机制

目前PaddleFlow Pipeline在开启Cache功能下，运行工作流命中Cache需要满足以下两个条件：

* 相同的Cache Fingerprint

* Cache未失效（max_expired_time参数控制）

其中，cache fingerprint计算机制如下：

### 4.2.1 cache fingerprint计算机制

为节点job计算cache fingerprint，主要关注的内容有三部分：

- 共享存储：包括main_fs和extra_fs

- 参数值：包括command，parameters，input artifacts，output artifacts，env等

- 运行脚本：通过fs_scope参数指定需要监控的脚本/目录路径

基于上述两部分关注点，在目前策略下，Paddleflow会为每个开启cache的节点job，计算两层fingerprint。每层fingerprint的计算，使用的参数名/参数值如下：

第一层Fingerprint
* docker_env (参数名 & 值)
* parameters (参数名 & 值)
* command (参数名 & 值)
* input artifact（参数名 & 参数）
* output artifact（only 参数名）
* env (参数名 & 值)
* main_fs
* extra_fs

第二层Fingerprint
* input artifact（内容，直接使用路径mtime）
* fs_scope中指定的路径（内容，直接使用路径mtime）

> 其中，计算第二层fingerprint时，需要注意的点有：
> - 判断当前节点job中，input artifact, fs_scope内容是否与cache job记录所使用内容一致，可以有两种办法： 
>   - 读取文件/目录下所有内容，计算对应hash值。
>   - 或者取文件/目录的stat modify time。
> - 为了方便目前采取第二种方式获取。

> 注意：
> 如果您使用了s3存储，则需要保证参与第二层fingerprint的路径（fs_scope所指定的路径，以及输入aritfact）均为文件，否则将无法命中cache
>  - 对于s3存储，目录的modify time在每次挂载时都会发生变化

### 4.2.2 cache fingerprint 与 artifact 的关系

由[4.2.1 cache fingerprint计算机制]可知，计算fingerprint时，input artifact，与output artifact的参数使用上，有一定差异：

1. input artifact: 参数名，参数值，以及路径内容（修改时间）都会拿来计算

2. output artifact: 只有参数名，会拿来计算

input artifact的使用逻辑很容易理解，因为每次节点job运行时，输入资源的名字，路径，内容是否被修改过，都会影响节点的运行结果。因此，必须被纳入fingerprint的计算。

至于output artifact，Paddleflow计算fingerprint时，只使用参数名，是因为：

- output artifact路径，是在节点job运行前由Paddleflow动态生成的。同一个节点的同一个output artifact参数，在不同job中的值不一样。直接把路径拿来计算，必然会导致不一样的fingerprint。

- output artifact的内容，在节点job运行前并不存在，因此无需判断内容是否存在。

所以，Paddleflow不会把output artifact的路径值，和路径内容（修改时间）拿来计算fingerprint

##### 4.2.2.1 cache机制下，artifact使用示例

我们强烈建议，当节点运行输出资源路径没有特殊意义的时候，将输出资源使用output artifact定义，而不是通过parameter参数显示定义路径值，并在代码中使用。

试想以下case：

如果用户需要定义一个输出文件，文件路径与pipeline run id挂钩，一个可行方法是定义以下parameters参数：

- output_path: ./{{PF_RUN_ID}}/{{PF_STEP_NAME}}/output

但是这种方式会有一个问题，即每次发起一个新的pipeline run，运行该节点job时，parameter替换模板后的的值都不一样，导致每次计算的fingerprint都不一致，cache永远无法命中。

对用户而言，输出路径的具体取值可能并不是他所关注的，可以在计算fingerprint的时候忽略掉。

因此，通过artifact，可以同时满足这两个要求：

1. artifact的路径值由平台自动生成，且与当前pipeline runid，jobid挂钩。

2. 计算cache fingerprint时，不会将output artifact的值纳入计算，因此output artifact路径中，与runid，jobid绑定的信息不会导致cache命中失败。


### 4.2.3 cache fingerprint 计算前，参数替换逻辑

由[4.1 cache运行流程]可知，目前计算fingerprint前，也会对env，parameters，artifact，command参数进行模板替换

但是，计算fingerprint前的节点参数替换逻辑，和节点运行前的参数替换逻辑稍有不同:

1. command参数中，output artifact的变量模板，不会被替换

其原因，与[4.2.2 cache fingerprint 与 artifact 的关系]中描述的原因类似，我们并不希望output artifact的路径值影响fingerprint的计算，因此并不会展开output artifact的变量模板。


[2_artifact.md]: /docs/zh_cn/reference/pipeline/yaml_definition/2_artifact.md
[cache_example]: /example/pipeline/cache_example
[2 pipeline定义]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md#2-pipeline%E5%AE%9A%E4%B9%89
[4.1 cache运行流程]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md#41-cache%E8%BF%90%E8%A1%8C%E6%B5%81%E7%A8%8B
[4.2 cache 命中机制]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md#42-cache-%E5%91%BD%E4%B8%AD%E6%9C%BA%E5%88%B6
[4.2.1 cache fingerprint计算机制]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md#421-cache-fingerprint%E8%AE%A1%E7%AE%97%E6%9C%BA%E5%88%B6
[4.2.2 cache fingerprint 与 artifact 的关系]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md#422-cache-fingerprint-%E4%B8%8E-artifact-%E7%9A%84%E5%85%B3%E7%B3%BB
