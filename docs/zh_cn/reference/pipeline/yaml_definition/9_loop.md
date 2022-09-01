在AI流程中，有时需要对一组数据进行遍历，Paddleflow提供了循环结构来支持这种场景。

# 1 pipeline定义
下面为一个定义了循环结构的示例Pipeline：

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/loop_example示例。
> 
> 示例链接：[loop_example][loop_example]

```yaml
name: loop_example

docker_env: python:3.7

entry_points:
  randint:
    command: "cd /randint && python3 randint.py {{num}} {{lower}} {{upper}}"
    parameters:
      lower: -10
      upper: 10
      num: 5
    artifacts:
      output:
      - random_num
    extra_fs:
    - {name: ppl, mount_path: /randint, sub_path: loop_example/randint, read_only: true}

  process:
    deps: randint
    command: "cd /process && python3 process.py {{PF_LOOP_ARGUMENT}}"
    loop_argument: "{{nums}}"
    artifacts:
      input:
        nums: "{{randint.random_num}}" 
      output:
      - result
    extra_fs:
    - {name: ppl, mount_path: /process, sub_path: loop_example/process, read_only: true}
  
  sum:
    deps: process
    command: "cd /sum && python3 sum.py"
    artifacts:
      input:
        nums: "{{process.result}}"
      output:
        - result
    extra_fs:
    - {name: ppl, mount_path: /sum, sub_path: loop_example/sum, read_only: true}


parallelism: 5

fs_options:
  main_fs: {name: ppl}
```

# 2 详解
## 2.1 loop_argument
在Paddleflow中，定义循环结构的方式非常简单，只需要给节点loop_argument字段赋值即可。

> **DAG节点**和**Step节点**均支持loop_argument字段

loop_argument支持以下几种数据类型：
1. list
2. json格式的list
3. 本节点的parameters的引用模板
   - 被引用的parameter的值需要满足条件1或者条件2
4. 本节点的**输入**artifact的引用模板
   - 输入artifact需要是一个文件
   - 文件的内容需要满足**条件2**
   - 文件的大小需要**小于1MB**

> 注意：
> 
> - 如果loop_argument的值为一个list或者json格式的list，则其中**不能包含**任何形式的**模板**
  
## 2.2 获取当次遍历的循环参数值
对于定义了loop_argument字段的节点，Paddleflow会对loop_argument的参数值做一次遍历，对于loop_argument中的每一项，都会调度执行一次当前节点。

那么节点在运行时，如何知道当前运行对应着loop_argument中的哪一项呢？

为了解决这个问题，Paddleflow pipeline提供了一个系统变量"PF_LOOP_ARGUMENT", 其value即为当次运行时所对应的loop_argument中的元素

> 与其余的系统变量一样，用户可以通过如下两种方式来获取PF_LOOP_ARGUMENT的值
>- 节点定义时： 通过模板"{{PF_LOOP_ARGUMENT}}"来引用
>- 节点运行时： 通过环境变量"PF_LOOP_ARGUMENT"获取

> TIPS:
> 
> 通过模板"{{PF_PARENT.PF_LOOP_ARGUMENT}}"可以父节点的loop_argument在当次运行时的元素值

## 2.3 下游节点引用循环结构的输出artifact
由于循环结构可能会运行多次，因此，如果下游节点引用循环结构的输出artifact，获取到路径会与引用非循环结构的输出artifact有所不同。

为了方便讨论，做如下假设：
- 节点A的输入artifact[a1]引用了节点b的输出artifact[b1]

如果节点b不是循环结构： 
- 则节点A的输入artifact[a1]的值即为节点b的输出artifact[b1]的存储路径

如果节点b是循环结构：
- 则节点A的输入artifact[a1]的值为节点b的所有运行生成的输出artifact[b1]路径的聚合，路径与路径之间以','分割
  - 一个可能值如下：
    - "/home/paddleflow/storage/mnt/fs-root-ppl/.pipeline/run-000015/loop_example/process-0-244d8cc3b7f925461e4520c24a2640cc/result,/home/paddleflow/storage/mnt/fs-root-ppl/.pipeline/run-000015/loop_example/process-1-5737bb0763051eb2940da7442cd7fb00/result",

# 3 pipeline运行流程
当Paddleflow 开始调度执行某个节点前，会先检查其loop_argument字段是否有值，如果有值则会开始执行如下流程：

1. 替换loop_argument中的模板参数
   - 如果是artifact模板，则会使用artifact的文件内容来替换相应的模板
2. 将loop_argument字段转化为list
3. 令serial_num=0
4. 根据serial_num和节点配置信息，创建节点运行时
   - 这里需要关注如下两点：
     - 节点运行时在开始运行前会从loop_argument中获取第$serial_num项元素，并将其赋值给系统变量PF_LOOP_ARGUMENT
     - 节点运行时的名字按照如下规则组织: 
       - serial_num不等于0： {{PF_RUN_ID}}-{{STEP_NAME}}-$serial_num
       - serial_num等于0： {{PF_RUN_ID}}-{{STEP_NAME}}
  
5. serial_num 加1
6. 判断loop_argument的长度是否大于 $serial_num
  - 是： 跳转至第3步
  - 否： 开始执行所有的节点运行时

为了便于理解，这里使用[1 pipeline定义]中的示例，来进行说明：
- 假设RunID为run-000001
- 假设节点`randint`的输出artifact[random_num]的内容如下：
  - `'[1, 2, 3, 4, 5]'`
- 则节点`process`将会运行5次，运行时名字以及系统变量PF_LOOP_ARGUMENT的值如下表所示：
  
>  | 节点运行时名字 | PF_LOOP_ARGUMENT |
>  | :---: |  :---: |
>  | run-000001-process | 1 |
>  | run-000001-process-1 | 2 |
>  | run-000001-process-2 | 3 |
>  | run-000001-process-3 | 4 |
>  | run-000001-process-4 | 5 |


[loop_example]: /example/pipeline/loop_example
