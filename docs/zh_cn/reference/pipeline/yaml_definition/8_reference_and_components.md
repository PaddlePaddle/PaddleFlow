在[7_dag]章节中，我们介绍了有效的降低编排复杂度的一个特性--DAG，在本章节中，我们将介绍提升节点复用性，进一步提升编排效率的特性： referenece && components

# 1 使用场景
在使用python，c++等编程语言编写程序时，对于一段需要在多处执行的逻辑，我们往往会将其抽象成一个函数，然后在需要的地方调用该函数，通过这种方式可以有效的降低维护成本。

在Paddleflow pipeline中, 也提供了类似的机制：用户可以通过[components]字段来定义'函数'，通过[reference]字段来调用指定的'函数'。

# 2 pipeline定义
下面为一个示例Pipeline定义

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/ref_components示例。
> 
> 示例链接：[ref_components]

```yaml
name: ref_components

docker_env: python:3.7

entry_points:
  randint:
    command: "cd /randint && python3 randint.py {{num}} {{lower}} {{upper}}"
    parameters:
      lower: -10
      upper: 10
      num: 10
    artifacts:
      output:
      - random_num
    extra_fs:
    - {name: ppl, mount_path: /randint, sub_path: ref_components/randint, read_only: true}

  process:
    deps: randint
    reference:
      component: process
    artifacts:
      input: 
        data: "{{randint.random_num}}"

  sum:
    deps: process
    command: "cd /sum && python3 sum.py"
    artifacts:
      input:
        nums: "{{process.result}}"
      output:
        - result
    extra_fs:
    - {name: ppl, mount_path: /sum, sub_path: ref_components/sum, read_only: true}

parallelism: 2

fs_options:
  main_fs: {name: ppl}

components:
  process:
    artifacts:
      input: 
        data: ""
      output:
        result: "{{collector.collection}}"
    entry_points:

      split:
        reference:
          component: split-by-threshold
        artifacts:
          input:
            nums: "{{PF_PARENT.data}}"

      process-negetive:
        command: "cd /process_negetive && python3 process_negetive.py"
        deps: split
        artifacts:
          input:
            negetive: "{{split.negetive}}"
          output:
          - result
        extra_fs:
        - {name: ppl, mount_path: /process_negetive, sub_path: ref_components/process_negetive, read_only: true}

      process-positive:
        command: "cd /process_positive && python3 process_positive.py"
        deps: split
        artifacts:
          input:
            positive: "{{split.positive}}"
          output:
          - result
        extra_fs:
        - {name: ppl, mount_path: /process_positive, sub_path: ref_components/process_positive, read_only: true}

      collector:
        deps: process-negetive,process-positive
        command: cd /collector && python3 collect.py
        artifacts:
          input:
            negetive: "{{process-negetive.result}}"
            positive: "{{process-positive.result}}"
          output:
          - collection
        extra_fs:
        - {name: ppl, mount_path: /collector, sub_path: ref_components/collector, read_only: true}
  
  split-by-threshold:
    command: "cd /split && python3 split.py {{threshold}}"
    artifacts:
      output:
      - negetive
      - positive
      input:
        nums: ""
    parameters:
      threshold: 0
    extra_fs:
      - {name: ppl, mount_path: /split, sub_path: ref_components/split, read_only: true}
```

# 3 详解
## 3.1 components
Paddleflow pipeline在全局级别新增了components字段，用于定义可以被[reference]的节点。

> components字段entry_points字段的区别？
> - entry_points是pipeline的执行入口，可以类比于程序的main函数，在发起Run时，定义在该字段中的节点都将会被调度执行（没有disable）。
> - components字段只是用于节点定义，在其中定义的节点，可类比于程序中的普通函数，在发起Run时，**只有被调用（直接或者间接refernece）**的节点才会被调度执行。

在components中节点的定义方式与在entry_points字段中节点的定义方式基本保持一致，只需注意以下几点约束即可：

### 3.1.1 deps 约束
components字段的定义的节点，只有在被调用([reference])时才会执行，因此，其依赖关系应该由调用方决定，所以其`deps`字段不能有值。

> 注意： 这里针对的是components中的最外层节点，子节点不受影响
> - 如[2 pipeline定义]中，components字段定义的最外层节点`process`和`split-by-threshold`的deps字段不能有值。但是`process`的子节点`process-positive`可以照常定义其deps字段。
 
## 3.2 reference
一旦在components字段中定义节点，便可以在**所有**可以定义节点的地方（entry_points, post_process, components）去引用components中的节点。

>注意：只能引用components中的最外层节点，以[2 pipeline定义]为例：
>- components字段中的节点`process`和`split-by-threshold`可以被reference
>- `process`的子节点如`process-positive`则不能被reference

引用的方式很简单，只需给节点的reference字段赋值即可，如下所示(摘抄自[2 pipeline定义])：
```yaml
  process:
    deps: randint
    reference:
      component: process
    artifacts:
      input: 
        data: "{{randint.random_num}}"
```

> reference的子字段`component`表明其引用的是components中哪一个节点。

如果一个节点的reference字段有值，则需要遵守如下的约束：

### 3.2.1 字段约束
如果一个节点的reference字段有值，则该节点只支持如下字段：
- deps
- reference
- parameters
- artifacts.input
  > 只支持定义输入artifact，不支持定义输出artifact：
  > - 当前节点的输出artifact由其所引用的节点（components字段中定义的节点）决定

### 3.2.2 parameters约束
如果节点A reference了节点B，则A的parameters字段需要遵守如下约束：
- A的parameters字段必须为B的parameters字段的子集
  > 如果B定义了两个parameter: p1 和 p2, 则A的parameter的名字必须为p1或p2, 不能定义一个名为p3的parameter

- 如果B中某个parameter[p1]指定了类型，则A的同名parameter[p1]必须满足相应的类型要求

### 3.2.3 输入artifact约束
如果节点A reference了节点B，则A的输入artifact必须是节点B的输入artifact的全集。

- 节点的输入artifact必须在节点运行前便处于ready的状态
- 节点的输入artifact必须来自于其上游节点的输出artifact，而components中的节点在定义时不能指定上游节点

### 3.2.4 reference约束
暂时不支持递归形式的reference， 比如下面两种情况将会报错：

- 节点reference自身
-  A reference B， B reference C，C reference A


# 4 pipeline运行流程
当使用pipeline创建run时，Paddleflow会根据依赖关系，依次调度entry_points中所定义的节点，如果当前节点的 reference字段不为空，则会执行如下的处理流程。

> 为了方便讨论，在这里将reference不为空的节点称为节点A，而其引用的components中的节点，称为节点B。

- 根据A的reference.component字段找到B
- 拷贝节点B，将得到副本称之为节点C
- 使用节点A的名字覆盖节点C的名字
- 使用节点A的deps字段覆盖节点C的deps字段
- 使用节点A的输入artifact字段覆盖节点C的输入artifact字段
- 使用节点A的parameter覆盖节点C中同名Parameter的值
  > 举个例子：
  >
  > 假设B有如下形式的parameters：
  >
  > ```{"p1":5, "p2": 6}```
  >
  > A有如下形式的parameters:
  >
  > ```{"p1":10}```
  >
  > 则最终节点C的paramter为：
  >
  > ```{"p1":10, "p2": 6} ```
- 调度执行节点C

相信看到这里，有部分细心的同学已经发现，本文所用的示例，与[7_dag]所用的示例，在运行Run时是完全等价的。感兴趣的用户可以进一步对照着这两个例子印证上述的处理流程。



[ref_components]: /example/pipeline/ref_components
[reference]: /docs/zh_cn/reference/pipeline/yaml_definition/8_reference_and_components.md#32-reference
[components]: /docs/zh_cn/reference/pipeline/yaml_definition/8_reference_and_components.md#31-components
[2 pipeline定义]: /docs/zh_cn/reference/pipeline/yaml_definition/8_reference_and_components.md#2-pipeline%E5%AE%9A%E4%B9%89
[7_dag]: /docs/zh_cn/reference/pipeline/yaml_definition/7_dag.md