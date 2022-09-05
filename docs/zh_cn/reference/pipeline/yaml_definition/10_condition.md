在某些情况下，对于pipeline中的部分节点，希望在满足特定的条件时才执行。

为此，Paddleflow pipeline提供了condition特性，只有满足condition的条件时，才会调度执行当前节点

# 1 pipeline定义
下面为一个使用了condition特性的示例Pipeline定义

> 该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/condition_example示例。
> 
> 示例链接：[condition_example]

```yaml
name: condition_example

entry_points:
  step1:
    command: "echo {{num}}"
    condition: "{{num}} > 0"
    parameters:
      num: 10

  step2:
    command: "echo {{num}}"
    condition: "{{num}} < 0"
    parameters:
      num: 10

parallelism: 2

docker_env: nginx:1.7.9
```

# 2 详解
### 2.1 condition字段
在Paddleflow中，使用condition特性的方式非常简单，只需要给节点的condition字段赋值即可。

> **DAG节点**和**Step节点**均支持condition字段

condition字段的值需要是一个条件判断式，支持一些常见的操作符，如：
- 比较运算符： >, <, >=, <=, ==, !=
- 逻辑运算符： &&, ||, !

支持操作数类型有以下几种：
- 字面值常量： 如 1，2，"xiaoming"等
- 本节点的parameters模板
- 本节点的**输入artifact**模板
  - 需要是一个文件
  - 文件大小需要**小于1KB**

> TIPS:
> - 为了Paddleflow能够正确的解析condition字段，请用**引号**将条件判断式引起来
> - condition的计算逻辑由[govaluate]支持, 感兴趣的同学可以点击查看更多信息
  
### 2.2 约束
当节点指定了condition时，该节点便有可能不会运行，因此，Paddleflow对与指定了condition字段的节点增加了如下的约束：

- 如果节点A指定了condition字段，则节点A不能被任何节点依赖


# 3 pipeline运行流程
当Paddleflow开始调度执行某个节点时，会检查其condition字段是否有值，如果有值则会开始执行如下流程：

1. 替换condition字段中的模板
   -  如果是artifact模板，则会使用artifact的文件内容来替换相应的模板
2. 计算条件判断式的值
  - 如果值为假：将节点状态置为 skipped，并结束当前节点的运行
  - 如果值为真：继续运行当前节点
  

为了便于理解，这里使用[1 pipeline定义]中的示例，来进行说明：
- 对于节点`step1`, 其condition(10 > 0)的计算结果为True, 因此`step1`将会被正常调度执行
- 对于节点`step2`, 其condition(10<0>)的计算结果为False, 因此`step2`的状态将会被置为Skipped


[condition_example]: /example/pipeline/condition_example
[govaluate]: https://pkg.go.dev/github.com/Knetic/govaluate#section-readme
