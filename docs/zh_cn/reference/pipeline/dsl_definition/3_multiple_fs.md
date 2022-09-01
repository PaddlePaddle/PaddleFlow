在本章节中，我们将介绍如何在DSL中使用[共享存储][共享存储], 关于在Pipeline中使用共享存储的详细介绍，请参考[这里][multiple_fs_yaml]。

# 1、pipeline示例
下面是一个在DSL中使用共享存储的案例：

>该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/multiple_fs示例
>
>示例链接: [multiple_fs][multiple_fs]

```python3
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import ExtraFS
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions

def main_fs():
    return ContainerStep(
        name="main-fs",
        outputs={"main": Artifact()},
        command="cd shells/main_fs && bash run.sh",
        docker_env="centos:centos7",
    )
    
def global_extra_fs():
    return ContainerStep(
        name="global-extra-fs",
        outputs={"global_extra": Artifact()},
        command="cd /home/global && bash run.sh",
        docker_env="centos:centos7"
    )

def step_extra_fs():
    return ContainerStep(
        name="extra-fs",
        outputs={"extra": Artifact()},
        command="cd /home/extra && bash run.sh",
        docker_env="centos:centos7",
        extra_fs=[ExtraFS(name="ppl", sub_path="multiple_fs/shells/extra_fs", mount_path= "/home/extra")],
    )
    

@Pipeline(name="multiple_fs", docker_env="nginx:1.7.9", parallelism=2)
def multiple_fs():
    main_fs()
    global_extra_fs()
    step_extra_fs()

if __name__ == "__main__":
    main = MainFS(
        name="ppl",
        sub_path="multiple_fs",
        mount_path="/home/main"
        )
    extra = ExtraFS(
        name="ppl",
        sub_path="multiple_fs/shells/global_extra_fs",
        mount_path="/home/global"
    )
    ppl = multiple_fs()
    ppl.fs_options = FSOptions(main, [extra])
    print(ppl.run())
```

# 2、节点级别的extra_fs
在DSL中，用户在定义节点时，可以通过extra_fs字段指定共享存储的挂载信息。

这里需要注意的是，节点的extra_fs需要是一个list，其中的每一个元素都应该是一个[ExtraFS]实例。

如上例中，Step `extra-fs` 便指定其在运行时需要挂载的共享存储信息。

```python3
def step_extra_fs():
    return ContainerStep(
        name="extra-fs",
        outputs={"extra": Artifact()},
        command="cd /home/extra && bash run.sh",
        docker_env="centos:centos7",
        extra_fs=[ExtraFS(name="ppl", sub_path="multiple_fs/shells/extra_fs", mount_path= "/home/extra")],
    )
```

# 3、Pipeline级别的fs_options
除了可以在定义节点时指定extra_fs外，也可以在pipeline级别设置共享存储信息。

Pipeline级别的共享存储信息，通过访问Pipeline实例的fs_options属性来完成设置。fs_options属性需要是一个[FSOptions][FSOptions]实例，其详细介绍请参考[这里][fs_options_yaml]，此处不再赘述。

在上面的示例中，便通过如下的代码设置了Pipeline级别得共享存储信息:
```python3
if __name__ == "__main__":
    main = MainFS(
        name="ppl",
        sub_path="multiple_fs",
        mount_path="/home/main"
        )
    extra = ExtraFS(
        name="ppl",
        sub_path="multiple_fs/shells/global_extra_fs",
        mount_path="/home/global"
    )
    ppl = multiple_fs()
    ppl.fs_options = FSOptions(main, [extra])
    print(ppl.run())
```


[共享存储]: /docs/zh_cn/reference/filesystem/filesystem_overview.md
[ExtraFS]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#11extrafs
[FSOptions]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#13fsoptions
[multiple_fs_yaml]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md
[multiple_fs]: /example/pipeline/multiple_fs
[fs_options_yaml]: /docs/zh_cn/reference/pipeline/yaml_definition/3_multiple_fs.md#22-%E5%85%A8%E5%B1%80%E7%BA%A7%E5%88%AB%E7%9A%84fs_options%E5%AD%97%E6%AE%B5