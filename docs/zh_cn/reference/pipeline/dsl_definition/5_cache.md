在某些情况下，对于已经运行过的Step，我们希望其可以直接使用上次运行的结果，以节省运行时间，加快迭代效率，此时我们便需要使用Cache机制。

关于Cache机制的详细解释，请点击[这里][Cache-ref]。本文主要讲解如何在使用DSL定义pipeline时如何使用Cache机制，不在对其定义进行赘述。

# 1、pipeline示例
下面的示例是基于[artifact_pipeline][artifact_pipeline]中的pipeline定义增加cache相关的配置升级而来
>该示例中pipeline定义，以及示例相关运行脚本，来自paddleflow项目下example/pipeline/cache_example示例。
>
>示例链接：[cache_example][cache_example]

```python3
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import Parameter
from paddleflow.pipeline import Artifact
from paddleflow.pipeline import PF_RUN_ID
from paddleflow.pipeline import MainFS
from paddleflow.pipeline import FSOptions
from paddleflow.pipeline import CacheOptions
from paddleflow.pipeline import FSScope

def preprocess():
    """ data preprocess step
    """
    fs_scope=FSScope(name="ppl", path="cache_example/run.yaml")
    cache = CacheOptions(enable=True, max_expired_time=300, fs_scope=[fs_scope])
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./cache_example/data/"},
        outputs={"train_data": Artifact(), "validate_data": Artifact()},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        cache_options=cache,
        command="bash -x cache_example/shells/data_artifact.sh {{data_path}} {{train_data}} {{validate_data}}",
    )
    return step
    
def train(epoch, train_data):
    """ train step
    """
    step = ContainerStep(
        name="train",
        command="bash -x cache_example/shells/train.sh {{epoch}} {{train_data}} {{train_model}}",
        inputs={"train_data": train_data},
        outputs={"train_model": Artifact()},
        parameters={"epoch": epoch}
    )
    return step
    
def validate(model, data):
    """ validate step
    """
    cache = CacheOptions(enable=False, max_expired_time=-1)
    step = ContainerStep(
        name="validate",
        command="bash cache_example/shells/validate.sh {{model}}",
        inputs={"model": model, "data": data},
        cache_options=cache,
    )    
    return step


fs_scope=FSScope(name="ppl", path="cache_example/shells")
cache = CacheOptions(enable=True, max_expired_time=600, fs_scope=[fs_scope])

@Pipeline(name="cache_example", docker_env="nginx:1.7.9", parallelism=1, cache_options=cache)
def cache_example(epoch=15):
    """ pipeline example for artifact
    """
    preprocess_step = preprocess()
    train_step = train(epoch, preprocess_step.outputs["train_data"])
    validate_step = validate(train_step.outputs["train_model"], preprocess_step.outputs["validate_data"])
    

if __name__ == "__main__":
    ppl = cache_example()
    
    print(ppl.run(fs_name="ppl"))
```
与直接通过编写yaml文件来使用Cache一样，DSL也支持pipeline级别和Step级别两种层级的Cache配置，接下来我们将会依次介绍


# 2、Pipeline级别的Cache
在调用Pipeline的初始化函数时，可以给其cache_options参数进行赋值，来设置pipeline级别的Cache。cache_options的参数值需要是一个[CacheOptions][CacheOptions]的实例。在上例中，我们通过如下的代码，设置了Pipeline的Cache配置：
```python3
fs_scope=FSScope(name="ppl", path="cache_example/shells")
cache = CacheOptions(enable=True, max_expired_time=600, fs_scope=[fs_scope])

@Pipeline(name="cache_example", docker_env="nginx:1.7.9", parallelism=1, cache_options=cache)
def cache_example(epoch=15):
    """ pipeline example for artifact
    """
``` 

# 3、Step级别的Cache
在调用ContainerStep的初始化函数时，可以给其cache_options参数进行赋值，来设置Step级别的Cache配置。

cache_options的参数值需要是一个[CacheOptions][CacheOptions]的实例。在上例中，我们通过如下的代码, 给preprocess_step设置了自身的cache配置：
```python3
def preprocess():
    """ data preprocess step
    """
    fs_scope=FSScope(name="ppl", path="cache_example/run.yaml")
    cache = CacheOptions(enable=True, max_expired_time=300, fs_scope=[fs_scope])
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./cache_example/data/"},
        outputs={"train_data": Artifact(), "validate_data": Artifact()},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        cache_options=cache,
        command="bash -x cache_example/shells/data_artifact.sh {{data_path}} {{train_data}} {{validate_data}}",
    )
    return step
``` 

[Cache-ref]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md
[DSL使用基础]: /docs/zh_cn/reference/pipeline/dsl_definition/1_pipeline_basic.md
[artifact_pipeline]: /docs/zh_cn/reference/pipeline/dsl_definition/2_artifact.md
[cache_example]: /example/pipeline/cache_example
[CacheOptions]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md#9CacheOptions
[DSL-PostProcess-And-FailureOpitons]: /docs/zh_cn/reference/pipeline/dsl_definition/4_failure_options_and_post_process.md
[DSL接口文档]: /docs/zh_cn/reference/sdk_reference/pipeline_dsl_reference.md
