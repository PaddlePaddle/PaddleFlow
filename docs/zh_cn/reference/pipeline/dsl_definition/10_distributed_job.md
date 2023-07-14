本章节主要介绍如何使用DSL来定义分布式任务。

# 1 pipeline定义
下面是添加了distributed_job的DSL格式pipeline定义。
> 该示例中pipeline定义，以及示例相关运行脚本，来自Paddleflow项目下example/pipeline/distributed_job_example示例。

```python3
def preprocess():
    """ data preprocess step
    """
    step = ContainerStep(
        name="preprocess",
        docker_env="centos:centos7",
        parameters={"data_path": f"./distributed_job_example/data/{PF_RUN_ID}"},
        env={"USER_ABC": "123_{{PF_USER_NAME}}"},
        command="bash distributed_job_example/shells/data.sh {{data_path}}"
    )
    return step

def train(epoch, train_data):
    """ distributed job
    """
    dist_jobs = DistributedJob(
        framework="paddle",
        members=[Member(role="pworker", replicas=2, image="paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
                        command="sleep 30; echo worker {{epoch}} {{train_data}} {{model_path}}"),
                 Member(role="pserver", replicas=2, image="paddlepaddle/paddle:2.0.2-gpu-cuda10.1-cudnn7",
                        command="sleep 30; echo ps {{epoch}} {{train_data}} {{model_path}}")]
    )

    """ train step
    """
    step = ContainerStep(
        name="train",
        parameters={
            "epoch": epoch,
            "model_path": f"./output/{PF_RUN_ID}",
            "train_data": train_data
        },
        env={"PS_NUM": "2", "WORKER_NUM": "2"},
        command="",
        distributed_job=dist_jobs,
    )
    return step


@Pipeline(name="distributed_pipeline", docker_env="nginx:1.7.9", parallelism=1)
def distributed_pipeline(epoch=15):
    """ pipeline example for distributed job
    """
    preprocess_step = preprocess()
    train_step = train(epoch, preprocess_step.parameters["data_path"])


if __name__ == "__main__":
    ppl = distributed_pipeline()

    main_fs = MainFS(name="ppl")
    ppl.fs_options = FSOptions(main_fs)
    print(ppl.run())
```

# 2、定义DistributedJob
将多个member成员组装成一个DistributedJob可以分成如下三步：
1. 创建DistributedJob实例
2. 定义分布式任务的framework
3. 根据role创建Member列表，依次配置副本数、镜像、flavour等字段。

