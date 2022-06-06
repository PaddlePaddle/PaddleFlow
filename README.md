**PaddleFlow**简称PF，基于云原生Kubernetes或K3s，提供面向AI开发的批量作业执行系统，并且提供易用的共享文件系统，在apache license2.0 开源协议下发布。PaddleFlow可以作为机器学习平台的资源核心，适用于机器学习和深度学习的单机和分布式作业。
# 核心特性
## 1.存储
#44444
- posix协议支持
- 支持快照（商业版本）
- 支持读缓存
## 2.调度
- 基于kubernetes的计算资源池化管理
- 基于华为开源的volcano的队列调度
- 内置主流分布式计算框架引擎（Paddle、Spark、Tensorflow等）
## 3.工作流
- DAG工作流调度（断点运行等）
- 抽象复杂的命令，模板化可多次调用
# 架构
PaddleFlow由四个部分组成：
- 1.PaddleFlow 客户端（包含PaddleFlow fuse）: 命令行工具方便用户在开发机安装和使用，其中PaddleFlow fs管理以及fuse主要用于缓存数据和快照等能力，加速远端数据读写，可以支持多种数据源的对接，比如bos，hdfs，本地文件系统。
- 2.PaddleFlow server: PaddleFlow核心服务，主要包含队列、存储、工作流等的管理。
- 3.volcano（基于开源volcano改造）: 主要增加elastic quota更灵活管理资源的能力，未来会逐步提交社区review。
- 4.pf-csi-plugin: 基于k8s csi插件机制实现了PaddleFlow的文件系统接入。

![PaddleFlow 功能架构](docs/zh_cn/images/pf-arch.png) 

PaddleFlow的部署主要分为客户端和服务端，其中客户端主要用于准备和打包作业，服务端主要用于作业解析和作业管理，其中执行作业如图中示例主要为kubernetes和k3s。其中，他们会共用一个共享的文件系统，这样会更加方便用户更加直观的查看作业状态和日志等。

![PaddleFlow 部署架构](docs/zh_cn/images/pf-deploy-arch.png)

# 开始使用
在使用PaddleFlow之前需要做一下准备：
- 1.准备kubernetes环境
- 2.下载安装PaddleFlow客户端
请参考快速上手指南立即开始使用PaddleFlow
## 1.命令行参考
点击[命令行操作说明](docs/zh_cn/reference/client_command_reference.md) 获取所有操作命令和示例。
## 2.python sdk参考
点击[sdk使用说明](docs/zh_cn/reference/sdk_reference.md) 获取sdk的使用说明。
# 开源协议
使用 apache license 2.0开源，详见 LICENSE。
# PaddlePaddle相关能力使用
待补充。



