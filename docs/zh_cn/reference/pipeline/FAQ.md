## 1、在Pipeline中是否支持使用hostpath类型的[共享存储]?

支持，但是需要注意以下几点：
- 如果是在分布式集群中运行pipeline, 且不同机器上的hostpath在底层是完全独立的存储，则需要注意如下两点
  - cache功能不一定符合预期：因为不同机器上的hostpath同名文件modtime不一定相同
  - 不能使用hostpath类型的存储来存放artifact：因为节点A和节点B有可能会调度到不同的机器上，此时节点A的输出artifact将无法被节点B访问到。
- 如果是单机集群，或者分布式集群中所有机器上的hostpath使用了同一个存储（比如在所有的机器hostpath目录下都挂载了同一个BOS存储），此时则可以正常使用Pipeline的所有功能

## 2、使用S3类型的共享存储时，无法命中cache？
因为目前在计算[第二层Fingerprint]时，会去获取相关路径的 modtime，但是在S3存储中，目录的modtime与其在 pod中的挂载时间相关，也即导致每运行一个节点，目录的modtime都会发生变动，进而导致[第二层Fingerprint]的值有变化，从而无法命中cache。

如果在使用了S3存储的同时，还想命中cache，则需要保证参与cache计算的相关路径均为文件，不能是目录

[共享存储]: /docs/zh_cn/reference/filesystem
[第二层Fingerprint]: /docs/zh_cn/reference/pipeline/yaml_definition/5_cache.md#42-cache-%E5%91%BD%E4%B8%AD%E6%9C%BA%E5%88%B6