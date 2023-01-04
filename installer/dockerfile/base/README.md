# base 镜像构建目录
## base
`paddleflow server`与`csi-plugin`都基于此镜像进行构建
```shell
docker build -t paddleflow/alpine:1.2 .
```
## base.gluster
构建有gluster包以及arm64v8架构的基础镜像