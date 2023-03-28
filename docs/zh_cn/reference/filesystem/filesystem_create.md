# 命令行安装教程
https://github.com/PaddlePaddle/PaddleFlow/blob/develop/docs/zh_cn/deployment/how_to_install_paddleflow.md
# 创建各类fs的命令行参考如下
## S3
```
 paddleflow fs create <fsname> s3://<bucketname>/<subpath> -o accessKey=<ak> -o secretKey=<sk> -o region=<region> -o endpoint=<endpoint>
```
> 注意：其中括号内是您需要填写的内容

```
参考命令
paddleflow fs create s3name s3://paddleflow/data -o accessKey=****** -o secretKey=****** -o region=bj -o endpoint=s3.bj.bcebos.com
```
## bos
```
 paddleflow fs create <fsname> s3://<bucketname>/<subpath) -o accessKey=<ak> -o secretKey=<sk> -o region=<region> -o endpoint=<endpoint>
```
> 注意：其中括号内是您需要填写的内容

bos和s3的不同之处是url前缀是bos以及endpoint的写法
```
参考命令
paddleflow fs create bosname bos://paddleflow/data -o accessKey=***** -o secretKey=***** -o region=bj -o endpoint=bj.bcebos.com
```
## minio
```
paddleflow fs create <fsname> s3://<bucket>/<subpath> -o accessKey=<ak> -o secretKey=<sk> -o endpoint=<endpoint> -o insecureSkipVerify=true -o s3ForcePathStyle=true
```
> 注意创建minio的时候,-o insecureSkipVerify=true -o s3ForcePathStyle=true为必填项

```
参考命令
paddleflow fs create minioname s3://paddleflow/data/ -o accessKey=**** -o secretKey=**** -o endpoint=127.0.0.1:9000 -o insecureSkipVerify=true -o s3ForcePathStyle=true
```
## hdfs
```
paddleflow fs create <fsname> hdfs://<hdfs_address>/<subpath> -o group=<group> -o user=<user>}{
```
> 如果您使用的是kerbos访问hdfs，请通过paddleflow fs create --help查看kerbos的填写参数

```
参考命令
paddleflow fs create hdfsname hdfs://127.0.0.1:9000/data/ -o group=*** -o user=***
```

## sftp
```
paddleflow fs create <fsname> sftp://<sftp_address>/<subpath>
 -o user=<username> -o password=<password>
```
> 注意，sftp的用户名和密码也就是机器ssh的登陆密码和用户名

```
参考命令
paddleflow fs create sftpname sftp://127.0.0.1:22/data
 -o user=*** -o password=***
```
## glusterfs
```
paddleflow fs create <fsname> glusterfs://<gluster_address>:<volume>
```
> 注意，使用paddleflow创建glusterfs的时候，paddleflow-server的镜像需要装glusterfs fuse的客户端，并且镜像默认是root用户

```
参考命令
paddleflow fs create glusterfsname glusterfs://127.0.0.1:default-volume
```

## cfs
[文件存储CFS](https://cloud.baidu.com/product/cfs.html)(Cloud File Storage)是百度智能云提供的安全、可扩展的文件存储服务。通过标准的文件访问协议，为云上的虚机、容器等计算资源提供无限扩展、高可靠、全球共享的文件存储能力
创建的命令如下
```
参考命令
paddleflow fs create cfsname cfs://<cfs-id>/<subpath>
```
> 需要paddleflow-server镜像支持nfs4协议，并且网络可访问云上cfs

```
参考命令
paddleflow fs create cfsname cfs://cfs-id/data/
```