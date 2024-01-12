# 1、 说明
本文档记录了有IB/RoCE高性能网络的情况下，PaddleFlow训练作业如何使用IB/RoCE硬件设备进行RDMA网络加速。

# 2、 环境准备
## 2.1 高性能网络硬件配置
在GPU节点上安装IB或者支持RoCE v2的网卡。  

在节点上配置网卡的ip策略路由。
```shell
# 配置网卡IP策略路由，该步骤仅需在GPU节点上执行一次
$ ip ro add 10.245.163.0/8 via 192.168.0.1 dev xgbe0 src 23.10.2.22 table 100

$ ip rule
0:	from all lookup local
10000:	from 23.10.2.22 lookup 100
32766:	from all lookup main
32767:	from all lookup default

$ ip ro show table 100
```

## 2.2 高性能网络软件初始化
需要在系统中安装RDMA相关的内核驱动以及用户态相关库。

### Ubuntu系统安装rdma相关用户态工具
```shell
apt-get install infiniband-diags perftest
```
注意：`show_gids`和`mlnx_perf` 是`Melleanox InfiniBand`相关工具，需要通过Mellanox OFED安装

## 2.3 高性能网络测试
### 基础测试
```shell
# sever端： 
$ ib_write_bw -d mlx5_0 -x 3
# client端：
$ ib_write_bw -d mlx5_0 -x 3  --run_infinitely ${server_ip}
# 查看rdma相关监控，需要5队列有值
$ mlnx_perf -i xgbe0
   ...
   rx_prio5_bytes: 2,748,712,164 Bps    = 21,989.69 Mbps
              rx_prio5_packets: 2,597,256
                tx_prio5_bytes: 2,746,951,112 Bps    = 21,975.60 Mbps
              tx_prio5_packets: 2,595,352
```

### 性能测试（nccl-test）
```shell
# all_reduce_perf
# 实际使用中，注意下 hostfile 和网卡设备名称
mpirun --allow-run-as-root \
        --np 2 \
        --map-by node \
        --mca btl self,tcp \
        --mca routed_radix 300 \
        --mca btl_tcp_if_include xgbe0 \
        --hostfile /home/nccl/hostfile \
        -x NCCL_SOCKET_IFNAME=xgbe0 \
        -x NCCL_IB_HCA=mlx5_0 \
        -x PATH \
        -x LD_LIBRARY_PATH \
        -x NCCL_IB_GID_INDEX=3 \
        /home/nccl/all_reduce_perf -b 1024 -e 4G -f 2 -g 1 -t 8 -c 0 -n 20
```

# 3、 PaddleFlow应用
PaddleFlow支持两种方式使用IB/RoCE网络进行RDMA加速, 分别是hostNetwork方式和device-plugin方式。

## 3.1 基于hostNetwork的高性能网络使用
PaddleFlow通过主机网络的方式使用RDMA网络，需要root权限、挂载相关设备到容器里面、NCCL相关配置。

|配置项|配置值|备注|
|---|---|---|
|容器root权限| |
|挂载设备到容器|挂载 /dev/infiniband 设备目录到容器内|
|NCCL相关配置|使用环境变量配置：<br> NCCL_SOCKET_IFNAME=xgbe0  <br> NCCL_IB_DISABLE="0"  <br> NCCL_IB_GID_INDEX="3"  <br> NCCL_IB_HCA="mlx5_0"  </br> NCCL_DEBUG=INFO </br> | 相关参数说明：<br> NCCL_SOCKET_IFNAME，该环境变量指定IB设备名称 <br> NCCL_IB_DISABLE，该环境变量指定是否使用 IB/RDMA 网络 <br> NCCL_IB_GID_INDEX，该环境变量指定用IB设备的哪个index来跑ROCE v2 <br> NCCL_IB_HCA， 该环境变量用于获取虚机用的哪个IB设备 |

注意：启动容器的时候，加上 --ulimit memlock=-1:-1，解决 "Couldn/t allocate MR" 报错。

## 3.2 基于device-plugin的高性能网络使用
作业提交时，在Flavour中直接设置rdma资源名称，例如：可以是 `rdma/hca: 1`