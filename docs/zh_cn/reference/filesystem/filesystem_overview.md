# paddleflow 存储介绍

为方便用户使用PaddleFlow存储功能，不过多依赖其他模块，现PaddleFlow存储模块提供存储管理的相关接口以及mount的相关接口，方便用户快速使用PaddleFlow的功能。

## 存储管理

`fs` 提供了`create`,`show`, `delete`, `list`, `mount`，`link`,`unlink`, `listlink`, `showlink`九种不同的方法。 九种不同操作的示例如下：

```bash
paddleflow fs list -u username// 展示fs列表 -u 表示特定用户的fs
paddleflow fs show fsname -u username// 显示某个fs详情 -u 表示特定用户的fs
paddleflow fs delete fsname -u username // 删除某个fs -u 表示特定用户的fs 
paddleflow fs create fsname url -o ak=xxx -o sk=xxx //创建fs 
paddleflow fs mount fsname /home/mountpath -u username //把某个文件系统挂载到本地 
paddleflow fs link fsname fspath url -o ak=xxx -o sk=xxx -u username //创建link -u 表示特定用户的fs
paddleflow fs unlink fsname fspath -u username // 删除某个特定用户特点文件系统下的link -u 表示特定用户的fs
paddleflow fs listlink fsname -u username// 展示某个文件系统下面的link列表 -u 表示特定用户的fs
paddleflow fs showlink fsname fspath -u username// 显示某个link详情 -u 表示特定用户的fs
```

### 示例

存储列表：用户输入```paddleflow fs list```

```bash
+----------+---------+--------+-----------------------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| name     | owner   | type   | server address                    | sub path           | properties                                                                                                                                                                                                                                  |
+==========+=========+========+===================================+====================+=============================================================================================================================================================================================================================================+
| sftp1 | root    | sftp   | localhost:8001                 | /data2             | {'password': 'xxx', 'user': 'xxx'}                                                                                                                                                                             |
+----------+---------+--------+-----------------------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| hdfs1 | root    | hdfs   | 192.168.1.2:9000,192.168.1.3:9000 | /elsiefs           | {'dfs.namenode.address': '192.168.1.2:9000,192.168.1.3:9000', 'group': 'test', 'user': 'test'}
```

存储详情：用户输入```paddleflow fs show {fs_name}```，界面上显示

```bash
+--------+---------+--------+------------------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| name   | owner   | type   | server address   | sub path   | properties                                                                                                                                                                                                                                  |
+========+=========+========+==================+============+=============================================================================================================================================================================================================================================+
| s3-1   | root    | s3     | s3.com | /myfs1     | {'accessKey': 'xxx', 'bucket': 'test', 'endpoint': 's3.com', 'region': 'bj', 'secretKey': 'xxx'} |
+--------+---------+--------+------------------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

创建存储：用户输入```paddleflow fs create {fs_name} {fs_url} -o {}```，可以通过```paddleflow fs create --help```查看具体创建选项,界面上显示
```fs[{fs_name}] create success```

删除存储：用户输入```paddleflow fs delete {fs_name}```，界面上显示
```fs[{fs_name}] delete success```

mount命令：用户输入```paddleflow fs mount {fs_name} {mountpath}```，界面上显示
```mount success```，还可以通过paddleflow fs mount --help查看挂载其他详细参数，如下
```azure
Usage: paddleflow fs mount [OPTIONS] FSNAME PATH

  mount fs
FSNAME: mount fs name which contain mount fs and mount path
PATH: your local mount path

Options:
  -u, --username TEXT  Mount the specified fs by username, only useful for root.
  -o, --o TEXT         mount options:
                         -o block-size: data cache block size (default: 20971520), if block-size equals to 0, it means that no data cache is used
                         -o data-cache-path: directory path of local data cache (default:"/var/cache/pfs-cache-dir/data-cache")
                         -o data-cache-expire: file data cache timeout (default 0s)
                         -o meta-cache-path: directory path of meta cache (default:"/var/cache/pfs-cache-dir/meta-cache")
                         -o meta-cache-driver: meta driver type (e.g. mem, disk)",
                         -o meta-cache-expire: file meta cache timeout (default 5s)
                         -o entry-cache-expire: file entry cache timeout (default 5s)
```

某个文件系统下的Link列表：用户输入```paddleflow fs listlink {fsname}```

```bash
+----------+---------+--------+------------+------------------+--------------------+-------------------+
| name     | owner   | type   | fs path    | server address   | sub path           | properties        |
+==========+=========+========+============+==================+====================+===================+
| local12  | root    | local  | /linktest1 |                  | /home/yoursubpath/1| {'debug': 'true'} |
+----------+---------+--------+------------+------------------+--------------------+-------------------+
| local12  | root    | local  | /linktest2 |                  | /home/yoursubpath/1| {'debug': 'true'} |
+----------+---------+--------+------------+------------------+--------------------+-------------------+
```

某个Link详情：用户输入```paddleflow fs showlink {fsname} {fspath}```，界面上显示

```bash
+----------+---------+--------+------------+------------------+--------------------+-------------------+
| name     | owner   | type   | fs path    | server address   | sub path           | properties        |
+==========+=========+========+============+==================+====================+===================+
| local12  | root    | local  | /linktest1 |                  | /home/yoursubpath/7| {'debug': 'true'} |
+----------+---------+--------+------------+------------------+--------------------+-------------------+
```

创建link：用户输入```paddleflow fs link {fs_name} {fspath} {fs_url} -o {}```，可以通过```paddleflow fs link --help```查看具体创建选项,界面上显示
```fs[{fs_name}] create link success```

删除link：用户输入```paddleflow fs unlink {fs_name} {fspath}```，界面上显示
```fs[{fs_name}] delete link success```