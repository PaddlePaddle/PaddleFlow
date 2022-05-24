### 创建存储
```python
ret, response = client.add_fs("fsname", "url")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|新建存储系统名称
|url| string (required)|访问地址如：sftp://192.168.1.2:9000/myfs
|username| string (optional)|指定用户，用于root用户为其他用户创建fs
|properties|dict (optional)| 后端存储的访问配置项，通过key:value键值对提供。如：S3校验{"accessKey":"test","secretKey":"test"}

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 存储详情
```python
ret, response = client.show_fs("fsname")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|存储系统名称
|username| string (optional)|指定用户，用于root用户展示特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回存储系统详情，类型为FSInfo，可以参考下面FSInfo类的定义获取对应的成员变量。

```python
class FSInfo(object):
    """the class of fs info"""
    def __init__(self, name, owner, fstype, server_address, subpath, properties):
        """init """
        self.name = name
        self.owner = owner
        self.fstype = fstype
        self.server_adddress = server_address
        self.subpath = subpath
        self.properties = properties
```

### 存储列表
```python
ret, response = client.list_fs()
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100
|username| string (optional)|指定用户，用于root用户列出特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回存储系统详情List，每个元素的类型为FSInfo，，可以参考上面FSInfo类的定义获取对应的成员变量。

### 删除存储
```python
ret, response = client.delete_fs("fsname")
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|存储系统名称
|username| string (optional)|指定用户，用于admin管理员删除特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 存储挂载
```python
ret, response = client.mount("fsname", "mount_path")
```

#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|存储系统名称
|path| string (required)|挂载点名称
|username| string (optional)|指定用户，用于root用户挂载特定用户的fs

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 创建link
```python
ret, response = client.add_link("fsname", "fspath", "url")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|fspath| string (required)|需要link到文件系统的目录
|url| string (required)|外部存储的访问地址如：hdfs://192.168.1.2:9000,192.168.1.3:9000/linkpath
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link
|properties| dict (optional)|外部存储的访问配置项，通过key:value键值对提供。比如HDFS支持透传HDFS的配置项

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 删除link
```python
ret, response = client.delete_link("fsname", "fspath")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|fspath| string (required)|需要link到文件系统的目录
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### link详情
```python
ret, response = client.show_link("fsname", "fspath")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|fspath| string (required)|需要link到文件系统的目录
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回link详情，类型为LinkInfo，可以参考下面LinkInfo类的定义获取对应的成员变量。

```python
class LinkInfo(object):
    """the class of link info"""
    def __init__(self, name, owner, fstype, fspath, server_address, subpath, properties):
        """init """
        self.name = name
        self.owner = owner
        self.fstype = fstype
        self.fspath = fspath
        self.server_adddress = server_address
        self.subpath = subpath
        self.properties = properties
```

### link列表
```python
ret, response = client.show_link("fsname")
```

#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|fsname| string (required)|关联的存储系统名称
|username| string (optional)|指定用户，用于root账号创建特定用户的fs的link
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回LinkList(LinkInfo)，每个元素的类型为LinkInfo，可以参考上面LinkInfo类的定义获取对应的成员变量。