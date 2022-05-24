### 用户登录
```python
ret, response = client.login('username', 'password') 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称
|password| string (required) | 用户密码

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户增加
```python
ret, response = client.add_user('username', 'password') 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称
|password| string (required) | 用户密码

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户删除
```python
ret, response = client.del_user('username') 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户密码更新
```python
ret, response = client.update_password(user_name, password) 
```
#### 接口入参说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|user_name| string (required)| 用户名称
|password| string (required)| 用户密码

#### 接口返回说明

|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message，成功返回None

### 用户列表展示
```python
ret, response = client.list_user(maxsize=100) 
```
#### 接口入参说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|maxsize| int (optional,default=100)| 展示列表数量上限，默认值为100

#### 接口返回说明
|字段名称 | 字段类型 | 字段含义
|:---:|:---:|:---:|
|ret| bool| 操作成功返回True，失败返回False
|response| -| 失败返回失败message(string)，成功返回用户列表userList(list)，每个元素为UserInfo对象，可以参考下面UserInfo类的定义获取对应的成员变量。

用户user类定义
```python
class UserInfo(object):
    """the class of user info"""

    def __init__(self, name, create_time):
        """init """
        self.name = name
        self.create_time = create_time
``` 