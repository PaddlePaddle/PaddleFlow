# 目录说明
## 安装与卸载
1. 安装
```
# 
pip3 install PaddleFlow-1.4.5-py3-none-any.whl
```

2. 卸载
```
pip3 uninstall PaddleFlow
```

## 编译生成client
```
# 在client目录下执行
python3 setup.py bdist_wheel && pip3 install `ls dist/*.whl` && rm -rf build dist paddleflow.egg-info
# 重新安装
pip3 install dist/*.whl
```

## 使用说明
- 展示客户端/服务端版本: `paddleflow version`