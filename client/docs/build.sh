#########################################################################
# File Name: build.sh
# Author: anhaozheng
# Created Time: 2021-06-18 17:20:37
#########################################################################
#!/bin/bash

# shdir=$(dirname $(readlink -f ${BASH_SOURCE[0]}))
shdir=$(dirname $(greadlink -f ${BASH_SOURCE[0]}))
pushd $shdir

# step1 生成source、build、Makefile 文件
# 需要交互是写入信息
# sphinx-quickstart 

# step2: 更改source/conf.py 文件
# 指定需要使用的主题(需要在 source/conf.py 文件中, import 相关主题, 并更改变量html_theme 的值)，将需要生成文档的模块加入 sys.path 中


# step3: sphinx-apidoc -f -o source ../../pipeline

# step4 生成 html 文件 
make html

# 参考资料：
# sphinx 官网： https://www.sphinx-doc.org/en/master/contents.html
# sphinx.ext.autodoc 官方文档： https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html

popd
