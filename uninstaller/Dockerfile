from iregistry.baidu-int.com/python_basic/python:3.8
copy pip.conf /etc/
run mkdir ~/.pip
copy pip.conf ~/.pip/pip.conf
run pip install kubernetes
run mkdir /root/pre-delete

#copy PaddleFlow-develop.zip /root/pre-delete/
run cd /root/pre-delete && wget https://github.com/PaddlePaddle/PaddleFlow/archive/refs/heads/develop.zip

run cd /root/pre-delete && unzip PaddleFlow-develop.zip && cd PaddleFlow-develop/client && pip install -r requirements.txt && python setup.py install

copy pre-delete.py /root/pre-delete/
