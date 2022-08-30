"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

#!/usr/bin/env python3
# -*- coding:utf8 -*-

import os
import re
from setuptools import setup, find_packages

NAME = 'PaddleFlow'
VERSION = '1.4.4'

with open('requirements.txt') as f:
    REQUIRES = f.readlines()


setup(
    name=NAME,
    version=VERSION,
    description='PaddleFlow SDK',
    author='The PaddleFlow Authors',
    url="https://github.com/PaddlePaddle/PaddleFlow:client",
    packages=find_packages(
        include=("paddleflow_python_sdk*")),
    install_requires=REQUIRES,
    python_requires='>=3.6.1',
    include_package_data=False,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'paddleflow=paddleflow.__main__:main'
        ]
    }
)
