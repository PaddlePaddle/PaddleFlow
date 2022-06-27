#!/usr/bin/env python3
# -*- coding:utf8 -*-
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

import os 


def get_default_config_path():
    """ get the default config file path of paddleflow
    """
    home_path = os.getenv('HOME')
    config_dir = os.path.join(home_path, '.paddleflow')
    config_file = os.path.join(config_dir, 'paddleflow.ini')
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    if not os.path.exists(config_file):
        fo = open(config_file, "w")
        fo.write("[user]\nname = root\npassword = paddleflow"
                 "\n[server]\n# paddleflow server 地址\npaddleflow_server_host = 127.0.0.1\n# paddleflow server 端口"
                 "\npaddleflow_server_port = 8999\n")
        fo.close()
    return config_file