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

import sys

class PaddleFlowSDKException(Exception):
    """paddleflowapi sdk 异常类"""

    def __init__(self, code=None, message=None, requestId=None):
        self.code = code
        self.message = message
        self.requestId = requestId

    def __str__(self):
        s = "[PaddleFlowSDKException] code:%s message:%s requestId:%s" % (
            self.code, self.message, self.requestId)
        return s

    def get_code(self):
        """get code"""
        return self.code

    def get_message(self):
        """get message"""
        return self.message

    def get_request_id(self):
        """get request_id"""
        return self.requestId