"""
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

import json
from urllib import parse
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.utils import api_client
from paddleflow.common import api


class VersionServiceApi(object):
    """version service api"""
    def __init__(self):
        """
        """

    @classmethod
    def get_version(self, host, header=None):
        """call get version api"""
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")

        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_SERVER_VERSION),
                                        headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "add user failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data