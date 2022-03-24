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

import requests
import time
import json
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

REQUESRID = 'requestID'
MESSAGE = 'message'
CODE = 'code'

def call_api(**kwargs):
    """call api function"""

    method = kwargs["method"]
    url = kwargs["url"]

    params = kwargs.get("params")
    data = kwargs.get("data")
    headers = kwargs.get("headers")
    cookies = kwargs.get("cookies")
    files = kwargs.get("files")
    auth = kwargs.get("auth")
    timeout = kwargs.get("timeout", 60)
    allow_redirects = kwargs.get("allow_redirects")
    proxies = kwargs.get("proxies")
    hooks = kwargs.get("hooks")
    stream = kwargs.get("stream")
    verify = kwargs.get("verify")
    cert = kwargs.get("cert")
    json_str = kwargs.get("json")

    done = False
    for _ in range(1):
        try:
            resp = requests.request(
                method,
                url,
                params=params,
                data=data,
                headers=headers,
                cookies=cookies,
                files=files,
                auth=auth,
                timeout=timeout,
                allow_redirects=allow_redirects,
                proxies=proxies,
                hooks=hooks,
                stream=stream,
                verify=verify,
                cert=cert,
                json=json_str)

            resp.raise_for_status()
            done = True
            break
        except requests.exceptions.HTTPError as errh:
            data = json.loads(resp.text)
            raise PaddleFlowSDKException(data[CODE], data[MESSAGE], data[REQUESRID])
        except requests.exceptions.ConnectionError as errc:
            done = False
            time.sleep(3)
        except requests.exceptions.Timeout as errt:
            done = False
            time.sleep(3)
        except requests.exceptions.RequestException as errr:
            data = json.loads(resp.text)
            raise PaddleFlowSDKException(data[CODE], data[MESSAGE], data[REQUESRID])

    if not done:
        return None

    return resp