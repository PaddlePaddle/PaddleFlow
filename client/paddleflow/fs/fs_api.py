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

import json
import os
import time
import subprocess
from urllib import parse
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.utils import api_client
from paddleflow.common import api
from paddleflow.fs.fs_info import FSInfo, LinkInfo
import signal

def callback_func_mount_time_out(*args):
    """
    callback_func_mount_time_out
    """
    print("Mount timeout.")


def time_out(interval, callback):
    """
    time out decorator
    """

    def decorator(func):
        """ decorator
        """
        def handler(signum, frame):
            """
            del time out
            """
            raise TimeoutError("run func timeout")
        def wrapper(*args, **kwargs):
            """wrapper
            """
            try:
                signal.signal(signal.SIGALRM, handler)
                signal.alarm(interval)       # interval秒后向进程发送SIGALRM信号
                result = func(*args, **kwargs)
                signal.alarm(0)              # 函数在规定时间执行完后关闭alarm闹钟
                return result
            except TimeoutError as e:
                callback(*args)
                return False, None
        return wrapper
    return decorator


class FSServiceApi(object):
    """fs service api """

    def __init__(self):
        """
        """

    @classmethod
    def add_fs(self, host, fsname, url, userid, properties=None, userInfo={'header': '', 'name': '', 'host': ''}):
        """
        add fs
        """
        if not userInfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        if properties and not isinstance(properties, dict):
            raise PaddleFlowSDKException("Wrong params type", "properties should be dict")
        body = {
            "name": fsname,
            "url": url
        }
        if userInfo['name']:
            body['username'] = userInfo['name']
        if properties:
            body['properties'] = properties
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_FS),
                                       headers=userInfo['header'], json=body)
        if not response:
            raise PaddleFlowSDKException("Create fs error", response.text)
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def show_fs(self, host, fsname, userid, userinfo={'header': '', 'name': '', 'host': ''}):
        """
        show fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")

        params = None
        if userinfo['name']:
            params = {
                'username': userinfo['name']
            }
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_FS + "/%s" % fsname),
                                       headers=userinfo['header'], params=params)
        if not response:
            raise PaddleFlowSDKException("Show fs error", response.text)
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        fsinfo = FSInfo(data['name'], data['username'], data['type'],
                        data['serverAddress'], data['subPath'], data['properties'])
        fsList = []
        fsList.append(fsinfo)
        return True, fsList

    @classmethod
    def delete_fs(self, host, fsname, userid, userinfo={'header': '', 'name': '', 'host': ''}):
        """
        delete fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        params = None
        if userinfo['name']:
            params = {
                'username': userinfo['name']
            }
        response = api_client.call_api(method="DELETE", url=parse.urljoin(host, api.PADDLE_FLOW_FS + "/%s" % fsname),
                                       headers=userinfo['header'], params=params)
        if not response:
            raise PaddleFlowSDKException("Delete fs error", response.text)
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return True, data['message']
        return True, None

    @classmethod
    def list_fs(self, host, userid, userinfo={'header': '', 'name': '', 'host': ''}, maxsize=100):
        """
        list fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "maxsize should be int and greater than 0")
        params = {
            "maxKeys": maxsize
        }
        if userinfo['name']:
            params['username'] = userinfo['name']
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_FS),
                                       headers=userinfo['header'], params=params)
        if not response:
            raise PaddleFlowSDKException("list fs error", response.text)
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        fsList = []
        if data is None or data['fsList'] is None:
            return True, fsList
        if len(data['fsList']):
            for fs in data['fsList']:
                fsinfo = FSInfo(fs['name'], fs['username'], fs['type'],
                                fs['serverAddress'], fs['subPath'], fs['properties'])
                fsList.append(fsinfo)
        return True, fsList

    @classmethod
    def create_cache(self, host, fsname, options, userinfo={'header': '', 'name': '', 'host': ''}):
        """
        create cache config for fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        body = options
        body['fsName'] = fsname
        if userinfo['name']:
            body['username'] = userinfo['name']
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_FS_CACHE),
                                       headers=userinfo['header'], json=body)
        if not response:
            raise PaddleFlowSDKException("create cache error", response.text)
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    @time_out(10, callback_func_mount_time_out)
    def mount(self, host, fsname, path, userid, password, mountoptins=None,
              userinfo={'header': '', 'name': '', 'host': ''}):
        """
        mount fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        if not os.path.exists(path):
            return False, "{} No such directory".format(path)
        params = {
            'username': ''
        }
        if userinfo['name']:
            params['username'] = userinfo['name']
        valid, fsinfo = self.show_fs(host, fsname, userid, userinfo)
        if len(fsinfo) == 0:
            return False, "fsName[{}] not exist".format(fsname)
        if not valid:
            return False, fsinfo
        fuse_env = os.getenv("fuse_env")

        cmd_mountpoint = "mountpoint {} >/dev/null".format(path)
        mount_point_result = subprocess.run(cmd_mountpoint, shell=True)
        if mount_point_result.returncode == 0:
            return False, "{} is a mountpoint, can not be mounted twice".format(path)

        log = "mount-{}.err.log".format(fsname)
        mount_op = "--fs-id={} --mount-point={} --server={} --user-name={} --password={} "
        mount_op += self.getMountOptions(mountoptins)

        cmd_prefix = "nohup {} mount " + mount_op + ">{} 2>&1 &"
        fsid = 'fs-' + fsinfo[0].owner + '-' + fsinfo[0].name
        cmd_mount = cmd_prefix.format(fuse_env, fsid, path, host.lstrip('http://'),
                                      userid, password, log)
        mount_result = subprocess.run(cmd_mount, shell=True)
        if mount_result.stderr is not None:
            return False, mount_result.stderr
        cmd_mountpoint = "mountpoint {} >/dev/null".format(path)
        for i in range(0, 5):
            mount_point_result = subprocess.run(cmd_mountpoint, shell=True)
            if mount_point_result.returncode == 0:
                os.remove(log)
                return True, None
            time.sleep(1)
        return False, mount_point_result.stderr

    @classmethod
    def add_link(self, host, fsname, fspath, url, userid, proper=None, user={'header': '', 'name': '', 'host': ''}):
        """
        add fs
        """
        if not user['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        if proper and not isinstance(proper, dict):
            raise PaddleFlowSDKException("Wrong params type", "properties should be dict")
        body = {
            "fsPath": fspath,
            "url": url,
            "fsName": fsname,
        }
        if user['name']:
            body['username'] = user['name']
        if proper:
            body['properties'] = proper
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_LINK),
                                       headers=user['header'], json=body)
        if not response:
            raise PaddleFlowSDKException("Create link error", response.text)
        return True, None

    @classmethod
    def delete_link(self, host, fsname, fsPath, userid, userinfo={'header': '', 'name': '', 'host': ''}):
        """
        delete fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        params = {}
        if userinfo['name']:
            params = {
                'username': userinfo['name']
            }
        params['fsPath'] = fsPath
        response = api_client.call_api(method="DELETE", url=parse.urljoin(host, api.PADDLE_FLOW_LINK + "/%s" % fsname),
                                       headers=userinfo['header'], params=params)
        if not response:
            raise PaddleFlowSDKException("Delete link error", response.text)
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return True, data['message']
        return True, None

    @classmethod
    def list_link(self, host, fsname, userid, userinfo={'header': '', 'name': '', 'host': ''}, maxsize=100):
        """
        list fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "maxsize should be int and greater than 0")
        params = {
            "maxKeys": maxsize,
        }
        if userinfo['name']:
            params['username'] = userinfo['name']
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_LINK + "/%s" % fsname),
                                       headers=userinfo['header'], params=params)
        if not response:
            raise PaddleFlowSDKException("List link error", response.text)
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        linkList = []
        if data is None or data['linkList'] is None:
            return True, linkList
        if len(data['linkList']):
            for link in data['linkList']:
                linkinfo = LinkInfo(link['fsName'], link['username'], link['type'],
                                    link['fsPath'], link['serverAddress'], link['subPath'], link['properties'])
                linkList.append(linkinfo)
        else:
            return True, ""
        return True, linkList

    @classmethod
    def show_link(self, host, fsname, fspath, userid, userinfo={'header': '', 'name': '', 'host': ''}):
        """
        show fs
        """
        if not userinfo['header']:
            raise PaddleFlowSDKException("Invalid request", "please login paddleflow first")
        params = {
            "fsPath": fspath,
        }
        if userinfo['name']:
            params['username'] = userinfo['name']
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_LINK + "/%s" % fsname),
                                       headers=userinfo['header'], params=params)
        if not response:
            raise PaddleFlowSDKException("Show link error", response.text)
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        linkList = []
        if len(data['linkList']):
            for link in data['linkList']:
                linkinfo = LinkInfo(link['fsName'], link['username'], link['type'],
                                    link['fsPath'], link['serverAddress'], link['subPath'], link['properties'])
                linkList.append(linkinfo)
        else:
            return False, "no link found"
        return True, linkList

    @classmethod
    def getMountOptions(self, mount_options):
        """
        getMountOptions
        """
        mount_str = ""
        for k, v in mount_options.items():
            mount_str += "--" + k + "=" + v + " "
        return mount_str
