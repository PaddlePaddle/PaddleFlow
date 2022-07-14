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
import base64
from urllib import parse
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.utils import api_client
from paddleflow.common import api
from paddleflow.run.run_info import RunInfo, JobInfo, RunCacheInfo, ArtifaceInfo

class RunServiceApi(object):
    """run service
    """

    def __init__(self):
        """
        """

    @classmethod
    def add_run(self, host, fsname, name=None, desc=None,
                param=None, username=None, runyamlpath=None, runyamlrawb64=None, pipelineid=None,
                header=None, disabled=None, dockerenv=None):
        """ add run 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {
            "fsname": fsname
        }
        if name:
            body['name'] = name
        if desc:
            body['desc'] = desc
        if runyamlpath:
            body['runYamlPath']=runyamlpath
        if runyamlrawb64:
            if isinstance(runyamlrawb64, bytes):
                body['runYamlRaw']=base64.b64encode(runyamlrawb64).decode()
            else:
                raise PaddleFlowSDKException("InvalidRequest", "runYamlRaw must be bytes type")
        if pipelineid:
            body['pipelineID']= pipelineid
        if param:
            body['parameters'] = param
        if username:
            body['username'] = username
        if disabled:
            body["disabled"] = disabled
        if dockerenv:
            body["dockerEnv"] = dockerenv
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_RUN),
                                       headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "add run failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data['runID']

    @classmethod
    def list_run(self, host, fsname=None, username=None, runid=None, runname=None,
                 header=None, maxsize=100, marker=None):
        """list run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        if not isinstance(maxsize, int) or maxsize <= 0:
            raise PaddleFlowSDKException("InvalidRequest", "maxsize should be int and greater than 0")
        params = {
            "maxKeys": maxsize
        }
        if username:
            params['userFilter'] = username
        if fsname:
            params['fsFilter'] = fsname
        if runid:
            params['runFilter'] = runid
        if runname:
            params['nameFilter']=runname
        if marker:
            params['marker'] = marker
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_RUN),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "list run failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        runList = []
        if len(data['runList']):
            for run in data['runList']:
                runInfo = RunInfo(run['runID'], run['fsname'], run['username'], run['status'], run['name'],
                                       None, None, None, None, None, None, None, None, None, None, None, None)
                runList.append(runInfo)
        return True, runList, data.get('nextMarker', None)

    @classmethod
    def status_run(self, host, runid, header=None):
        """status run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_RUN + "/%s" % runid),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "status run failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']

        runtimeList = []
        runtime = data['runtime']
        if runtime:
            for key in runtime.keys():
                runtimeInfo = JobInfo(None, runtime[key].get('deps', ' '), runtime[key]['parameters'],
                                runtime[key]['command'], runtime[key]['env'],
                                runtime[key]['status'], runtime[key]['startTime'],
                                runtime[key].get('endTime', ' '), runtime[key].get('dockerEnv'),
                                runtime[key]['jobID'])
                runtimeInfo.name = key
                runtimeList.append(runtimeInfo)

        postProcessList = []
        post = data['postProcess']
        if post:
            for key in post.keys():
                postInfo = JobInfo(None, post[key].get('deps', ' '), post[key]['parameters'],
                                post[key]['command'], post[key]['env'],
                                post[key]['status'], post[key]['startTime'],
                                post[key].get('endTime', ' '), post[key].get('dockerEnv'),
                                post[key]['jobID'])
                postInfo.name = key
                postProcessList.append(postInfo)

        runInfo = RunInfo(data['runID'], data['fsname'], data['username'], data['status'], data['name'],
                               data['description'], data['entry'], data['parameters'], data['runYaml'], runtimeList, postProcessList,
                               data['dockerEnv'], data.get('updateTime', " "), data['source'],
                               data['runMsg'], data.get('createTime', " "), data.get('activateTime', ' '))
        
        return True, runInfo

    @classmethod
    def stop_run(self, host, runid, header=None, force=False):
        """stop run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        url = host + api.PADDLE_FLOW_RUN + "/%s" % runid
        params = {
            "action": "stop"
        }
        
        body = {"stopForce": force}

        response = api_client.call_api(method = "PUT", url =url, params=params, headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "stop run failed due to HTTPError")
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, None

    @classmethod
    def delete_run(self, host, runid, header=None):
        """delete run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="DELETE",
                                        url=parse.urljoin(host, api.PADDLE_FLOW_RUN + "/%s" % runid),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete run failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, None

    @classmethod
    def list_runcache(self, host, userfilter=None, fsfilter=None, runfilter=None, maxkeys=None, marker=None,
                     header=None):
        """list run cache
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {}
        if userfilter:
            params['userFilter']=userfilter
        if fsfilter:
            params['fsFilter']=fsfilter
        if runfilter:
            params['runFilter']=runfilter
        if maxkeys:
            params['maxKeys']=maxkeys
        if marker:
            params['marker']=marker
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_RUNCACHE),
                                        params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "runcache list failed due to HTTPError")
        if not response.text:
            return True, None
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        cachelist = []
        if len(data['runCacheList']):
            for cache in data['runCacheList']:
                cacheinfo = RunCacheInfo(cache['cacheID'], cache['firstFp'],
                              cache['secondFp'], cache['runID'], cache['source'], 
                              cache['step'], cache['fsname'], cache['username'],
                              cache['expiredTime'], cache['strategy'],
                              cache['custom'], cache['createTime'],
                              cache.get('updateTime', ' '))
                cachelist.append(cacheinfo)
        return True, cachelist, data.get('nextMarker', None)

    @classmethod
    def show_runcache(self, host, runcacheid, header=None):
        """show run cache
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET", url=parse.urljoin(host,
                                        api.PADDLE_FLOW_RUNCACHE + "/%s" % runcacheid),
                                        headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "runcache show failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        ri = RunCacheInfo(data['cacheID'], data['firstFp'], data['secondFp'], data['runID'],
                data['source'], data['step'], data['fsname'], data['username'], data['expiredTime'],
                data['strategy'], data['custom'], data['createTime'], data['updateTime'])
        return True, ri

    @classmethod
    def delete_runcache(self, host, runcacheid, header=None):
        """delete run cache
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="DELETE",
                                        url=parse.urljoin(host, api.PADDLE_FLOW_RUNCACHE + "/%s" % runcacheid),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete runcache failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, runcacheid

    @classmethod
    def retry_run(self, host, runid, header=None):
        """retry run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params={'action':'retry'}
        response = api_client.call_api(method="PUT", url=parse.urljoin(host, api.PADDLE_FLOW_RUN + "/%s" % runid),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "retry run failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, runid

    @classmethod
    def artifact(self, host, userfilter=None, fsfilter=None, runfilter=None, typefilter=None, pathfilter=None,
                maxKeys=None, marker=None, header=None):
        """artifact
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params={}
        if userfilter:
            params['userFilter'] = userfilter
        if fsfilter:
            params['fsFilter'] = fsfilter
        if runfilter:
            params['runFilter'] = runfilter
        if typefilter:
            params['typeFilter'] = typefilter
        if pathfilter:
            params['pathFilter'] = pathfilter
        if maxKeys:
            params['maxKeys'] = maxKeys
        if marker:
            params['marker'] = marker
        response = api_client.call_api(method="GET",
                                        url=parse.urljoin(host, api.PADDLE_FLOW_ARTIFACT),
                                        params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "artifact failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        actifacelist=[]
        for i in data['artifactEventList']:
            actiface = ArtifaceInfo(i['runID'], i['fsname'], i['username'], i['artifactPath'],
                    i['step'], i['type'], i['artifactName'], i['meta'],
                    i['createTime'], i['updateTime'])
            actifacelist.append(actiface)
        return True, actifacelist, data.get('nextMarker', None)
