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
from paddleflow.run.run_info import RunInfo, JobInfo, DagInfo, RunCacheInfo, ArtifactInfo

class RunServiceApi(object):
    """run service
    """

    def __init__(self):
        """
        """

    @classmethod
    def add_run(self, host, fs_name=None, name=None, desc=None,
                param=None, username=None, run_yaml_path=None, run_yaml_raw_b64=None, pipeline_id=None, pipeline_version_id=None,
                header=None, disabled=None, docker_env=None):
        """ add run 
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {}
        if fs_name:
            body["fsname"] = fs_name
        if name:
            body['name'] = name
        if desc:
            body['desc'] = desc
        if run_yaml_path:
            body['runYamlPath']=run_yaml_path
        if run_yaml_raw_b64:
            if isinstance(run_yaml_raw_b64, bytes):
                body['runYamlRaw']=base64.b64encode(run_yaml_raw_b64).decode()
            else:
                raise PaddleFlowSDKException("InvalidRequest", "runYamlRaw must be bytes type")
        if pipeline_id:
            body['pipelineID']= pipeline_id
        if pipeline_version_id:
            body['pipelineVersionID'] = pipeline_version_id
        if param:
            body['parameters'] = param
        if username:
            body['username'] = username
        if disabled:
            body["disabled"] = disabled
        if docker_env:
            body["dockerEnv"] = docker_env
        response = api_client.call_api(method="POST", url=parse.urljoin(host, api.PADDLE_FLOW_RUN),
                                       headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "add run failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        return True, data['runID']

    @classmethod
    def list_run(self, host, fs_name=None, username=None, run_id=None, run_name=None, status=None,
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
        if fs_name:
            params['fsFilter'] = fs_name
        if run_id:
            params['runFilter'] = run_id
        if run_name:
            params['nameFilter'] = run_name
        if status:
            params['statusFilter'] = status
        if marker:
            params['marker'] = marker
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_RUN),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "list run failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        run_list = []
        if len(data['runList']):
            for run in data['runList']:
                run_info = RunInfo(run['runID'], run['fsName'], run['username'], run['status'], run['name'],
                                  run['description'], None, None, None, None, None, run['updateTime'],
                                  run['source'], run['runMsg'], run['scheduleID'], run['scheduledTime'],
                                  None, None, None, None, run['createTime'], run['activateTime'])
                run_list.append(run_info)

        return True, {'runList': run_list, 'nextMarker': data.get('nextMarker', None)}

    @classmethod
    def show_run(self, host, run_id, header=None):
        """status run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET", url=parse.urljoin(host, api.PADDLE_FLOW_RUN + "/%s" % run_id),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "status run failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']

        def trans_dict_to_comp_info(comp_dict):
            new_comp_dict = {}
            for key in comp_dict.keys():
                comp_list = comp_dict[key]
                new_comp_list = []
                for comp in comp_list:
                    if 'entryPoints' in comp.keys():
                        new_comp = DagInfo(comp['id'], comp['name'], comp['type'], comp['dagName'],
                                          comp['parentDagID'], comp['deps'], comp['parameters'],
                                          comp['artifacts'], comp['startTime'], comp['endTime'],
                                          comp['status'], comp['message'], trans_dict_to_comp_info(comp['entryPoints']))
                    else:
                        new_comp = JobInfo(comp['name'], comp['deps'], comp['parameters'],
                                        comp['command'], comp['env'], comp['status'], comp['startTime'],
                                        comp['endTime'], comp['dockerEnv'], comp['jobID'],
                                        comp['type'], comp['stepName'], comp['parentDagID'],
                                        comp['extraFS'], comp['artifacts'], comp['cache'],
                                        comp['jobMessage'], comp['cacheRunID'], comp['cacheJobID'])
                    new_comp_list.append(new_comp)
                new_comp_dict[key] = new_comp_list
            return new_comp_dict

        runtime = data['runtime']
        runtime_info = {}
        if runtime:
            runtime_info = trans_dict_to_comp_info(runtime)

        post = data['postProcess']
        new_post_dict = {}
        if post:
            for key in post.keys():
                comp = post[key]
                new_comp = JobInfo(comp['name'], comp['deps'], comp['parameters'],
                                        comp['command'], comp['env'], comp['status'], comp['startTime'],
                                        comp['endTime'], comp['dockerEnv'], comp['jobID'],
                                        comp['type'], comp['stepName'], comp['parentDagID'],
                                        comp['extraFS'], comp['artifacts'], comp['cache'],
                                        comp['jobMessage'], comp['cacheRunID'], comp['cacheJobID'])
                new_post_dict[key] = new_comp

        run_info = RunInfo(data['runID'], data['fsName'], data['username'], data['status'], data['name'],
                            data['description'], data['parameters'], data['runYaml'],
                            runtime_info, new_post_dict,
                            data['dockerEnv'], data['updateTime'], data['source'], data['runMsg'],data['scheduleID'], None,
                            data['fsOptions'], data['failureOptions'], data['disabled'], data['runCachedIDs'],
                            data['createTime'], data['activateTime'])
        
        return True, run_info

    @classmethod
    def stop_run(self, host, run_id, header=None, force=False):
        """stop run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        url = host + api.PADDLE_FLOW_RUN + "/%s" % run_id
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
    def delete_run(self, host, run_id, check_cache=True, header=None):
        """delete run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        body = {"checkCache": check_cache}
        response = api_client.call_api(method="DELETE",
                                       url=parse.urljoin(host, api.PADDLE_FLOW_RUN + "/%s" % run_id),
                                       headers=header, json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete run failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, None

    @classmethod
    def list_runcache(self, host, user_filter=None, fs_filter=None, run_filter=None, max_keys=None, marker=None,
                      header=None):
        """list run cache
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params = {}
        if user_filter:
            params['userFilter']=user_filter
        if fs_filter:
            params['fsFilter']=fs_filter
        if run_filter:
            params['runFilter']=run_filter
        if max_keys:
            params['maxKeys']=max_keys
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
        cache_list = []
        if len(data['runCacheList']):
            for cache in data['runCacheList']:
                cache_info = RunCacheInfo(cache['cacheID'], cache['firstFp'],
                              cache['secondFp'], cache['runID'], cache['source'], 
                              cache['jobID'], cache['fsname'], cache['username'],
                              cache['expiredTime'], cache['strategy'],
                              cache['custom'], cache['createTime'],
                              cache.get('updateTime', ' '))
                cache_list.append(cache_info)
        return True, {'runCacheList': cache_list, 'nextMarker': data.get('nextMarker', None)}

    @classmethod
    def show_runcache(self, host, run_cache_id, header=None):
        """show run cache
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="GET", url=parse.urljoin(host,
                                                                       api.PADDLE_FLOW_RUNCACHE + "/%s" % run_cache_id),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "runcache show failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
        ri = RunCacheInfo(data['cacheID'], data['firstFp'], data['secondFp'], data['runID'],
                data['source'], data['jobID'], data['fsname'], data['username'], data['expiredTime'],
                data['strategy'], data['custom'], data['createTime'], data['updateTime'])
        return True, ri

    @classmethod
    def delete_runcache(self, host, run_cache_id, header=None):
        """delete run cache
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        response = api_client.call_api(method="DELETE",
                                       url=parse.urljoin(host, api.PADDLE_FLOW_RUNCACHE + "/%s" % run_cache_id),
                                       headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "delete runcache failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
        else:
            return True, None

    @classmethod
    def retry_run(self, host, run_id, header=None):
        """retry run
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params={'action':'retry'}
        response = api_client.call_api(method="PUT", url=parse.urljoin(host, api.PADDLE_FLOW_RUN + "/%s" % run_id),
                                       params=params, headers=header)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "retry run failed due to HTTPError")
        if response.text:
            data = json.loads(response.text)
            if 'message' in data:
                return False, data['message']
            else:
                return True, data['runID']
        else:
            return False, 'missing text in response'

    @classmethod
    def list_artifact(self, host, user_filter=None, fs_filter=None, run_filter=None, type_filter=None, path_filter=None,
                      max_keys=None, marker=None, header=None):
        """artifact
        """
        if not header:
            raise PaddleFlowSDKException("InvalidRequest", "paddleflow should login first")
        params={}
        if user_filter:
            params['userFilter'] = user_filter
        if fs_filter:
            params['fsFilter'] = fs_filter
        if run_filter:
            params['runFilter'] = run_filter
        if type_filter:
            params['typeFilter'] = type_filter
        if path_filter:
            params['pathFilter'] = path_filter
        if max_keys:
            params['maxKeys'] = max_keys
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
        actiface_list=[]
        for i in data['artifactEventList']:
            actifact = ArtifactInfo(i['runID'], i['fsname'], i['username'], i['artifactPath'],
                    i['step'], i['type'], i['artifactName'], i['meta'],
                    i['createTime'], i['updateTime'])
            actiface_list.append(actifact)
        return True, {'artifactList': actiface_list, 'nextMarker': data.get('nextMarker', None)}
