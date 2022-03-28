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
from urllib import parse
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.common import api
from paddleflow.log import LogServiceApi
from paddleflow.user import UserServiceApi
from paddleflow.queue import QueueServiceApi
from paddleflow.fs import FSServiceApi
from paddleflow.run import RunServiceApi
from paddleflow.pipeline import PipelineServiceApi
from paddleflow.utils import api_client
from paddleflow.cluster import ClusterServiceApi
from paddleflow.flavour import FlavouriceApi

class Client(object):
    """Client class """
    def __init__(self, paddleflow_server, username, password, paddleflow_port=8080):
        """
        :param paddleflow_server: the address of paddleflow server
        :type paddleflow_server: str
        :param fs_server: the address of fs server
        :type fs_server:str
        """
        self.paddleflow_server = None
        self.token = None
        self.user_id = username
        self.header = None
        self.password = password
        if paddleflow_server is None or paddleflow_server.strip() == "":
            raise PaddleFlowSDKException("InvalidServer", "paddleflow server should not be none or empty")
        self.paddleflow_server = "http://%s:%s" % (paddleflow_server, paddleflow_port)

    def login(self, user_name, password):
        """
        :param user_name: 
        :type user_name: str
        :param passWord
        :type password: str
        """
        if self.paddleflow_server is None:
            raise PaddleFlowSDKException("InvalidClient", "client should be initialized")
        if user_name is None or user_name.strip() == "":
            raise PaddleFlowSDKException("InvalidUser", "user_name should not be none or empty")
        if password is None or password.strip() == "":
            raise PaddleFlowSDKException("InvalidPassWord", "password should not be none or empty")
        body = {
            "username": user_name,
            "password": password
        }
        response = api_client.call_api(method="POST", url=parse.urljoin(self.paddleflow_server, api.PADDLE_FLOW_LOGIN),
                                       json=body)
        if not response:
            raise PaddleFlowSDKException("Connection Error", "login failed due to HTTPError")
        data = json.loads(response.text)
        if 'message' in data:
            return False, data['message']
    
        self.user_id = user_name
        self.password = password
        self.header = {
            "x-pf-authorization": data['authorization']
        }
        return True, None

    def pre_check(self):
        """
        precheck to check header
        """
        if not self.user_id or not self.header:
            raise PaddleFlowSDKException("InvalidOperator", "should login first")

    def add_user(self, user_name, password):
        """
        :param user_name: 
        :type user_name: str
        :param passWord
        :type password: str
        :return 
        true, None   if success 
        false, message   if failed
        """ 
        self.pre_check()
        if user_name is None or user_name.strip() == "":
            raise PaddleFlowSDKException("InvalidUser", "user_name should not be none or empty")
        if password is None or password.strip() == "":
            raise PaddleFlowSDKException("InvalidPassWord", "password should not be none or empty")
        return UserServiceApi.add_user(self.paddleflow_server, user_name, password, self.header)

    def del_user(self, user_name):
        """
        :param user_name: 
        :type user_name: str
        :param passWord
        :type password: str
        :return 
        :true,None if success 
        :false, message if failed
        """ 
        self.pre_check()
        if user_name is None or user_name.strip() == "":
            raise PaddleFlowSDKException("InvalidUser", "user_name should not be none or empty")
        return UserServiceApi.del_user(self.paddleflow_server, user_name, self.header)

    def list_user(self, maxsize=100):
        """list user info"""
        self.pre_check()
        return UserServiceApi.list_user(self.paddleflow_server, self.header, maxsize)

    def update_password(self, name, password):
        """update name's password"""
        self.pre_check()
        if name is None or name.strip() == "":
            raise PaddleFlowSDKException("InvalidUser", "user_name should not be none or empty")
        if password is None or password.strip() == "":
            raise PaddleFlowSDKException("InvalidPassWord", "password should not be none or empty")
        return UserServiceApi.update_password(self.paddleflow_server, name, password, self.header)

    def add_queue(self, name, namespace, clusterName, maxResources, minResources=None,
                  schedulingPolicy=None, location=None, quotaType=None):
        """ add queue"""
        self.pre_check()
        if namespace is None or namespace.strip() == "":
            raise PaddleFlowSDKException("InvalidNameSpace", "namesapce should not be none or empty")   
        if name is None or name.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueName", "queuename should not be none or empty")
        if clusterName is None or clusterName.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueClusterName", "clustername should not be none or empty")
        if maxResources is None or maxResources['cpu'] is None or maxResources['mem'] is None:
            raise PaddleFlowSDKException("InvalidQueueMaxResources", "queue maxResources cpu or mem should not be none or empty")

        return QueueServiceApi.add_queue(self.paddleflow_server, name, namespace, clusterName, maxResources,
               minResources, schedulingPolicy, location, quotaType, self.header)

    def grant_queue(self, username, queuename):
        """ grant queue"""
        self.pre_check()
        if username is None or username.strip() == "":
            raise PaddleFlowSDKException("InvalidName", "name should not be none or empty")   
        if queuename is None or queuename.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueName", "queuename should not be none or empty")
        return QueueServiceApi.grant_queue(self.paddleflow_server, username, queuename, self.header)

    def ungrant_queue(self, username, queuename):
        """ grant queue"""
        self.pre_check()
        if username is None or username.strip() == "":
            raise PaddleFlowSDKException("InvalidName", "name should not be none or empty")   
        if queuename is None or queuename.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueName", "queuename should not be none or empty")
        return QueueServiceApi.ungrant_queue(self.paddleflow_server, username, queuename, self.header)

    def show_queue_grant(self, username=None, maxsize=100):
        """show queue grant info """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidName", "name should not be none or empty")
        return QueueServiceApi.show_grant(self.paddleflow_server, username, self.header, maxsize)

    def del_queue(self, queuename):
        """ delete queue"""
        self.pre_check()   
        if queuename is None or queuename.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueName", "queuename should not be none or empty")
        return QueueServiceApi.del_queue(self.paddleflow_server, queuename, self.header)   

    def list_queue(self, maxsize=100, marker=None):
        """
        list queue
        """
        self.pre_check()
        return QueueServiceApi.list_queue(self.paddleflow_server, self.header, maxsize, marker)

    def show_queue(self, queuename):
        """
        show queue info 
        """
        self.pre_check()
        if queuename is None or queuename.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueName", "queuename should not be none or empty")
        return QueueServiceApi.show_queue(self.paddleflow_server, queuename, self.header)
    
    def stop_queue(self, queuename, action=None):
        """
        show queue info 
        """
        self.pre_check()
        if queuename is None or queuename.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueName", "queuename should not be none or empty")
        return QueueServiceApi.stop_queue(self.paddleflow_server, queuename, action, self.header)
    
    def flavour(self):
        """
        list flavour
        """
        self.pre_check()
        return FlavouriceApi.flavour(self.paddleflow_server, self.header)

    def add_fs(self, fsname, url, username=None, properties=None):
        """
        add fs 
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if url is None or url.strip() == "":
            raise PaddleFlowSDKException("InvalidURL", "url should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.add_fs(self.paddleflow_server, fsname, url, self.user_id, properties, userinfo)

    def show_fs(self, fsname, username=None):
        """
        show fs
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.show_fs(self.paddleflow_server, fsname, self.user_id, userinfo)
    
    def delete_fs(self, fsname, username=None):
        """
        delete fs 
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.delete_fs(self.paddleflow_server, fsname, self.user_id, userinfo)

    def list_fs(self, username=None, maxsize=100):
        """
        list fs
        """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.list_fs(self.paddleflow_server, self.user_id, userinfo, maxsize)

    def mount(self, fsname, path, mountOptions, username=None):
        """
        mount fs
        """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        if fsname == "":
            raise PaddleFlowSDKException("InvalidFsName", "fsname should not be none or empty")
        if path == "":
            raise PaddleFlowSDKException("InvalidPath", "path should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.mount(self.paddleflow_server, fsname, path,
                                  self.user_id, self.password, mountOptions, userinfo)

    def add_link(self, fsname, fspath, url, username=None, properties=None):
        """
        add link
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if url is None or url.strip() == "":
            raise PaddleFlowSDKException("InvalidURL", "url should not be none or empty")
        if fspath is None or fspath.strip() == "":
            raise PaddleFlowSDKException("InvalidFSPath", "fspath should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.add_link(self.paddleflow_server, fsname, fspath, url, self.user_id, properties, userinfo)

    def delete_link(self, fsname, fspath, username=None):
        """
        delete fs
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        if fspath is None or fspath.strip() == "":
            raise PaddleFlowSDKException("InvalidFSPath", "fspath should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.delete_link(self.paddleflow_server, fsname, fspath, self.user_id, userinfo)
    
    def list_link(self, fsname, username=None, maxsize=100):
        """
        list fs
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.list_link(self.paddleflow_server, fsname, self.user_id, userinfo, maxsize)
    
    def show_link(self, fsname, fspath, username=None):
        """
        show fs
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if fspath is None or fspath.strip() == "":
            raise PaddleFlowSDKException("InvalidFSPath", "fspath should not be none or empty")
        userinfo={'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.show_link(self.paddleflow_server, fsname, fspath, self.user_id, userinfo)

    def create_run(self, fsname, username=None, runname=None, desc=None, entry=None, 
                        runyamlpath=None, runyamlraw=None, param=None):
        """
        create run
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty") 
        if runname and runname.strip() == "":
            raise PaddleFlowSDKException("InvalidRunName", "runname should not be none or empty") 
        return RunServiceApi.add_run(self.paddleflow_server, fsname, runname, desc, 
                                          entry, param, username, runyamlpath, runyamlraw, self.header)
    
    def list_run(self, fsname=None, username=None, run_id=None, maxsize=100, marker=None):
        """
        list run
        """
        self.pre_check()
        if fsname and fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty") 
        if  run_id and run_id.strip() == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        return RunServiceApi.list_run(self.paddleflow_server, fsname, 
                                           username, run_id, self.header, maxsize, marker)

    def status_run(self, run_id):
        """
        status run
        """
        self.pre_check()
        if run_id is None or run_id.strip() == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        return RunServiceApi.status_run(self.paddleflow_server, run_id, self.header)

    def stop_run(self, run_id, job_id=None):
        """
        stop run
        """
        self.pre_check()
        if run_id is None or run_id.strip() == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        if job_id and job_id.strip() == "":
            raise PaddleFlowSDKException("InvalidJobID", "jobid should not be none or empty")            
        return RunServiceApi.stop_run(self.paddleflow_server, run_id, job_id, self.header)

    def create_cluster(self, clustername, endpoint, clustertype, credential=None,
                        description=None, source=None, setting=None, status=None, namespacelist=None, version=None):
        """
        create cluster
        """
        self.pre_check()
        if clustername is None or clustername.strip() == "":
            raise PaddleFlowSDKException("InvalidClusterName", "clustername should not be none or empty")
        if endpoint is None or endpoint.strip() == "":
            raise PaddleFlowSDKException("InvalidEndpoint", "endpoint should not be none or empty")
        if clustertype is None or clustertype.strip() == "":
            raise PaddleFlowSDKException("InvalidClusterType", "clustertype should not be none or empty")
        return ClusterServiceApi.create_cluster(self.paddleflow_server, clustername, endpoint, clustertype, 
        credential, description, source, setting, status, namespacelist, version, self.header)

    def list_cluster(self, maxkeys=None, marker=None, clustername=None, clusterstatus=None):
        """
        list cluster
        """
        self.pre_check()
        return ClusterServiceApi.list_cluster(self.paddleflow_server, maxkeys, marker, 
        clustername, clusterstatus, self.header)
    
    def show_cluster(self, clustername):
        """
        status cluster
        """
        self.pre_check()
        if clustername is None or clustername == "":
            raise PaddleFlowSDKException("InvalidClusterName", "clustername should not be none or empty")
        return ClusterServiceApi.show_cluster(self.paddleflow_server, clustername, self.header)
    
    def delete_cluster(self, clustername):
        """
        delete cluster
        """
        self.pre_check()
        if clustername is None or clustername == "":
            raise PaddleFlowSDKException("InvalidClusterName", "clustername should not be none or empty")
        return ClusterServiceApi.delete_cluster(self.paddleflow_server, clustername, self.header)

    def update_cluster(self, clustername, endpoint=None, credential=None, clustertype=None,
                        description=None, source=None, setting=None, status=None, namespacelist=None, version=None):
        """
        update cluster
        """
        self.pre_check()
        if clustername is None or clustername == "":
            raise PaddleFlowSDKException("InvalidClusterName", "clustername should not be none or empty")
        return ClusterServiceApi.update_cluster(self.paddleflow_server, clustername, endpoint, credential, 
        clustertype, description, source, setting, status, namespacelist, version, self.header)

    def list_cluster_resource(self, clustername=None):
        """
        list cluster resource
        """
        self.pre_check()
        return ClusterServiceApi.list_cluster_resource(self.paddleflow_server, clustername, self.header)

    def create_pipeline(self, fsname, yamlpath, name=None, username=None):
        """
        create pipeline
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFsName", "fsname should not be none or empty")
        if yamlpath is None or yamlpath.strip() == "":
            raise PaddleFlowSDKException("InvalidYamlPath", "yamlpath should not be none or empty")
        return PipelineServiceApi.create_pipeline(self.paddleflow_server, fsname, yamlpath, name, 
                username, self.header)

    def list_pipeline(self, userfilter=None, fsfilter=None, namefilter=None, maxkeys=None, marker=None):
        """
        list pipeline
        """
        self.pre_check()
        return PipelineServiceApi.list_pipeline(self.paddleflow_server, userfilter, fsfilter, 
                namefilter, maxkeys, marker, self.header)
    
    def show_pipeline(self, pipelineid):
        """
        status pipeline
        """
        self.pre_check()
        if pipelineid is None or pipelineid == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipelineid should not be none or empty")
        return PipelineServiceApi.show_pipeline(self.paddleflow_server, pipelineid, self.header)
    
    def delete_pipeline(self, pipelineid):
        """
        delete pipeline
        """
        self.pre_check()
        if pipelineid is None or pipelineid == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipelineid should not be none or empty")
        return PipelineServiceApi.delete_pipeline(self.paddleflow_server, pipelineid, self.header)

    def retry_run(self, runid):
        """
        retry run
        """
        self.pre_check()
        if runid is None or runid == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        return RunServiceApi.retry_run(self.paddleflow_server, runid, self.header)
    
    def delete_run(self, runid):
        """
        status run
        """
        self.pre_check()
        if runid is None or runid == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        return RunServiceApi.delete_run(self.paddleflow_server, runid, self.header)

    def artifact(self, userfilter=None, fsfilter=None, runfilter=None, typefilter=None, pathfilter=None, 
                maxkeys=None, marker=None):
        """
        artifact
        """
        self.pre_check()
        return RunServiceApi.artifact(self.paddleflow_server, userfilter, fsfilter, 
                runfilter, typefilter, pathfilter, maxkeys, marker, self.header)

    def list_cache(self, userfilter=None, fsfilter=None, runfilter=None, 
                maxkeys=None, marker=None):
        """
        list run cache
        """
        self.pre_check()

        return RunServiceApi.list_runcache(self.paddleflow_server, userfilter, fsfilter, 
                runfilter, maxkeys, marker, self.header)
        
    def show_cache(self, cacheid):
        """
        status pipeline
        """
        self.pre_check()
        if cacheid is None or cacheid == "":
            raise PaddleFlowSDKException("InvalidCacheID", "cacheid should not be none or empty")
        return RunServiceApi.show_runcache(self.paddleflow_server, cacheid, self.header)
    
    def delete_cache(self, cacheid):
        """
        status pipeline
        """
        self.pre_check()
        if cacheid is None or cacheid == "":
            raise PaddleFlowSDKException("InvalidCacheID", "cacheid should not be none or empty")
        return RunServiceApi.delete_runcache(self.paddleflow_server, cacheid, self.header)

    def show_log(self, runid, jobid=None, pagesize=None, pageno=None, logfileposition=None):
        """
        show run log
        """
        self.pre_check()
        if runid is None or runid == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        return LogServiceApi.get_log_info(self.paddleflow_server, runid, jobid, pagesize, pageno, logfileposition,
                                          self.header)