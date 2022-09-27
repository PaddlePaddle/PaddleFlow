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
from paddleflow.job import JobServiceApi,JobRequest
from paddleflow.log import LogServiceApi
from paddleflow.statistics import StatisticsServiceApi
from paddleflow.user import UserServiceApi
from paddleflow.queue import QueueServiceApi
from paddleflow.fs import FSServiceApi
from paddleflow.run import RunServiceApi
from paddleflow.pipeline import PipelineServiceApi
from paddleflow.schedule import ScheduleServiceApi
from paddleflow.utils import api_client
from paddleflow.cluster import ClusterServiceApi
from paddleflow.flavour import FlavouriceApi
from paddleflow.version import VersionServiceApi


class Client(object):
    """Client class """

    def __init__(self, paddleflow_server_host, username, password, paddleflow_server_port=8999):
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
        if paddleflow_server_host is None or paddleflow_server_host.strip() == "":
            raise PaddleFlowSDKException("InvalidServer", "paddleflow server should not be none or empty")
        self.paddleflow_server = "http://%s:%s" % (paddleflow_server_host, paddleflow_server_port)

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

    def get_version(self):
        """
        show paddleflow server version
        """
        self.pre_check()
        return VersionServiceApi.get_version(self.paddleflow_server, self.header)

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
        if maxResources is None or maxResources['cpu'] is None or maxResources['mem'] is None:
            raise PaddleFlowSDKException("InvalidQueueMaxResources",
                                         "queue maxResources cpu or mem should not be none or empty")

        return QueueServiceApi.add_queue(self.paddleflow_server, name, namespace, clusterName, maxResources,
                                         minResources, schedulingPolicy, location, quotaType, self.header)

    def update_queue(self, queuename, maxResources, minResources=None, schedulingPolicy=None, location=None):
        """ update queue"""
        self.pre_check()
        if queuename is None or queuename.strip() == "":
            raise PaddleFlowSDKException("InvalidQueueName", "queuename should not be none or empty")
        return QueueServiceApi.update_queue(self.paddleflow_server, queuename, maxResources, minResources,
                                            schedulingPolicy, location, self.header)

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

    def list_flavour(self, maxsize=100, marker=None, clustername="", key=""):
        """
        list flavour
        """
        self.pre_check()
        return FlavouriceApi.list_flavour(host=self.paddleflow_server, header=self.header,
                                          maxsize=maxsize, marker=marker, clustername=clustername, key=key)

    def show_flavour(self, name):
        """
        show flavour
        """
        self.pre_check()
        if name is None or name.strip() == "":
            raise PaddleFlowSDKException("InvalidFlavourName", "name should not be none or empty")
        return FlavouriceApi.show_flavour(self.paddleflow_server, name, self.header)

    def add_flavour(self, name, cpu, memory, scalar_resources=None, cluster_name=None):
        """ add flavour"""
        self.pre_check()
        if name is None or name.strip() == "":
            raise PaddleFlowSDKException("InvalidFlavourName", "name should not be none or empty")
        if cpu is None or cpu.strip() == "":
            raise PaddleFlowSDKException("InvalidFlavourName", "cpu should not be none or empty")
        if memory is None or memory.strip() == "":
            raise PaddleFlowSDKException("InvalidFlavourName", "memory should not be none or empty")

        return FlavouriceApi.add_flavour(self.paddleflow_server, name, cpu=cpu, mem=memory,
                                         scalar_resources=scalar_resources,
                                         cluster_name=cluster_name, header=self.header)

    def del_flavour(self, name):
        """ delete flavour"""
        self.pre_check()
        if name is None or name.strip() == "":
            raise PaddleFlowSDKException("InvalidFlavourName", "flavourname should not be none or empty")
        return FlavouriceApi.del_flavour(self.paddleflow_server, name, self.header)

    def update_flavour(self, name, cpu=None, memory=None, scalar_resources=None, cluster_name=None):
        """
        update flavour
        """
        self.pre_check()
        if name is None or name.strip() == "":
            raise PaddleFlowSDKException("InvalidFlavourName", "name should not be none or empty")
        return FlavouriceApi.update_flavour(self.paddleflow_server, name, cpu=cpu, mem=memory,
                                            scalar_resources=scalar_resources,
                                            cluster_name=cluster_name, header=self.header)

    def add_fs(self, fsname, url, username=None, properties=None):
        """
        add fs 
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if url is None or url.strip() == "":
            raise PaddleFlowSDKException("InvalidURL", "url should not be none or empty")
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.add_fs(self.paddleflow_server, fsname, url, self.user_id, properties, userinfo)

    def show_fs(self, fsname, username=None):
        """
        show fs
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
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
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.delete_fs(self.paddleflow_server, fsname, self.user_id, userinfo)

    def list_fs(self, username=None, maxsize=100):
        """
        list fs
        """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
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
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.mount(self.paddleflow_server, fsname, path,
                                  self.user_id, self.password, mountOptions, userinfo)

    def create_cache(self, fsname, options, username=None):
        """
        create cache config for fs
        """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        if fsname == "":
            raise PaddleFlowSDKException("InvalidFsName", "fsname should not be none or empty")
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.create_cache(self.paddleflow_server, fsname, options, userinfo)

    def update_fs_cache(self, fsname, params, username=None):
        """
        update cache config for fs
        """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        if fsname == "":
            raise PaddleFlowSDKException("InvalidFsName", "fsname should not be none or empty")
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.update_cache(self.paddleflow_server, fsname, params, userinfo)

    def get_fs_cache(self, fsname, username=None):
        """
        get cache config for fs
        """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        if fsname == "":
            raise PaddleFlowSDKException("InvalidFsName", "fsname should not be none or empty")
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.get_cache(self.paddleflow_server, fsname, userinfo)

    def delete_fs_cache(self, fsname, username=None):
        """
        delete fs cache config
        """
        self.pre_check()
        if fsname is None or fsname.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.delete_cache(self.paddleflow_server, fsname, userinfo)

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
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
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
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
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
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
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
        userinfo = {'header': self.header, 'name': username, 'host': self.paddleflow_server}
        return FSServiceApi.show_link(self.paddleflow_server, fsname, fspath, self.user_id, userinfo)

    def create_run(self, fs_name=None, username=None, run_name=None, desc=None,
                   run_yaml_path=None, run_yaml_raw=None, pipeline_id=None, pipeline_version_id=None, param=None,
                   disabled=None, docker_env=None):
        """
        create run
        """
        self.pre_check()
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        if run_name and run_name.strip() == "":
            raise PaddleFlowSDKException("InvalidRunName", "runname should not be none or empty")
        return RunServiceApi.add_run(self.paddleflow_server, fs_name, run_name, desc,
                                     param, username, run_yaml_path, run_yaml_raw, pipeline_id, pipeline_version_id, self.header, disabled,
                                     docker_env)

    def list_run(self, fs_name=None, username=None, run_id=None, run_name=None, status=None, maxsize=100, marker=None):
        """
        list run
        """
        self.pre_check()
        if fs_name and fs_name.strip() == "":
            raise PaddleFlowSDKException("InvalidFSName", "fsname should not be none or empty")
        if username and username.strip() == "":
            raise PaddleFlowSDKException("InvalidUserName", "username should not be none or empty")
        if run_id and run_id.strip() == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        return RunServiceApi.list_run(self.paddleflow_server, fs_name,
                                      username, run_id, run_name, status, self.header, maxsize, marker)

    def show_run(self, run_id):
        """
        show run
        """
        self.pre_check()
        if run_id is None or run_id.strip() == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        return RunServiceApi.show_run(self.paddleflow_server, run_id, self.header)

    def stop_run(self, run_id, force=False):
        """
        stop run
        """
        self.pre_check()
        if run_id is None or run_id.strip() == "":
            raise PaddleFlowSDKException("InvalidRunID", "runid should not be none or empty")
        if not isinstance(force, bool):
            raise PaddleFlowSDKException("InvalidParam", "the Parameter [force] should be an instance of bool")
        return RunServiceApi.stop_run(self.paddleflow_server, run_id, self.header, force)

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
                                                credential, description, source, setting, status, namespacelist,
                                                version, self.header)

    def list_cluster(self, maxkeys=100, marker=None, clustername=None, clusterstatus=None):
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
                                                clustertype, description, source, setting, status, namespacelist,
                                                version, self.header)

    def list_cluster_resource(self, clustername=None):
        """
        list cluster resource
        """
        self.pre_check()
        return ClusterServiceApi.list_cluster_resource(self.paddleflow_server, clustername, self.header)

    def create_pipeline(self, fs_name, yaml_path=None, desc=None, username=None):
        """
        create pipeline
        """
        self.pre_check()
        if fs_name is None or fs_name.strip() == "":
            raise PaddleFlowSDKException("InvalidFsName", "fsname should not be none or empty")
        return PipelineServiceApi.create_pipeline(self.paddleflow_server, fs_name, yaml_path, desc,
                                                  username, self.header)

    def list_pipeline(self, user_filter=None, name_filter=None, max_keys=None, marker=None):
        """
        list pipeline
        """
        self.pre_check()
        return PipelineServiceApi.list_pipeline(self.paddleflow_server, user_filter,
                                                name_filter, max_keys, marker, self.header)

    def show_pipeline(self, pipeline_id, fs_filter=None, max_keys=None, marker=None):
        """
        show pipeline
        """
        self.pre_check()
        if pipeline_id is None or pipeline_id == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipelineid should not be none or empty")
        return PipelineServiceApi.show_pipeline(self.paddleflow_server, pipeline_id, fs_filter, max_keys, marker, self.header)

    def delete_pipeline(self, pipeline_id):
        """
        delete pipeline
        """
        self.pre_check()
        if pipeline_id is None or pipeline_id == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipelineid should not be none or empty")
        return PipelineServiceApi.delete_pipeline(self.paddleflow_server, pipeline_id, self.header)

    def update_pipeline(self, pipeline_id, fs_name, yaml_path, username=None, desc=None):
        """
            update pipeline
        """
        self.pre_check()
        if pipeline_id is None or pipeline_id == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipeline_id should not be none or empty")
        if fs_name is None or fs_name == "":
            raise PaddleFlowSDKException("InvalidFSName", "fs_name should not be none or empty")
        if yaml_path is None or yaml_path == "":
            raise PaddleFlowSDKException("InvalidYamlPath", "yaml_path should not be none or empty")
        return PipelineServiceApi.update_pipeline(self.paddleflow_server, self.header, pipeline_id, fs_name, yaml_path,
                                                  username, desc)

    def show_pipeline_version(self, pipeline_id, pipeline_version_id):
        """
            show pipeline version
        """
        self.pre_check()
        if pipeline_id is None or pipeline_id == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipeline_id should not be none or empty")
        if pipeline_version_id is None or pipeline_version_id == "":
            raise PaddleFlowSDKException("InvalidPipelineVersionID", "pipeline_version_id should not be none or empty")
        return PipelineServiceApi.show_pipeline_version(self.paddleflow_server, self.header,
                                                        pipeline_id, pipeline_version_id)

    def delete_pipeline_version(self, pipeline_id, pipeline_version_id):
        """
            delete pipeline version
        """
        self.pre_check()
        if pipeline_id is None or pipeline_id == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipeline_id should not be none or empty")
        if pipeline_version_id is None or pipeline_version_id == "":
            raise PaddleFlowSDKException("InvalidPipelineVersionID", "pipeline_version_id should not be none or empty")
        return PipelineServiceApi.delete_pipeline_version(self.paddleflow_server, self.header,
                                                            pipeline_id, pipeline_version_id)

    def retry_run(self, run_id):
        """
        retry run
        """
        self.pre_check()
        if run_id is None or run_id == "":
            raise PaddleFlowSDKException("InvalidRunID", "run_id should not be none or empty")
        return RunServiceApi.retry_run(self.paddleflow_server, run_id, self.header)

    def delete_run(self, run_id, check_cache=None):
        """
        status run
        """
        self.pre_check()
        if run_id is None or run_id == "":
            raise PaddleFlowSDKException("InvalidRunID", "run_id should not be none or empty")
        return RunServiceApi.delete_run(self.paddleflow_server, run_id, check_cache, self.header)

    def list_artifact(self, user_filter=None, fs_filter=None, run_filter=None, type_filter=None, path_filter=None,
                 maxkeys=None, marker=None):
        """
        artifact
        """
        self.pre_check()
        return RunServiceApi.list_artifact(self.paddleflow_server, user_filter, fs_filter,
                                      run_filter, type_filter, path_filter, maxkeys, marker, self.header)

    def list_cache(self, user_filter=None, fs_filter=None, run_filter=None,
                   max_keys=None, marker=None):
        """
        list run cache
        """
        self.pre_check()

        return RunServiceApi.list_runcache(self.paddleflow_server, user_filter, fs_filter,
                                           run_filter, max_keys, marker, self.header)

    def show_cache(self, cache_id):
        """
        status pipeline
        """
        self.pre_check()
        if cache_id is None or cache_id == "":
            raise PaddleFlowSDKException("InvalidCacheID", "cache_id should not be none or empty")
        return RunServiceApi.show_runcache(self.paddleflow_server, cache_id, self.header)

    def delete_cache(self, cache_id):
        """
        status pipeline
        """
        self.pre_check()
        if cache_id is None or cache_id == "":
            raise PaddleFlowSDKException("InvalidCacheID", "cache_id should not be none or empty")
        return RunServiceApi.delete_runcache(self.paddleflow_server, cache_id, self.header)

    def create_schedule(self, name, pipeline_id, pipeline_version_id, crontab,
                 desc=None, start_time=None, end_time=None, concurrency=None, concurrency_policy=None, expire_interval=None,
                 catchup=None, username=None):
        """ create schedule """
        self.pre_check()
        if name is None or name == "":
            raise PaddleFlowSDKException("InvalidName", "name should not be none or empty")
        if pipeline_id is None or pipeline_id == "":
            raise PaddleFlowSDKException("InvalidPipelineID", "pipeline_id should not be none or empty")
        if pipeline_version_id is None or pipeline_version_id == "":
            raise PaddleFlowSDKException("InvalidPipelineVersionID", "pipeline_version_id should not be none or empty")
        if crontab is None or crontab == "":
            raise PaddleFlowSDKException("InvalidPipelineVersionID", "pipeline_version_id should not be none or empty")
        return ScheduleServiceApi.create_schedule(self.paddleflow_server, self.header,
                                                name, pipeline_id, pipeline_version_id, crontab,
                                                desc, start_time, end_time, concurrency, concurrency_policy,
                                                expire_interval, catchup, username)

    def list_schedule(self, user_filter=None, ppl_filter=None, ppl_version_filter=None, schedule_filter=None,
                      name_filter=None, status_filter=None, marker=None, max_keys=None):
        """ list schedule """
        self.pre_check()
        return ScheduleServiceApi.list_schedule(self.paddleflow_server, self.header, user_filter, ppl_filter,
                                                ppl_version_filter, schedule_filter, name_filter, status_filter,
                                                marker, max_keys)

    def show_schedule(self, schedule_id, run_filter=None, status_filter=None, marker=None, max_keys=None):
        """ show schedule """
        self.pre_check()
        if schedule_id is None or schedule_id == "":
            raise PaddleFlowSDKException("InvalidScheduleID", "schedule_id should not be none or empty")
        return ScheduleServiceApi.show_schedule(self.paddleflow_server, self.header, schedule_id, run_filter, status_filter,
                                                marker, max_keys)

    def stop_schedule(self, schedule_id):
        self.pre_check()
        if schedule_id is None or schedule_id == "":
            raise PaddleFlowSDKException("InvalidScheduleID", "schedule_id should not be none or empty")
        return ScheduleServiceApi.stop_schedule(self.paddleflow_server, self.header, schedule_id)

    def delete_schedule(self, schedule_id):
        self.pre_check()
        if schedule_id is None or schedule_id == "":
            raise PaddleFlowSDKException("InvalidScheduleID", "schedule_id should not be none or empty")
        return ScheduleServiceApi.delete_schedule(self.paddleflow_server, self.header, schedule_id)

    def show_log(self, run_id, job_id=None, page_size=None, page_no=None, log_file_position=None):
        """
        show run log
        """
        self.pre_check()
        if run_id is None or run_id == "":
            raise PaddleFlowSDKException("InvalidRunID", "run_id should not be none or empty")
        return LogServiceApi.get_log_info(self.paddleflow_server, run_id, job_id, page_size, page_no, log_file_position,
                                          self.header)

    def create_job(self, job_type, job_request):
        """
        create_job
        """
        self.pre_check()
        queueName = job_request.get('schedulingPolicy', {}).get('queue', None)
        if queueName is None or queueName == '':
            raise PaddleFlowSDKException("InvalidJobRequest",
                                         "job_request {} queue should not be none or empty".format(job_request))
        if job_type is None or (job_type != 'single' and job_type != 'distributed' and job_type != 'workflow'):
            raise PaddleFlowSDKException("InvalidJobType",
                                         "job_type should not be none and should be single, distributed or workflow")

        job_request_obj = JobRequest(
            job_request.get('schedulingPolicy', {}).get('queue', None),
            job_request.get('image', None),
            job_request.get('id', None), job_request.get('name', None),
            job_request.get('labels', None), job_request.get('annotations', None),
            job_request.get('schedulingPolicy', {}).get('priority', None),
            job_request.get('flavour', None),
            job_request.get('fs', None), job_request.get('extraFS', None),
            job_request.get('env', None), job_request.get('command', None),
            job_request.get('args', None), job_request.get('port', None),
            job_request.get('extensionTemplate', None),
            job_request.get('framework', None),
            job_request.get('members', None)
        )
        # if job_request.queue is None or job_request.queue == '':
        #     raise PaddleFlowSDKException("InvalidJobRequest", "job_request queue should not be none or empty")
        return JobServiceApi.create_job(self.paddleflow_server, job_type, job_request_obj, self.header)

    def show_job(self, jobid):
        """
        show_job
        """
        self.pre_check()
        if jobid is None or jobid == "":
            raise PaddleFlowSDKException("InvalidJobID", "jobid should not be none or empty")
        return JobServiceApi.show_job(self.paddleflow_server, jobid, self.header)

    def list_job(self, status=None, timestamp=None, start_time=None, queue=None, labels=None, maxkeys=None,
                 marker=None):
        """
        list_job
        """
        self.pre_check()
        return JobServiceApi.list_job(self.paddleflow_server, status, timestamp, start_time, queue, labels, maxkeys,
                                      marker, self.header)

    def update_job(self, jobid, priority=None, labels=None, annotations=None):
        """
        update_job
        """
        self.pre_check()
        if jobid is None or jobid == "":
            raise PaddleFlowSDKException("InvalidJobID", "jobid should not be none or empty")
        return JobServiceApi.update_job(self.paddleflow_server, jobid, priority, labels, annotations, self.header)

    def stop_job(self, jobid):
        """
        stop_job
        """
        self.pre_check()
        if jobid is None or jobid == "":
            raise PaddleFlowSDKException("InvalidJobID", "jobid should not be none or empty")
        return JobServiceApi.stop_job(self.paddleflow_server, jobid, self.header)

    def delete_job(self, jobid):
        """
        delete_job
        """
        self.pre_check()
        if jobid is None or jobid == "":
            raise PaddleFlowSDKException("InvalidJobID", "jobid should not be none or empty")
        return JobServiceApi.delete_job(self.paddleflow_server, jobid, self.header)

    def get_statistics(self, jobid: str, runid: str = None):
        """
        get_statistics
        """
        self.pre_check()
        if jobid is None or jobid == "":
            raise PaddleFlowSDKException("InvalidJobID", "jobid should not be none or empty")
        return StatisticsServiceApi.get_statistics(self.paddleflow_server, jobid, run_id=runid, header=self.header)

    def get_statistics_detail(self, jobid: str, start: int = None, end: int = None, step: int = None,
                              runid: str = None) :
        """
        get_statistics_detail
        """
        # prams check
        self.pre_check()
        if jobid is None or jobid == "":
            raise PaddleFlowSDKException("InvalidJobID", "jobid should not be none or empty")

        # check optional params
        if start is not None and int(start) < 0:
            raise PaddleFlowSDKException("InvalidStart", "start is not none and less than 0")
        if end is not None and (start is None or int(end) < int(start)):
            raise PaddleFlowSDKException("InvalidEnd", "end is not none and less than start")
        if step is not None and int(step) <= 0:
            raise PaddleFlowSDKException("InvalidStep", "step is not none and less than 0")

        ret, res = StatisticsServiceApi.get_statistics_detail(self.paddleflow_server, jobid, start, end, step, run_id=runid,
                                                          header=self.header)
        if not ret:
            return ret, res, False
        return ret, res, res.truncated
