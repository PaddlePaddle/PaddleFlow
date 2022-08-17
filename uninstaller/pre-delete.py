#!/usr/bin/env python
#-*- coding:utf8 -*-
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

"""
# 当没有默认clusterIP的service的时候，需要根据service名字，找对应的service，然后获取host信息
# 目前paddleflow-server的service account没有service的任意权限，需要补充
from kubernetes import client, config

config.load_incluster_config()
v1 = client.CoreV1Api()
ret = v1.read_namespaced_service(service_name, namespace)

"""

import time
import paddleflow
import os


def check_pfserver_status(service_name, namespace, port, user, password):
    print("check_pfserver_status service_name=[%s] namespace=[%s] port=[%s] user=[%s] password=[%s]" % (service_name, namespace, port, user, password))
    host = "%s.%s.svc.cluster.local" % (service_name, namespace)
    print(host)
    
    client = paddleflow.Client(host, user, password, port)
    ret, response = client.login(user, password)
    if not ret:
        print(response)
        err_msg = "client login failed, with host[%s], user[%s], password[%s], port[%s], response[%s]" % (host, user, password, port, response)
        raise Exception(err_msg)
    
    active_status_list = ["initiating", "pending", "running", "terminating"]
    while True:
        ret, response = client.list_run(status=".".join(active_status_list))
        if not ret:
            print(response)
            err_msg = "list run failed, with response[%s]" % (response)
            raise Exception(err_msg)
        
        run_list = response["runList"]
        if len(run_list) != 0:
            err_msg = "there are [%s] active runs" % len(run_list)
            print(err_msg)
            time.sleep(5) 
        else:
            err_msg = "no active run, quit pre-delete check"
            print(err_msg)
            return 0

if __name__ == '__main__':
    print("start pre-delete check")
    service_name = os.getenv("service_name")
    namespace = os.getenv("namespace")
    port = int(os.getenv("port"))
    user = os.getenv("user")
    password = os.getenv("password")

    check_pfserver_status(service_name, namespace, port, user, password)
    print("pre-delete check finished")

