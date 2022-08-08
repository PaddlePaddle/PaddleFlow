#!/usr/bin/env python
#-*- coding:utf8 -*-
"""
Authors: moxianyuan(moxianyuan@baidu.com)
Date:    2022-08-01 20:20:40
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


def check_pfserver_status(service_name, namespace, port, user, password):
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
    service_name = "paddleflow-server-cluster-ip"
    namespace = "paddleflow"
    port = 8999
    user = "root"
    password = "paddleflow"

    check_pfserver_status(service_name, namespace, port, user, password)

