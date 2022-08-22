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
    print("check_pfserver_status service_name=[%s] namespace=[%s] port=[%s] user=[%s] password=[%s]" % (
    service_name, namespace, port, user, password))
    host = service_name

    client = paddleflow.Client(host, user, password, port)
    ret, response = client.login(user, password)
    if not ret:
        print(response)
        err_msg = "client login failed, with host[%s], user[%s], password[%s], port[%s], response[%s]" % (
        host, user, password, port, response)
        raise Exception(err_msg)
    sum = 1
    while sum != 0:
        print("\nscheduling clean paddleflow resource, current resource is %d" % sum)
        sum = clean_pipelines(client)
        jobs_nums = clean_jobs(client)
        if jobs_nums == 0:
            sum += clean_queue(client)
        sum += jobs_nums
        clean_storage(client)
        if sum != 0:
            time.sleep(5)


def clean_pipelines(client):
    print("clean_pipelines starting")
    active_status_list = ["initiating", "pending", "running", "terminating"]
    ret, response = client.list_run(status=".".join(active_status_list))
    if not ret:
        print(response)
        err_msg = "list run failed, with response[%s]" % (response)
        raise Exception(err_msg)

    run_list = response["runList"]
    if len(run_list) != 0:
        err_msg = "there are [%s] active runs" % len(run_list)
        print(err_msg)
    else:
        err_msg = "no active run, quit clean_pipelines check"
        print(err_msg)
    print("clean_pipelines end\n\n")
    return len(run_list)


def clean_storage(client):
    print("clean_storage")
    print("clean_storage end\n\n")
    pass


def clean_queue(client):
    print("clean_queue")
    ret, response, next_marker = client.list_queue()
    if not ret:
        print("clean queue failed, %s" % response)
        err_msg = "list %s queue failed, with response[%s]" % (response)
        raise Exception(err_msg)
    queue_list = response
    if len(queue_list) != 0:
        for q in queue_list:
            print(q.name, q.namespace)
            delete_queue(client, q.name)

        print("there are [%s] active queue" % len(queue_list))
    else:
        print("no active job, quit clean_queue check")
    print("clean_queue finished\n\n")
    return len(queue_list)


def delete_queue(client, queue_name):
    print("try to delete queue %s" % queue_name)
    is_success, err_msg = client.del_queue(queue_name)
    if not is_success:
        print("failed to delete queue %s, err_msg= %s" % (queue_name, err_msg))
    else:
        print("delete queue %s successed." % queue_name)


def clean_jobs(client):
    print("clean_jobs")
    job_status = ["pending", "running"]
    jobs_nums = 0
    for status in job_status:
        next_marker = None
        nums, next_marker = clean_jobs_with_status(client, status, next_marker=next_marker)
        print("there are [%s] %s job" % (nums, status))
        jobs_nums = jobs_nums + nums
    return jobs_nums


def clean_jobs_with_status(client, status, next_marker=None):
    print("clean_jobs with [%s] status" % status)
    ret, response, next_marker = client.list_job(status=status, marker=next_marker)
    if not ret:
        print(response)
        err_msg = "list %s job failed, with response[%s]" % (status, response)
        raise Exception(err_msg)
    job_list = response
    if len(job_list) != 0:
        err_msg = "there are [%s] active jobs" % len(job_list)
        print(err_msg)
    else:
        err_msg = "no [%s] job, quit clean_jobs_with_status check" % status
        print(err_msg)
    return len(job_list), next_marker


if __name__ == '__main__':
    print("pre-delete check starting")
    service_name = os.getenv("service_name")
    namespace = os.getenv("namespace")
    port = int(os.getenv("port"))
    user = os.getenv("user")
    password = os.getenv("password")

    try:
        check_pfserver_status(service_name, namespace, port, user, password)
    except Exception as e:
        print(e)

    print("pre-delete check finished")
