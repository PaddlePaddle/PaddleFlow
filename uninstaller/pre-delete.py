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
        ppl_num = clean_pipelines(client)
        job_num = clean_jobs(client)
        sum = ppl_num + job_num
        queue_num = 0
        if sum == 0:
            # clean queue only if no running ppl or job
            queue_num = clean_default_queue(client)
            sum += queue_num
        storage_num = clean_storage(client)
        sum += storage_num
        if sum != 0:
            print("\nscheduling clean paddleflow resource, total resource is %d, [%s] ppl, [%s]job, [%s]queue, "
                  "[%s]storage" % (sum, ppl_num, job_num, queue_num, storage_num))
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
    print("clean_storage todo")
    return 0


def clean_default_queue(client):
    print("clean_queue default-queue")
    default_queue_name = "default-queue"
    try:
        ret, default_queue = client.del_queue(default_queue_name)
        if ret:
            print("default-queue cleaned succeed")
    except Exception as e:
        if str(e).__contains__("not exist"):
            print("default-queue has been cleaned")
        else:
            print(e)

    return 0


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
