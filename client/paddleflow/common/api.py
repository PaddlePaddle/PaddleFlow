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

PADDLE_FLOW_VERSION  = 1
FS_SERVER_VERSION = 1

PADDLE_FLOW_LOGIN = '/api/paddleflow/v%d/login' % PADDLE_FLOW_VERSION
PADDLE_FLOW_USER = '/api/paddleflow/v%d/user' % PADDLE_FLOW_VERSION
PADDLE_FLOW_QUEUE = '/api/paddleflow/v%d/queue' % PADDLE_FLOW_VERSION
PADDLE_FLOW_GRANT = '/api/paddleflow/v%d/grant' % PADDLE_FLOW_VERSION
PADDLE_FLOW_FS = '/api/paddleflow/v%d/fs' % FS_SERVER_VERSION
PADDLE_FLOW_FS_CACHE = '/api/paddleflow/v%d/fsCache' % FS_SERVER_VERSION
PADDLE_FLOW_RUN = '/api/paddleflow/v%d/run' % PADDLE_FLOW_VERSION
PADDLE_FLOW_LINK = '/api/paddleflow/v%d/link' % FS_SERVER_VERSION
PADDLE_FLOW_CLUSTER = '/api/paddleflow/v%d/cluster' % PADDLE_FLOW_VERSION
PADDLE_FLOW_PIPELINE = '/api/paddleflow/v%d/pipeline' % PADDLE_FLOW_VERSION
PADDLE_FLOW_RUNCACHE = '/api/paddleflow/v%d/runCache' % PADDLE_FLOW_VERSION
PADDLE_FLOW_ARTIFACT = '/api/paddleflow/v%d/artifact' % PADDLE_FLOW_VERSION
PADDLE_FLOW_SCHEDULE = '/api/paddleflow/v%d/schedule' % PADDLE_FLOW_VERSION
PADDLE_FLOW_FLAVOUR = '/api/paddleflow/v%d/flavour' % PADDLE_FLOW_VERSION
PADDLE_FLOW_LOG = '/api/paddleflow/v%d/log/run' % PADDLE_FLOW_VERSION
PADDLE_FLOW_JOB = '/api/paddleflow/v%d/job' % PADDLE_FLOW_VERSION
PADDLE_FLOW_STATISTIC = '/api/paddleflow/v%d/statistics' % PADDLE_FLOW_VERSION
PADDLE_FLOW_SERVER_VERSION = '/api/paddleflow/v%d/version' % PADDLE_FLOW_VERSION