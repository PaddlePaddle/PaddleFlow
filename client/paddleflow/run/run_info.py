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

class RunInfo(object):
    """the class of RunInfo info"""   

    def __init__(self, runID, fsname, username, status, name, description, parameters,
                 runYaml, runtime, postProcess, dockerEnv, updateTime, source, runMsg, scheduleID,
                 fsOptions, failureOptions, disabled, runCachedIDs, createTime, activateTime):
        """init """
        self.runId = runID
        self.fsname = fsname
        self.username = username
        self.status = status
        self.name = name
        self.description = description
        self.parameters = parameters
        self.runYaml = runYaml
        self.runtime = runtime
        self.postProcess = postProcess
        self.dockerEnv = dockerEnv
        self.updateTime = updateTime
        self.source = source
        self.runMsg = runMsg
        self.fsOptions = fsOptions
        self.failureOptions = failureOptions
        self.disabled = disabled
        self.runCachedIDs = runCachedIDs
        self.scheduleID = scheduleID
        self.createTime = createTime
        self.activateTime = activateTime


class JobInfo(object):
    """ the class of job info"""

    def __init__(self, name, deps, parameters, command, env, status, start_time, end_time, dockerEnv, jobid,
                 compType, stepName, parentDagID, extraFS, artifacts, cache, jobMessage, cacheRunID, cacheJobID):
        self.artifacts = artifacts
        self.cache = cache
        self.jobMessage = jobMessage
        self.cacheRunID = cacheRunID
        self.cacheJobID = cacheJobID
        self.extraFS = extraFS
        self.jobId = jobid
        self.name = name
        self.deps = deps
        self.parameters = parameters
        self.command = command
        self.env = env
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.dockerEnv = dockerEnv
        self.type = compType
        self.stepName = stepName
        self.parentDagID = parentDagID


class DagInfo(object):
    """ the class of dag info"""

    def __init__(self, dagid, name, compType, dagName, parentDagID, deps, parameters, artifacts, startTime, endtime,
                 status, message, entryPoints):
        self.dagId = dagid
        self.name = name
        self.type = compType
        self.dagName = dagName
        self.parentDagID = parentDagID
        self.deps = deps
        self.parameters = parameters
        self.artifacts = artifacts
        self.startTime = startTime
        self.endTime = endtime
        self.status = status
        self.message = message
        self.entryPoints = entryPoints


class RunCacheInfo(object):
    """ the class of runcache info"""

    def __init__(self, cacheid, firstfp, secondfp, runid, source, jobid, fsname, username, expiredtime, strategy, custom,
                createtime, updatetime):
        self.cacheid = cacheid
        self.firstfp = firstfp
        self.secondfp = secondfp
        self.runid = runid
        self.source = source
        self.jobid = jobid
        self.fsname = fsname
        self.username = username
        self.expiredtime = expiredtime
        self.strategy = strategy
        self.custom = custom
        self.createtime = createtime
        self.updatetime = updatetime


class ArtifaceInfo(object):
    """ the class of artiface info"""

    def __init__(self, runid, fsname, username, artifactpath, atype, step, artifactname, meta, 
                createtime, updatetime):
        self.runid = runid
        self.fsname = fsname
        self.username = username
        self.artifactpath = artifactpath
        self.type = atype
        self.step = step
        self.artifactname = artifactname
        self.meta = meta
        self.createtime = createtime
        self.updatetime = updatetime