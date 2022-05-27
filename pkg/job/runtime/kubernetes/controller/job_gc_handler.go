/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	DefaultJobTTLSeconds = 600
)

func (j *JobGarbageCollector) update(old, new interface{}) {
	newObj := new.(*unstructured.Unstructured)
	// get job status
	getStatusFunc := k8s.GVKJobStatusMap[newObj.GroupVersionKind()]
	oldStatusInfo, err := getStatusFunc(old)
	if old != nil && err != nil {
		return
	}
	newStatusInfo, err := getStatusFunc(new)
	if err != nil {
		return
	}
	if oldStatusInfo.OriginStatus != newStatusInfo.OriginStatus {
		jobStatus := newStatusInfo.Status
		log.Infof("update job[%s/%s] status to [%s]", newObj.GetNamespace(), newObj.GetName(), jobStatus)
		// 当任务结束时：Succeeded 或 Failed 入队
		if j.isCleanJob(jobStatus) {
			finishedJob := FinishedJobInfo{
				Name:            newObj.GetName(),
				Namespace:       newObj.GetNamespace(),
				Duration:        getJobTTLSeconds(newObj.GetAnnotations(), jobStatus),
				GVK:             newObj.GroupVersionKind(),
				OwnerReferences: newObj.GetOwnerReferences(),
			}
			j.finishedJobDelayEnqueue(finishedJob)
		}
	}
}

func getJobTTLSeconds(annotation map[string]string, status schema.JobStatus) time.Duration {
	// get job TTL seconds from annotation first
	if annotation != nil && len(annotation[schema.JobTTLSeconds]) != 0 {
		ttlStr := annotation[schema.JobTTLSeconds]
		ttl, err := strconv.Atoi(ttlStr)
		if err == nil {
			return time.Duration(ttl) * time.Second
		}
		log.Warnf("convert ttl second string %s to int failed, err: %v", ttlStr, err)
	}
	// get job TTL seconds from config
	ttlSeconds := DefaultJobTTLSeconds
	switch status {
	case schema.StatusJobSucceeded:
		if config.GlobalServerConfig.Job.Reclaim.SucceededJobTTLSeconds > 0 {
			ttlSeconds = config.GlobalServerConfig.Job.Reclaim.SucceededJobTTLSeconds
		}
	case schema.StatusJobTerminated, schema.StatusJobFailed:
		if config.GlobalServerConfig.Job.Reclaim.FailedJobTTLSeconds > 0 {
			ttlSeconds = config.GlobalServerConfig.Job.Reclaim.FailedJobTTLSeconds
		}
	default:
		log.Warnf("job status %s is not supported", status)
	}
	return time.Duration(ttlSeconds) * time.Second
}

func (j *JobGarbageCollector) isCleanJob(jobStatus schema.JobStatus) bool {
	if !config.GlobalServerConfig.Job.Reclaim.CleanJob {
		return false
	}
	if config.GlobalServerConfig.Job.Reclaim.SkipCleanFailedJob {
		return schema.StatusJobSucceeded == jobStatus
	}
	return schema.StatusJobSucceeded == jobStatus || schema.StatusJobTerminated == jobStatus || schema.StatusJobFailed == jobStatus
}

func (j *JobGarbageCollector) finishedJobDelayEnqueue(job FinishedJobInfo) {
	duration := job.Duration
	if !job.LastTransitionTime.IsZero() && time.Now().After(job.LastTransitionTime.Add(duration)) {
		duration = 0
	}
	job.OwnerName = FindOwnerReferenceName(job.OwnerReferences)
	log.Infof("finishedJobDelayEnqueue vcjob[%s] ownerName[%s] in ns[%s] duration[%v]",
		job.Name, job.OwnerName, job.Namespace, duration)
	j.WaitedCleanQueue.AddAfter(&job, duration)
}
