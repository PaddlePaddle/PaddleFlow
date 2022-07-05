/*
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
*/

package log

import (
	"errors"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

type GetRunLogRequest struct {
	JobID           string `json:"jobID"`
	PageNo          int    `json:"pageNo"`
	PageSize        int    `json:"pageSize"`
	LogFilePosition string `json:"logFilePosition"`
}

type GetRunLogResponse struct {
	SubmitLog string              `json:"submitLog"`
	RunLog    []schema.JobLogInfo `json:"runLog"`
	RunID     string              `json:"runID"`
}

func GetRunLog(ctx *logger.RequestContext, runID string, request GetRunLogRequest) (*GetRunLogResponse, error) {
	run, err := models.GetRunByID(ctx.Logging(), runID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RunNotFound
			ctx.Logging().Errorf("the run[%s] is not found. error:%s", runID, err.Error())
			return nil, common.NotFoundError(common.ResourceTypeRun, runID)
		} else {
			ctx.ErrorCode = common.InternalError
			ctx.Logging().Errorf("get the run[%s] failed. error:%s", runID, err.Error())
			return nil, err
		}
	}
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != run.UserName {
		err := common.NoAccessError(ctx.UserName, common.ResourceTypeRun, runID)
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorf("get the run[%s] auth failed. error:%s", runID, err.Error())
		return nil, err
	}
	jobList, err := getJobListByRunID(ctx, runID, request.JobID)
	if err != nil {
		ctx.Logging().Errorf("runID[%s] get job list failed. error:%s.", runID, err.Error())
		return nil, err
	}

	response := &GetRunLogResponse{
		RunID:  runID,
		RunLog: make([]schema.JobLogInfo, 0),
	}
	if len(jobList) == 0 {
		return response, nil
	}
	clusterInfo, queue, err := getClusterQueueByQueueID(ctx, jobList[0].QueueID)
	if err != nil {
		ctx.Logging().Errorf("get cluster by queue[%s] failed. error:%s.", jobList[0].QueueID, err.Error())
		return nil, err
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(*clusterInfo)
	if err != nil {
		ctx.Logging().Errorf("get cluster client failed. error:%s.", err.Error())
		return nil, err
	}

	for _, job := range jobList {
		jobLogRequest := schema.JobLogRequest{
			JobID:           job.ID,
			JobType:         job.Type,
			Namespace:       queue.Namespace,
			LogFilePosition: request.LogFilePosition,
			LogPageSize:     request.PageSize,
			LogPageNo:       request.PageNo,
		}
		jobLogInfo, err := runtimeSvc.GetJobLog(jobLogRequest)
		if err != nil {
			ctx.Logging().Errorf("jobID[%s] get queue[%s] failed. error:%s.", job.ID, job.QueueID, err.Error())
			return nil, err
		}
		response.RunLog = append(response.RunLog, jobLogInfo)
	}
	trace, ok := trace_logger.GetTraceFromCache(runID)
	if !ok {
		ctx.Logging().Warnf("get trace log failed. runID[%s]", runID)
	} else {
		response.SubmitLog = trace.String()
	}

	return response, nil
}

func getJobListByRunID(ctx *logger.RequestContext, runID string, jobID string) ([]models.Job, error) {
	jobList, err := models.GetJobsByRunID(runID, jobID)
	if err != nil {
		return nil, err
	}
	return jobList, nil
}

func getClusterQueueByQueueID(ctx *logger.RequestContext, queueID string) (*models.ClusterInfo, *models.Queue, error) {
	queue, err := models.GetQueueByID(queueID)
	if err != nil {
		return nil, nil, err
	}
	clusterInfo, err := models.GetClusterById(queue.ClusterId)
	if err != nil {
		return nil, nil, err
	}
	return &clusterInfo, &queue, nil
}
