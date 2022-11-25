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

package v1

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	runLog "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/log"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	defaultMemory    = "100M"
	defaultLineLimit = "1000"
	maxLineLimit     = 1000000
	maxSizeLimit     = "1G"
)

type LogRouter struct {
}

func (lr *LogRouter) Name() string {
	return "LogRouter"
}

func (lr *LogRouter) AddRouter(r chi.Router) {
	log.Info("add pipeline router")
	r.Get("/log/run/{runID}", lr.getRunLog)
	r.Get("/log/job", lr.getJobLog)
}

// getRunLog
// @Summary 获取作业日志
// @Description 获取作业日志
// @Id getRunLog
// @tags Log
// @Accept  json
// @Produce json
// @Param runID path string true "运行ID"
// @Param jobID query string false "作业ID"
// @Param logFilePosition query string false "日志内容开始位置(begin or end)"
// @Param pageNo query int false "日志页数"
// @Param pageSize query int false "日志每页大小(行数)"
// @Success 200 {object} log.GetRunLogResponse "日志详情"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /log/run/{runID} [GET]
func (lr *LogRouter) getRunLog(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	runID := chi.URLParam(request, util.ParamKeyRunID)
	jobID := request.URL.Query().Get(util.ParamKeyJobID)
	logPageNo, err := strconv.Atoi(request.URL.Query().Get(util.ParamKeyPageNo))
	if err != nil {
		if request.URL.Query().Get(util.ParamKeyPageNo) == "" {
			logPageNo = common.LogPageNoDefault
		} else {
			ctx.Logging().Errorf("runID[%s] request param logPageNo parse int failed. error:%s.", runID, err.Error())
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	}
	logPageSize, err := strconv.Atoi(request.URL.Query().Get(util.ParamKeyPageSize))
	if err != nil {
		if request.URL.Query().Get(util.ParamKeyPageSize) == "" {
			logPageSize = common.LogPageSizeDefault
		} else {
			ctx.Logging().Errorf("runID[%s] request param logPageSize parse int failed. error:%s.", runID, err.Error())
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	}
	if logPageNo == 0 {
		logPageNo = common.LogPageNoDefault
	}
	if logPageSize == 0 {
		logPageSize = common.LogPageSizeDefault
	} else if logPageSize > common.LogPageSizeMax {
		err := common.LogPageSizeOverMaxError()
		ctx.Logging().Errorf("runID[%s] request param logPageSize value over maxsize. error:%s.", runID, err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	logFilePosition := request.URL.Query().Get(util.ParamKeyLogFilePosition)
	if logFilePosition == "" {
		logFilePosition = common.EndFilePosition
	}
	if logFilePosition != common.BeginFilePosition && logFilePosition != common.EndFilePosition {
		err := common.LogFilePositionInvalidValueError()
		ctx.Logging().Errorf("runID[%s] request param logFilePosition has wrong value. error:%s.", runID, err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	runLogRequest := runLog.GetRunLogRequest{
		JobID:           jobID,
		PageNo:          logPageNo,
		PageSize:        logPageSize,
		LogFilePosition: logFilePosition,
	}
	response, err := runLog.GetRunLog(&ctx, runID, runLogRequest)
	if err != nil {
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.Render(writer, http.StatusOK, response)
}

// getJobLog
// @Summary 获取作业或pod/deploy日志
// @Description 获取作业或pod/deploy日志
// @Id getJobLog
// @tags Log
// @Accept  json
// @Produce json
// @Param name query string false "作业ID"
// @Param jobID query string false "pod名/deploy名"
// @Param clusterName query string false "集群名"
// @Param namespace query string false "作业对应的命名空间"
// @Param readFromTail query string false "日志内容开始位置(begin or end)"
// @Param lineLimit query int false "返回的日志行数"
// @Param sizeLimit query int false "返回的日志数据大小"
// @Param type query int false "job type, in {single, distributed, workflow, deploy, pod}"
// @Param framework query int false "job framework such as paddle、pytorch、tensorflow"
// @Success 200 {object} schema.JobLogInfo "日志详情"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /log/job [GET]
func (lr *LogRouter) getJobLog(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	ctx.Logging().Infof("get resource logs")
	logRequest, err := constructJobLogRequest(&ctx, request)
	if err != nil {
		ctx.Logging().Errorf("parse job log request failed, err: %v", err)
		ctx.ErrorCode = common.InvalidURI
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	var response schema.JobLogInfo
	jobID := request.URL.Query().Get(util.ParamKeyJobID)
	if jobID == "" {
		ctx.Logging().Debugf("get kubernetes resource logs")
		response, err = runLog.GetLogs(&ctx, logRequest)
	} else {
		ctx.Logging().Debugf("get paddleflow job %s logs", jobID)
		response, err = runLog.GetPFJobLogs(&ctx, logRequest)
	}
	if err != nil {
		err = fmt.Errorf("get k8s logs %s failed. err:%v", logRequest.Name, err)
		ctx.Logging().Errorln(err)
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(writer, http.StatusOK, &response)
}

func constructJobLogRequest(ctx *logger.RequestContext, request *http.Request) (runLog.GetMixedLogRequest, error) {
	ctx.Logging().Infof("constructJobLogRequest, request: %v", request)
	logRequest := runLog.GetMixedLogRequest{
		Namespace: request.URL.Query().Get(util.QueryKeyNamespace),
		Name:      request.URL.Query().Get(util.QueryKeyName),
	}
	var err error
	// ClusterName
	logRequest.ClusterName = request.URL.Query().Get(util.QueryKeyClusterName)
	clusterInfo, err := storage.Cluster.GetClusterByName(logRequest.ClusterName)
	if err != nil {
		err = fmt.Errorf("get cluster %s failed. err:%v", logRequest.ClusterName, err)
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorln(err)
		return logRequest, err
	}
	logRequest.ClusterInfo = clusterInfo

	// validate jobID
	jobID := request.URL.Query().Get(util.ParamKeyJobID)
	if jobID != "" {
		if err = validateJobInfo(ctx, request, jobID); err != nil {
			return logRequest, err
		}
		logRequest.Name = jobID
	}

	// readFromTail
	readFromTail := request.URL.Query().Get(util.QueryKeyReadFromTail)
	isReadFromTail := false
	if readFromTail != "" {
		isReadFromTail, err = strconv.ParseBool(readFromTail)
		if err != nil {
			err = fmt.Errorf("resource[%s] request param isReadFromTail value failed, error:%s", logRequest.Name, err.Error())
			ctx.Logging().Errorln(err)
			return logRequest, err
		}
	}
	logRequest.IsReadFromTail = isReadFromTail

	// lineLimit, check by resource
	lineLimit := request.URL.Query().Get(util.QueryKeyLineLimit)
	if lineLimit != "" {
		lineLimitInt, err := strconv.Atoi(lineLimit)
		if lineLimitInt <= 0 || lineLimitInt > maxLineLimit {
			log.Warnf("lineLimit is out of range")
			lineLimit = defaultLineLimit
		}
		if err != nil {
			err = fmt.Errorf("resource[%s] request param lineLimit value failed, error:%s", logRequest.Name, err.Error())
			ctx.Logging().Errorln(err)
			return logRequest, err
		}
	} else {
		lineLimit = defaultLineLimit
	}
	logRequest.LineLimit = lineLimit

	// SizeLimit
	sizeLimit := request.URL.Query().Get(util.QueryKeySizeLimit)
	defaultSizeLimit, _ := resources.ParseQuantity(defaultMemory)
	var memory resources.Quantity
	if sizeLimit != "" {
		memory, err = resources.ParseQuantity(sizeLimit)
		if err != nil {
			err = fmt.Errorf("resource[%s] request param sizelimit %s. error:%s", logRequest.Name, sizeLimit, err.Error())
			ctx.Logging().Errorln(err)
			return logRequest, err
		}
		maxSizeLimitRes, _ := resources.ParseQuantity(maxSizeLimit)
		if memory.AsInt64() <= 0 || memory.AsInt64() > maxSizeLimitRes.AsInt64() {
			memory = defaultSizeLimit
		}
	} else {
		memory = defaultSizeLimit
	}
	sizeLimitInt := memory.AsInt64()
	logRequest.SizeLimit = sizeLimitInt
	logRequest.ResourceType = request.URL.Query().Get(util.QueryKeyType)
	logRequest.Framework = request.URL.Query().Get(util.QueryKeyFramework)

	return logRequest, nil
}

func validateJobInfo(ctx *logger.RequestContext, request *http.Request, jobID string) error {
	ctx.Logging().Debugf("validateJobInfo for jobID: %s", jobID)
	job, err := job.GetJob(ctx, jobID)
	if err != nil {
		err = fmt.Errorf("get job by jobID %s failed. err:%v", jobID, err)
		ctx.Logging().Errorln(err)
		return err
	}
	queue, err := queue.GetQueueByName(ctx, job.SchedulingPolicy.Queue)
	if err != nil {
		err = fmt.Errorf("get queue by job %s queue failed. err:%v", jobID, err)
		ctx.Logging().Errorln(err)
		return err
	}
	// check clusterName
	clusterName := request.URL.Query().Get(util.QueryKeyClusterName)
	if queue.ClusterName != clusterName {
		err = fmt.Errorf("job %s not in cluster %s", jobID, clusterName)
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorln(err)
		return err
	}
	return nil
}
