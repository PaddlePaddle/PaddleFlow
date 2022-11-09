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
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	runLog "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/log"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
)

const defaultMemory = "100MB"

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
// @Param name query string false "作业ID/pod名/deploy名"
// @Param clusterName query string false "集群名"
// @Param namespace query string false "作业对应的命名空间"
// @Param readFromTail query string false "日志内容开始位置(begin or end)"
// @Param lineLimit query int false "返回的日志行数"
// @Param sizeLimit query int false "返回的日志数据大小"
// @Param type query int false "job type, in {single, distributed, workflow, deploy, pod}"
// @Param framework query int false "job framework such as paddle、pytorch、tensorflow"
// @Success 200 {object} log.GetRunLogResponse "日志详情"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /log/job [GET]
func (lr *LogRouter) getJobLog(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	var err error
	// get query param, name
	name := request.URL.Query().Get(util.QueryKeyName)
	// check clusterName
	clusterName := request.URL.Query().Get(util.QueryKeyClusterName)
	clusterInfo, err := storage.Cluster.GetClusterByName(clusterName)
	if err != nil {
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("get cluster %s failed. err:%v", clusterName, err)
		common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	// namespace, todo whether be checked or not
	namespace := request.URL.Query().Get(util.QueryKeyNamespace)
	// readFromTail
	readFromTail := request.URL.Query().Get(util.QueryKeyReadFromTail)
	isReadFromTail := false
	if readFromTail != "" {
		isReadFromTail, err = strconv.ParseBool(readFromTail)
		if err != nil {
			ctx.Logging().Errorf("resource[%s/%s] request param isReadFromTail value failed, error:%s.", namespace, name, err.Error())
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	}
	// sizeLimit, check by resource
	lineLimit := request.URL.Query().Get(util.QueryKeyLineLimit)
	_, err = strconv.Atoi(lineLimit)
	if err != nil {
		ctx.Logging().Errorf("resource[%s/%s] request param lineLimit value failed, error:%s.", namespace, name, err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	// todo check lineLimit 0~200
	//if strings.Contains(lineLimit, ",") {
	//	if strings.Split("")
	//}
	sizeLimit := request.URL.Query().Get(util.QueryKeySizeLimit)
	var memory resources.Quantity
	if sizeLimit != "" {
		memory, err = resources.ParseQuantity(sizeLimit)
		if err != nil {
			ctx.Logging().Errorf("resource[%s/%s] request param sizelimit %s. error:%s.", namespace, name, sizeLimit, err.Error())
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	} else {
		memory, _ = resources.ParseQuantity(defaultMemory)
	}
	// type
	resourceType := request.URL.Query().Get(util.QueryKeyType)
	framework := request.URL.Query().Get(util.QueryKeyFramework)
	runLogRequest := runLog.GetMixedLogRequest{
		Name:           name,
		Namespace:      namespace,
		ClusterInfo:    clusterInfo,
		LineLimit:      lineLimit,
		SizeLimit:      memory.AsInt64(),
		IsReadFromTail: isReadFromTail,
		ResourceType:   resourceType,
		Framework:      framework,
	}

	response, err := runLog.GetK8sLog(&ctx, runLogRequest)
	if err != nil {
		ctx.Logging().Errorf("get k8s logs %s failed. err:%v", clusterName, err)
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(writer, http.StatusOK, response)
}
