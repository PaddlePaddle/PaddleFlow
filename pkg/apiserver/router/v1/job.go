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

package v1

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"paddleflow/pkg/apiserver/router/util"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/controller/job"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
)

// JobRouter is job api router
type JobRouter struct{}

// Name indicate name of job router


var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)


func (jr *JobRouter) Name() string {
	return "JobRouter"
}


// AddRouter add job router to root router
func (jr *JobRouter) AddRouter(r chi.Router) {
	log.Info("add job router")
	r.Post("/job/single", jr.CreateSingleJob)
	r.Post("/job/distributed", jr.CreateDistributedJob)
	r.Post("/job/workflow", jr.CreateWorkflowJob)
	r.Get("/job/{jobID}", jr.GetJob)
	r.Delete("/job/{jobID}", jr.DeleteJob)
	r.Get("/wsjob", jr.GetJobByWebsocket)
}

// CreateSingleJob create single job
// @Summary 创建single类型作业
// @Description 创建single类型作业
// @Id createSingleJob
// @tags User
// @Accept  json
// @Produce json
// @Success 200 {object} job.CreateJobResponse "创建single类型作业的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /job/single [POST]
func (jr *JobRouter) CreateSingleJob(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var request job.CreateSingleJobRequest
	if err := common.BindJSON(r, &request); err != nil {
		ctx.ErrorCode = common.MalformedJSON
		logger.LoggerForRequest(&ctx).Errorf("parsing request body failed:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	log.Debugf("create single job request:%#v", request)

	request.CommonJobInfo.UserName = ctx.UserName

	// validate Job
	if err := validateSingleJob(&ctx, &request); err != nil {
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response, err := job.CreateSingleJob(&request)
	if err != nil {
		ctx.Logging().Errorf("create job failed. job request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("CreateJob job:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

// CreateDistributedJob create distributed job
// @Summary 创建Distributed类型作业
// @Description 创建Distributed类型作业
// @Id createDistributedJob
// @tags User
// @Accept  json
// @Produce json
// @Success 200 {object} job.CreateJobResponse "创建distributed类型作业的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /job/distributed [POST]
func (jr *JobRouter) CreateDistributedJob(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var request job.CreateDisJobRequest
	if err := common.BindJSON(r, &request); err != nil {
		ctx.ErrorCode = common.MalformedJSON
		logger.LoggerForRequest(&ctx).Errorf("parsing request body failed:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	log.Debugf("create distributed job request:%+v", request)

	// validate Job
	if err := validateDistributedJob(&ctx, &request); err != nil {
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response, err := job.CreateDistributedJob(&request)
	if err != nil {
		ctx.Logging().Errorf("create job failed. job request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("CreateJob job:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

// CreateWorkflowJob create workflow job
// @Summary 创建Workflow类型作业
// @Description 创建Workflow类型作业
// @Id createWorkflowJob
// @tags User
// @Accept  json
// @Produce json
// @Success 200 {object} job.CreateJobResponse "创建Workflow类型作业的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /job/workflow [POST]
func (jr *JobRouter) CreateWorkflowJob(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var request job.CreateWfJobRequest
	if err := common.BindJSON(r, &request); err != nil {
		ctx.ErrorCode = common.MalformedJSON
		logger.LoggerForRequest(&ctx).Errorf("parsing request body failed:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	log.Debugf("create workflow job request:%+v", request)
	if err := validateWorkflowJob(&ctx, &request); err != nil {
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response, err := job.CreateWorkflowJob(&request)
	if err != nil {
		ctx.Logging().Errorf("create job failed. job request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("CreateJob job:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateSingleJob(ctx *logger.RequestContext, request *job.CreateSingleJobRequest) error {
	// ensure required fields
	emptyFields := validateEmptyField(request)
	if len(emptyFields) != 0 {
		emptyFieldStr := strings.Join(emptyFields, ",")
		err := fmt.Errorf("required fields in {%s} are empty, please fill it", emptyFieldStr)
		ctx.Logging().Errorf("create single job failed. error: %s", err.Error())
		ctx.ErrorCode = common.RequiredFieldEmpty
		return err
	}
	if request.FileSystem.Name != "" && !common.IsRootUser(ctx.UserName) {
		// check grant
		fsID := common.ID(ctx.UserName, request.FileSystem.Name)
		// todo(zhongzichao) router will call controller function instead of models function
		if !models.HasAccessToResource(ctx, common.ResourceTypeFs, fsID) {
			ctx.ErrorCode = common.AccessDenied
			err := common.NoAccessError(ctx.UserName, common.ResourceTypeFs, fsID)
			ctx.Logging().Errorf("create run failed. error: %v", err)
			return err
		}
	}
	return nil
}

func validateEmptyField(request *job.CreateSingleJobRequest) []string {
	var emptyFields []string
	if request.CommonJobInfo.SchedulingPolicy.QueueID == "" {
		emptyFields = append(emptyFields, "queue")
	}
	if request.Image == "" {
		emptyFields = append(emptyFields, "image")
	}
	if request.Flavour.Name == "" {
		emptyFields = append(emptyFields, "flavour.name")
	}
	if request.FileSystem.Name == "" {
		emptyFields = append(emptyFields, "fileSystem.name")
	}
	return emptyFields
}

func validateDistributedJob(ctx *logger.RequestContext, request *job.CreateDisJobRequest) error {
	// todo(zhongzichao)
	return nil
}

func validateWorkflowJob(ctx *logger.RequestContext, request *job.CreateWfJobRequest) error {
	// todo(zhongzichao)
	return nil
}

// listJob
// @Summary 获取作业列表
// @Description 获取作业列表
// @Id listJob
// @tags Job
// @Accept  json
// @Produce json
// @Param status query string false "作业状态过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} job.ListJobResponse "获取作业列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /job [GET]
func (jr *JobRouter) listJob(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	status := request.URL.Query().Get(util.QueryKeyStatus)
	marker := request.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, request)
	if err != nil {
		common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	listJobRequest := job.ListJobRequest{
		Status:  status,
		Marker:  marker,
		MaxKeys: maxKeys,
	}
	response, err := job.ListJob(&ctx, listJobRequest)
	if err != nil {
		ctx.Logging().Errorf("list job failed, error:%s", err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(writer, http.StatusOK, response)
}

// GetJob
// @Summary 获取作业详情
// @Description 获取作业详情
// @Id getJob
// @tags Job
// @Accept  json
// @Produce json
// @Param jobID path string true "作业ID"
// @Success 200 {object} job.GetJobResponse "作业详情"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /job/{jobID} [GET]
func (jr *JobRouter) GetJob(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	jobID := chi.URLParam(request, util.ParamKeyJobID)
	response, err := job.GetJob(&ctx, jobID)
	if err != nil {
		ctx.Logging().Errorf("jobID[%s] get failed. error:%s.", jobID, err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(writer, http.StatusOK, response)
}

// DeleteJob
// @Summary 删除作业
// @Description 删除作业
// @Id deleteJob
// @tags Job
// @Accept  json
// @Produce json
// @Param jobID path string true "作业ID"
// @Success 200 {string} string "删除作业的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /job/{jobID} [DELETE]
func (jr *JobRouter) DeleteJob(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	jobID := chi.URLParam(request, util.ParamKeyJobID)
	err := job.DeleteJob(&ctx, jobID)
	if err != nil {
		ctx.Logging().Errorf("jobID[%s] delete failed. error:%s.", jobID, err.Error())
		common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(writer, http.StatusOK)
}

func (jr *JobRouter) GetJobByWebsocket(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	clientID := request.Header.Get(common.HeaderClientIDKey)
	wsConn, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	conn, err := job.InitConnection(wsConn, &ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	job.WSManager.Register(conn, clientID)

	// heartbeat response
	for {
		_, data, err := conn.WsConnect.ReadMessage()
		if err != nil {
			return
		}
		err = conn.WriteMessage(string(data), job.HeartbeatMsg)
		if err != nil {
			return
		}
	}

}
