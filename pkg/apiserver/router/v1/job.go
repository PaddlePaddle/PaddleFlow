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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// JobRouter is job api router
type JobRouter struct{}

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

// Name indicate name of job router
func (jr *JobRouter) Name() string {
	return "JobRouter"
}

// AddRouter add job router to root router
func (jr *JobRouter) AddRouter(r chi.Router) {
	log.Info("add job router")
	r.Post("/job/single", jr.CreateSingleJob)
	r.Post("/job/distributed", jr.CreateDistributedJob)
	r.Post("/job/workflow", jr.CreateWorkflowJob)

	r.Delete("/job/{jobID}", jr.DeleteJob)
	r.Put("/job/{jobID}", func(w http.ResponseWriter, r *http.Request) {
		ctx := common.GetRequestContext(r)
		action := r.URL.Query().Get(util.QueryKeyAction)
		switch action {
		case util.QueryActionStop:
			jr.StopJob(w, r)
		case util.QueryActionModify:
			jr.UpdateJob(w, r)
		default:
			common.RenderErr(w, ctx.RequestID, common.ActionNotAllowed)
		}
	})

	r.Get("/wsjob", jr.GetJobByWebsocket)
	r.Get("/job", jr.ListJob)
	r.Get("/job/{jobID}", jr.GetJob)
}

// CreateSingleJob create single job
// @Summary 创建single类型作业
// @Description 创建single类型作业
// @Id createSingleJob
// @tags Job
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
		ctx.ErrorCode = common.JobInvalidField
		ctx.Logging().Errorf("validate job request failed. request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response, err := job.CreateSingleJob(&ctx, &request)
	if err != nil {
		ctx.ErrorCode = common.JobCreateFailed
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
// @tags Job
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
	request.UserName = ctx.UserName
	response, err := job.CreateDistributedJob(&ctx, &request)
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
// @tags Job
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

	response, err := job.CreateWorkflowJob(&ctx, &request)
	if err != nil {
		ctx.Logging().Errorf("create job failed. job request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("CreateJob job:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateSingleJob(ctx *logger.RequestContext, request *job.CreateSingleJobRequest) error {
	// validate job id
	if request.ID != "" {
		// check namespace format
		if errStr := common.IsDNS1123Label(request.ID); len(errStr) != 0 {
			return fmt.Errorf("ID[%s] of Job is invalid, err: %s", request.ID, strings.Join(errStr, ","))
		}
	}
	// ensure required fields
	emptyFields := validateEmptyField(request)
	if len(emptyFields) != 0 {
		emptyFieldStr := strings.Join(emptyFields, ",")
		err := fmt.Errorf("required fields in {%s} are empty, please fill it", emptyFieldStr)
		ctx.Logging().Errorf("create single job failed. error: %s", err.Error())
		ctx.ErrorCode = common.RequiredFieldEmpty
		return err
	}
	// SchedulingPolicy
	if err := checkPriority(&request.SchedulingPolicy, nil); err != nil {
		ctx.Logging().Errorf("Failed to check priority: %v", err)
		ctx.ErrorCode = common.JobInvalidField
		return err
	}
	if err := validateQueue(ctx, &request.SchedulingPolicy); err != nil {
		ctx.Logging().Errorf("validate queue failed. error: %s", err.Error())
		return err
	}
	return nil
}

// checkPriority check priority and fill parent's priority if schedulingPolicy.Priority is empty
func checkPriority(schedulingPolicy, parentSP *job.SchedulingPolicy) error {
	priority := schedulingPolicy.Priority
	// check job priority
	if priority == "" {
		if parentSP != nil {
			schedulingPolicy.Priority = parentSP.Priority
		} else {
			schedulingPolicy.Priority = schema.EnvJobNormalPriority
		}
	} else if priority != schema.EnvJobLowPriority &&
		priority != schema.EnvJobNormalPriority && priority != schema.EnvJobHighPriority {
		return errors.InvalidJobPriorityError(priority)
	}
	return nil
}

func validateEmptyField(request *job.CreateSingleJobRequest) []string {
	var emptyFields []string
	if request.CommonJobInfo.SchedulingPolicy.Queue == "" {
		emptyFields = append(emptyFields, "queue")
	}
	if request.Image == "" {
		emptyFields = append(emptyFields, "image")
	}

	return emptyFields
}

func validateDistributedJob(ctx *logger.RequestContext, request *job.CreateDisJobRequest) error {
	// validate job id
	if request.ID != "" {
		// check namespace format
		if errStr := common.IsDNS1123Label(request.ID); len(errStr) != 0 {
			return fmt.Errorf("ID[%s] of Job is invalid, err: %s", request.ID, strings.Join(errStr, ","))
		}
	}
	switch request.Framework {
	case schema.FrameworkSpark, schema.FrameworkPaddle:
		break
	case schema.FrameworkTF, schema.FrameworkMPI:
		ctx.Logging().Errorf("framework: %s will be supported in the future", request.Framework)
		ctx.ErrorCode = common.JobInvalidField
		return fmt.Errorf("framework: %s will be supported in the future", request.Framework)
	default:
		ctx.Logging().Errorf("invalid framework: %s", request.Framework)
		ctx.ErrorCode = common.JobInvalidField
		return fmt.Errorf("invalid framework: %s", request.Framework)
	}
	// check job priority
	if err := checkPriority(&request.SchedulingPolicy, nil); err != nil {
		ctx.Logging().Errorf("Failed to check priority: %v", err)
		ctx.ErrorCode = common.JobInvalidField
		return err
	}

	// request.SchedulingPolicy and request.Members[x].SchedulingPolicy should be the same
	if err := validateQueue(ctx, &request.SchedulingPolicy); err != nil {
		ctx.Logging().Errorf("validate queue failed. error: %s", err.Error())
		return err
	}
	queueName := request.SchedulingPolicy.Queue

	if request.Members == nil || len(request.Members) == 0 {
		err := fmt.Errorf("request.Members is empty")
		ctx.Logging().Errorf("create distributed job failed. error: %s", err.Error())
		ctx.ErrorCode = common.RequiredFieldEmpty
		return err
	}
	for _, member := range request.Members {
		// validate queue
		mQueueName := member.SchedulingPolicy.Queue
		if mQueueName != queueName {
			if mQueueName == "" {
				// set value by default as request.SchedulingPolicy.Queue
				member.SchedulingPolicy.Queue = queueName
			} else {
				err := fmt.Errorf("schedulingPolicy.Queue should be the same, there are %s and %s", queueName, mQueueName)
				ctx.Logging().Errorf("create distributed job failed. error: %s", err.Error())
				ctx.ErrorCode = common.JobInvalidField
				return err
			}
		}
		member.SchedulingPolicy.QueueID = request.SchedulingPolicy.QueueID
		// check members priority
		if err := checkPriority(&member.SchedulingPolicy, &request.SchedulingPolicy); err != nil {
			ctx.Logging().Errorf("Failed to check priority: %v", err)
			ctx.ErrorCode = common.JobInvalidField
			return err
		}
	}

	return nil
}

// validateQueue validate queue and set queueID in request.SchedulingPolicy
func validateQueue(ctx *logger.RequestContext, schedulingPolicy *job.SchedulingPolicy) error {
	queueName := schedulingPolicy.Queue
	if queueName == "" {
		ctx.Logging().Errorf("queue is empty")
		ctx.ErrorCode = common.JobInvalidField
		return fmt.Errorf("queue is empty")
	}
	queue, err := models.GetQueueByName(queueName)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			ctx.ErrorCode = common.JobInvalidField
			log.Errorf("create job failed. error: %s", err.Error())
			return fmt.Errorf("queue not found")
		}
		ctx.ErrorCode = common.InternalError
		log.Errorf("Get queue failed when creating job, err=%v", err)
		return err
	}

	schedulingPolicy.QueueID = queue.ID
	return nil
}

func validateWorkflowJob(ctx *logger.RequestContext, request *job.CreateWfJobRequest) error {
	// validate job id
	if request.ID != "" {
		// check namespace format
		if errStr := common.IsDNS1123Label(request.ID); len(errStr) != 0 {
			return fmt.Errorf("ID[%s] of Job is invalid, err: %s", request.ID, strings.Join(errStr, ","))
		}
	}
	if request.ExtensionTemplate == nil {
		ctx.ErrorCode = common.RequiredFieldEmpty
		err := fmt.Errorf("ExtensionTemplate for workflow job is needed, and now is empty")
		ctx.Logging().Errorf("create workflow job failed. error: %s", err.Error())
		return err
	}
	// todo(zhongzichao)
	return nil
}

// DeleteJob delete job
// @Summary 删除作业
// @Description 删除作业
// @Id DeleteJob
// @tags Job
// @Accept  json
// @Produce json
// @Param jobID path string true "作业ID"
// @Success 200 {string} "删除作业的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /job/{jobID} [DELETE]
func (jr *JobRouter) DeleteJob(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	jobID := chi.URLParam(r, util.ParamKeyJobID)
	if err := validateJob(&ctx, jobID); err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	err := job.DeleteJob(&ctx, jobID)
	if err != nil {
		ctx.ErrorMessage = fmt.Sprintf("delete job failed, err: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

func validateJob(ctx *logger.RequestContext, jobID string) error {
	if len(jobID) == 0 {
		ctx.ErrorCode = common.JobInvalidField
		ctx.Logging().Errorf("job id is empty")
		return errors.EmptyJobIDError()
	}
	return nil
}

// StopJob stop job
// @Summary 停止作业
// @Description 停止作业
// @Id StopJob
// @tags Job
// @Accept  json
// @Produce json
// @Param jobID path string true "作业ID"
// @Success 200 {string} "停止作业的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /job/{jobID}?action=stop [PUT]
func (jr *JobRouter) StopJob(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	jobID := chi.URLParam(r, util.ParamKeyJobID)
	if err := validateJob(&ctx, jobID); err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	err := job.StopJob(&ctx, jobID)
	if err != nil {
		ctx.ErrorMessage = fmt.Sprintf("stop job failed, err: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// UpdateJob update job
// @Summary 更新作业
// @Description 更新作业
// @Id UpdateJob
// @tags Job
// @Accept  json
// @Produce json
// @Param jobID path string true "作业ID"
// @Success 200 {string} "更新作业的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /job/{jobID}?action=modify [PUT]
func (jr *JobRouter) UpdateJob(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	jobID := chi.URLParam(r, util.ParamKeyJobID)

	var request job.UpdateJobRequest
	if err := common.BindJSON(r, &request); err != nil {
		ctx.ErrorCode = common.MalformedJSON
		logger.LoggerForRequest(&ctx).Errorf("parsing request body failed: %v. err: %s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	request.JobID = jobID
	log.Debugf("update job request: %v", request)

	// TODO: check update job request
	if err := validateJob(&ctx, jobID); err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	err := job.UpdateJob(&ctx, &request)
	if err != nil {
		ctx.ErrorMessage = fmt.Sprintf("update job failed, err: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// ListJob
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
func (jr *JobRouter) ListJob(writer http.ResponseWriter, request *http.Request) {
	ctx := common.GetRequestContext(request)
	status := request.URL.Query().Get(util.QueryKeyStatus)
	timestampStr := request.URL.Query().Get(util.QueryKeyTimestamp)
	var err error
	var timestamp int64 = 0
	if timestampStr != "" {
		timestamp, err = strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
		if timestamp < 0 {
			ctx.ErrorMessage = fmt.Sprintf("invalid timestamp params[%s]", timestampStr)
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, ctx.ErrorMessage)
			return
		}
	}
	startTime := request.URL.Query().Get(util.QueryKeyStartTime)
	if startTime != "" {
		_, err = time.ParseInLocation(models.TimeFormat, startTime, time.Local)
		if err != nil {
			ctx.ErrorMessage = fmt.Sprintf("invalid startTime params[%s]", startTime)
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, ctx.ErrorMessage)
			return
		}
	}
	queue := request.URL.Query().Get(util.QueryKeyQueue)
	labelsStr := request.URL.Query().Get(util.QueryKeyLabels)
	labels := make(map[string]string)
	if labelsStr != "" {
		err := json.Unmarshal([]byte(labelsStr), &labels)
		if err != nil {
			common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
			return
		}
	}
	marker := request.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, request)
	if err != nil {
		common.RenderErrWithMessage(writer, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	listJobRequest := job.ListJobRequest{
		Status:    status,
		Queue:     queue,
		StartTime: startTime,
		Labels:    labels,
		Timestamp: timestamp,
		Marker:    marker,
		MaxKeys:   maxKeys,
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
