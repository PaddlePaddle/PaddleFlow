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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

type RunRouter struct{}

func (rr *RunRouter) Name() string {
	return "RunRouter"
}

func (rr *RunRouter) AddRouter(r chi.Router) {
	log.Info("add run router")
	r.Post("/run", rr.createRun)
	r.Post("/runjson", rr.createRunByJson)
	r.Get("/run", rr.listRun)
	r.Get("/run/{runID}", rr.getRunByID)
	r.Put("/run/{runID}", rr.updateRun)
	r.Delete("/run/{runID}", rr.deleteRun)
}

// createRun
// @Summary 创建运行
// @Description 创建运行
// @Id createRun
// @tags Run
// @Accept  json
// @Produce json
// @Param request body run.CreateRunRequest true "创建运行请求"
// @Success 201 {object} run.CreateRunResponse "创建运行响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /run [POST]
func (rr *RunRouter) createRun(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	requestId := ctx.RequestID
	var createRunInfo pipeline.CreateRunRequest

	if err := common.BindJSON(r, &createRunInfo); err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"create run failed parsing request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	// add trace logger
	trace_logger.Key(requestId).Infof("creating run for request:%+v", createRunInfo)
	// create run
	response, err := pipeline.CreateRun(ctx, &createRunInfo, nil)
	if err != nil {
		errMsg := fmt.Sprintf(
			"create run failed. createRunInfo:%v error:%s", createRunInfo, err.Error())

		// if run id has generated, log err msg
		if response.RunID != "" {
			trace_logger.Key(response.RunID).Errorf(errMsg)
		}
		logger.LoggerForRequest(&ctx).Errorf(errMsg)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	trace_logger.Key(response.RunID).Infof("create run complete")
	common.Render(w, http.StatusCreated, response)
}

// createRunByJson
// @Summary 通过Json格式的run.yaml创建运行
// @Description 通过Json格式的run.yaml创建运行
// @Id createRunByJson
// @tags Run
// @Accept  json
// @Produce json
// @Param request body run.CreateRunByJsonRequest true "创建运行请求"
// @Success 201 {object} run.CreateRunResponse "创建运行响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /run/json [POST]
func (rr *RunRouter) createRunByJson(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"read body failed. error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	// 检查 json 请求体的格式
	if !json.Valid(bodyBytes) {
		errMsg := "request body json format invalid"
		logger.LoggerForRequest(&ctx).Errorf(errMsg)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, errMsg)
		return
	}

	bodyUnstructured := unstructured.Unstructured{}
	if err := bodyUnstructured.UnmarshalJSON(bodyBytes); err != nil && !runtime.IsMissingKind(err) {
		// MissingKindErr不影响Json的解析
		logger.LoggerForRequest(&ctx).Errorf(
			"unmarshal body failed. error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	bodyMap := bodyUnstructured.UnstructuredContent()

	trace_logger.Key(ctx.RequestID).Infof("creating run by json for request body map:%+v", bodyMap)
	// create run
	response, err := pipeline.CreateRunByJson(ctx, bodyMap)
	if err != nil {
		if response.RunID != "" {
			trace_logger.Key(response.RunID).Errorf("create run fail: %s", err)
		}
		logger.LoggerForRequest(&ctx).Errorf(
			"create run by json failed. error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusCreated, response)
}

// listRun
// @Summary 获取运行列表
// @Description 获取运行列表
// @Id listRun
// @tags Run
// @Accept  json
// @Produce json
// @Param userFilter query string false "(root用户)用户过滤"
// @Param fsFilter query string false "存储过滤"
// @Param runFilter query string false "ID过滤"
// @Param nameFilter query string false "名称过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} run.ListRunResponse "获取运行列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /run [GET]
func (rr *RunRouter) listRun(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	userNames, fsNames := r.URL.Query().Get(util.QueryKeyUserFilter), r.URL.Query().Get(util.QueryKeyFsFilter)
	runIDs, names := r.URL.Query().Get(util.QueryKeyRunFilter), r.URL.Query().Get(util.QueryKeyNameFilter)
	status := r.URL.Query().Get(util.QueryKeyStatusFilter)

	userFilter, fsFilter, runFilter, nameFilter := make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0)
	statusFilter := make([]string, 0)

	if userNames != "" {
		userFilter = strings.Split(userNames, common.SeparatorComma)
	}
	if fsNames != "" {
		fsFilter = strings.Split(fsNames, common.SeparatorComma)
	}
	if runIDs != "" {
		runFilter = strings.Split(runIDs, common.SeparatorComma)
	}
	if names != "" {
		nameFilter = strings.Split(names, common.SeparatorComma)
	}
	if status != "" {
		statusFilter = strings.Split(status, common.SeparatorComma)
	}
	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] ListRun marker:[%s] maxKeys:[%d] userFilter:%v fsFilter:%v runFilter:%v nameFilter:%v",
		ctx.UserName, marker, maxKeys, userFilter, fsFilter, runFilter, nameFilter)
	listRunResponse, err := pipeline.ListRun(&ctx, marker, maxKeys, userFilter, fsFilter, runFilter, nameFilter, statusFilter, nil)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, listRunResponse)
}

// getRunByID
// @Summary 获取运行
// @Description 获取运行
// @Id getRunByID
// @tags Run
// @Accept  json
// @Produce json
// @Param runID path int true "运行ID"
// @Success 200 {object} models.Run "运行详情"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /run/{runID} [GET]
func (rr *RunRouter) getRunByID(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	runID := chi.URLParam(r, util.ParamKeyRunID)
	runInfo, err := pipeline.GetRunByID(ctx.Logging(), ctx.UserName, runID)

	// 优化RuntimeView结构，使显示结果更友好
	runInfo.Runtime = runInfo.RemoveOuterDagView(runInfo.Runtime)

	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, runInfo)
}

// updateRun
// @Summary 修改运行
// @Description 修改运行
// @Id updateRun
// @tags Run
// @Accept  json
// @Produce json
// @Param runID path int true "运行ID"
// @Param action query string true "修改动作"
// @Success 200 "修改运行成功"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /run/{runID} [PUT]
func (rr *RunRouter) updateRun(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	runID := chi.URLParam(r, util.ParamKeyRunID)
	action := r.URL.Query().Get(util.QueryKeyAction)
	logger.LoggerForRequest(&ctx).Debugf("StopRun id:%v", runID)
	var err error
	request := pipeline.UpdateRunRequest{}
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = fmt.Errorf("get body err: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	if len(bodyBytes) > 0 {
		// body为空的话，解析会报错
		err = json.Unmarshal(bodyBytes, &request)
		if err != nil {
			logger.LoggerForRequest(&ctx).Errorf(
				"stop run failed to unmarshal body, error:%s", err.Error())
			common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
			return
		}
	}

	switch action {
	case util.QueryActionStop:
		err = pipeline.StopRun(ctx.Logging(), ctx.UserName, runID, request)
		if err != nil {
			ctx.ErrorCode = common.InternalError
		}
	case util.QueryActionRetry:
		runID, err = pipeline.RetryRun(&ctx, runID)
	default:
		ctx.ErrorCode = common.InvalidURI
		err = fmt.Errorf("invalid action[%s] for UpdateRun", action)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	if action == util.QueryActionRetry {
		rsp := pipeline.UpdateRunResponse{RunID: runID}
		common.Render(w, http.StatusOK, rsp)
	} else {
		common.RenderStatus(w, http.StatusOK)
	}
}

// deleteRun
// @Summary 删除运行
// @Description 删除运行
// @Id deleteRun
// @tags Run
// @Accept  json
// @Produce json
// @Param runID path string true "运行ID"
// @Success 200 {string} string "删除运行的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /run/{runID} [DELETE]
func (rr *RunRouter) deleteRun(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	runID := chi.URLParam(r, util.ParamKeyRunID)
	request := pipeline.DeleteRunRequest{
		CheckCache: true, // 默认为true
	}
	// 不能多次读取r.body，因此没法验证完body长度后再调用BindJson
	bodyByte, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"delete run failed to read request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	if len(string(bodyByte)) > 0 {
		err = json.Unmarshal(bodyByte, &request)
		if err != nil {
			logger.LoggerForRequest(&ctx).Errorf(
				"delete run failed to unmarshal body, error:%s", err.Error())
			common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
			return
		}
	}

	err = pipeline.DeleteRun(&ctx, runID, &request)
	if err != nil {
		ctx.Logging().Errorf("delete run: %s failed. error:%s", runID, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}
