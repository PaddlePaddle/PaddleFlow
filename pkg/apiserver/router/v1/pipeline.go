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
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/controller/pipeline"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/logger"
)

type PipelineRouter struct{}

func (pr *PipelineRouter) Name() string {
	return "PipelineRouter"
}

func (pr *PipelineRouter) AddRouter(r chi.Router) {
	log.Info("add pipeline router")
	r.Post("/pipeline", pr.createPipeline)
	r.Get("/pipeline", pr.listPipeline)
	r.Post("/pipeline/{pipelineID}", pr.updatePipeline)
	r.Get("/pipeline/{pipelineID}", pr.getPipeline)
	r.Delete("/pipeline/{pipelineID}", pr.deletePipeline)
	r.Get("/pipeline/{pipelineID}/{pipelineDetailPk}", pr.getPipelineDetail)
	r.Delete("/pipeline/{pipelineID}/{pipelineDetailPk}", pr.deletePipelineDtail)
}

// createPipeline
// @Summary 创建工作流
// @Description 创建工作流
// @Id createPipeline
// @tags Pipeline
// @Accept  json
// @Produce json
// @Param request body pipeline.CreatePipelineRequest true "创建工作流请求"
// @Success 201 {object} pipeline.CreatePipelineResponse "创建工作流响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /pipeline [POST]
func (pr *PipelineRouter) createPipeline(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var createPplReq pipeline.CreatePipelineRequest
	if err := common.BindJSON(r, &createPplReq); err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"create pipeline failed parsing request body:%+v. error:%v", r.Body, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	if createPplReq.FsName == "" {
		logger.LoggerForRequest(&ctx).Errorf(
			"create pipeline failed. fsname shall not be empty")
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, "create pipeline failed. fsname in request body shall not be empty")
		return
	}
	if createPplReq.YamlPath == "" {
		createPplReq.YamlPath = "./run.yaml"
	}

	// check grant
	fsID, err := getFsIDAndCheckPermission(&ctx, createPplReq.UserName, createPplReq.FsName)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	// create in service
	response, err := pipeline.CreatePipeline(&ctx, createPplReq, fsID)
	if err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"create pipeline failed. createPplReq:%v error:%v", createPplReq, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusCreated, response)
}

// listPipeline
// @Summary 获取工作流列表
// @Description 获取工作流列表
// @Id listPipeline
// @tags Pipeline
// @Accept  json
// @Produce json
// @Param userFilter query string false "(root用户)username过滤"
// @Param fsFilter query string false "fsname过滤"
// @Param nameFilter query string false "工作流名称过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} pipeline.ListPipelineResponse "获取工作流列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /pipeline [GET]
func (pr *PipelineRouter) listPipeline(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	userNames, pipelineNames := r.URL.Query().Get(util.QueryKeyUserFilter), r.URL.Query().Get(util.QueryKeyNameFilter)
	userFilter, nameFilter := make([]string, 0), make([]string, 0)
	if userNames != "" {
		userFilter = strings.Split(userNames, common.SeparatorComma)
	}
	if pipelineNames != "" {
		nameFilter = strings.Split(pipelineNames, common.SeparatorComma)
	}
	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] listPipeline marker:[%s] maxKeys:[%d] userFilter:[%v]",
		ctx.UserName, marker, maxKeys, userFilter)
	listPipelineResponse, err := pipeline.ListPipeline(&ctx, marker, maxKeys, userFilter, nameFilter)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, listPipelineResponse)
}

// updatePipeline
// @Summary 创建工作流
// @Description 创建工作流
// @Id updatePipeline
// @tags Pipeline
// @Accept  json
// @Produce json
// @Param request body pipeline.UpdatePipelineRequest true "创建工作流请求"
// @Success 201 {object} pipeline.UpdatePipelineResponse "创建工作流响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /pipeline [POST]
func (pr *PipelineRouter) updatePipeline(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var updatePplReq pipeline.UpdatePipelineRequest
	if err := common.BindJSON(r, &updatePplReq); err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"update pipeline failed parsing request body:%+v. error:%v", r.Body, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	if updatePplReq.FsName == "" {
		logger.LoggerForRequest(&ctx).Errorf(
			"update pipeline failed. fsname shall not be empty")
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, "update pipeline failed. fsname in request body shall not be empty")
		return
	}
	if updatePplReq.YamlPath == "" {
		updatePplReq.YamlPath = "./run.yaml"
	}

	// check grant
	fsID, err := getFsIDAndCheckPermission(&ctx, updatePplReq.UserName, updatePplReq.FsName)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	// update in service
	response, err := pipeline.UpdatePipeline(&ctx, updatePplReq, fsID)
	if err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"update pipeline failed. updatePplReq:%v error:%v", updatePplReq, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusCreated, response)
}

// getPipeline
// @Summary 通过ID获取pipeline，以及pipeline details
// @Description  通过ID获取pipeline，以及pipeline details
// @Id getPipeline
// @tags Pipeline
// @Accept  json
// @Produce json
// @Param pipelineID path string true "工作流ID"
// @Success 201 {object} models.Pipeline "工作流结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /pipeline/{pipelineID} [GET]
func (pr *PipelineRouter) getPipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, util.ParamKeyPipelineID)

	ctx := common.GetRequestContext(r)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	marker := r.URL.Query().Get(util.QueryKeyMarker)
	fsNames := r.URL.Query().Get(util.QueryKeyFsFilter)
	fsFilter := make([]string, 0)
	if fsNames != "" {
		fsFilter = strings.Split(fsNames, common.SeparatorComma)
	}

	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] getPipeline marker:[%s] maxKeys:[%d] fsFilter:[%v]",
		ctx.UserName, marker, maxKeys, fsFilter)
	getPipelineResponse, err := pipeline.GetPipeline(&ctx, pipelineID, marker, maxKeys, fsFilter)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, getPipelineResponse)
}

// DeletePipeline
// @Summary 删除工作流
// @Description 删除工作流
// @Id DeletePipeline
// @tags Pipeline
// @Accept  json
// @Produce json
// @Param pipelineID path string true "工作流ID"
// @Success 200 {string} string "删除工作流的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /pipeline/{pipelineID} [DELETE]
func (pr *PipelineRouter) deletePipeline(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	pipelineID := chi.URLParam(r, util.ParamKeyPipelineID)
	err := pipeline.DeletePipeline(&ctx, pipelineID)
	if err != nil {
		ctx.Logging().Errorf("delete pipeline failed.  error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// getPipelineDetail
// @Summary 通过ID获取pipeline detail，以及pipeline信息
// @Description 通过ID获取pipeline detail，以及pipeline信息
// @Id getPipeline
// @tags Pipeline
// @Accept  json
// @Produce json
// @Param pipelineID path string true "工作流ID"
// @Success 201 {object} models.Pipeline "工作流结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /pipeline/{pipelineID}/{detailID} [GET]
func (pr *PipelineRouter) getPipelineDetail(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	pipelineID := chi.URLParam(r, util.ParamKeyPipelineID)
	strPplDetailID := chi.URLParam(r, util.ParamKeyPipelineDetailPk)
	pipelineDetailPk, err := strconv.ParseInt(strPplDetailID, 10, 64)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] get Pipeline detail:[%d], pipelineID[%s]", ctx.UserName, pipelineDetailPk, pipelineID)
	pplDetail, err := pipeline.GetPipelineDetail(&ctx, pipelineID, pipelineDetailPk)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, pplDetail)
}

// DeletePipelineDetail
// @Summary 删除pipeline detail
// @Description 删除pipeline detail
// @Id DeletePipeline
// @tags Pipeline
// @Accept  json
// @Produce json
// @Param pipelineID path string true "工作流ID"
// @Success 200 {string} string "删除工作流的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /pipeline/{pipelineID} [DELETE]
func (pr *PipelineRouter) deletePipelineDtail(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	pipelineID := chi.URLParam(r, util.ParamKeyPipelineID)
	strPplDetailID := chi.URLParam(r, util.ParamKeyPipelineDetailPk)
	pipelineDetailPk, err := strconv.ParseInt(strPplDetailID, 10, 64)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	err = pipeline.DeletePipelineDetail(&ctx, pipelineID, pipelineDetailPk)
	if err != nil {
		ctx.Logging().Errorf("delete pipeline detail failed. error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}
