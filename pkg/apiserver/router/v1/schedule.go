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

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

type ScheduleRouter struct{}

func (sr *ScheduleRouter) Name() string {
	return "ScheduleRouter"
}

func (sr *ScheduleRouter) AddRouter(r chi.Router) {
	log.Info("add schedule router")
	r.Post("/schedule", sr.createSchedule)
	r.Get("/schedule", sr.listSchedule)
	r.Get("/schedule/{scheduleID}", sr.getSchedule)
	r.Put("/schedule/{scheduleID}", sr.stopSchedule)
	r.Delete("/schedule/{scheduleID}", sr.deleteSchedule)
}

func (sr *ScheduleRouter) createSchedule(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	// 默认catchup为true
	createScheduleInfo := pipeline.CreateScheduleRequest{Catchup: true}
	if err := common.BindJSON(r, &createScheduleInfo); err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"create schedule failed parsing request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	// create schedule
	response, err := pipeline.CreateSchedule(&ctx, &createScheduleInfo)
	if err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"create schedule failed. createScheduleInfo:%v error:%s", createScheduleInfo, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusCreated, response)
}

func (sr *ScheduleRouter) listSchedule(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	userNames := r.URL.Query().Get(util.QueryKeyUserFilter)
	pplIDs, pplDetailIDs := r.URL.Query().Get(util.QueryKeyPplFilter), r.URL.Query().Get(util.QueryKeyPplDetailFilter)
	scheduleIDs, names := r.URL.Query().Get(util.QueryKeyScheduleFilter), r.URL.Query().Get(util.QueryKeyNameFilter)
	statuses := r.URL.Query().Get(util.QueryKeyStatusFilter)
	userFilter, pplFilter, pplDetailFilter, scheduleFilter, nameFilter, statusFilter := make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0)
	if userNames != "" {
		userFilter = util.SplitFilter(userNames, common.SeparatorComma, true)
	}
	if pplIDs != "" {
		pplFilter = util.SplitFilter(pplIDs, common.SeparatorComma, true)
	}
	if pplDetailIDs != "" {
		pplDetailFilter = util.SplitFilter(pplDetailIDs, common.SeparatorComma, true)
	}
	if scheduleIDs != "" {
		scheduleFilter = util.SplitFilter(scheduleIDs, common.SeparatorComma, true)
	}
	if names != "" {
		nameFilter = util.SplitFilter(names, common.SeparatorComma, true)
	}
	if statuses != "" {
		statusFilter = util.SplitFilter(statuses, common.SeparatorComma, true)
	}
	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] ListSchedule marker:[%s] maxKeys:[%d] pipelineID:%v pplDetailFilter:%v userFilter:%v scheduleFilter:%v nameFilter:%v statusFilter:%v",
		ctx.UserName, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	listScheduleResponse, err := pipeline.ListSchedule(&ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, listScheduleResponse)
}

func (sr *ScheduleRouter) getSchedule(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	// 获取schedule信息
	scheduleID := chi.URLParam(r, util.ParamKeyScheduleID)

	// 获取schedule发起的run信息
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	runIDs, RunStatuses := r.URL.Query().Get(util.QueryKeyRunFilter), r.URL.Query().Get(util.QueryKeyStatusFilter)
	runFilter, RunStatusFilter := make([]string, 0), make([]string, 0)
	if RunStatuses != "" {
		RunStatusFilter = util.SplitFilter(RunStatuses, common.SeparatorComma, true)
	}
	if runIDs != "" {
		runFilter = util.SplitFilter(runIDs, common.SeparatorComma, true)
	}

	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] get schedule[%s] with marker:[%s] maxKeys:[%d] statusFilter:%v runFilter:%v",
		ctx.UserName, scheduleID, marker, maxKeys, RunStatusFilter, runFilter)
	getScheduleResponse, err := pipeline.GetSchedule(&ctx, scheduleID, marker, maxKeys, runFilter, RunStatusFilter)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.Render(w, http.StatusOK, getScheduleResponse)
}

func (sr *ScheduleRouter) stopSchedule(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	scheduleID := chi.URLParam(r, util.ParamKeyScheduleID)
	logger.LoggerForRequest(&ctx).Debugf("stop schedule id:%v", scheduleID)

	err := pipeline.StopSchedule(&ctx, scheduleID)
	if err != nil {
		ctx.Logging().Errorf("stop schedule: %s failed. error:%s", scheduleID, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

func (sr *ScheduleRouter) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	scheduleID := chi.URLParam(r, util.ParamKeyScheduleID)
	logger.LoggerForRequest(&ctx).Debugf("delete schedule id:%v", scheduleID)

	err := pipeline.DeleteSchedule(&ctx, scheduleID)
	if err != nil {
		ctx.Logging().Errorf("delete schedule: %s failed. error:%s", scheduleID, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}
