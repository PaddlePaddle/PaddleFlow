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
	"strings"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/run"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

type TrackRouter struct{}

func (tr *TrackRouter) Name() string {
	return "TrackRouter"
}

func (tr *TrackRouter) AddRouter(r chi.Router) {
	log.Info("add run track router")
	r.Get("/runCache/{runCacheID}", tr.getRunCache)
	r.Get("/runCache", tr.listRunCache)
	r.Delete("/runCache/{runCacheID}", tr.deleteRunCache)
	r.Get("/artifact", tr.listArtifactEvent)
	r.Delete("/artifact", tr.deleteArtifactEvent)
}

// getRunCache
// @Summary 获取运行缓存
// @Description 获取运行缓存
// @Id getRunCache
// @tags RunCache
// @Accept  json
// @Produce json
// @Param runID path int true "运行缓存ID"
// @Success 200 {object} models.RunCache "运行缓存详情"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /runCache/{runCacheID} [GET]
func (tr *TrackRouter) getRunCache(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	id := chi.URLParam(r, util.ParamKeyRunCacheID)
	cacheInfo, err := run.GetRunCache(&ctx, id)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, cacheInfo)
}

// listRunCache
// @Summary 获取运行缓存列表
// @Description 获取运行缓存列表
// @Id listRunCache
// @tags RunCache
// @Accept  json
// @Produce json
// @Param userFilter query string false "(root用户)用户过滤"
// @Param fsFilter query string false "存储过滤"
// @Param runFilter query string false "运行过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} run.ListRunCacheResponse "获取运行缓存列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /runCache [GET]
func (tr *TrackRouter) listRunCache(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	maxKeys := util.DefaultMaxKeys
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	limitCustom := r.URL.Query().Get(util.QueryKeyMaxKeys)
	if limitCustom != "" {
		var err error
		maxKeys, err = strconv.Atoi(limitCustom)
		if err != nil || maxKeys <= 0 || maxKeys > util.ListPageMax {
			err := fmt.Errorf("invalid query pageLimit[%s]. should be an integer between 1~1000",
				limitCustom)
			ctx.ErrorCode = common.InvalidURI
			common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
			return
		}
	}
	userNames, fsNames, runIDs := r.URL.Query().Get(util.QueryKeyUserFilter), r.URL.Query().Get(util.QueryKeyFsFilter), r.URL.Query().Get(util.QueryKeyRunFilter)
	userFilter, fsFilter, runFilter := make([]string, 0), make([]string, 0), make([]string, 0)
	if userNames != "" {
		userFilter = strings.Split(userNames, common.SeparatorComma)
	}
	if fsNames != "" {
		fsFilter = strings.Split(fsNames, common.SeparatorComma)
	}
	if runIDs != "" {
		runFilter = strings.Split(runIDs, common.SeparatorComma)
	}
	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] ListRunCache marker:[%s] maxKeys:[%d] userFilter:%v fsFilter:%v runFilter:%v",
		ctx.UserName, marker, maxKeys, userFilter, fsFilter, runFilter)
	listRunCacheResponse, err := run.ListRunCache(&ctx, marker, maxKeys, userFilter, fsFilter, runFilter)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, listRunCacheResponse)
}

// DeleteRunCache
// @Summary 删除运行缓存
// @Description 删除运行缓存
// @Id DeleteRunCache
// @tags RunCache
// @Accept  json
// @Produce json
// @Param pipelineID path string true "运行缓存ID"
// @Success 200 {string} string "删除运行的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /runCache/{runCacheID} [DELETE]
func (tr *TrackRouter) deleteRunCache(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	id := chi.URLParam(r, util.ParamKeyRunCacheID)
	err := run.DeleteRunCache(&ctx, id)
	if err != nil {
		ctx.Logging().Errorf("delete run_cache: %s failed. error:%s", id, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// listArtifactEvent
// @Summary 获取运行产物列表
// @Description 获取运行产物列表
// @Id listArtifactEvent
// @tags ArtifactEvent
// @Accept  json
// @Produce json
// @Param userFilter query string false "(root用户)用户过滤"
// @Param fsFilter query string false "存储过滤"
// @Param runFilter query string false "运行过滤"
// @Param typeFilter query string false "类型过滤"
// @Param pathFilter query string false "路径过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} run.ListArtifactEventResponse "获取运行产物列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /run [GET]
func (tr *TrackRouter) listArtifactEvent(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	maxKeys := util.DefaultMaxKeys
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	limitCustom := r.URL.Query().Get(util.QueryKeyMaxKeys)
	if limitCustom != "" {
		var err error
		maxKeys, err = strconv.Atoi(limitCustom)
		if err != nil || maxKeys <= 0 || maxKeys > util.ListPageMax {
			err := fmt.Errorf("invalid query pageLimit[%s]. should be an integer between 1~1000",
				limitCustom)
			ctx.ErrorCode = common.InvalidURI
			common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
			return
		}
	}
	userNames, fsNames, runIDs := r.URL.Query().Get(util.QueryKeyUserFilter), r.URL.Query().Get(util.QueryKeyFsFilter), r.URL.Query().Get(util.QueryKeyRunFilter)
	types, paths := r.URL.Query().Get(util.QueryKeyTypeFilter), r.URL.Query().Get(util.QueryKeyPathFilter)
	userFilter, fsFilter, runFilter, typeFilter, pathFilter := make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0), make([]string, 0)
	if userNames != "" {
		userFilter = strings.Split(userNames, common.SeparatorComma)
	}
	if fsNames != "" {
		fsFilter = strings.Split(fsNames, common.SeparatorComma)
	}
	if runIDs != "" {
		runFilter = strings.Split(runIDs, common.SeparatorComma)
	}
	if types != "" {
		typeFilter = strings.Split(types, common.SeparatorComma)
	}
	if paths != "" {
		pathFilter = strings.Split(paths, common.SeparatorComma)
	}
	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] ListArtifactEvent marker:[%s] maxKeys:[%d] userFilter:%v, fsFilter:%v, runFilter:%v, typeFilter:%v, pathFilter:%v",
		ctx.UserName, marker, maxKeys, userFilter, fsFilter, runFilter, typeFilter, pathFilter)
	listArtifactEventResponse, err := run.ListArtifactEvent(&ctx, marker, maxKeys, userFilter, fsFilter, runFilter, typeFilter, pathFilter)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, listArtifactEventResponse)
}

// DeleteArtifactEvent
// @Summary 删除运行产物
// @Description 删除运行产物
// @Id DeleteArtifactEvent
// @tags ArtifactEvent
// @Accept  json
// @Produce json
// @Param user query string false "(root用户)用户"
// @Param fsname query string true "存储名称"
// @Param runID query string true "运行ID"
// @Param path query string true "路径"
// @Success 200 {string} string "删除运行的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /artifact [DELETE]
func (tr *TrackRouter) deleteArtifactEvent(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	username, fsname, runID, artifactPath := r.URL.Query().Get(util.QueryKeyUserName), r.URL.Query().Get(util.QueryFsname), r.URL.Query().Get(util.ParamKeyRunID), r.URL.Query().Get(util.QueryPath)
	// check query
	if fsname == "" || runID == "" || artifactPath == "" {
		err := fmt.Errorf("runID, fsID, path - shall not be empty")
		ctx.ErrorCode = common.InvalidURI
		ctx.Logging().Errorf("delete artifact_event failed. error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	// permission
	if !common.IsRootUser(ctx.UserName) { // non-root user can only delete self's
		username = ctx.UserName
	} else {
		if username == "" { // root user use self's if not specified
			username = ctx.UserName
		}
	}
	// service
	err := run.DeleteArtifactEvent(&ctx, username, fsname, runID, artifactPath)
	if err != nil {
		ctx.Logging().Errorf("delete artifact_event failed. error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}
