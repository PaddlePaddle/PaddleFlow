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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/grant"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
)

type GrantRouter struct {
}

func (gr *GrantRouter) Name() string {
	return "GrantRouter"
}

func (gr *GrantRouter) AddRouter(r chi.Router) {
	log.Info("add grant router")
	r.Post("/grant", gr.createGrant)
	r.Delete("/grant", gr.deleteGrant)
	r.Get("/grant", gr.listGrant)
}

// createGrant
// @Summary 创建授权
// @Description 创建授权
// @Id createGrant
// @tags Grant
// @Accept  json
// @Produce json
// @Param request body grant.CreateGrantRequest true "创建授权请求"
// @Success 200 {object} grant.CreateGrantResponse "创建队列响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /grant [POST]
func (gr *GrantRouter) createGrant(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var grantInfo grant.CreateGrantRequest
	err := common.BindJSON(r, &grantInfo)
	if err != nil {
		ctx.Logging().Errorf("createGrant bindjson failed. error:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	response, err := grant.CreateGrant(&ctx, grantInfo)
	if err != nil {
		ctx.Logging().Errorf(
			"create grant failed. grantInfo:%v error:%s", grantInfo, err.Error())
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	common.Render(w, http.StatusOK, response)
}

// deleteGrant
// @Summary 删除授权
// @Description 删除授权
// @Id deleteGrant
// @tags Grant
// @Accept  json
// @Produce json
// @Param username query string true "用户名称"
// @Param resourceType query string true "资源类型"
// @Param resourceID query string true "资源ID/资源名称"
// @Success 200 {string} string "成功删除授权的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /grant [DELETE]
func (gr *GrantRouter) deleteGrant(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	userName := r.URL.Query().Get(util.QueryKeyUserName)
	resourceType := r.URL.Query().Get(util.QueryResourceType)
	resourceID := r.URL.Query().Get(util.QueryResourceID)

	if err := grant.DeleteGrant(&ctx, userName, resourceID, resourceType); err != nil {
		ctx.Logging().Errorf(
			"delete grant failed. userName:%s, resourceID:%s error:%s", userName, resourceID, err.Error())
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// listGrant
// @Summary 获取队列列表
// @Description 获取队列列表
// @Id listQueue
// @tags Queue
// @Accept  json
// @Produce json
// @Param username query string false "用户名称过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} grant.ListGrantResponse "获取授权列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /grant [GET]
func (gr *GrantRouter) listGrant(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	userName := r.URL.Query().Get(util.QueryKeyUserName)
	ctx.Logging().Debugf(
		"ListGrant marker:[%s] maxKeys:[%d] userName:[%s]",
		marker, maxKeys, userName)
	response, err := grant.ListGrant(&ctx, marker, maxKeys, userName)
	if err != nil {
		ctx.Logging().Errorf("list grants failed. error:%s.", err.Error())
		common.RenderErr(w, ctx.RequestID, ctx.ErrorCode)
		return
	}
	common.Render(w, http.StatusOK, response)
}
