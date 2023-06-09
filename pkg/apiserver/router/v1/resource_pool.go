/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/resourcepool"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
)

type ResourcePoolRouter struct{}

func (pr *ResourcePoolRouter) Name() string {
	return "ResourcePoolRouter"
}

func (pr *ResourcePoolRouter) AddRouter(r chi.Router) {
	log.Info("add resource pool router")
	r.Post("/resourcepool", pr.create)
	r.Put("/resourcepool/{name}", pr.update)
	r.Get("/resourcepool", pr.list)
	r.Get("/resourcepool/{name}", pr.get)
	r.Delete("/resourcepool/{name}", pr.delete)
}

// create
// @Summary 创建资源池
// @Description 创建资源池
// @Id create
// @tags ResourcePool
// @Accept  json
// @Produce json
// @Param request body models.ResourcePool true "创建资源池请求"
// @Success 200 {object} resourcepool.CreateRPResponse "创建资源池响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /resourcepool [POST]
func (pr *ResourcePoolRouter) create(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var request resourcepool.CreateRequest
	err := common.BindJSON(r, &request)
	if err != nil {
		log.Errorf("Create resource pool bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	log.Debugf("Create resource pool info:%s", config.PrettyFormat(request))
	response, err := resourcepool.Create(&ctx, &request)
	if err != nil {
		ctx.Logging().Errorf("create resource pool failed. info:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("Create resource pool:%v successfully", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

// update
// @Summary 修改资源池
// @Description 修改资源池
// @Id update
// @tags ResourcePool
// @Accept  json
// @Produce json
// @Param name path string true "资源池名称"
// @Success 200 {string} string "成功修改的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /resourcepool/{name} [PUT]
func (pr *ResourcePoolRouter) update(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var rpInfo resourcepool.UpdateRequest
	err := common.BindJSON(r, &rpInfo)
	if err != nil {
		log.Errorf("update ResourcePool bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	// name is URL parameter, and must be not empty
	rpInfo.Name = chi.URLParam(r, util.ParamKeyName)
	log.Debugf("update ResourcePool: %s", config.PrettyFormat(rpInfo))
	response, err := resourcepool.Update(&ctx, &rpInfo)
	if err != nil {
		ctx.Logging().Errorf("update ResourcePool failed. info:%v error:%s", rpInfo, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("update ResourcePool finised:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

// list
// @Summary 获取资源池列表
// @Description 获取资源池列表
// @Id list
// @tags ResourcePool
// @Accept  json
// @Produce json
// @Param name query string false "资源池名称过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} queue.ListQueueResponse "获取资源池列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /resourcepool [GET]
func (pr *ResourcePoolRouter) list(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	name := r.URL.Query().Get(util.QueryKeyName)
	ctx.Logging().Debugf("List resource pool marker:[%s] maxKeys:[%d] name:[%s]", marker, maxKeys, name)
	request := resourcepool.ListRequest{
		Marker:  marker,
		MaxKeys: maxKeys,
		Name:    name,
	}
	listRPResponse, err := resourcepool.List(&ctx, request)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	ctx.Logging().Debugf("List resource pool:%v", string(config.PrettyFormat(listRPResponse)))
	common.Render(w, http.StatusOK, listRPResponse)
}

// get
// @Summary 通过名称获取资源池详情
// @Description  通过名称获取资源池详情
// @Id get
// @tags ResourcePool
// @Accept  json
// @Produce json
// @Param name path string true "资源池名称"
// @Success 200 {object} resourcepool.RPResponse "资源池结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /resourcepool/{name} [GET]
func (pr *ResourcePoolRouter) get(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	rpName := chi.URLParam(r, util.ParamKeyName)
	rpData, err := resourcepool.GetByName(&ctx, rpName)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	ctx.Logging().Debugf("Get resource pool:%v", string(config.PrettyFormat(rpData)))
	common.Render(w, http.StatusOK, rpData)
}

// deleteQueue
// @Summary 删除资源池
// @Description 删除资源池
// @Id delete
// @tags ResourcePool
// @Accept  json
// @Produce json
// @Param name path string true "资源池名称"
// @Success 200 {string} string "成功删除的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /resourcepool/{name} [DELETE]
func (pr *ResourcePoolRouter) delete(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	rpName := chi.URLParam(r, util.ParamKeyName)
	err := resourcepool.Delete(&ctx, rpName)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}
