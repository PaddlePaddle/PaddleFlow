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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
)

type QueueRouter struct{}

func (qr *QueueRouter) Name() string {
	return "QueueRouter"
}

func (qr *QueueRouter) AddRouter(r chi.Router) {
	log.Info("add queue router")
	r.Post("/queue", qr.createQueue)
	r.Get("/queue", qr.listQueue)
	r.Get("/queue/{queueName}", qr.getQueueByName)
	r.Put("/queue/{queueName}", qr.updateQueue)
	r.Delete("/queue/{queueName}", qr.deleteQueue)
}

// createQueue
// @Summary 创建队列
// @Description 创建队列
// @Id createQueue
// @tags Queue
// @Accept  json
// @Produce json
// @Param request body models.Queue true "创建队列请求"
// @Success 200 {object} queue.CreateQueueResponse "创建队列响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /queue [POST]
func (qr *QueueRouter) createQueue(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var queueInfo queue.CreateQueueRequest
	err := common.BindJSON(r, &queueInfo)
	if err != nil {
		log.Errorf("CreateQueue bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	log.Debugf("CreateQueue QueueInfo:%s", config.PrettyFormat(queueInfo))
	response, err := queue.CreateQueue(&ctx, &queueInfo)
	if err != nil {
		ctx.Logging().Errorf(
			"create queue failed. queueInfo:%v error:%s", queueInfo, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("CreateQueue queue:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

// listQueue
// @Summary 获取队列列表
// @Description 获取队列列表
// @Id listQueue
// @tags Queue
// @Accept  json
// @Produce json
// @Param name query string false "队列名称过滤"
// @Param maxKeys query int false "每页包含的最大数量，缺省值为50"
// @Param marker query string false "批量获取列表的查询的起始位置，是一个由系统生成的字符串"
// @Success 200 {object} queue.ListQueueResponse "获取队列列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /queue [GET]
func (qr *QueueRouter) listQueue(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	name := r.URL.Query().Get(util.QueryKeyName)
	ctx.Logging().Debugf(
		"ListQueue marker:[%s] maxKeys:[%d] name:[%s]",
		marker, maxKeys, name)
	listQueueResponse, err := queue.ListQueue(&ctx, marker, maxKeys, name)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	ctx.Logging().Debugf("ListQueue queue:%v", string(config.PrettyFormat(listQueueResponse)))
	common.Render(w, http.StatusOK, listQueueResponse)
}

// getQueueByName
// @Summary 通过队列名称获取队列详情
// @Description  通过队列名称获取队列详情
// @Id getQueueByName
// @tags Queue
// @Accept  json
// @Produce json
// @Param queueName path string true "队列名称"
// @Success 200 {object} queue.GetQueueResponse "队列结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /queue/{queueName} [GET]
func (qr *QueueRouter) getQueueByName(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	queueName := chi.URLParam(r, util.ParamKeyQueueName)
	queueData, err := queue.GetQueueByName(&ctx, queueName)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	ctx.Logging().Debugf("GetQueueByName queue:%v", string(config.PrettyFormat(queueData)))
	common.Render(w, http.StatusOK, queueData)
}

// updateQueue
// @Summary 修改队列
// @Description 修改队列
// @Id updateQueue
// @tags Queue
// @Accept  json
// @Produce json
// @Param queueName path string true "队列名称"
// @Success 200 {string} string "成功修改队列的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /queue/{queueName} [PUT]
func (qr *QueueRouter) updateQueue(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var queueInfo queue.UpdateQueueRequest
	err := common.BindJSON(r, &queueInfo)
	if err != nil {
		log.Errorf("updateQueue bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	queueInfo.Name = chi.URLParam(r, util.ParamKeyQueueName)

	log.Debugf("updateQueue QueueInfo:%s", config.PrettyFormat(queueInfo))
	response, err := queue.UpdateQueue(&ctx, &queueInfo)
	if err != nil {
		ctx.Logging().Errorf("update queue failed. queueInfo:%v error:%s", queueInfo, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("update queue finised:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

// deleteQueue
// @Summary 删除队列
// @Description 删除队列
// @Id deleteQueue
// @tags Queue
// @Accept  json
// @Produce json
// @Param queueName path string true "队列名称"
// @Success 200 {string} string "成功删除队列的响应码"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /queue/{queueName} [DELETE]
func (qr *QueueRouter) deleteQueue(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	queueName := chi.URLParam(r, util.ParamKeyQueueName)
	err := queue.DeleteQueue(&ctx, queueName)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	common.RenderStatus(w, http.StatusOK)
}
