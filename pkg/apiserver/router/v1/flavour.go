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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/flavour"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

// FlavourRouter is flavour api router
type FlavourRouter struct{}

// Name indicate name of flavour router
func (fr *FlavourRouter) Name() string {
	return "FlavourRouter"
}

// AddRouter add flavour router to root router
func (fr *FlavourRouter) AddRouter(r chi.Router) {
	log.Info("add flavour router")
	r.Get("/flavour", fr.listFlavour)
	r.Get("/flavour/{flavourName}", fr.getFlavour)
	r.Put("/flavour/{flavourName}", fr.updateFlavour)
	r.Post("/flavour", fr.createFlavour)
	r.Delete("/flavour/{flavourName}", fr.deleteFlavour)
}

// listFlavour
// @Summary 获取套餐列表
// @Description 获取套餐列表
// @Id listFlavour
// @tags User
// @Accept  json
// @Produce json
// @Success 200 {object} []schema.Flavour "获取套餐列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /flavour [GET]
func (fr *FlavourRouter) listFlavour(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	clusterName := r.URL.Query().Get(util.ParamKeyClusterName)
	queryKey := r.URL.Query().Get(util.QueryKeyName)

	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	logger.LoggerForRequest(&ctx).Debugf(
		"user[%s] ListRun marker:[%s] maxKeys:[%d] ", ctx.UserName, marker, maxKeys)
	listRunResponse, err := flavour.ListFlavour(maxKeys, marker, clusterName, queryKey)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, listRunResponse)

}

// getFlavour
// @Summary 获取套餐详情
// @Description 获取套餐详情
// @Id getFlavour
// @tags User
// @Accept  json
// @Produce json
// @Param flavourName path string true "套餐名称"
// @Success 200 {object} models.Flavour "获取套餐详情的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /flavour [GET]
func (fr *FlavourRouter) getFlavour(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	flavourName := strings.TrimSpace(chi.URLParam(r, util.ParamFlavourName))
	if flavourName == "" {
		ctx.ErrorCode = common.FlavourNameEmpty
		ctx.Logging().Error("flavour name should not be empty")
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, "")
		return
	}
	flavour, err := flavour.GetFlavour(flavourName)
	if err != nil {
		ctx.ErrorCode = common.FlavourNotFound
		ctx.Logging().Errorf("get flavour %s failed, err=%v", flavourName, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.Render(w, http.StatusOK, flavour)
}

// updateFlavour
// @Summary 修改套餐
// @Description 修改套餐
// @Id updateFlavour
// @tags User
// @Accept  json
// @Produce json
// @Param flavourName path string true "套餐名称"
// @Success 200 {object} flavour.UpdateFlavourResponse "修改套餐的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /flavour [PUT]
func (fr *FlavourRouter) updateFlavour(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var request flavour.UpdateFlavourRequest
	if err := common.BindJSON(r, &request); err != nil {
		ctx.ErrorCode = common.MalformedJSON
		logger.LoggerForRequest(&ctx).Errorf("parsing update flavour RequestBody failed:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	request.Name = strings.TrimSpace(chi.URLParam(r, util.ParamFlavourName))

	if err := validateUpdateFlavour(&ctx, &request); err != nil {
		ctx.Logging().Errorf("validate flavour request failed. request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response, err := flavour.UpdateFlavour(&ctx, &request)
	if err != nil {
		ctx.Logging().Errorf("update flavour failed. flavour request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("update flavour:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateUpdateFlavour(ctx *logger.RequestContext, request *flavour.UpdateFlavourRequest) error {
	if request.Name == "" {
		ctx.ErrorCode = common.FlavourNameEmpty
		msg := "flavour name should not be empty"
		ctx.Logging().Error(msg)
		return fmt.Errorf(msg)
	}
	if request.CPU != "" {
		if err := schema.ValidateResourceItem(request.CPU); err != nil {
			ctx.Logging().Errorf("create flavour failed. error: %v", err)
			ctx.ErrorCode = common.FlavourInvalidField
			return err
		}
	}
	if request.Mem != "" {
		if err := schema.ValidateResourceItem(request.Mem); err != nil {
			ctx.Logging().Errorf("create flavour failed. error: %s", err)
			ctx.ErrorCode = common.FlavourInvalidField
			return err
		}
	}

	if request.ScalarResources != nil {
		err := schema.ValidateScalarResourceInfo(request.ScalarResources, []string{})
		if err != nil {
			ctx.Logging().Errorf("create flavour failed. error: %v", err)
			ctx.ErrorCode = common.FlavourInvalidField
			return err
		}
	}

	return nil
}

// createFlavour
// @Summary 获取套餐列表
// @Description 获取套餐列表
// @Id createFlavour
// @tags User
// @Accept  json
// @Produce json
// @Success 200 {object} flavour.CreateFlavourResponse "获取套餐列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /flavour [POST]
func (fr *FlavourRouter) createFlavour(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var request flavour.CreateFlavourRequest
	if err := common.BindJSON(r, &request); err != nil {
		ctx.ErrorCode = common.MalformedJSON
		logger.LoggerForRequest(&ctx).Errorf("parsing request body failed:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	if err := validateCreateFlavour(&ctx, &request); err != nil {
		ctx.Logging().Errorf("validate flavour request failed. request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	f, err := flavour.GetFlavour(request.Name)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		ctx.Logging().Errorf("get flavour failed. flavour request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	} else if f.Name != "" {
		ctx.ErrorCode = common.DuplicatedName
		ctx.Logging().Infof("flavour %s has exist.", request.Name)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, "")
		return
	}

	response, err := flavour.CreateFlavour(&request)
	if err != nil {
		ctx.Logging().Errorf("create flavour failed. flavour request:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("CreateFlavour flavour:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateCreateFlavour(ctx *logger.RequestContext, request *flavour.CreateFlavourRequest) error {
	if request.Name == "" {
		ctx.ErrorCode = common.FlavourNotFound
		return errors.New("field not be empty")
	}
	if request.ClusterName != "" {
		clusterInfo, err := storage.Cluster.GetClusterByName(request.ClusterName)
		if err != nil {
			ctx.ErrorCode = common.ClusterNameNotFound
			return err
		}
		request.ClusterID = clusterInfo.ID
	}
	if request.ScalarResources == nil {
		request.ScalarResources = make(schema.ScalarResourcesType)
	}
	resourceInfo := schema.ResourceInfo{
		CPU:             request.CPU,
		Mem:             request.Mem,
		ScalarResources: request.ScalarResources,
	}
	if err := schema.ValidateResource(resourceInfo, []string{}); err != nil {
		ctx.Logging().Errorf("create flavour failed. error: %s", err.Error())
		ctx.ErrorCode = common.FlavourInvalidField
		return err
	}
	return nil
}

// deleteFlavour
// @Summary 删除套餐
// @Description 删除套餐
// @Id deleteFlavour
// @tags User
// @Accept  json
// @Produce json
// @Param flavourName path string true "套餐名称"
// @Success 200 {} "删除套餐的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /flavour [DELETE]
func (fr *FlavourRouter) deleteFlavour(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	flavourName := strings.TrimSpace(chi.URLParam(r, util.ParamFlavourName))
	if flavourName == "" {
		ctx.ErrorCode = common.FlavourNameEmpty
		ctx.Logging().Error("flavour name should not be empty")
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, "")
		return
	}

	if err := flavour.DeleteFlavour(&ctx, flavourName); err != nil {
		ctx.Logging().Errorf("delete flavour %s failed, error:%s", flavourName, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("delete flavour %s success", flavourName)
	common.RenderStatus(w, http.StatusOK)
}
