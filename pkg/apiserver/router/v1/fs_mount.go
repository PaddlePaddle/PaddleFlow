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
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	api "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

// createFsMount handles requests of creating filesystem mount record
// @Summary createFsMount
// @Description 创建mount记录
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body fs.CreateMountRequest true "request body"
// @Success 201 {string} string Created
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsMount [post]
func (pr *PFSRouter) createFsMount(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var req api.CreateMountRequest
	err := common.BindJSON(r, &req)
	if err != nil {
		ctx.Logging().Errorf("createFSMount bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	if req.Username == "" {
		req.Username = ctx.UserName
	}

	ctx.Logging().Debugf("create file system cache with req[%v]", req)

	err = validateCreateMount(&ctx, &req)
	if err != nil {
		ctx.Logging().Errorf("validateCreateMount failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = api.CreateMount(&ctx, req)
	if err != nil {
		ctx.Logging().Errorf("create mount with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusCreated)
}

func validateCreateMount(ctx *logger.RequestContext, req *api.CreateMountRequest) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		for _, err = range err.(validator.ValidationErrors) {
			ctx.ErrorCode = common.InappropriateJSON
			return err
		}
	}
	return nil
}

// listFsMount the function that handle the list mount by fsID and nodename
// @Summary listFsMount
// @Description 获取mount列表
// @tag fs
// @Accept   json
// @Produce  json
// @Param fsName query string true "存储名称"
// @Param username query string false "用户名"
// @Param clusterID query string true "集群ID"
// @Param nodename query string true "结点名称"
// @Param marker query string false "查询起始条目加密条码"
// @Param maxKeys query string false "每页条数"
// @Success 200 {object} fs.ListFileSystemResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsMount [get]
func (pr *PFSRouter) listFsMount(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	req := api.ListMountRequest{
		FsName:    r.URL.Query().Get(util.QueryFsName),
		Username:  r.URL.Query().Get(util.QueryKeyUserName),
		Marker:    r.URL.Query().Get(util.QueryKeyMarker),
		MaxKeys:   int32(maxKeys),
		ClusterID: r.URL.Query().Get(util.QueryClusterID),
		NodeName:  r.URL.Query().Get(util.QueryNodeName),
	}
	log.Debugf("list file mount with req[%v]", req)
	if req.Username == "" {
		req.Username = ctx.UserName
	}

	err = validateListMount(&ctx, &req)
	if err != nil {
		ctx.Logging().Errorf("validateListMount failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	listMounts, nextMarker, err := api.ListMount(&ctx, req)
	if err != nil {
		ctx.Logging().Errorf("list mount with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	response := *getListMountResult(listMounts, nextMarker, req.Marker)
	ctx.Logging().Debugf("List mount:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateListMount(ctx *logger.RequestContext, req *api.ListMountRequest) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		for _, err = range err.(validator.ValidationErrors) {
			ctx.ErrorCode = common.InappropriateJSON
			return err
		}
	}
	return nil
}

func getListMountResult(fsMounts []models.FsMount, nextMarker, marker string) *api.ListMountResponse {
	var fsMountLists []*api.MountResponse
	for _, fsMount := range fsMounts {
		FsList := &api.MountResponse{
			MountID:    fsMount.MountID,
			FsID:       fsMount.FsID,
			MountPoint: fsMount.MountPoint,
			NodeName:   fsMount.NodeName,
			ClusterID:  fsMount.NodeName,
		}
		fsMountLists = append(fsMountLists, FsList)
	}
	ListFsResponse := &api.ListMountResponse{
		Marker:    marker,
		MountList: fsMountLists,
		Truncated: false,
	}
	if nextMarker != "" {
		ListFsResponse.Truncated = true
		ListFsResponse.NextMarker = nextMarker
	}
	return ListFsResponse
}

// deleteFsMount handles requests of deleting filesystem mount record
// @Summary deleteFsMount
// @Description 删除mount记录
// @tag fs
// @Accept   json
// @Produce  json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Param mountpoint query string true "挂载点"
// @Param clusterID query string true "集群ID"
// @Param nodename query string true "结点名称"
// @Success 200
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsMount/{fsName} [delete]
func (pr *PFSRouter) deleteFsMount(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	fsName := chi.URLParam(r, util.QueryFsName)
	req := api.DeleteMountRequest{
		Username:   r.URL.Query().Get(util.QueryKeyUserName),
		FsName:     fsName,
		MountPoint: r.URL.Query().Get(util.QueryMountPoint),
		ClusterID:  r.URL.Query().Get(util.QueryClusterID),
		NodeName:   r.URL.Query().Get(util.QueryNodeName),
	}

	if req.Username == "" {
		req.Username = ctx.UserName
	}

	ctx.Logging().Debugf("delete fs mount with req[%v]", req)

	err := validateDeleteMount(&ctx, &req)
	if err != nil {
		ctx.Logging().Errorf("validateDeleteMount failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = api.DeleteMount(&ctx, req)
	if err != nil {
		ctx.Logging().Errorf("delete fs mount with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusOK)
}

func validateDeleteMount(ctx *logger.RequestContext, req *api.DeleteMountRequest) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		for _, err = range err.(validator.ValidationErrors) {
			ctx.ErrorCode = common.InappropriateJSON
			return err
		}
	}
	return nil
}
