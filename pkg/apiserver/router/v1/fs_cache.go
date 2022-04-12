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

	"github.com/go-chi/chi"
	"gorm.io/gorm"

	"paddleflow/pkg/apiserver/common"
	fsCtrl "paddleflow/pkg/apiserver/controller/fs"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/logger"
)

// createFSCacheConfig handles requests of creating filesystem cache config
// @Summary createFSCacheConfig
// @Description
// @tag fs
// @Accept   json
// @Produce  json
// @Param username query string false "用户名"
// @Param request body fs.CreateOrUpdateFSCacheRequest true "request body"
// @Success 201 {string} string Created
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fs/cache [post]
func (pr *PFSRouter) createFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var createRequest fsCtrl.CreateOrUpdateFSCacheRequest
	err := common.BindJSON(r, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("CreateFSCacheConfig bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}

	username := r.URL.Query().Get(util.QueryKeyUserName)
	fsID, err := getFsIDAndCheckPermission(&ctx, username, createRequest.FsName)
	if err != nil {
		ctx.Logging().Errorf("getFSCacheConfig check fs permission failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	createRequest.ID = fsID
	ctx.Logging().Debugf("create file system cache with req[%v]", createRequest)

	err = validateCreateFSCacheConfig(&ctx, &createRequest)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = fsCtrl.CreateFileSystemCacheConfig(&ctx, createRequest)
	if err != nil {
		ctx.Logging().Errorf("create file system cache with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusCreated)
}

func validateCreateFSCacheConfig(ctx *logger.RequestContext, req *fsCtrl.CreateOrUpdateFSCacheRequest) error {
	// fs exists?
	_, err := models.GetFileSystemWithFsID(req.ID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.FileSystemNotExist
			ctx.Logging().Errorf("validateCreateFileSystemCache fsID[%s] not exist", req.ID)
		} else {
			ctx.Logging().Errorf("validateCreateFileSystemCache fsID[%s] err:%v", req.ID, err)
		}
		return err
	}
	// fs_cache_config already exists?
	_, err = models.GetFSCacheConfig(ctx.Logging(), req.ID)
	if err == nil {
		ctx.ErrorCode = common.InvalidArguments
		err := fmt.Errorf("fs[%s] cache config already exits", req.ID)
		ctx.Logging().Errorf("validateCreateFileSystemCache err:%v", err)
		return err
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("validateCreateFileSystemCache get fsID[%s] cache config err:%v", req.ID, err)
		return err
	}

	return nil
}

// getFSCacheConfig
// @Summary 通过FsID获取缓存配置
// @Description  通过FsID获取缓存配置
// @Id getFSCacheConfig
// @tags FSCacheConfig
// @Accept  json
// @Produce json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Success 200 {object} models.FSCacheConfig "缓存配置结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /fs/cache/{fsName} [GET]
func (pr *PFSRouter) getFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)
	ctx := common.GetRequestContext(r)

	fsID, err := getFsIDAndCheckPermission(&ctx, username, fsName)
	if err != nil {
		ctx.Logging().Errorf("getFSCacheConfig check fs permission failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	fsCacheConfig, err := fsCtrl.GetFileSystemCacheConfig(&ctx, fsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RecordNotFound
		} else {
			ctx.ErrorCode = common.InternalError
		}
		logger.LoggerForRequest(&ctx).Errorf("GetFSCacheConfig[%s] failed. error:%v", fsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, fsCacheConfig)
}

// updateFSCacheConfig
// @Summary 更新FsID的缓存配置
// @Description  更新FsID的缓存配置
// @Id updateFSCacheConfig
// @tags FSCacheConfig
// @Accept  json
// @Produce json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Success 200 {object} models.FSCacheConfig "缓存配置结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /fs/cache/{fsName} [PUT]
func (pr *PFSRouter) updateFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)
	ctx := common.GetRequestContext(r)

	fsID, err := getFsIDAndCheckPermission(&ctx, username, fsName)
	if err != nil {
		ctx.Logging().Errorf("getFSCacheConfig check fs permission failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	var req fsCtrl.CreateOrUpdateFSCacheRequest
	err = common.BindJSON(r, &req)
	if err != nil {
		ctx.Logging().Errorf("UpdateFSCacheConfig[%s] bindjson failed. err:%s", fsID, err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}

	req.ID = fsID
	err = fsCtrl.UpdateFileSystemCacheConfig(&ctx, req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.RenderErr(w, ctx.RequestID, common.RecordNotFound)
		} else {
			common.RenderErr(w, ctx.RequestID, common.InternalError)
		}
		logger.LoggerForRequest(&ctx).Errorf(
			"GetFSCacheConfig[%s] failed. error:%v", fsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}
