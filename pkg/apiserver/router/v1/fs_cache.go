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
	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"paddleflow/pkg/apiserver/common"
	api "paddleflow/pkg/apiserver/controller/fs"
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
// @Param request body fs.CreateFileSystemCacheRequest true "request body"
// @Success 201 {string} string Created
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsCache [post]
func (pr *PFSRouter) createFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var createRequest api.CreateFileSystemCacheRequest
	err := common.BindJSON(r, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("CreateFSCacheConfig bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	realUserName := getRealUserName(&ctx, createRequest.Username)
	createRequest.FsID = common.ID(realUserName, createRequest.FsName)

	ctx.Logging().Debugf("create file system cache with req[%v]", createRequest)

	err = validateCreateFSCacheConfig(&ctx, &createRequest)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = api.CreateFileSystemCacheConfig(&ctx, createRequest)
	if err != nil {
		ctx.Logging().Errorf("create file system cache with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusCreated)
}

func validateCreateFSCacheConfig(ctx *logger.RequestContext, req *api.CreateFileSystemCacheRequest) error {
	// fs exists?
	_, err := models.GetFileSystemWithFsID(req.FsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.FileSystemNotExist
			ctx.Logging().Errorf("validateCreateFileSystemCache fsID[%s] not exist", req.FsID)
		} else {
			ctx.Logging().Errorf("validateCreateFileSystemCache fsID[%s] err:%v", req.FsID, err)
		}
		return err
	}
	// TODO param check rule

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
// @Router /fsCache/{fsName} [GET]
func (pr *PFSRouter) getFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)
	ctx := common.GetRequestContext(r)

	realUserName := getRealUserName(&ctx, username)
	fsID := common.ID(realUserName, fsName)

	fsCacheConfigResp, err := api.GetFileSystemCacheConfig(&ctx, fsID)
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
	common.Render(w, http.StatusOK, fsCacheConfigResp)
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
// @Param request body fs.UpdateFileSystemCacheRequest true "request body"
// @Success 200 {object} models.FSCacheConfig "缓存配置结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /fsCache/{fsName} [PUT]
func (pr *PFSRouter) updateFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)
	ctx := common.GetRequestContext(r)

	var req api.UpdateFileSystemCacheRequest
	err := common.BindJSON(r, &req)
	if err != nil {
		ctx.Logging().Errorf("UpdateFSCacheConfig[%s] bindjson failed. err:%s", fsName, err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}

	realUserName := getRealUserName(&ctx, username)
	req.FsID = common.ID(realUserName, fsName)

	// validate fs_cache_config existence
	_, err = models.GetFSCacheConfig(ctx.Logging(), req.FsID)
	if err != nil {
		ctx.Logging().Errorf("validateUpdateFileSystemCache err:%v", err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.RenderErr(w, ctx.RequestID, common.RecordNotFound)
		} else {
			common.RenderErr(w, ctx.RequestID, common.InternalError)
		}
		return
	}

	err = api.UpdateFileSystemCacheConfig(&ctx, req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.RenderErr(w, ctx.RequestID, common.RecordNotFound)
		} else {
			common.RenderErr(w, ctx.RequestID, common.InternalError)
		}
		logger.LoggerForRequest(&ctx).Errorf(
			"GetFSCacheConfig[%s] failed. error:%v", req.FsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// deleteFSCacheConfig api delete file system cache config request
// @Summary deleteFSCacheConfig
// @Description 删除指定文件系统缓存配置
// @tag fs
// @Accept   json
// @Produce  json
// @Param fsName path string true "文件系统名称"
// @Param username query string false "用户名"
// @Success 200
// @Router /fsCache/{fsName} [delete]
func (pr *PFSRouter) deleteFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)

	log.Debugf("delete fs cache config with fsName[%s] username[%s]", fsName, username)
	realUserName := getRealUserName(&ctx, username)
	fsID := common.ID(realUserName, fsName)

	_, err := models.GetFileSystemWithFsID(fsID)
	if err != nil {
		ctx.Logging().Errorf("delete fsID[%s] failed by getting file system error[%v]", fsID, err)
		ctx.ErrorMessage = fmt.Sprintf("username[%s] not create fsName[%s]", username, fsName)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.RenderErrWithMessage(w, ctx.RequestID, common.RecordNotFound, ctx.ErrorMessage)
		} else {
			common.RenderErrWithMessage(w, ctx.RequestID, common.FileSystemDataBaseError, err.Error())
		}
		return
	}

	err = api.DeleteFileSystemCacheConfig(&ctx, fsID)
	if err != nil {
		ctx.Logging().Errorf("delete file system with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// FSCacheReport
// @Summary 上报FsID的缓存信息
// @Description  上报FsID的缓存信息
// @Id FSCacheReport
// @tags FSCacheConfig
// @Accept  json
// @Produce json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Param request body fs.CacheReportRequest true "request body"
// @Success 200 {object}
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /fsCache/report [POST]
func (pr *PFSRouter) fsCacheReport(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var request api.CacheReportRequest
	err := common.BindJSON(r, &request)
	if err != nil {
		ctx.Logging().Errorf("FSCachReport bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	if request.Username == "" {
		request.Username = ctx.UserName
	}

	err = validateFsCacheReport(&ctx, &request)
	if err != nil {
		ctx.Logging().Errorf("validateFsCacheReport request[%v] failed: [%v]", request, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	ctx.Logging().Debugf("report cache with req[%v]", request)

	err = api.ReportCache(&ctx, request)
	if err != nil {
		ctx.Logging().Errorf("report cache with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusOK)
}

func validateFsCacheReport(ctx *logger.RequestContext, req *api.CacheReportRequest) error {
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
