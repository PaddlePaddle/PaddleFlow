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
	"path/filepath"

	"github.com/go-chi/chi"
	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	api "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
	if err := common.BindJSON(r, &createRequest); err != nil {
		ctx.Logging().Errorf("CreateFSCacheConfig bindjson failed. err:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, common.MalformedJSON, err.Error())
		return
	}
	realUserName := getRealUserName(&ctx, createRequest.Username)
	createRequest.FsID = common.ID(realUserName, createRequest.FsName)
	ctx.Logging().Tracef("create file system cache with req[%v]", createRequest)
	// validate can be modified
	if err := fsExistsForModify(&ctx, createRequest.FsID); err != nil {
		ctx.Logging().Errorf("checkCanModifyFs[%s] err: %v", createRequest.FsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	// validate request
	if err := validateCacheConfigCreate(&ctx, &createRequest); err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	// create in db
	if err := api.CreateFileSystemCacheConfig(&ctx, createRequest); err != nil {
		ctx.Logging().Errorf("create file system cache with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusCreated)
}

func validationReturnError(ctx *logger.RequestContext, err error) error {
	ctx.ErrorCode = common.InvalidArguments
	ctx.Logging().Errorf(err.Error())
	return err
}

func validateCacheConfigCreate(ctx *logger.RequestContext, req *api.CreateFileSystemCacheRequest) error {
	if req.MetaDriver != "" && !schema.IsValidFsMetaDriver(req.MetaDriver) {
		return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: meta driver[%s] not valid",
			req.FsID, req.MetaDriver))
	}
	// BlockSize
	if req.BlockSize < 0 {
		return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: data cache blockSize[%d] should not be negative",
			req.FsID, req.BlockSize))
	}
	// cacheDir must be absolute path or ""
	if req.CacheDir != "" && !filepath.IsAbs(req.CacheDir) {
		return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: cacheDir[%s] should be empty or an absolute path",
			req.FsID, req.CacheDir))
	}
	// must assign cacheDir when cache in use
	if req.CacheDir == "" && req.MetaDriver == schema.FsMetaLevelDB {
		return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: cacheDir[%s] should be an absolute path when cache in use",
			req.FsID, req.CacheDir))
	}
	// check resource
	rcs := req.Resource
	if rcs.CpuLimit != "" {
		if _, err := resource.ParseQuantity(rcs.CpuLimit); err != nil {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: invalid resource CpuLimit [%s]",
				req.FsID, rcs.CpuLimit))
		}
	}
	if rcs.MemoryLimit != "" {
		if _, err := resource.ParseQuantity(rcs.MemoryLimit); err != nil {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: invalid resource MemoryLimit [%s]",
				req.FsID, rcs.MemoryLimit))
		}
	}
	if rcs.CpuRequest != "" {
		if _, err := resource.ParseQuantity(rcs.CpuRequest); err != nil {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: invalid resource CpuRequest [%s]",
				req.FsID, rcs.CpuRequest))
		}
	}
	if rcs.MemoryRequest != "" {
		if _, err := resource.ParseQuantity(rcs.MemoryRequest); err != nil {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: invalid resource MemoryRequest [%s]",
				req.FsID, rcs.MemoryRequest))
		}
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

	if err := fsExistsForModify(&ctx, fsID); err != nil {
		ctx.Logging().Errorf("checkCanModifyFs[%s] err: %v", fsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	// validate fs_cache_config existence
	_, err := api.GetFileSystemCacheConfig(&ctx, fsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RecordNotFound
		} else {
			ctx.ErrorCode = common.InternalError
		}
		logger.LoggerForRequest(&ctx).Errorf("validate fs_cache_config[%s] failed. error:%v", fsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	if err := api.DeleteFileSystemCacheConfig(&ctx, fsID); err != nil {
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
