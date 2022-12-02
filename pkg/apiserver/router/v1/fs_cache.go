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
		return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: meta driver[%s] not valid, must mem or disk",
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
	if req.CacheDir == "" && req.MetaDriver == schema.FsMetaDisk {
		return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: cacheDir[%s] should be an absolute path when cache in use",
			req.FsID, req.CacheDir))
	}

	// check resource
	rcs := req.Resource
	if rcs.CpuLimit != "" {
		cpu, err := resource.ParseQuantity(rcs.CpuLimit)
		if err != nil {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: invalid resource cpuLimit [%s]",
				req.FsID, rcs.CpuLimit))
		}
		if cpu.Cmp(resource.MustParse(api.MaxMountPodCpuLimit)) > 0 ||
			cpu.Cmp(resource.MustParse("0")) <= 0 {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: cpuLimit[%s] should be positive and no greater than %s",
				req.FsID, rcs.CpuLimit, api.MaxMountPodCpuLimit))
		}
	}
	if rcs.MemoryLimit != "" {
		memory, err := resource.ParseQuantity(rcs.MemoryLimit)
		if err != nil {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: invalid resource memoryLimit [%s]",
				req.FsID, rcs.MemoryLimit))
		}
		if memory.Cmp(resource.MustParse(api.MaxMountPodMemLimit)) > 0 ||
			memory.Cmp(resource.MustParse("0")) <= 0 {
			return validationReturnError(ctx, fmt.Errorf("fs[%s] cache config: memoryLimit[%s] should be positive and no greater than %s",
				req.FsID, rcs.MemoryLimit, api.MaxMountPodMemLimit))
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
