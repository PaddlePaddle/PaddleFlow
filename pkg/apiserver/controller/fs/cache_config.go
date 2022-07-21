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

package fs

import (
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	utils "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type UpdateFileSystemCacheRequest struct {
	FsID                string                 `json:"-"`
	CacheDir            string                 `json:"cacheDir"`
	Quota               int                    `json:"quota"`
	MetaDriver          string                 `json:"metaDriver"`
	BlockSize           int                    `json:"blockSize"`
	Debug               bool                   `json:"debug"`
	NodeAffinity        map[string]interface{} `json:"nodeAffinity"`
	NodeTaintToleration map[string]interface{} `json:"nodeTaintToleration"`
	ExtraConfig         map[string]string      `json:"extraConfig"`
}

func (req *UpdateFileSystemCacheRequest) toModel() model.FSCacheConfig {
	return model.FSCacheConfig{
		FsID:                   req.FsID,
		CacheDir:               req.CacheDir,
		Quota:                  req.Quota,
		MetaDriver:             req.MetaDriver,
		BlockSize:              req.BlockSize,
		Debug:                  req.Debug,
		NodeAffinityMap:        req.NodeAffinity,
		ExtraConfigMap:         req.ExtraConfig,
		NodeTaintTolerationMap: req.NodeTaintToleration,
	}
}

type CreateFileSystemCacheRequest struct {
	Username string `json:"username"`
	FsName   string `json:"fsName"`
	UpdateFileSystemCacheRequest
}

func (req *CreateFileSystemCacheRequest) toModel() model.FSCacheConfig {
	return req.UpdateFileSystemCacheRequest.toModel()
}

type FileSystemCacheResponse struct {
	CacheDir            string                 `json:"cacheDir"`
	Quota               int                    `json:"quota"`
	MetaDriver          string                 `json:"metaDriver"`
	BlockSize           int                    `json:"blockSize"`
	NodeAffinity        map[string]interface{} `json:"nodeAffinity"`
	NodeTaintToleration map[string]interface{} `json:"nodeTaintToleration"`
	ExtraConfig         map[string]string      `json:"extraConfig"`
	FsName              string                 `json:"fsName"`
	Username            string                 `json:"username"`
	CreateTime          string                 `json:"createTime"`
	UpdateTime          string                 `json:"updateTime,omitempty"`
}

func (resp *FileSystemCacheResponse) fromModel(config model.FSCacheConfig) {
	resp.CacheDir = config.CacheDir
	resp.Quota = config.Quota
	resp.MetaDriver = config.MetaDriver
	resp.BlockSize = config.BlockSize
	resp.NodeAffinity = config.NodeAffinityMap
	resp.NodeTaintToleration = config.NodeTaintTolerationMap
	resp.ExtraConfig = config.ExtraConfigMap
	resp.FsName, resp.Username = utils.FsIDToFsNameUsername(config.FsID)
	// format time
	resp.CreateTime = config.CreatedAt.Format("2006-01-02 15:04:05")
	resp.UpdateTime = config.UpdatedAt.Format("2006-01-02 15:04:05")
}

func checkFsMountedAndCleanResource(ctx *logger.RequestContext, fsID string) error {
	// check not fs mounted. if not mounted, clean up pods and pv/pvcs
	isMounted, err := GetFileSystemService().CheckFsMountedAndCleanResources(fsID)
	if err != nil {
		ctx.Logging().Errorf("CheckFsMountedAndCleanResources failed: %v", err)
		return err
	}
	if isMounted {
		err := fmt.Errorf("fs[%s] is mounted. creation, modification or deletion is not allowed", fsID)
		ctx.Logging().Errorf(err.Error())
		ctx.ErrorCode = common.ActionNotAllowed
		return err
	}
	return nil
}

func CreateFileSystemCacheConfig(ctx *logger.RequestContext, req CreateFileSystemCacheRequest) error {
	if err := checkFsMountedAndCleanResource(ctx, req.FsID); err != nil {
		return err
	}
	cacheConfig := req.toModel()
	if err := storage.Filesystem.CreateFSCacheConfig(&cacheConfig); err != nil {
		ctx.Logging().Errorf("CreateFSCacheConfig fs[%s] err:%v", cacheConfig.FsID, err)
		return err
	}
	return nil
}

func UpdateFileSystemCacheConfig(ctx *logger.RequestContext, req UpdateFileSystemCacheRequest) error {
	if err := checkFsMountedAndCleanResource(ctx, req.FsID); err != nil {
		return err
	}
	cacheConfig := req.toModel()
	if err := storage.Filesystem.UpdateFSCacheConfig(&cacheConfig); err != nil {
		ctx.Logging().Errorf("UpdateFSCacheConfig fs[%s] err:%v", cacheConfig.FsID, err)
		return err
	}
	return nil
}

func GetFileSystemCacheConfig(ctx *logger.RequestContext, fsID string) (FileSystemCacheResponse, error) {
	fsCacheConfig, err := storage.Filesystem.GetFSCacheConfig(fsID)
	if err != nil {
		ctx.Logging().Errorf("GetFileSystemCacheConfig fs[%s] err:%v", fsID, err)
		return FileSystemCacheResponse{}, err
	}
	var resp FileSystemCacheResponse
	resp.fromModel(fsCacheConfig)
	return resp, nil
}

func DeleteFileSystemCacheConfig(ctx *logger.RequestContext, fsID string) error {
	if err := checkFsMountedAndCleanResource(ctx, fsID); err != nil {
		return err
	}
	_, err := storage.Filesystem.GetFSCacheConfig(fsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RecordNotFound
		} else {
			ctx.ErrorCode = common.FileSystemDataBaseError
		}
		ctx.Logging().Errorf("GetFileSystemCacheConfig fs[%s] err:%v", fsID, err)
		return err
	}
	if err := storage.Filesystem.DeleteFSCacheConfig(storage.DB, fsID); err != nil {
		ctx.Logging().Errorf("delete fs cache config failed error[%v]", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}
	return err
}
