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
	"strings"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/logger"
)

type UpdateFileSystemCacheRequest struct {
	FsID                string                 `json:"-"`
	CacheDir            string                 `json:"cacheDir"`
	Quota               int                    `json:"quota"`
	CacheType           string                 `json:"cacheType"`
	BlockSize           int                    `json:"blockSize"`
	NodeAffinity        map[string]interface{} `json:"nodeAffinity"`
	NodeTaintToleration map[string]interface{} `json:"nodeTaintToleration"`
	ExtraConfig         map[string]string      `json:"extraConfig"`
}

func (req *UpdateFileSystemCacheRequest) toModel() models.FSCacheConfig {
	return models.FSCacheConfig{
		FsID:                   req.FsID,
		CacheDir:               req.CacheDir,
		Quota:                  req.Quota,
		CacheType:              req.CacheType,
		BlockSize:              req.BlockSize,
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

func (req *CreateFileSystemCacheRequest) toModel() models.FSCacheConfig {
	return req.UpdateFileSystemCacheRequest.toModel()
}

type FileSystemCacheResponse struct {
	CacheDir            string                 `json:"cacheDir"`
	Quota               int                    `json:"quota"`
	CacheType           string                 `json:"cacheType"`
	BlockSize           int                    `json:"blockSize"`
	NodeAffinity        map[string]interface{} `json:"nodeAffinity"`
	NodeTaintToleration map[string]interface{} `json:"nodeTaintToleration"`
	ExtraConfig         map[string]string      `json:"extraConfig"`
	FsName              string                 `json:"fsName"`
	Username            string                 `json:"username"`
	CreateTime          string                 `json:"createTime"`
	UpdateTime          string                 `json:"updateTime,omitempty"`
}

func (resp *FileSystemCacheResponse) fromModel(config models.FSCacheConfig) {
	resp.CacheDir = config.CacheDir
	resp.Quota = config.Quota
	resp.CacheType = config.CacheType
	resp.BlockSize = config.BlockSize
	resp.NodeAffinity = config.NodeAffinityMap
	resp.NodeTaintToleration = config.NodeTaintTolerationMap
	resp.ExtraConfig = config.ExtraConfigMap
	resp.FsName, resp.Username = fsIDToName(config.FsID)
	// format time
	resp.CreateTime = config.CreatedAt.Format("2006-01-02 15:04:05")
	resp.UpdateTime = config.UpdatedAt.Format("2006-01-02 15:04:05")
}

func fsIDToName(fsID string) (fsName, username string) {
	fsArr := strings.Split(fsID, "-")
	if len(fsArr) < 3 {
		return "", ""
	}
	fsName = fsArr[len(fsArr)-1]
	username = strings.Join(fsArr[1:len(fsArr)-1], "")
	return
}

func CreateFileSystemCacheConfig(ctx *logger.RequestContext, req CreateFileSystemCacheRequest) error {
	cacheConfig := req.toModel()
	err := models.CreateFSCacheConfig(ctx.Logging(), &cacheConfig)
	if err != nil {
		ctx.Logging().Errorf("CreateFSCacheConfig fs[%s] err:%v", cacheConfig.FsID, err)
		return err
	}
	return nil
}

func UpdateFileSystemCacheConfig(ctx *logger.RequestContext, req UpdateFileSystemCacheRequest) error {
	cacheConfig := req.toModel()
	err := models.UpdateFSCacheConfig(ctx.Logging(), cacheConfig)
	if err != nil {
		ctx.Logging().Errorf("UpdateFSCacheConfig fs[%s] err:%v", cacheConfig.FsID, err)
		return err
	}
	return nil
}

func GetFileSystemCacheConfig(ctx *logger.RequestContext, fsID string) (FileSystemCacheResponse, error) {
	fsCacheConfig, err := models.GetFSCacheConfig(ctx.Logging(), fsID)
	if err != nil {
		ctx.Logging().Errorf("GetFileSystemCacheConfig fs[%s] err:%v", fsID, err)
		return FileSystemCacheResponse{}, err
	}
	var resp FileSystemCacheResponse
	resp.fromModel(fsCacheConfig)
	return resp, nil
}
