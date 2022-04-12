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
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/logger"
)

type CreateOrUpdateFSCacheRequest struct {
	models.FSCacheConfig
}

func (req *CreateOrUpdateFSCacheRequest) toModel() models.FSCacheConfig {
	return models.FSCacheConfig{
		Dir:             req.Dir,
		Quota:           req.Quota,
		CacheType:       req.CacheType,
		BlockSize:       req.BlockSize,
		NodeAffinityMap: req.NodeAffinityMap,
		ExtraConfigMap:  req.ExtraConfigMap,
		Model: models.Model{
			ID: req.ID,
		},
	}
}

func CreateFileSystemCacheConfig(ctx *logger.RequestContext, req CreateOrUpdateFSCacheRequest) error {
	fs := req.toModel()
	err := models.CreateFSCacheConfig(ctx.Logging(), &fs)
	if err != nil {
		ctx.Logging().Errorf("CreateFSCacheConfig fs[%s] err:%v", fs.ID, err)
		return err
	}
	return nil
}

func UpdateFileSystemCacheConfig(ctx *logger.RequestContext, req CreateOrUpdateFSCacheRequest) error {
	fs := req.toModel()
	err := models.UpdateFSCacheConfig(ctx.Logging(), fs)
	if err != nil {
		ctx.Logging().Errorf("UpdateFSCacheConfig fs[%s] err:%v", fs.ID, err)
		return err
	}
	return nil
}

func GetFileSystemCacheConfig(ctx *logger.RequestContext, fsID string) (models.FSCacheConfig, error) {
	fsCacheConfig, err := models.GetFSCacheConfig(ctx.Logging(), fsID)
	if err != nil {
		ctx.Logging().Errorf("GetFileSystemCacheConfig fs[%s] err:%v", fsID, err)
		return models.FSCacheConfig{}, err
	}
	return fsCacheConfig, nil
}
