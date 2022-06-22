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
	"crypto/md5"
	"encoding/hex"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type CacheReportRequest struct {
	FsName    string `json:"fsName" validate:"required"`
	Username  string `json:"username"`
	ClusterID string `json:"clusterID"`
	CacheDir  string `json:"cacheDir" validate:"required"`
	NodeName  string `json:"nodename" validate:"required"`
	UsedSize  int    `json:"usedsize" validate:"required"`
}

func ReportCache(ctx *logger.RequestContext, req CacheReportRequest) error {
	cacheID := GetCacheID(req.ClusterID, req.NodeName, req.CacheDir)
	fsID := common.ID(req.Username, req.FsName)

	fsCache := &model.FSCache{
		CacheID:   cacheID,
		FsID:      fsID,
		CacheDir:  req.CacheDir,
		NodeName:  req.NodeName,
		UsedSize:  req.UsedSize,
		ClusterID: req.ClusterID,
	}

	n, err := storage.FsCache.Update(fsCache)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("ReportCache Update[%s] err:%v", fsID, err)
		return err
	}
	if n == 0 {
		err = storage.FsCache.Add(fsCache)
	}
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("ReportCache Create[%s] err:%v", fsID, err)
		return err
	}
	return nil
}

func GetCacheID(clusterID, nodeName, CacheDir string) string {
	hash := md5.Sum([]byte(clusterID + nodeName + CacheDir))
	return hex.EncodeToString(hash[:])
}
