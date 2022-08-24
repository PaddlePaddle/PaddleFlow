/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package pipeline

import (
	"errors"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type ListRunCacheResponse struct {
	common.MarkerInfo
	RunCacheList []models.RunCache `json:"runCacheList"`
}

type ListArtifactEventResponse struct {
	common.MarkerInfo
	ArtifactEventList []model.ArtifactEvent `json:"artifactEventList"`
}

func logCacheReqToModel(req schema.LogRunCacheRequest) models.RunCache {
	return models.RunCache{
		FirstFp:     req.FirstFp,
		SecondFp:    req.SecondFp,
		JobID:       req.JobID,
		RunID:       req.RunID,
		FsID:        req.FsID,
		FsName:      req.FsName,
		UserName:    req.UserName,
		Source:      req.Source,
		ExpiredTime: req.ExpiredTime,
		Strategy:    req.Strategy,
	}
}

func logArtifactReqToModel(req schema.LogRunArtifactRequest) model.ArtifactEvent {
	return model.ArtifactEvent{
		Md5:          req.Md5,
		RunID:        req.RunID,
		FsID:         req.FsID,
		FsName:       req.FsName,
		UserName:     req.UserName,
		ArtifactPath: req.ArtifactPath,
		Step:         req.Step,
		JobID:        req.JobID,
		Type:         req.Type,
		ArtifactName: req.ArtifactName,
		Meta:         req.Meta,
	}
}

func LogCache(req schema.LogRunCacheRequest) (string, error) {
	logEntry := logger.LoggerForRun(req.RunID)
	logEntry.Debugf("log cache[%+v] starts", req)
	newCache := logCacheReqToModel(req)
	cacheID, err := models.CreateRunCache(logEntry, &newCache)
	if err != nil {
		logEntry.Errorf("log cache[%+v] failed error:%v", req, err)
		return "", err
	}
	return cacheID, nil
}

func ListCacheByFirstFp(firstFp, fsID, source string) ([]models.RunCache, error) {
	logEntry := logger.Logger()
	cacheList, err := models.ListRunCacheByFirstFp(logEntry, firstFp, fsID, source)
	if err != nil {
		logEntry.Errorf("ListRunCacheByFirstFp failed. firstFp[%s] fsID[%s] source[%s]. error:%v",
			firstFp, fsID, source, err)
		return nil, err
	}
	logEntry.Debugf("ListRunCacheByFirstFp: %+v", cacheList)
	return cacheList, nil
}

func LogArtifactEvent(req schema.LogRunArtifactRequest) error {
	logEntry := logger.LoggerForRun(req.RunID)
	logEntry.Debugf("log artifactEvent[%+v] starts", req)
	artifactEvent := logArtifactReqToModel(req)
	if err := storage.Artifact.CreateArtifactEvent(logEntry, artifactEvent); err != nil {
		logEntry.Errorf("log new artifactEvent[%+v] failed error:%v", req, err)
		return err
	}
	return nil
}

//-------------CRUD-----------------//
func GetRunCache(ctx *logger.RequestContext, id string) (models.RunCache, error) {
	ctx.Logging().Debugf("begin get run_cache by id:%s", id)
	cache, err := models.GetRunCache(ctx.Logging(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RunCacheNotFound
			ctx.Logging().Errorln(err.Error())
			return models.RunCache{}, common.NotFoundError(common.ResourceTypeRunCache, id)
		}
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorln(err.Error())
		return models.RunCache{}, err
	}
	// check permission
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != cache.UserName {
		err := common.NoAccessError(ctx.UserName, common.ResourceTypeRunCache, id)
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln(err.Error())
		return models.RunCache{}, err
	}
	return cache, nil
}

func ListRunCache(ctx *logger.RequestContext, marker string, maxKeys int, userFilter, fsFilter, runFilter []string) (ListRunCacheResponse, error) {
	ctx.Logging().Debugf("begin list runCache.")
	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return ListRunCacheResponse{}, err
		}
	}
	// normal user list its own
	if !common.IsRootUser(ctx.UserName) {
		userFilter = []string{ctx.UserName}
	}
	// model list
	runCacheList, err := models.ListRunCache(ctx.Logging(), pk, maxKeys, userFilter, fsFilter, runFilter)
	if err != nil {
		ctx.Logging().Errorf("models list runCache failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
	}
	listRunCacheResponse := ListRunCacheResponse{RunCacheList: runCacheList}

	// get next marker
	listRunCacheResponse.IsTruncated = false
	if len(runCacheList) > 0 {
		runCache := runCacheList[len(runCacheList)-1]
		if !isLastRunCachePk(ctx, runCache.Pk) {
			nextMarker, err := common.EncryptPk(runCache.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					runCache.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return ListRunCacheResponse{}, err
			}
			listRunCacheResponse.NextMarker = nextMarker
			listRunCacheResponse.IsTruncated = true
		}
	}
	listRunCacheResponse.MaxKeys = maxKeys
	return listRunCacheResponse, nil
}

func isLastRunCachePk(ctx *logger.RequestContext, pk int64) bool {
	lastRun, err := models.GetLastRunCache(ctx.Logging())
	if err != nil {
		ctx.Logging().Errorf("get last run failed. error:[%s]", err.Error())
	}
	if lastRun.Pk == pk {
		return true
	}
	return false
}

func DeleteRunCache(ctx *logger.RequestContext, id string) error {
	ctx.Logging().Debugf("begin delete run_cache by id:%s", id)
	cache, err := models.GetRunCache(ctx.Logging(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RunCacheNotFound
			ctx.Logging().Errorln(err.Error())
			return common.NotFoundError(common.ResourceTypeRunCache, id)
		}
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorln(err.Error())
		return err
	}
	// check permission
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != cache.UserName {
		err := common.NoAccessError(ctx.UserName, common.ResourceTypeRunCache, id)
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln(err.Error())
		return err
	}
	// model delete
	err = models.DeleteRunCache(ctx.Logging(), id)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("delete run_cache failed. err: %v", err)
		return err
	}
	return nil
}

//---------------------artifact_event---------------------//
func DeleteArtifactEvent(ctx *logger.RequestContext, username, fsname, runID, artifactPath string) error {
	ctx.Logging().Debugf("begin delete artifact_event. username:%s, fsname:%s, runID:%s, artifactPath:%s", username, fsname, runID, artifactPath)
	err := storage.Artifact.DeleteArtifactEvent(ctx.Logging(), username, fsname, runID, artifactPath)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.ArtifactEventNotFound
			ctx.Logging().Errorln(err.Error())
			return common.NotFoundError(common.ResourceTypeArtifactEvent, artifactPath)
		}
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorln(err.Error())
		return err
	}
	return nil
}

func ListArtifactEvent(ctx *logger.RequestContext, marker string, maxKeys int, userFilter, fsFilter, runFilter, typeFilter, pathFilter []string) (ListArtifactEventResponse, error) {
	ctx.Logging().Debugf("begin list ArtifactEvent")
	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return ListArtifactEventResponse{}, err
		}
	}
	// normal user list its own
	if !common.IsRootUser(ctx.UserName) {
		userFilter = []string{ctx.UserName}
	}
	// model list
	runCacheList, err := storage.Artifact.ListArtifactEvent(ctx.Logging(), pk, maxKeys, userFilter, fsFilter, runFilter, typeFilter, pathFilter)
	if err != nil {
		ctx.Logging().Errorf("models list runCache failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
	}
	listArtifactEventResponse := ListArtifactEventResponse{ArtifactEventList: runCacheList}

	// get next marker
	listArtifactEventResponse.IsTruncated = false
	if len(runCacheList) > 0 {
		runCache := runCacheList[len(runCacheList)-1]
		if !isLastArtifactEventPk(ctx, runCache.Pk) {
			nextMarker, err := common.EncryptPk(runCache.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					runCache.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return ListArtifactEventResponse{}, err
			}
			listArtifactEventResponse.NextMarker = nextMarker
			listArtifactEventResponse.IsTruncated = true
		}
	}
	listArtifactEventResponse.MaxKeys = maxKeys
	return listArtifactEventResponse, nil
}

func isLastArtifactEventPk(ctx *logger.RequestContext, pk int64) bool {
	lastRun, err := storage.Artifact.GetLastArtifactEvent(ctx.Logging())
	if err != nil {
		ctx.Logging().Errorf("get last ArtifactEvent failed. error:[%s]", err.Error())
	}
	if lastRun.Pk == pk {
		return true
	}
	return false
}
