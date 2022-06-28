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

package mount

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type Info struct {
	Server        string
	FsID          string
	FsInfoStr     string
	FsCacheConfig model.FSCacheConfig
	TargetPath    string
	LocalPath     string
	UsernameRoot  string
	PasswordRoot  string
	ClusterID     string
	UID           int
	GID           int
	ReadOnly      bool
}

func ProcessMountInfo(id, server, fsInfoBase64, fsCacheBase64 string, readOnly bool) (Info, error) {
	// fs info
	fsInfoStr, err := processFsInfo(fsInfoBase64)
	if err != nil {
		retErr := fmt.Errorf("fs[%s] process fs info err: %v", id, err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}
	// fs cache config
	cacheConfig, err := processCacheConfig(id, fsInfoBase64)
	if err != nil {
		retErr := fmt.Errorf("fs[%s] process fs cacheConfig err: %v", id, err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}
	return Info{
		FsID:          id,
		Server:        server,
		FsInfoStr:     fsInfoStr,
		FsCacheConfig: cacheConfig,
		UID:           common.GetDefaultUID(),
		GID:           common.GetDefaultGID(),
		ReadOnly:      readOnly,
	}, nil
}

func processFsInfo(fsInfoBase64 string) (string, error) {
	fsInfoByte, err := base64.StdEncoding.DecodeString(fsInfoBase64)
	if err != nil {
		log.Errorf("base64 dcoding PfsFsInfo err: %v", err)
		return "", err
	}
	fs := model.FileSystem{}
	if err := json.Unmarshal(fsInfoByte, &fs); err != nil {
		log.Errorf("json unmarshal fs [%s] err: %v", string(fsInfoByte), err)
		return "", err
	}
	if fs.ID == "" ||
		fs.Type == "" ||
		fs.ServerAddress == "" {
		err := fmt.Errorf("processFsInfo failed as id or type of server address empty")
		log.Errorf(err.Error())
		return "", err
	}
	return string(fsInfoByte), nil
}

func processCacheConfig(fsID, fsCacheBase64 string) (model.FSCacheConfig, error) {
	fsCacheByte, err := base64.StdEncoding.DecodeString(fsCacheBase64)
	if err != nil {
		retErr := fmt.Errorf("fs[%s] base64 decoding PfsFsCache err: %v", fsID, err)
		log.Errorf(retErr.Error())
		return model.FSCacheConfig{}, retErr
	}
	cacheConfig := model.FSCacheConfig{}
	if err := json.Unmarshal(fsCacheByte, &cacheConfig); err != nil {
		retErr := fmt.Errorf("fs[%s] unmarshal cacheConfig [%s] err: %v", fsID, string(fsCacheByte), err)
		log.Errorf(retErr.Error())
		return model.FSCacheConfig{}, retErr
	}
	if cacheConfig.CacheDir == "" {
		cacheConfig.CacheDir = path.Join(csiconfig.HostMntDir, fsID)
	}
	if cacheConfig.MetaDriver == "" {
		cacheConfig.MetaDriver = schema.FsMetaDefault
	}
	return cacheConfig, nil
}
