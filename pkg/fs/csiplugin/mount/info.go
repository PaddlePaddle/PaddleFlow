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
	fsInfoByte, err := base64.StdEncoding.DecodeString(fsInfoBase64)
	if err != nil {
		log.Errorf("base64 dcoding PfsFsInfo err: %v", err)
		return Info{}, err
	}
	fs := model.FileSystem{}
	if err := json.Unmarshal(fsInfoByte, &fs); err != nil {
		log.Errorf("json unmarshal fs [%s] err: %v", string(fsInfoByte), err)
		return Info{}, err
	}
	fsInfoStr := string(fsInfoByte)
	// fs cache config
	fsCacheByte, err := base64.StdEncoding.DecodeString(fsCacheBase64)
	if err != nil {
		log.Errorf("base64 dcoding PfsFsCache err: %v", err)
		return Info{}, err
	}
	cacheConfig := model.FSCacheConfig{}
	if err := json.Unmarshal(fsCacheByte, &cacheConfig); err != nil {
		log.Errorf("json unmarshal cacheConfig [%s] err: %v", string(fsCacheByte), err)
		return Info{}, err
	}
	completeCacheConfig(&cacheConfig, id)
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

func completeCacheConfig(config *model.FSCacheConfig, fsID string) {
	if config.CacheDir == "" {
		config.CacheDir = path.Join(csiconfig.HostMntDir, fsID)
	}
	if config.MetaDriver == "" {
		config.MetaDriver = schema.FsMetaDefault
	}
}
