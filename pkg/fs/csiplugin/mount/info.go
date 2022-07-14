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
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	common_ "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	fscommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

const (
	mountName   = "mount"
	fileMountSh = "/home/paddleflow/mount.sh"
	filePfsFuse = "/home/paddleflow/pfs-fuse"
)

type Info struct {
	Server                  string
	FsID                    string
	FsBase64Str             string
	IndependentMountProcess bool
	MountCmd                string
	Type                    string
	ServerAddress           string
	SubPath                 string
	Properties              map[string]string
	MountArgs               []string
	FsCacheConfig           model.FSCacheConfig
	TargetPath              string
	LocalPath               string
	UsernameRoot            string
	PasswordRoot            string
	ClusterID               string
	UID                     int
	GID                     int
	ReadOnly                bool
}

func ProcessMountInfo(username, password, targetPath, fsID, server, fsInfoBase64, fsCacheBase64 string, readOnly bool) (Info, error) {
	// fs info
	fs, err := ProcessFsInfo(fsInfoBase64)
	if err != nil {
		retErr := fmt.Errorf("fs[%s] process fs info err: %v", fsID, err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}
	// fs cache config
	cacheConfig := model.FSCacheConfig{}
	if !fs.IndependentMountProcess {
		cacheConfig, err = processCacheConfig(fsID, fsCacheBase64)
		if err != nil {
			retErr := fmt.Errorf("fs[%s] process fs cacheConfig err: %v", fsID, err)
			log.Errorf(retErr.Error())
			return Info{}, retErr
		}
	}
	info := Info{
		UsernameRoot:            username,
		PasswordRoot:            password,
		TargetPath:              targetPath,
		ServerAddress:           fs.ServerAddress,
		SubPath:                 fs.SubPath,
		Properties:              fs.PropertiesMap,
		FsID:                    fsID,
		Server:                  server,
		FsBase64Str:             fsInfoBase64,
		IndependentMountProcess: fs.IndependentMountProcess,
		FsCacheConfig:           cacheConfig,
		UID:                     common.GetDefaultUID(),
		GID:                     common.GetDefaultGID(),
		Type:                    fs.Type,
		ReadOnly:                readOnly,
	}
	info.mountCmd()
	return info, nil
}

func ProcessFsInfo(fsInfoBase64 string) (model.FileSystem, error) {
	fsInfoByte, err := base64.StdEncoding.DecodeString(fsInfoBase64)
	if err != nil {
		log.Errorf("base64 dcoding PfsFsInfo err: %v", err)
		return model.FileSystem{}, err
	}
	fs := model.FileSystem{}
	if err := json.Unmarshal(fsInfoByte, &fs); err != nil {
		log.Errorf("json unmarshal fs [%s] err: %v", string(fsInfoByte), err)
		return model.FileSystem{}, err
	}
	if fs.ID == "" ||
		fs.Type == "" ||
		fs.ServerAddress == "" {
		err := fmt.Errorf("processFsInfo failed as id or type of server address empty")
		log.Errorf(err.Error())
		return model.FileSystem{}, err
	}
	return fs, nil
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
	return cacheConfig, nil
}

func (m *Info) mountCmd() {
	baseArgs := []string{
		"--fs-info=" + m.FsBase64Str,
		"--fs-id=" + m.FsID,
	}
	if m.ReadOnly {
		baseArgs = append(baseArgs, "--mount-options=ro")
	}
	if m.Type == common_.Glusterfs {
		m.MountCmd = mountName
		var args []string
		args = append(args, "-t", m.Type)
		args = append(args, strings.Join([]string{m.ServerAddress, m.SubPath}, ":"), m.TargetPath)
		m.MountArgs = args
	} else if m.IndependentMountProcess {
		m.MountCmd = fileMountSh
		m.MountArgs = append(baseArgs, "--mount-point="+m.TargetPath)
	} else {
		m.MountCmd = filePfsFuse
		m.MountArgs = []string{"mount", "--mount-point=" + FusePodMountPoint}
		m.MountArgs = append(m.MountArgs, baseArgs...)
		if m.FsCacheConfig.FsID != "" {
			// data cache
			if m.FsCacheConfig.BlockSize > 0 {
				m.MountArgs = append(m.MountArgs, "--block-size="+strconv.Itoa(m.FsCacheConfig.BlockSize),
					"--data-cache-path="+FusePodCachePath+DataCacheDir)
			}
			// meta cache
			m.MountArgs = append(m.MountArgs, "--meta-cache-driver="+m.FsCacheConfig.MetaDriver)
			if m.FsCacheConfig.MetaDriver == schema.FsMetaLevelDB || m.FsCacheConfig.MetaDriver == schema.FsMetaNutsDB {
				m.MountArgs = append(m.MountArgs, "--meta-cache-path="+FusePodCachePath+MetaCacheDir)
			}
			if m.FsCacheConfig.ExtraConfigMap != nil {
				for configName, item := range m.FsCacheConfig.ExtraConfigMap {
					m.MountArgs = append(m.MountArgs, fmt.Sprintf("--%s=%s", configName, item))
				}
			}

			// s3 default mount permission
			if m.Type == common_.S3 {
				if m.Properties[fscommon.FileMode] != "" {
					m.MountArgs = append(m.MountArgs, fmt.Sprintf("--%s=%s", "file-mode", m.Properties[fscommon.FileMode]))
				} else {
					m.MountArgs = append(m.MountArgs, fmt.Sprintf("--%s=%s", "file-mode", "0666"))
				}
				if m.Properties[fscommon.DirMode] != "" {
					m.MountArgs = append(m.MountArgs, fmt.Sprintf("--%s=%s", "dir-mode", m.Properties[fscommon.DirMode]))
				} else {
					m.MountArgs = append(m.MountArgs, fmt.Sprintf("--%s=%s", "dir-mode", "0777"))
				}
			}
		}
		if m.FsCacheConfig.Debug {
			m.MountArgs = append(m.MountArgs, "--log-level=trace")
		}
	}
}
