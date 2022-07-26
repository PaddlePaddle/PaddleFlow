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
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

const (
	mountName                             = "mount"
	PfsFuseIndependentMountProcessCMDName = "/home/paddleflow/mount.sh"
	pfsFuseMountPodCMDName                = "/home/paddleflow/pfs-fuse mount"
	ReadOnly                              = "ro"
)

type Info struct {
	CacheConfig model.FSCacheConfig
	FS          model.FileSystem
	FSBase64Str string
	TargetPath  string
	Cmd         string
	Args        []string
	K8sClient   utils.Client
}

func ProcessMountInfo(fsInfoBase64, fsCacheBase64, targetPath string, k8sClient utils.Client, readOnly bool) (Info, error) {
	// FS info
	fs, err := utils.ProcessFSInfo(fsInfoBase64)
	if err != nil {
		retErr := fmt.Errorf("FSprocess FS info err: %v", err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}

	// FS CacheConfig config
	cacheConfig := model.FSCacheConfig{}
	if !fs.IndependentMountProcess {
		cacheConfig, err = utils.ProcessCacheConfig(fsCacheBase64)
		if err != nil {
			retErr := fmt.Errorf("FS process FS CacheConfig err: %v", err)
			log.Errorf(retErr.Error())
			return Info{}, retErr
		}
	}

	info := Info{
		CacheConfig: cacheConfig,
		FS:          fs,
		FSBase64Str: fsInfoBase64,
		TargetPath:  targetPath,
		K8sClient:   k8sClient,
	}
	info.Cmd, info.Args = info.cmdAndArgs(info.options(readOnly))
	return info, nil
}

func (mountInfo *Info) cmdAndArgs(options []string) (string, []string) {
	var cmd string
	var args []string
	if mountInfo.FS.Type == common.GlusterFSType {
		cmd = mountName
		args = append(args, "-t", mountInfo.FS.Type,
			strings.Join([]string{mountInfo.FS.ServerAddress, mountInfo.FS.SubPath}, ":"), mountInfo.TargetPath)
		if len(options) != 0 {
			args = append(args, "-o", strings.Join(options, ","))
		}
	} else if mountInfo.FS.IndependentMountProcess {
		cmd = PfsFuseIndependentMountProcessCMDName
		args = append(args, fmt.Sprintf("--%s=%s", "mount-point", mountInfo.TargetPath))
		args = append(args, options...)
	} else {
		cmd = pfsFuseMountPodCMDName
		args = append(args, fmt.Sprintf("--%s=%s", "mount-point", FusePodMountPoint))
		args = append(args, options...)
	}
	return cmd, args
}

func (mountInfo *Info) options(readOnly bool) []string {
	if mountInfo.FS.Type == common.GlusterFSType {
		return []string{}
	}

	var options []string
	options = append(options, fmt.Sprintf("--%s=%s", "fs-info", mountInfo.FSBase64Str))
	options = append(options, fmt.Sprintf("--%s=%s", "fs-id", mountInfo.FS.ID))

	if readOnly {
		options = append(options, fmt.Sprintf("--%s=%s", "mount-options", ReadOnly))
	}

	if mountInfo.CacheConfig.BlockSize > 0 {
		options = append(options, fmt.Sprintf("--%s=%d", "block-size", mountInfo.CacheConfig.BlockSize))
	}
	if mountInfo.CacheConfig.CacheDir != "" {
		var dataCacheDir string
		if mountInfo.FS.IndependentMountProcess {
			dataCacheDir = mountInfo.CacheConfig.CacheDir
		} else {
			dataCacheDir = FusePodCachePath + DataCacheDir
		}
		options = append(options, fmt.Sprintf("--%s=%s", "data-cache-path", dataCacheDir))
	}
	if mountInfo.CacheConfig.MetaDriver != "" {
		options = append(options, fmt.Sprintf("--%s=%s", "meta-cache-driver", mountInfo.CacheConfig.MetaDriver))
	}
	if mountInfo.CacheConfig.ExtraConfigMap != nil {
		for configName, item := range mountInfo.CacheConfig.ExtraConfigMap {
			options = append(options, fmt.Sprintf("--%s=%s", configName, item))
		}
	}
	if mountInfo.CacheConfig.Debug {
		options = append(options, "--log-level=debug")
	}
	if mountInfo.CacheConfig.MetaDriver != schema.FsMetaDefault &&
		mountInfo.CacheConfig.MetaDriver != schema.FsMetaMemory &&
		mountInfo.CacheConfig.CacheDir != "" {
		var cachePath string
		if mountInfo.FS.IndependentMountProcess {
			cachePath = mountInfo.CacheConfig.CacheDir
		} else {
			cachePath = FusePodCachePath + MetaCacheDir
		}
		options = append(options, fmt.Sprintf("--%s=%s", "meta-cache-path", cachePath))
	}

	// s3 default mount permission
	if mountInfo.FS.Type == common.S3Type {
		if mountInfo.FS.PropertiesMap[common.FileMode] != "" {
			options = append(options, fmt.Sprintf("--%s=%s", "file-mode", mountInfo.FS.PropertiesMap[common.FileMode]))
		} else {
			options = append(options, fmt.Sprintf("--%s=%s", "file-mode", "0666"))
		}
		if mountInfo.FS.PropertiesMap[common.DirMode] != "" {
			options = append(options, fmt.Sprintf("--%s=%s", "dir-mode", mountInfo.FS.PropertiesMap[common.DirMode]))
		} else {
			options = append(options, fmt.Sprintf("--%s=%s", "dir-mode", "0777"))
		}
	}
	return options
}

func (mountInfo *Info) CacheWorkerCmd() string {
	options := []string{
		"--server=" + csiconfig.PaddleFlowServer,
		"--username=" + csiconfig.UserNameRoot,
		"--password=" + csiconfig.PassWordRoot,
		"--nodename=" + csiconfig.NodeName,
		"--clusterID=" + csiconfig.ClusterID,
		"--podCachePath=" + FusePodCachePath,
		"--cacheDir=" + mountInfo.CacheConfig.CacheDir,
		"--fsID=" + mountInfo.FS.ID,
	}
	return CacheWorkerBin + " " + strings.Join(options, " ")
}
