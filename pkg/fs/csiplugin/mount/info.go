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
	ReadOnly    bool
	K8sClient   utils.Client
}

func ConstructMountInfo(fsInfoBase64, fsCacheBase64, targetPath string, k8sClient utils.Client, readOnly bool) (Info, error) {
	// FS info
	fs, err := utils.ProcessFSInfo(fsInfoBase64)
	if err != nil {
		retErr := fmt.Errorf("FSprocess FS info err: %v", err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}

	// FS CacheConfig config
	cacheConfig, err := utils.ProcessCacheConfig(fsCacheBase64)
	if err != nil {
		retErr := fmt.Errorf("FS process FS CacheConfig err: %v", err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}

	info := Info{
		CacheConfig: cacheConfig,
		FS:          fs,
		FSBase64Str: fsInfoBase64,
		TargetPath:  targetPath,
		ReadOnly:    readOnly,
		K8sClient:   k8sClient,
	}
	info.Cmd, info.Args = info.cmdAndArgs()
	return info, nil
}

func (mountInfo *Info) cmdAndArgs() (string, []string) {
	if mountInfo.FS.Type == common.GlusterFSType {
		return mountName, mountInfo.glusterArgs()
	} else if mountInfo.FS.IndependentMountProcess {
		return PfsFuseIndependentMountProcessCMDName, mountInfo.processMountArgs()
	} else {
		return pfsFuseMountPodCMDName, mountInfo.podMountArgs()
	}
}

func (mountInfo *Info) glusterArgs() (args []string) {
	args = append(args, "-t", mountInfo.FS.Type,
		strings.Join([]string{mountInfo.FS.ServerAddress, mountInfo.FS.SubPath}, ":"), mountInfo.TargetPath)
	return args
}

func (mountInfo *Info) processMountArgs() (args []string) {
	args = append(args, fmt.Sprintf("--%s=%s", "mount-point", mountInfo.TargetPath))
	args = append(args, mountInfo.commonOptions()...)
	args = append(args, mountInfo.cachePathArgs(true)...)
	return args
}

func (mountInfo *Info) podMountArgs() (args []string) {
	args = append(args, fmt.Sprintf("--%s=%s", "mount-point", FusePodMountPoint))
	args = append(args, mountInfo.commonOptions()...)
	args = append(args, mountInfo.cachePathArgs(false)...)
	return args
}

func (mountInfo *Info) cachePathArgs(independentProcess bool) (args []string) {
	cacheDir := ""
	if independentProcess {
		cacheDir = mountInfo.CacheConfig.CacheDir
	} else {
		cacheDir = FusePodCachePath
	}

	if mountInfo.CacheConfig.CacheDir != "" {
		args = append(args, fmt.Sprintf("--%s=%s", "data-cache-path", cacheDir+DataCacheDir))
	}
	if mountInfo.CacheConfig.MetaDriver != schema.FsMetaDefault &&
		mountInfo.CacheConfig.MetaDriver != schema.FsMetaMemory &&
		mountInfo.CacheConfig.CacheDir != "" {
		args = append(args, fmt.Sprintf("--%s=%s", "meta-cache-path", cacheDir+MetaCacheDir))
	}
	return args
}

func (mountInfo *Info) commonOptions() []string {
	var options []string
	options = append(options, fmt.Sprintf("--%s=%s", "fs-id", mountInfo.FS.ID))
	options = append(options, fmt.Sprintf("--%s=%s", "fs-info", mountInfo.FSBase64Str))

	if mountInfo.ReadOnly {
		options = append(options, fmt.Sprintf("--%s=%s", "mount-options", ReadOnly))
	}

	if mountInfo.CacheConfig.BlockSize > 0 {
		options = append(options, fmt.Sprintf("--%s=%d", "block-size", mountInfo.CacheConfig.BlockSize))
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
