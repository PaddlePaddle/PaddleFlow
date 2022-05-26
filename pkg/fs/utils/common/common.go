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

package common

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/middleware"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	VolumePluginName       = "kubernetes.io~csi"
	volumePathLastDir      = "mount"
	volumeMountPathLastDir = "source"

	KubeletDataPathEnv        = "KUBELET_DATA_PATH"
	NotRootUserEnableEnv      = "NOT_ROOT_USER_ENABLE"
	MountPointIntervalTimeEnv = "MOUNT_POINT_INTERVAL_TIME"
	PodsHandleConcurrencyEnv  = "PODS_HANDLE_CONCURRENCY"
	PodUpdateIntervalTimeEnv  = "POD_UPDATE_INTERVAL_TIME"
	DefaultUIDEnv             = "DEFAULT_UID_ENV"
	DefaultGIDEnv             = "DEFAULT_GID_ENV"
	K8SConfigPathEnv          = "K8S_CONFIG_PATH"
	K8SClientTimeoutEnv       = "K8S_CLIENT_TIMEOUT"

	DefaultKubeletDataPath       = "/var/lib/kubelet"
	DefaultNOTROOTUserEnable     = true
	DefaultK8SConfigPath         = "/home/paddleflow/config/kube.config"
	DefaultCheckIntervalTime     = 15
	DefaultK8SClientTimeout      = 0
	DefaultPodsHandleConcurrency = 10
	DefaultUpdateIntervalTime    = 15
	DefaultUID                   = 601
	DefaultGID                   = 601
)

// GetVolumeMountPath default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/mount
func GetVolumeMountPath(pathPrefix string) string {
	return filepath.Join(pathPrefix, volumePathLastDir)
}

// GetVolumeSourceMountPath default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/source
func GetVolumeSourceMountPath(pathPrefix string) string {
	return filepath.Join(pathPrefix, volumeMountPathLastDir)
}

// GetSourceMountPathByPod default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/source
func GetSourceMountPathByPod(podUID string, volumeName string) string {
	return fmt.Sprintf("%s/pods/%s/volumes/%s/%s/source", GetKubeletDataPath(),
		podUID, VolumePluginName, volumeName)
}

// GetVolumeBindMountPathByPod default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/mount
func GetVolumeBindMountPathByPod(podUID, volumeName string) string {
	return fmt.Sprintf("%s/pods/%s/volumes/%s/%s/mount", GetKubeletDataPath(),
		podUID, VolumePluginName, volumeName)
}

func GetKubeletDataPath() string {
	path := os.Getenv(KubeletDataPathEnv)
	if len(path) == 0 {
		return DefaultKubeletDataPath
	}
	return path
}

func GetPodUIDFromTargetPath(targetPath string) string {
	prefix := GetKubeletDataPath()
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	items := strings.Split(strings.TrimPrefix(targetPath, prefix), "/")
	if len(items) > 0 {
		return items[0]
	}
	return ""
}

func GetDefaultUID() int {
	uid := os.Getenv(DefaultUIDEnv)
	if len(uid) == 0 {
		return DefaultUID
	}

	uidInt, err := strconv.Atoi(uid)
	if err != nil {
		return DefaultUID
	}
	return uidInt
}

func GetDefaultGID() int {
	gid := os.Getenv(DefaultGIDEnv)
	if len(gid) == 0 {
		return DefaultGID
	}

	gidInt, err := strconv.Atoi(gid)
	if err != nil {
		return DefaultGID
	}
	return gidInt
}

func GetK8SConfigPathEnv() string {
	path := os.Getenv(K8SConfigPathEnv)
	if len(path) == 0 {
		return DefaultK8SConfigPath
	}
	return path
}

func GetK8STimeoutEnv() int {
	timeout := os.Getenv(K8SClientTimeoutEnv)
	if len(timeout) == 0 {
		return DefaultK8SClientTimeout
	}
	timeoutInt, err := strconv.Atoi(timeout)
	if err != nil {
		return DefaultK8SClientTimeout
	}
	return timeoutInt
}

func GetPodsHandleConcurrency() int {
	concurrency := os.Getenv(PodsHandleConcurrencyEnv)
	if len(concurrency) == 0 {
		return DefaultPodsHandleConcurrency
	}

	concurrencyInt, err := strconv.Atoi(concurrency)
	if err != nil {
		return DefaultPodsHandleConcurrency
	}
	return concurrencyInt
}

func GetPodsUpdateIntervalTime() int {
	defaultTime := DefaultUpdateIntervalTime
	interval := os.Getenv(PodUpdateIntervalTimeEnv)
	checkTime := GetMountPointCheckIntervalTime()
	if len(interval) != 0 {
		intervalInt, err := strconv.Atoi(interval)
		if err == nil && intervalInt > checkTime {
			return intervalInt
		}
	}
	return defaultTime
}

func GetMountPointCheckIntervalTime() int {
	interval := os.Getenv(MountPointIntervalTimeEnv)
	if len(interval) == 0 {
		return DefaultCheckIntervalTime
	}

	intervalInt, err := strconv.Atoi(interval)
	if err != nil {
		return DefaultCheckIntervalTime
	}
	return intervalInt
}

func GetRootToken(ctx *logger.RequestContext) (string, error) {
	u, err := models.GetUserByName(ctx, common.RootKey)
	if err != nil {
		return "", err
	}
	token, err := middleware.GenerateToken(u.Name, u.Password)
	if err != nil {
		return "", err
	}
	return token, err
}

func GetRandID(randNum int) string {
	b := make([]byte, randNum/2)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func FsIDToFsNameUsername(fsID string) (fsName, username string) {
	fsArr := strings.Split(fsID, "-")
	if len(fsArr) < 3 {
		return "", ""
	}
	fsName = fsArr[len(fsArr)-1]
	username = strings.Join(fsArr[1:len(fsArr)-1], "")
	return
}
