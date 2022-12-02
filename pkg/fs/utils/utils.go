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

package utils

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/middleware"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
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
	DefaultCheckIntervalTime     = 15
	DefaultK8SClientTimeout      = 0
	DefaultPodsHandleConcurrency = 10
	DefaultUpdateIntervalTime    = 15
	DefaultUID                   = 601
	DefaultGID                   = 601
)

var p *process.Process

func init() {
	// NewProcess 会返回一个持有PID的Process对象，方法会检查PID是否存在，如果不存在会返回错误
	// 通过Process对象上定义的其他方法我们可以获取关于进程的各种信息。
	var err error
	p, err = process.NewProcess(int32(os.Getpid()))
	if err != nil {
		panic(err)
	}
}

// GetVolumeMountPath default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/mount
func GetVolumeMountPath(pathPrefix string) string {
	return filepath.Join(pathPrefix, volumePathLastDir)
}

// GetVolumeSourceMountPath default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/source
func GetVolumeSourceMountPath(pathPrefix string) string {
	return filepath.Join(pathPrefix, volumeMountPathLastDir)
}

// GetSourceMountPathByPod default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/source
func GetSourceMountPathByPod(podUID, volumeName string) string {
	return fmt.Sprintf("%s/pods/%s/volumes/%s/%s/source", GetKubeletDataPath(),
		podUID, VolumePluginName, volumeName)
}

// GetVolumeBindMountPathByPod default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/mount
func GetVolumeBindMountPathByPod(podUID, volumeName string) string {
	return fmt.Sprintf("%s/pods/%s/volumes/%s/%s/mount", GetKubeletDataPath(),
		podUID, VolumePluginName, volumeName)
}

func GetSubPathSourcePath(volumePath, subpath string) string {
	return filepath.Join(volumePath, subpath)
}

// default value: /var/lib/kubelet/pods/{podUID}/volumes-subpaths/{volumeName}/{containerName}/{volumeMountIndex}
func GetSubPathTargetPath(podUID string, volumeName string, containerName string, volumeMountIndex int) string {
	return fmt.Sprintf("%s/pods/%s/volume-subpaths/%s/%s/%d", GetKubeletDataPath(),
		podUID, volumeName, containerName, volumeMountIndex)
}

// default value: /var/lib/kubelet/pods/{podUID}/volumes/{volumePluginName}/{volumeName}/source
func GetSourceMountPath(pathPrefix string) string {
	return filepath.Join(pathPrefix, "source")
}

func GetKubeletDataPath() string {
	path := os.Getenv(KubeletDataPathEnv)
	if len(path) == 0 {
		return DefaultKubeletDataPath
	}
	return path
}

func GetPodUIDFromTargetPath(targetPath string) string {
	// target path: /var/lib/kubelet/pods/cb0b4bb0-98de-4cd5-9d73-146a226dcf93/volumes/kubernetes.io~csi/pfs-fs-root-mxy-default-pv/mount
	prefix := GetKubeletDataPath()
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	prefix += "pods/"
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
	return os.Getenv(K8SConfigPathEnv)
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
	u, err := storage.Auth.GetUserByName(ctx, common.RootKey)
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

func GetFsNameAndUserNameByFsID(fsID string) (fsName, username string, err error) {
	fsArray := strings.Split(fsID, "-")
	if len(fsArray) < 3 {
		err = fmt.Errorf("fsID[%s] is not valid", fsID)
		return
	}
	if len(fsArray) > 3 {
		// such as fs-root-v-xxxx
		fsName = strings.Join(fsArray[2:], "-")
		username = fsArray[1]
		return
	}
	username = fsArray[1]
	fsName = fsArray[2]
	return
}

func ProcessFSInfo(fsInfoBase64 string) (model.FileSystem, error) {
	fsInfoByte, err := base64.StdEncoding.DecodeString(fsInfoBase64)
	if err != nil {
		return model.FileSystem{}, err
	}
	fs := model.FileSystem{}
	if err = json.Unmarshal(fsInfoByte, &fs); err != nil {
		return model.FileSystem{}, err
	}
	if fs.ID == "" ||
		fs.Type == "" ||
		fs.ServerAddress == "" {
		err = fmt.Errorf("processFsInfo failed as id or type of server address empty")
		return model.FileSystem{}, err
	}
	return fs, nil
}

func ProcessCacheConfig(fsCacheBase64 string) (model.FSCacheConfig, error) {
	fsCacheByte, err := base64.StdEncoding.DecodeString(fsCacheBase64)
	if err != nil {
		return model.FSCacheConfig{}, err
	}
	cacheConfig := model.FSCacheConfig{}
	if err = json.Unmarshal(fsCacheByte, &cacheConfig); err != nil {
		return model.FSCacheConfig{}, err
	}
	return cacheConfig, nil
}

func GetSysCpuPercent() float64 {
	percent, _ := cpu.Percent(time.Second, false)
	return percent[0]
}

func GetSysMemPercent() (uint64, float64) {
	memInfo, _ := mem.VirtualMemory()
	return memInfo.Available / 1024 / 1024, memInfo.UsedPercent
}

func GetProcessCPUPercent() float64 {
	cpuPercent, err := p.Percent(time.Second)
	if err != nil {
		log.Errorf("GetProcessCPUPercent error %v", err)
		return 0
	}
	return cpuPercent
}

func GetProcessMemPercent() float32 {
	mp, _ := p.MemoryPercent()
	return mp
}

func GetDiskPercent() float64 {
	parts, _ := disk.Partitions(true)
	diskInfo, _ := disk.Usage(parts[0].Mountpoint)
	return diskInfo.UsedPercent
}
