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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"

	"paddleflow/pkg/client"
	"paddleflow/pkg/common/http/api"
	"paddleflow/pkg/common/http/core"
	"paddleflow/pkg/fs/common"
	"paddleflow/pkg/fs/csiplugin/client/k8s"
	"paddleflow/pkg/fs/csiplugin/client/pfs"
	"paddleflow/pkg/fs/csiplugin/csiconfig"
	utils "paddleflow/pkg/fs/utils/common"
	"paddleflow/pkg/fs/utils/io"
	mountUtil "paddleflow/pkg/fs/utils/mount"
)

const (
	VolumesKeyMount     = "pfs-mount"
	VolumesKeyDataCache = "data-cache"
	VolumesKeyMetaCache = "meta-cache"
	HostPathMnt         = "/data/paddleflow-fs/mnt"
	MountDir            = "/home/paddleflow/mnt"
	MountPoint          = MountDir + "/storage"
	CachePath           = "/home/paddleflow/pfs-cache"
	DataCacheDir        = "/data-cache"
	MetaCacheDir        = "/meta-cache"
)

type PodMount struct {
	K8sClient *k8s.K8SInterface
}

var umountLock sync.RWMutex

func UMount(volumeID, targetPath string, mountInfo pfs.MountInfo) error {
	podName := GeneratePodNameByFsID(volumeID)
	log.Infof("umount pod name is %s", podName)
	umountLock.Lock()
	defer umountLock.Unlock()
	podUID := utils.GetPodUIDFromTargetPath(targetPath)
	if podUID != "" {
		// clean up mount points
		pathsToCleanup := []string{targetPath}
		sourcePath := utils.GetVolumeSourceMountPath(filepath.Dir(targetPath))
		pathsToCleanup = append(pathsToCleanup, sourcePath)
		if err := cleanUpMountPoints(pathsToCleanup); err != nil {
			log.Errorf("UMount: cleanup mount points[%v] err: %s", pathsToCleanup, err.Error())
			return err
		}
	}

	k8sClient, err := k8s.GetK8sClient()
	if err != nil {
		log.Errorf("Umount: Get k8s client failed: %v", err)
		return err
	}
	pod, err := k8sClient.GetPod(podName, csiconfig.Namespace)
	if err != nil && !k8serrors.IsNotFound(err) {
		log.Errorf("Umount: Get pod %s err: %v", podName, err)
		return err
	}
	// if mount pod not exists.
	if pod == nil {
		log.Infof("Umount: Mount pod %s not exists.", podName)
		return nil
	}

	mountInfo.Server = pod.Annotations["server"]
	mountInfo.FSID = pod.Annotations["fsID"]
	mountInfo.TargetPath = targetPath
	if mountInfo.Server == "" || mountInfo.FSID == "" {
		log.Errorf("UMount: pod[%s] annotations[%v] missing field", pod.Name, pod.Annotations)
		return fmt.Errorf("pod[%s] annotations[%v] missing field", pod.Name, pod.Annotations)
	}

	httpClient := client.NewHttpClient(mountInfo.Server, client.DefaultTimeOut)
	login := api.LoginParams{
		UserName: mountInfo.UsernameRoot,
		Password: mountInfo.PasswordRoot,
	}
	loginResponse, err := api.LoginRequest(login, httpClient)
	if err != nil {
		log.Errorf("UMount: login failed: %v", err)
		return err
	}

	fsMountListResp, err := fsMountList(mountInfo, httpClient, loginResponse.Authorization)
	if err != nil {
		log.Errorf("UMount: fsMountList faield: %v", err)
		return err
	}

	if len(fsMountListResp.MountList) <= 1 {
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			errDelete := k8sClient.DeletePod(pod)
			if k8serrors.IsNotFound(errDelete) {
				log.Infof("DeletePod : pod %s not exists.", pod.Name)
				return nil
			}
			if errDelete != nil {
				return errDelete
			}
			return nil
		})
		if err != nil {
			log.Errorf("DeletePod: podName[%s] err[%v]", pod.Name, err)
			return err
		}
	}
	if len(fsMountListResp.MountList) >= 1 {
		errDeleteMount := deleteMount(mountInfo, httpClient, loginResponse.Authorization)
		if errDeleteMount != nil {
			log.Errorf("DeleteMount: mountInfo[%+v] err[%v]", mountInfo, err)
			return errDeleteMount
		}
	}
	return nil
}

func MountThroughPod(volumeID string, mountInfo pfs.MountInfo) error {
	if err := createOrAddRef(volumeID, mountInfo); err != nil {
		log.Errorf("MountThroughPod info: %+v err: %v", mountInfo, err)
		return err
	}
	return waitUtilPodReady(GeneratePodNameByFsID(volumeID))
}

func createOrAddRef(volumeID string, mountInfo pfs.MountInfo) error {
	podName := GeneratePodNameByFsID(volumeID)
	log.Infof("pod name is %s", podName)

	httpClient := client.NewHttpClient(mountInfo.Server, client.DefaultTimeOut)
	login := api.LoginParams{
		UserName: mountInfo.UsernameRoot,
		Password: mountInfo.PasswordRoot,
	}
	loginResponse, err := api.LoginRequest(login, httpClient)
	if err != nil {
		log.Errorf("createOrAddRef: login failed: %v", err)
		return err
	}

	for i := 0; i < 120; i++ {
		k8sClient, err := k8s.GetK8sClient()
		if err != nil {
			log.Errorf("get k8s client failed: %v", err)
			return err
		}
		// wait for old pod deleted
		oldPod, errGetPod := k8sClient.GetPod(podName, csiconfig.Namespace)
		if errGetPod == nil && oldPod.DeletionTimestamp != nil {
			log.Infof("createOrAddRef: wait for old mount pod deleted.")
			time.Sleep(time.Millisecond * 500)
			continue
		} else if errGetPod != nil {
			if k8serrors.IsNotFound(errGetPod) {
				// pod not exist, create
				log.Infof("createOrAddRef: Need to create pod %s.", podName)
				if createPodErr := createMountPod(k8sClient, httpClient,
					volumeID, loginResponse.Authorization, mountInfo); createPodErr != nil {
					return createPodErr
				}
			} else {
				// unexpect error
				log.Errorf("createOrAddRef: Get pod %s err: %v", podName, errGetPod)
				return errGetPod
			}
		}
		return addRefOfMount(mountInfo, httpClient, loginResponse.Authorization)
	}
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min", mountInfo.FSID, podName)
}

func createMountPod(k8sClient k8s.K8SInterface, httpClient *core.PFClient, volumeID, token string,
	mountInfo pfs.MountInfo) error {
	// get config
	cacheConfig, err := fsCacheConfig(mountInfo, httpClient, token)
	if err != nil {
		if strings.Contains(err.Error(), gorm.ErrRecordNotFound.Error()) {
			log.Infof("fs[%s] has not set cacheConfig. mount with default settings.", mountInfo.FSID)
		} else {
			log.Errorf("get fs[%s] cacheConfig from pfs server[%s] failed: %v",
				mountInfo.FSID, mountInfo.Server, err)
			return err
		}
	}
	// create pod
	newPod := BuildMountPod(volumeID, mountInfo, cacheConfig)
	newPod = BuildCacheWorkerPod(newPod, mountInfo)
	_, err = k8sClient.CreatePod(newPod)
	if err != nil {
		log.Errorf("createMount: Create pod for fsID %s err: %v", mountInfo.FSID, err)
		return err
	}
	return nil
}

func GeneratePodNameByFsID(volumeID string) string {
	return fmt.Sprintf("pfs-%s-%s", csiconfig.NodeName, volumeID)
}

func waitUtilPodReady(podName string) error {
	k8sClient, err := k8s.GetK8sClient()
	if err != nil {
		log.Errorf("get k8s client failed: %v", err)
		return err
	}
	// Wait until the mount pod is ready
	for i := 0; i < 60; i++ {
		pod, err := k8sClient.GetPod(podName, csiconfig.Namespace)
		if err != nil {
			return status.Errorf(codes.Internal, "waitUtilPodReady: Get pod %v failed: %v", podName, err)
		}
		if isPodReady(pod) {
			log.Infof("waitUtilPodReady: Pod %v is successful", podName)
			return nil
		}
		time.Sleep(time.Millisecond * 500)
	}
	podLog, err := getErrContainerLog(k8sClient, podName)
	if err != nil {
		log.Errorf("waitUtilPodReady: get pod %s log error %v", podName, err)
	}
	return status.Errorf(codes.Internal, "waitUtilPodReady: mount pod %s isn't ready in 30 seconds: %v", podName, podLog)
}

func isPodReady(pod *corev1.Pod) bool {
	conditionsTrue := 0
	for _, cond := range pod.Status.Conditions {
		if cond.Status == corev1.ConditionTrue && (cond.Type == corev1.ContainersReady || cond.Type == corev1.PodReady) {
			conditionsTrue++
		}
	}
	return conditionsTrue == 2
}

func getErrContainerLog(K8sClient k8s.K8SInterface, podName string) (log string, err error) {
	pod, err := K8sClient.GetPod(podName, csiconfig.Namespace)
	if err != nil {
		return
	}
	for _, cn := range pod.Status.InitContainerStatuses {
		if !cn.Ready {
			log, err = K8sClient.GetPodLog(pod.Name, pod.Namespace, cn.Name)
			return
		}
	}
	for _, cn := range pod.Status.ContainerStatuses {
		if !cn.Ready {
			log, err = K8sClient.GetPodLog(pod.Name, pod.Namespace, cn.Name)
			return
		}
	}
	return
}

func BuildCacheWorkerPod(pod *v1.Pod, mountInfo pfs.MountInfo) *v1.Pod {
	cmd := getCacheWorkerCmd(mountInfo)
	pod.Spec.Containers[1].Command = []string{"sh", "-c", cmd}
	mp := corev1.MountPropagationBidirectional
	volumeMounts := []corev1.VolumeMount{
		{
			Name:             VolumesKeyDataCache,
			MountPath:        CachePath + DataCacheDir,
			MountPropagation: &mp,
		},
		{
			Name:             VolumesKeyMetaCache,
			MountPath:        CachePath + MetaCacheDir,
			MountPropagation: &mp,
		},
	}
	pod.Spec.Containers[1].VolumeMounts = volumeMounts
	return pod
}

func BuildMountPod(volumeID string, mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) *v1.Pod {
	pod := csiconfig.GeneratePodTemplate()
	pod.Name = GeneratePodNameByFsID(volumeID)
	cmd := getcmd(mountInfo, cacheConf)
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}
	pod.Annotations = make(map[string]string)
	pod.Annotations["server"] = mountInfo.Server
	pod.Annotations["fsID"] = mountInfo.FSID

	if cacheConf.CacheDir == "" {
		cacheConf.CacheDir = HostPathMnt + "/" + mountInfo.FSID
	}

	typeDir := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: VolumesKeyDataCache,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: cacheConf.CacheDir + DataCacheDir,
					Type: &typeDir,
				},
			},
		},
		{
			Name: VolumesKeyMetaCache,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: cacheConf.CacheDir + MetaCacheDir,
					Type: &typeDir,
				},
			},
		},
		{
			Name: VolumesKeyMount,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: HostPathMnt,
					Type: &typeDir,
				},
			},
		},
	}
	mp := corev1.MountPropagationBidirectional
	volumeMounts := []corev1.VolumeMount{
		{
			Name:             VolumesKeyDataCache,
			MountPath:        CachePath + DataCacheDir,
			MountPropagation: &mp,
		},
		{
			Name:             VolumesKeyMetaCache,
			MountPath:        CachePath + MetaCacheDir,
			MountPropagation: &mp,
		},
		{
			Name:             VolumesKeyMount,
			MountPath:        MountDir,
			SubPath:          mountInfo.FSID,
			MountPropagation: &mp,
		},
	}
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	return pod
}

func getcmd(mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) string {
	mkdir := "mkdir -p " + MountPoint + ";"
	pfsMountPath := "/home/paddleflow/pfs-fuse mount "
	mountPath := "--mount-point=" + MountPoint + " "
	options := []string{
		"--server=" + mountInfo.Server,
		"--user-name=" + mountInfo.UsernameRoot,
		"--password=" + mountInfo.PasswordRoot,
		"--block-size=" + strconv.Itoa(cacheConf.BlockSize),
		"--fs-id=" + mountInfo.FSID,
		"--data-disk-cache-path=" + CachePath + DataCacheDir,
		"--meta-path=" + CachePath + MetaCacheDir,
	}
	cmd := mkdir + pfsMountPath + mountPath + strings.Join(options, " ")
	return cmd
}

func getCacheWorkerCmd(mountInfo pfs.MountInfo) string {
	cacheWorker := "/home/paddleflow/cache-worker "
	options := []string{
		"--server=" + mountInfo.Server,
		"--username=" + mountInfo.UsernameRoot,
		"--password=" + mountInfo.PasswordRoot,
		"--cacheDir=" + CachePath,
		"--nodename=" + csiconfig.NodeName,
		"--clusterID=" + mountInfo.ClusterID,
		"--fsID=" + mountInfo.FSID,
	}
	return cacheWorker + strings.Join(options, " ")
}

func cleanUpMountPoints(paths []string) error {
	var retErr error

	if len(paths) == 0 {
		return nil
	}

	cleanUp := func(path string, cleanAll bool) error {
		isMountPoint, err := mountUtil.IsMountPoint(path)
		if err != nil && !isMountPoint {
			if exist, exErr := io.Exist(path); exErr != nil {
				log.Errorf("check path[%s] exist failed: %v", path, exErr)
				return exErr
			} else if !exist {
				return nil
			}
			log.Errorf("check path[%s] mountpoint failed: %v", path, err)
			return err
		}

		if isMountPoint {
			return mountUtil.CleanUpMountPoint(path)
		}
		log.Infof("path [%s] is not a mountpoint, begin to remove path[%s]", path, path)
		if err := os.Remove(path); err != nil {
			log.Errorf("remove path[%s] failed: %v", path, err)
			return err
		}
		return nil
	}

	for _, path := range paths {
		log.Infof("cleanup mountPoint in path[%s] start", path)
		if err := cleanUp(path, false); err != nil {
			log.Errorf("cleanup path[%s] failed: %v", path, err)
			retErr = err
		}
	}
	return retErr
}
