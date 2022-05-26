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

	"github.com/PaddlePaddle/PaddleFlow/pkg/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/client/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/client/pfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	utils "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	mountUtil "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/mount"
)

const (
	VolumesKeyMount     = "pfs-mount"
	VolumesKeyDataCache = "data-cache"
	VolumesKeyMetaCache = "meta-cache"
	MountDir            = "/home/paddleflow/mnt"
	CacheWorkerBin      = "/home/paddleflow/cache-worker"
	MountPoint          = MountDir + "/storage"
	CachePath           = "/home/paddleflow/pfs-cache"
	DataCacheDir        = "/data-cache"
	MetaCacheDir        = "/meta-cache"

	AnnoKeyServer = "server"
	AnnoKeyFsID   = "fsID"
)

var umountLock sync.RWMutex

func PodUnmount(volumeID, targetPath string, mountInfo pfs.MountInfo) error {
	podName := GeneratePodNameByFsID(volumeID)
	log.Infof("PodUnmount pod name is %s", podName)
	umountLock.Lock()
	defer umountLock.Unlock()

	k8sClient, err := k8s.GetK8sClient()
	if err != nil {
		log.Errorf("PodUnmount: Get k8s client failed: %v", err)
		return err
	}
	pod, err := k8sClient.GetPod(podName, csiconfig.Namespace)
	if err != nil && !k8serrors.IsNotFound(err) {
		log.Errorf("PodUnmount: Get pod %s err: %v", podName, err)
		return err
	}
	// if mount pod not exists.
	if pod == nil {
		log.Infof("PodUnmount: Mount pod %s not exists.", podName)
		return nil
	}

	mountInfo.Server = pod.Annotations[AnnoKeyServer]
	mountInfo.FSID = pod.Annotations[AnnoKeyFsID]
	mountInfo.TargetPath = targetPath
	if mountInfo.Server == "" || mountInfo.FSID == "" {
		log.Errorf("PodUnmount: pod[%s] annotations[%v] missing field", pod.Name, pod.Annotations)
		return fmt.Errorf("PodUnmount: pod[%s] annotations[%v] missing field", pod.Name, pod.Annotations)
	}

	httpClient := client.NewHttpClient(mountInfo.Server, client.DefaultTimeOut)
	login := api.LoginParams{
		UserName: mountInfo.UsernameRoot,
		Password: mountInfo.PasswordRoot,
	}
	loginResponse, err := api.LoginRequest(login, httpClient)
	if err != nil {
		log.Errorf("PodUnmount: login failed: %v", err)
		return err
	}

	fsMountListResp, err := listMount(mountInfo, httpClient, loginResponse.Authorization)
	if err != nil {
		log.Errorf("PodUnmount: fsMountList faield: %v", err)
		return err
	}

	podUID := utils.GetPodUIDFromTargetPath(targetPath)
	if podUID != "" {
		// clean up mount points
		pathsToCleanup := []string{targetPath}
		if err := mountUtil.CleanUpMountPoints(pathsToCleanup); err != nil {
			log.Errorf("PodUnmount: cleanup mount points[%v] err: %s", pathsToCleanup, err.Error())
			return err
		}
	}

	if len(fsMountListResp.MountList) <= 1 {
		err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
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
			log.Errorf("PodUMount: DeleteMount mountInfo[%+v] err[%v]", mountInfo, err)
			return errDeleteMount
		}
	}
	return nil
}

func PodMount(volumeID string, mountInfo pfs.MountInfo) error {
	// login server
	httpClient := client.NewHttpClient(mountInfo.Server, client.DefaultTimeOut)
	login := api.LoginParams{
		UserName: mountInfo.UsernameRoot,
		Password: mountInfo.PasswordRoot,
	}
	loginResponse, err := api.LoginRequest(login, httpClient)
	if err != nil {
		log.Errorf("PodMount: login failed: %v", err)
		return err
	}
	token := loginResponse.Authorization
	// validate fs
	if _, err := getFs(mountInfo.FSID, httpClient, token); err != nil {
		log.Errorf("PodMount: validate fs exist [%s] err: %v", mountInfo.FSID, err)
		return err
	}
	// create mount pod or add ref in server db
	if err := createOrAddRef(httpClient, token, volumeID, mountInfo); err != nil {
		log.Errorf("PodMount: info: %+v err: %v", mountInfo, err)
		return err
	}
	return waitUtilPodReady(GeneratePodNameByFsID(volumeID))
}

func createOrAddRef(httpClient *core.PaddleFlowClient, token, volumeID string, mountInfo pfs.MountInfo) error {
	podName := GeneratePodNameByFsID(volumeID)
	log.Infof("pod name is %s", podName)

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
				if createPodErr := createMountPod(k8sClient, httpClient, token,
					volumeID, mountInfo); createPodErr != nil {
					return createPodErr
				}
			} else {
				// unexpect error
				log.Errorf("createOrAddRef: Get pod %s err: %v", podName, errGetPod)
				return errGetPod
			}
		}
		return addRefOfMount(mountInfo, httpClient, token)
	}
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min", mountInfo.FSID, podName)
}

func addRefOfMount(mountInfo pfs.MountInfo, httpClient *core.PaddleFlowClient, token string) error {
	listMountResp, err := listMount(mountInfo, httpClient, token)
	if err != nil {
		log.Errorf("addRefOfMount: listMount faield: %v", err)
		return err
	}

	for _, mountRecord := range listMountResp.MountList {
		if mountRecord.MountPoint == mountInfo.TargetPath {
			log.Infof("addRefOfMount: mount record already in db: %+v. no need to insert again.", mountRecord)
			return nil
		}
	}
	return createMount(mountInfo, httpClient, token)
}

func createMountPod(k8sClient k8s.K8SInterface, httpClient *core.PaddleFlowClient, token, volumeID string,
	mountInfo pfs.MountInfo) error {
	// get config
	cacheConfig, err := fsCacheConfig(mountInfo, httpClient, token)
	if err != nil {
		if strings.Contains(err.Error(), gorm.ErrRecordNotFound.Error()) {
			log.Infof("fs[%s] has not set cacheConfig. mount with default settings.", mountInfo.FSID)
			cacheConfig = defaultCacheConfig(mountInfo.FSID)
		} else {
			log.Errorf("get fs[%s] cacheConfig from pfs server[%s] failed: %v",
				mountInfo.FSID, mountInfo.Server, err)
			return err
		}
	} else {
		completeCacheConfig(&cacheConfig, mountInfo.FSID)
	}
	// create pod
	newPod := BuildMountPod(volumeID, mountInfo, cacheConfig)
	_, err = k8sClient.CreatePod(newPod)
	if err != nil {
		log.Errorf("createMount: Create pod for fsID %s err: %v", mountInfo.FSID, err)
		return err
	}
	return nil
}

func defaultCacheConfig(fsID string) common.FsCacheConfig {
	return common.FsCacheConfig{
		CacheDir:   schema.DefaultCacheDir(fsID),
		MetaDriver: schema.FsMetaDefault,
	}
}

func completeCacheConfig(config *common.FsCacheConfig, fsID string) {
	if config.CacheDir == "" {
		config.CacheDir = schema.DefaultCacheDir(fsID)
	}
	if config.MetaDriver == "" {
		config.MetaDriver = schema.FsMetaDefault
	}
	if config.FsName == "" || config.Username == "" {
		config.FsName, config.Username = utils.FsIDToFsNameUsername(fsID)
	}
}

func BuildMountPod(volumeID string, mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) *v1.Pod {
	pod := csiconfig.GeneratePodTemplate()
	pod.Name = GeneratePodNameByFsID(volumeID)
	buildMountContainer(pod, mountInfo, cacheConf)
	buildCacheWorkerContainer(pod, mountInfo, cacheConf)
	return pod
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

func buildCacheWorkerContainer(pod *v1.Pod, mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) {
	cmd := getCacheWorkerCmd(mountInfo, cacheConf)
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
}

func buildMountContainer(pod *v1.Pod, mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) {
	cmd := getMountCmd(mountInfo, cacheConf)
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}
	statCmd := "stat -c %i " + MountPoint
	pod.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
		Handler: corev1.Handler{
			Exec: &corev1.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"if [ x$(%v) = x1 ]; then exit 0; else exit 1; fi ", statCmd)},
			}},
		InitialDelaySeconds: 1,
		PeriodSeconds:       1,
	}
	pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
		PreStop: &corev1.Handler{
			Exec: &corev1.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"umount %s && rmdir %s", MountPoint, MountPoint)}},
		},
	}
	pod.Annotations = make(map[string]string)
	pod.Annotations[AnnoKeyServer] = mountInfo.Server
	pod.Annotations[AnnoKeyFsID] = mountInfo.FSID

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
					Path: schema.HostMntDir,
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
}

func getMountCmd(mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) string {
	mkdir := "mkdir -p " + MountPoint + ";"
	pfsMountPath := "/home/paddleflow/pfs-fuse mount "
	mountPath := "--mount-point=" + MountPoint + " "
	options := []string{
		"--server=" + mountInfo.Server,
		"--user-name=" + mountInfo.UsernameRoot,
		"--password=" + mountInfo.PasswordRoot,
		"--block-size=" + strconv.Itoa(cacheConf.BlockSize),
		"--fs-id=" + mountInfo.FSID,
		"--data-cache-path=" + CachePath + DataCacheDir,
		"--meta-cache-path=" + CachePath + MetaCacheDir,
		"--meta-cache-driver=" + cacheConf.MetaDriver,
	}
	cmd := mkdir + pfsMountPath + mountPath + strings.Join(options, " ")
	return cmd
}

func getCacheWorkerCmd(mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) string {
	options := []string{
		"--server=" + mountInfo.Server,
		"--username=" + mountInfo.UsernameRoot,
		"--password=" + mountInfo.PasswordRoot,
		"--podCachePath=" + CachePath,
		"--cacheDir=" + cacheConf.CacheDir,
		"--nodename=" + csiconfig.NodeName,
		"--clusterID=" + mountInfo.ClusterID,
		"--fsID=" + mountInfo.FSID,
	}
	return CacheWorkerBin + " " + strings.Join(options, " ")
}
