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
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	k8sCore "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/PaddlePaddle/PaddleFlow/pkg/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	utils "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/k8s"
	mountUtil "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/mount"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

const (
	VolumesKeyMount     = "pfs-mount"
	VolumesKeyDataCache = "data-cache"
	VolumesKeyMetaCache = "meta-cache"
	PodAnnoMTime        = "modifiedTime"

	CacheWorkerBin = "/home/paddleflow/cache-worker"
	MountPoint     = schema.PodMntDir + "/storage"
	CachePath      = "/home/paddleflow/pfs-cache"
	DataCacheDir   = "/data-cache"
	MetaCacheDir   = "/meta-cache"

	AnnoKeyServer = "server"
	AnnoKeyFsID   = "fsID"
)

var umountLock sync.RWMutex

func PodUnmount(volumeID string, mountInfo Info) error {
	podName := GeneratePodNameByVolumeID(volumeID)
	log.Infof("PodUnmount pod name is %s", podName)
	umountLock.Lock()
	defer umountLock.Unlock()

	k8sClient, err := k8s.GetK8sClient()
	if err != nil {
		log.Errorf("PodUnmount: Get k8s client failed: %v", err)
		return err
	}
	pod, err := k8sClient.GetPod(csiconfig.Namespace, podName)
	if err != nil && !k8sErrors.IsNotFound(err) {
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
	if mountInfo.Server == "" || mountInfo.FSID == "" {
		log.Errorf("PodUnmount: pod[%s] annotations[%v] missing field", pod.Name, pod.Annotations)
		return fmt.Errorf("PodUnmount: pod[%s] annotations[%v] missing field", pod.Name, pod.Annotations)
	}

	workPodUID := utils.GetPodUIDFromTargetPath(mountInfo.TargetPath)
	if workPodUID != "" {
		// clean up mount points
		pathsToCleanup := []string{mountInfo.TargetPath}
		if err := mountUtil.CleanUpMountPoints(pathsToCleanup); err != nil {
			log.Errorf("PodUnmount: cleanup mount points[%v] err: %s", pathsToCleanup, err.Error())
			return err
		}
		return removeRef(k8sClient, pod, workPodUID)
	}
	// TODO whether delete mount pod
	return nil
}

func PodMount(volumeID string, mountInfo Info) error {
	// login server
	httpClient, err := client.NewHttpClient(mountInfo.Server, client.DefaultTimeOut)
	if err != nil {
		return err
	}
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
	if err := createOrUpdatePod(httpClient, token, volumeID, mountInfo); err != nil {
		log.Errorf("PodMount: info: %+v err: %v", mountInfo, err)
		return err
	}
	return waitUtilPodReady(GeneratePodNameByVolumeID(volumeID))
}

func createOrUpdatePod(httpClient *core.PaddleFlowClient, token, volumeID string, mountInfo Info) error {
	podName := GeneratePodNameByVolumeID(volumeID)
	log.Infof("pod name is %s", podName)
	k8sClient, err := k8s.GetK8sClient()
	if err != nil {
		log.Errorf("get k8s client failed: %v", err)
		return err
	}
	for i := 0; i < 120; i++ {
		// wait for old pod deleted
		oldPod, errGetPod := k8sClient.GetPod(csiconfig.Namespace, podName)
		if errGetPod != nil {
			if k8sErrors.IsNotFound(errGetPod) {
				// mount pod not exist, create
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
		} else if oldPod.DeletionTimestamp != nil {
			// mount pod deleting. wait for 1 min.
			log.Infof("createOrAddRef: wait for old mount pod deleted.")
			time.Sleep(time.Millisecond * 500)
			continue
		} else {
			// mount pod exist, update annotation
			return addRef(k8sClient, oldPod, mountInfo.TargetPath)
		}
	}
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min", mountInfo.FSID, podName)
}

func addRef(c k8s.Client, pod *k8sCore.Pod, targetPath string) error {
	// annotate target path and modified time
	if err := annotatePod(pod, targetPath); err != nil {
		return err
	}
	// update pod
	updatedPod, err := c.UpdatePod(csiconfig.Namespace, pod)
	if err != nil {
		err := fmt.Errorf("updatePod[%s] anno err: %v", pod.Name, err)
		log.Errorf(err.Error())
		return err

	}
	log.Infof("updated pod[%s] anno: %v", updatedPod.Name, updatedPod.Annotations)
	return nil
}

func removeRef(c k8s.Client, pod *k8sCore.Pod, workPodUID string) error {
	ann := pod.ObjectMeta.Annotations
	if ann == nil {
		log.Warnf("removeRef: pod[%s] has no anno", pod.Name)
		return nil
	}
	delete(ann, workPodUID)
	ann[PodAnnoMTime] = time.Now().Format(model.TimeFormat)
	pod.ObjectMeta.Annotations = ann
	// update pod
	updatedPod, err := c.UpdatePod(csiconfig.Namespace, pod)
	if err != nil {
		err := fmt.Errorf("updatePod[%s] removeRef[%s] err: %v", pod.Name, workPodUID, err)
		log.Errorf(err.Error())
		return err

	}
	log.Infof("updated pod[%s] removeRef[%s]", updatedPod.Name, workPodUID)
	return nil
}

func createMountPod(k8sClient k8s.Client, httpClient *core.PaddleFlowClient, token, volumeID string,
	mountInfo Info) error {
	// get config
	cacheConfig, err := fsCacheConfig(mountInfo, httpClient, token)
	if err != nil && !strings.Contains(err.Error(), gorm.ErrRecordNotFound.Error()) {
		log.Errorf("get fs[%s] cacheConfig from pfs server[%s] failed: %v",
			mountInfo.FSID, mountInfo.Server, err)
		return err
	}
	completeCacheConfig(&cacheConfig, mountInfo.FSID)
	mountPod, err := buildMountPod(volumeID, mountInfo, cacheConfig)
	if err != nil {
		log.Errorf("createMount: buildMountPod[%s] err: %v", mountInfo.FSID, err)
		return err
	}
	_, err = k8sClient.CreatePod(mountPod)
	if err != nil {
		log.Errorf("createMount: Create pod for fsID %s err: %v", mountInfo.FSID, err)
		return err
	}
	return nil
}

func completeCacheConfig(config *common.FsCacheConfig, fsID string) {
	if config.CacheDir == "" {
		config.CacheDir = path.Join(csiconfig.HostMntDir, fsID)
	}
	if config.MetaDriver == "" {
		config.MetaDriver = schema.FsMetaDefault
	}
	if config.FsName == "" || config.Username == "" {
		config.FsName, config.Username = utils.FsIDToFsNameUsername(fsID)
	}
}

func buildMountPod(volumeID string, mountInfo Info, cacheConf common.FsCacheConfig) (*k8sCore.Pod, error) {
	pod := csiconfig.GeneratePodTemplate()
	pod.Name = GeneratePodNameByVolumeID(volumeID)
	buildMountContainer(pod, mountInfo, cacheConf)
	buildCacheWorkerContainer(pod, mountInfo, cacheConf)
	if err := annotatePod(pod, mountInfo.TargetPath); err != nil {
		return nil, err
	}
	return pod, nil
}

func annotatePod(pod *k8sCore.Pod, targetPath string) error {
	// annotate target path and modified time
	ann := pod.ObjectMeta.Annotations
	if ann == nil {
		ann = make(map[string]string)
	}
	workPodUID := utils.GetPodUIDFromTargetPath(targetPath)
	if workPodUID == "" {
		err := fmt.Errorf("mount pod[%s] failed obtain workPodUID from target path: %s", pod.Name, targetPath)
		log.Errorf(err.Error())
		return err
	}
	ann[workPodUID] = targetPath
	ann[PodAnnoMTime] = time.Now().Format(model.TimeFormat)
	pod.ObjectMeta.Annotations = ann
	return nil
}

func GeneratePodNameByVolumeID(volumeID string) string {
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
		pod, err := k8sClient.GetPod(csiconfig.Namespace, podName)
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

func isPodReady(pod *k8sCore.Pod) bool {
	conditionsTrue := 0
	for _, cond := range pod.Status.Conditions {
		if cond.Status == k8sCore.ConditionTrue && (cond.Type == k8sCore.ContainersReady || cond.Type == k8sCore.PodReady) {
			conditionsTrue++
		}
	}
	return conditionsTrue == 2
}

func getErrContainerLog(K8sClient k8s.Client, podName string) (log string, err error) {
	pod, err := K8sClient.GetPod(csiconfig.Namespace, podName)
	if err != nil {
		return
	}
	for _, cn := range pod.Status.InitContainerStatuses {
		if !cn.Ready {
			log, err = K8sClient.GetPodLog(pod.Namespace, pod.Name, cn.Name)
			return
		}
	}
	for _, cn := range pod.Status.ContainerStatuses {
		if !cn.Ready {
			log, err = K8sClient.GetPodLog(pod.Namespace, pod.Name, cn.Name)
			return
		}
	}
	return
}

func buildCacheWorkerContainer(pod *k8sCore.Pod, mountInfo Info, cacheConf common.FsCacheConfig) {
	cmd := getCacheWorkerCmd(mountInfo, cacheConf)
	pod.Spec.Containers[1].Command = []string{"sh", "-c", cmd}
	mp := k8sCore.MountPropagationBidirectional
	volumeMounts := []k8sCore.VolumeMount{
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

func buildMountContainer(pod *k8sCore.Pod, mountInfo Info, cacheConf common.FsCacheConfig) {
	cmd := getMountCmd(mountInfo, cacheConf)
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}
	statCmd := "stat -c %i " + MountPoint
	pod.Spec.Containers[0].ReadinessProbe = &k8sCore.Probe{
		Handler: k8sCore.Handler{
			Exec: &k8sCore.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"if [ x$(%v) = x1 ]; then exit 0; else exit 1; fi ", statCmd)},
			}},
		InitialDelaySeconds: 1,
		PeriodSeconds:       1,
	}
	pod.Spec.Containers[0].Lifecycle = &k8sCore.Lifecycle{
		PreStop: &k8sCore.Handler{
			Exec: &k8sCore.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"umount %s && rmdir %s", MountPoint, MountPoint)}},
		},
	}
	pod.Annotations = make(map[string]string)
	pod.Annotations[AnnoKeyServer] = mountInfo.Server
	pod.Annotations[AnnoKeyFsID] = mountInfo.FSID

	typeDir := k8sCore.HostPathDirectoryOrCreate
	volumes := []k8sCore.Volume{
		{
			Name: VolumesKeyDataCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: cacheConf.CacheDir + DataCacheDir,
					Type: &typeDir,
				},
			},
		},
		{
			Name: VolumesKeyMetaCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: cacheConf.CacheDir + MetaCacheDir,
					Type: &typeDir,
				},
			},
		},
		{
			Name: VolumesKeyMount,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: csiconfig.HostMntDir,
					Type: &typeDir,
				},
			},
		},
	}
	mp := k8sCore.MountPropagationBidirectional
	volumeMounts := []k8sCore.VolumeMount{
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
			MountPath:        schema.PodMntDir,
			SubPath:          mountInfo.FSID,
			MountPropagation: &mp,
		},
	}
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
}

func getMountCmd(mountInfo Info, cacheConf common.FsCacheConfig) string {
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
	if cacheConf.Debug {
		options = append(options, "--log-level=trace")
	}
	if mountInfo.ReadOnly {
		options = append(options, "--mount-options=ro")
	}
	cmd := mkdir + pfsMountPath + mountPath + strings.Join(options, " ")
	return cmd
}

func getCacheWorkerCmd(mountInfo Info, cacheConf common.FsCacheConfig) string {
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
