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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8sCore "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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

	FusePodMountPoint = schema.FusePodMntDir + "/storage"
	FusePodCachePath  = "/home/paddleflow/pfs-cache"
	DataCacheDir      = "/data-cache"
	MetaCacheDir      = "/meta-cache"
	CacheWorkerBin    = "/home/paddleflow/cache-worker"

	AnnoKeyMTime  = "modifiedTime"
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
	mountInfo.FsID = pod.Annotations[AnnoKeyFsID]
	if mountInfo.Server == "" || mountInfo.FsID == "" {
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
	return nil
}

func PodMount(volumeID string, mountInfo Info) error {
	// create mount pod or add ref in server db
	if err := createOrUpdatePod(volumeID, mountInfo); err != nil {
		log.Errorf("PodMount: info: %+v err: %v", mountInfo, err)
		return err
	}
	return waitUtilPodReady(GeneratePodNameByVolumeID(volumeID))
}

func createOrUpdatePod(volumeID string, mountInfo Info) error {
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
				if createPodErr := createMountPod(k8sClient, volumeID, mountInfo); createPodErr != nil {
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
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min", mountInfo.FsID, podName)
}

func addRef(c k8s.Client, pod *k8sCore.Pod, targetPath string) error {
	annotation, err := buildAnnotation(pod, targetPath)
	if err != nil {
		log.Errorf("mount_pod[%s] addRef: buildAnnotation err: %v", pod.Name, err)
		return err
	}
	if err := patchPodAnnotation(c, pod, annotation); err != nil {
		retErr := fmt.Errorf("mount_pod addRef: patch pod[%s] annotation:%+v err:%v", pod.Name, annotation, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	return nil
}

func removeRef(c k8s.Client, pod *k8sCore.Pod, workPodUID string) error {
	annotation := pod.ObjectMeta.Annotations
	if annotation == nil {
		log.Warnf("mount_pod removeRef: pod[%s] has no annotation", pod.Name)
		return nil
	}
	if _, ok := annotation[workPodUID]; !ok {
		log.Infof("mount_pod removeRef: workPodUID [%s] in pod [%s] already not exists.", workPodUID, pod.Name)
		return nil
	}
	delete(annotation, workPodUID)
	annotation[AnnoKeyMTime] = time.Now().Format(model.TimeFormat)
	if err := patchPodAnnotation(c, pod, annotation); err != nil {
		retErr := fmt.Errorf("mount_pod removeRef: patch pod[%s] annotation:%+v err:%v", pod.Name, annotation, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	return nil
}

func patchPodAnnotation(c k8s.Client, pod *k8sCore.Pod, annotation map[string]string) error {
	payload := []k8s.PatchMapValue{{
		Op:    "replace",
		Path:  "/metadata/annotations",
		Value: annotation,
	}}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Errorf("parse annotation json error: %v", err)
		return err
	}
	if err := c.PatchPod(pod, payloadBytes); err != nil {
		log.Errorf("patch pod %s error: %v", pod.Name, err)
		return err
	}
	return nil
}

func createMountPod(k8sClient k8s.Client, volumeID string, mountInfo Info) error {
	mountPod, err := buildMountPod(volumeID, mountInfo)
	if err != nil {
		log.Errorf("buildMountPod[%s] err: %v", mountInfo.FsID, err)
		return err
	}
	log.Debugf("\ncreating mount pod: %+v\n", *mountPod)
	_, err = k8sClient.CreatePod(mountPod)
	if err != nil {
		log.Errorf("createMountPod for fsID %s err: %v", mountInfo.FsID, err)
		return err
	}
	return nil
}

func buildMountPod(volumeID string, mountInfo Info) (*k8sCore.Pod, error) {
	pod := csiconfig.GeneratePodTemplate()
	pod.Name = GeneratePodNameByVolumeID(volumeID)
	buildMountContainer(pod, mountInfo)
	buildCacheWorkerContainer(pod, mountInfo)
	anno, err := buildAnnotation(pod, mountInfo.TargetPath)
	if err != nil {
		return nil, err
	}
	pod.ObjectMeta.Annotations = anno
	return pod, nil
}

func buildAnnotation(pod *k8sCore.Pod, targetPath string) (map[string]string, error) {
	annotation := pod.ObjectMeta.Annotations
	if annotation == nil {
		annotation = make(map[string]string)
	}
	workPodUID := utils.GetPodUIDFromTargetPath(targetPath)
	if workPodUID == "" {
		err := fmt.Errorf("mount_pod[%s] buildAnnotation: failed obtain workPodUID from target path: %s", pod.Name, targetPath)
		log.Errorf(err.Error())
		return nil, err
	}
	annotation[workPodUID] = targetPath
	annotation[AnnoKeyMTime] = time.Now().Format(model.TimeFormat)
	return annotation, nil
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

func buildCacheWorkerContainer(pod *k8sCore.Pod, mountInfo Info) {
	cmd := getCacheWorkerCmd(mountInfo)
	pod.Spec.Containers[1].Command = []string{"sh", "-c", cmd}
	mp := k8sCore.MountPropagationBidirectional
	volumeMounts := []k8sCore.VolumeMount{
		{
			Name:             VolumesKeyDataCache,
			MountPath:        FusePodCachePath + DataCacheDir,
			MountPropagation: &mp,
		},
		{
			Name:             VolumesKeyMetaCache,
			MountPath:        FusePodCachePath + MetaCacheDir,
			MountPropagation: &mp,
		},
	}
	pod.Spec.Containers[1].VolumeMounts = volumeMounts
}

func buildMountContainer(pod *k8sCore.Pod, mountInfo Info) {
	cmd := getMountCmd(mountInfo)
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}
	statCmd := "stat -c %i " + FusePodMountPoint
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
				"umount %s && rmdir %s", FusePodMountPoint, FusePodMountPoint)}},
		},
	}
	pod.Annotations = make(map[string]string)
	pod.Annotations[AnnoKeyServer] = mountInfo.Server
	pod.Annotations[AnnoKeyFsID] = mountInfo.FsID

	typeDir := k8sCore.HostPathDirectoryOrCreate
	volumes := []k8sCore.Volume{
		{
			Name: VolumesKeyDataCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: mountInfo.FsCacheConfig.CacheDir + DataCacheDir,
					Type: &typeDir,
				},
			},
		},
		{
			Name: VolumesKeyMetaCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: mountInfo.FsCacheConfig.CacheDir + MetaCacheDir,
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
			MountPath:        FusePodCachePath + DataCacheDir,
			MountPropagation: &mp,
		},
		{
			Name:             VolumesKeyMetaCache,
			MountPath:        FusePodCachePath + MetaCacheDir,
			MountPropagation: &mp,
		},
		{
			Name:             VolumesKeyMount,
			MountPath:        schema.FusePodMntDir,
			SubPath:          mountInfo.FsID,
			MountPropagation: &mp,
		},
	}
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
}

func getMountCmd(mountInfo Info) string {
	cacheConf := mountInfo.FsCacheConfig
	mkdir := "mkdir -p " + FusePodMountPoint + ";"
	pfsMountPath := "/home/paddleflow/pfs-fuse mount "
	mountPath := "--mount-point=" + FusePodMountPoint + " "
	options := []string{
		"--fs-info=" + mountInfo.FsBase64Str,
		"--user-name=" + mountInfo.UsernameRoot,
		"--password=" + mountInfo.PasswordRoot,
		"--block-size=" + strconv.Itoa(cacheConf.BlockSize),
		"--data-cache-path=" + FusePodCachePath + DataCacheDir,
		"--meta-cache-path=" + FusePodCachePath + MetaCacheDir,
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

func getCacheWorkerCmd(mountInfo Info) string {
	options := []string{
		"--server=" + mountInfo.Server,
		"--username=" + mountInfo.UsernameRoot,
		"--password=" + mountInfo.PasswordRoot,
		"--podCachePath=" + FusePodCachePath,
		"--cacheDir=" + mountInfo.FsCacheConfig.CacheDir,
		"--nodename=" + csiconfig.NodeName,
		"--clusterID=" + mountInfo.ClusterID,
		"--fsID=" + mountInfo.FsID,
	}
	return CacheWorkerBin + " " + strings.Join(options, " ")
}
