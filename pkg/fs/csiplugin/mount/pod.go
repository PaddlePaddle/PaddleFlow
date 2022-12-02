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
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8sCore "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
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

	ContainerNameCacheWorker = "cache-worker"
	ContainerNamePfsMount    = "pfs-mount"
)

var umountLock sync.RWMutex

func PodUnmount(volumeID string, mountInfo Info) error {
	podName := GeneratePodNameByVolumeID(volumeID)
	log.Infof("PodUnmount pod name is %s", podName)
	umountLock.Lock()
	defer umountLock.Unlock()

	k8sClient, err := utils.GetK8sClient()
	if err != nil {
		log.Errorf("PodUnmount: Get k8s client failed: %v", err)
		return err
	}
	pod, err := k8sClient.GetPod(csiconfig.Namespace, podName)
	if err != nil && !k8sErrors.IsNotFound(err) {
		log.Errorf("PodUnmount: Get pod %s err: %v", podName, err)
		return err
	}
	// if mount pod not exists. might be process mount
	if pod == nil {
		//log.Infof("PodUnmount: Mount pod %s not exists.", podName)
		return nil
	}

	workPodUID := utils.GetPodUIDFromTargetPath(mountInfo.TargetPath)
	if workPodUID != "" {
		return removeRef(k8sClient, pod, workPodUID)
	}
	return nil
}

func PFSMount(volumeID string, mountInfo Info) error {
	// create mount pod or add ref in server db
	if err := createOrUpdatePod(volumeID, mountInfo); err != nil {
		log.Errorf("PodMount: info: %+v err: %v", mountInfo, err)
		return err
	}
	return waitUtilPodReady(mountInfo.K8sClient, GeneratePodNameByVolumeID(volumeID))
}

func createOrUpdatePod(volumeID string, mountInfo Info) error {
	podName := GeneratePodNameByVolumeID(volumeID)
	log.Infof("pod name is %s", podName)
	for i := 0; i < 120; i++ {
		// wait for old pod deleted
		oldPod, errGetPod := mountInfo.K8sClient.GetPod(csiconfig.Namespace, podName)
		if errGetPod != nil {
			if k8sErrors.IsNotFound(errGetPod) {
				// mount pod not exist, create
				log.Infof("createOrAddRef: Need to create pod %s.", podName)
				if createPodErr := createMountPod(mountInfo.K8sClient, volumeID, mountInfo); createPodErr != nil {
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
			return addRef(mountInfo.K8sClient, oldPod, mountInfo.TargetPath)
		}
	}
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min",
		mountInfo.FS.ID, podName)
}

func addRef(c utils.Client, pod *k8sCore.Pod, targetPath string) error {
	err := buildAnnotation(pod, targetPath)
	if err != nil {
		log.Errorf("mount_pod[%s] addRef: buildAnnotation err: %v", pod.Name, err)
		return err
	}
	if err = c.PatchPodAnnotation(pod); err != nil {
		retErr := fmt.Errorf("mount_pod addRef: patch pod[%s] annotation:%+v err:%v", pod.Name, pod.Annotations, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	return nil
}

func removeRef(c utils.Client, pod *k8sCore.Pod, workPodUID string) error {
	annotation := pod.ObjectMeta.Annotations
	if annotation == nil {
		log.Warnf("mount_pod removeRef: pod[%s] has no annotation", pod.Name)
		return nil
	}
	mountKey := schema.AnnotationKeyMountPrefix + workPodUID
	if _, ok := annotation[mountKey]; !ok {
		log.Infof("mount_pod removeRef: workPodUID [%s] in pod [%s] already not exists.", workPodUID, pod.Name)
		return nil
	}
	delete(annotation, mountKey)
	annotation[schema.AnnotationKeyMTime] = time.Now().Format(model.TimeFormat)
	pod.ObjectMeta.Annotations = annotation
	if err := c.PatchPodAnnotation(pod); err != nil {
		retErr := fmt.Errorf("mount_pod removeRef: patch pod[%s] annotation:%+v err:%v", pod.Name, annotation, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	return nil
}

func createMountPod(k8sClient utils.Client, volumeID string, mountInfo Info) error {
	mountPod, err := buildMountPod(volumeID, mountInfo)
	if err != nil {
		log.Errorf("buildMountPod[%s] err: %v", mountInfo.FS.ID, err)
		return err
	}
	log.Debugf("creating mount pod: %+v\n", *mountPod)
	_, err = k8sClient.CreatePod(mountPod)
	if err != nil {
		log.Errorf("createMountPod for fsID %s err: %v", mountInfo.FS.ID, err)
		return err
	}
	return nil
}

func buildMountPod(volumeID string, mountInfo Info) (*k8sCore.Pod, error) {
	pod := csiconfig.GeneratePodTemplate()
	pod.Name = GeneratePodNameByVolumeID(volumeID)
	// annotate mount point & modified time
	err := buildAnnotation(pod, mountInfo.TargetPath)
	if err != nil {
		return nil, err
	}
	// build volumes & containers
	pod.Spec.Volumes = generatePodVolumes(mountInfo.CacheConfig.CacheDir)
	pod.Spec.Containers[0] = buildMountContainer(baseContainer(pod.Name, mountInfo.PodResource), mountInfo)
	pod.Spec.Containers[1] = buildCacheWorkerContainer(baseContainer(pod.Name, mountInfo.PodResource), mountInfo)

	// label for pod listing
	pod.Labels[schema.LabelKeyFsID] = mountInfo.FS.ID
	pod.Labels[schema.LabelKeyNodeName] = csiconfig.NodeName
	// labels for cache stats
	pod.Labels[schema.LabelKeyCacheID] = model.CacheID(csiconfig.ClusterID,
		csiconfig.NodeName, mountInfo.CacheConfig.CacheDir, mountInfo.FS.ID)
	// cache dir has "/" and is not allowed in label
	pod.Annotations[schema.AnnotationKeyCacheDir] = mountInfo.CacheConfig.CacheDir
	return pod, nil
}

func buildAnnotation(pod *k8sCore.Pod, targetPath string) error {
	workPodUID := utils.GetPodUIDFromTargetPath(targetPath)
	if workPodUID == "" {
		err := fmt.Errorf("mount_pod[%s] buildAnnotation: failed obtain workPodUID from target path: %s", pod.Name, targetPath)
		log.Errorf(err.Error())
		return err
	}
	pod.ObjectMeta.Annotations[schema.AnnotationKeyMountPrefix+workPodUID] = targetPath
	pod.ObjectMeta.Annotations[schema.AnnotationKeyMTime] = time.Now().Format(model.TimeFormat)
	return nil
}

func GeneratePodNameByVolumeID(volumeID string) string {
	return fmt.Sprintf("pfs-%s-%s", csiconfig.NodeName, volumeID)
}

func waitUtilPodReady(k8sClient utils.Client, podName string) error {
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

func getErrContainerLog(K8sClient utils.Client, podName string) (log string, err error) {
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

func baseContainer(podName string, podResource k8sCore.ResourceRequirements) k8sCore.Container {
	isPrivileged := true
	return k8sCore.Container{
		//Name:  containerName, to be set at invoker side
		Image: csiconfig.MountImage,
		SecurityContext: &k8sCore.SecurityContext{
			Privileged: &isPrivileged,
		},
		Env: []k8sCore.EnvVar{
			{
				Name:  schema.EnvKeyMountPodName,
				Value: podName,
			},
			{
				Name:  schema.EnvKeyNamespace,
				Value: csiconfig.Namespace,
			},
		},
		Resources: podResource,
	}
}

func buildMountContainer(mountContainer k8sCore.Container, mountInfo Info) k8sCore.Container {
	mountContainer.Name = ContainerNamePfsMount
	mkdir := "mkdir -p " + FusePodMountPoint + ";"

	cmd := mkdir + mountInfo.Cmd + " " + strings.Join(mountInfo.Args, " ")
	mountContainer.Command = []string{"sh", "-c", cmd}
	statCmd := "stat -c %i " + FusePodMountPoint
	mountContainer.ReadinessProbe = &k8sCore.Probe{
		Handler: k8sCore.Handler{
			Exec: &k8sCore.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"if [ x$(%v) = x1 ]; then exit 0; else exit 1; fi ", statCmd)},
			}},
		InitialDelaySeconds: 1,
		PeriodSeconds:       1,
	}
	mountContainer.Lifecycle = &k8sCore.Lifecycle{
		PreStop: &k8sCore.Handler{
			Exec: &k8sCore.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"umount %s && rmdir %s", FusePodMountPoint, FusePodMountPoint)}},
		},
	}

	mp := k8sCore.MountPropagationBidirectional
	volumeMounts := []k8sCore.VolumeMount{
		{
			Name:             VolumesKeyMount,
			MountPath:        schema.FusePodMntDir,
			SubPath:          mountInfo.FS.ID,
			MountPropagation: &mp,
		},
	}
	if mountInfo.CacheConfig.CacheDir != "" {
		dataCacheVM := k8sCore.VolumeMount{
			Name:             VolumesKeyDataCache,
			MountPath:        FusePodCachePath + DataCacheDir,
			MountPropagation: &mp,
		}
		metaCacheVM := k8sCore.VolumeMount{
			Name:             VolumesKeyMetaCache,
			MountPath:        FusePodCachePath + MetaCacheDir,
			MountPropagation: &mp,
		}
		volumeMounts = append(volumeMounts, dataCacheVM, metaCacheVM)
	}
	mountContainer.VolumeMounts = volumeMounts
	return mountContainer
}

func generatePodVolumes(cacheDir string) []k8sCore.Volume {
	typeDir := k8sCore.HostPathDirectoryOrCreate
	volumes := []k8sCore.Volume{
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
	if cacheDir != "" {
		// data CacheConfig
		dataCacheVolume := k8sCore.Volume{
			Name: VolumesKeyDataCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: cacheDir + DataCacheDir,
					Type: &typeDir,
				},
			},
		}
		// meta CacheConfig
		metaCacheVolume := k8sCore.Volume{
			Name: VolumesKeyMetaCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: cacheDir + MetaCacheDir,
					Type: &typeDir,
				},
			},
		}
		volumes = append(volumes, dataCacheVolume, metaCacheVolume)
	}
	return volumes
}

func buildCacheWorkerContainer(cacheContainer k8sCore.Container, mountInfo Info) k8sCore.Container {
	cacheContainer.Name = ContainerNameCacheWorker
	cacheContainer.Command = []string{"sh", "-c", mountInfo.CacheWorkerCmd()}
	if mountInfo.CacheConfig.CacheDir != "" {
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
		cacheContainer.VolumeMounts = volumeMounts
	}
	return cacheContainer
}
