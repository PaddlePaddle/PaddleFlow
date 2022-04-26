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
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"paddleflow/pkg/common/http/api"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/common"
	"paddleflow/pkg/fs/csiplugin/client/k8s"
	"paddleflow/pkg/fs/csiplugin/client/pfs"
	"paddleflow/pkg/fs/csiplugin/csiconfig"
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

func MountThroughPod(mountInfo pfs.MountInfo) error {
	if err := createOrAddRef(mountInfo); err != nil {
		log.Errorf("MountThroughPod info: %+v err: %v", mountInfo, err)
		return err
	}
	return waitUtilPodReady(GeneratePodNameByFsID(mountInfo.FSID))
}

func createOrAddRef(mountInfo pfs.MountInfo) error {
	podName := GeneratePodNameByFsID(mountInfo.FSID)

	for i := 0; i < 120; i++ {
		k8sClient, err := k8s.GetK8sClient()
		if err != nil {
			log.Errorf("get k8s client failed: %v", err)
			return err
		}
		// wait for old pod deleted
		oldPod, err := k8sClient.GetPod(podName, csiconfig.Namespace)
		if err == nil && oldPod.DeletionTimestamp != nil {
			log.Infof("createOrAddRef: wait for old mount pod deleted.")
			time.Sleep(time.Millisecond * 500)
			continue
		} else if err != nil {
			if k8serrors.IsNotFound(err) {
				// pod not exist, create
				log.Infof("createOrAddRef: Need to create pod %s.", podName)
				if err := createMountPod(k8sClient, mountInfo); err != nil {
					return err
				}
			} else {
				// unexpect error
				log.Errorf("createOrAddRef: Get pod %s err: %v", podName, err)
				return err
			}
		}
		return addRefOfMount(mountInfo)
	}
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min", mountInfo.FSID, podName)
}

func createMountPod(k8sClient k8s.K8SInterface, mountInfo pfs.MountInfo) error {
	// get config
	cacheConfig, err := base.Client.GetFSCacheConfig()
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
	newPod := BuildMountPod(mountInfo, cacheConfig)
	_, err = k8sClient.CreatePod(newPod)
	if err != nil {
		log.Errorf("createMount: Create pod for fsID %s err: %v", mountInfo.FSID, err)
		return err
	}
	return nil
}

func addRefOfMount(mountInfo pfs.MountInfo) error {
	createMountReq := api.CreateMountRequest{
		FsParams: api.FsParams{
			FsName:   base.Client.FsName,
			UserName: base.Client.UserName,
			Token:    base.Client.Token,
		},
		ClusterID:  "",
		MountPoint: mountInfo.TargetPath,
		NodeName:   csiconfig.NodeName,
	}
	if err := base.Client.CreateFsMount(createMountReq); err != nil {
		log.Errorf("CreateFsMount[%s] failed: %v", mountInfo.FSID, err)
		return err
	}
	return nil
}

func GeneratePodNameByFsID(fsID string) string {
	return fmt.Sprintf("pfs-%s-%s", csiconfig.NodeName, fsID)
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

func BuildMountPod(mountInfo pfs.MountInfo, cacheConf common.FsCacheConfig) *v1.Pod {
	pod := csiconfig.GeneratePodTemplate()
	pod.Name = GeneratePodNameByFsID(mountInfo.FSID)
	cmd := getcmd(mountInfo, cacheConf)
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}

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
