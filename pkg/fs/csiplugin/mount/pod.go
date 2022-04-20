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
	"paddleflow/pkg/fs/csiplugin/client/pfs"
	"paddleflow/pkg/fs/csiplugin/csiconfig"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"paddleflow/pkg/fs/csiplugin/client/k8s"
)

type PodMount struct {
	K8sClient *k8s.K8SInterface
}

func MountThroughPod(targetPath string, mountInfo pfs.MountInfo) error {
	if err := createOrAddRef(targetPath, mountInfo); err != nil {
		return err
	}
	return waitUtilPodReady(GeneratePodNameByFsID(mountInfo.FSID))
}

func createOrAddRef(targetPath string, mountInfo pfs.MountInfo) error {
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
				return createMount(k8sClient, targetPath, mountInfo)
			}
			// unexpect error
			log.Errorf("createOrAddRef: Get pod %s err: %v", podName, err)
			return err
		}
		return addRefOfMount(targetPath, mountInfo)
	}
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min", mountInfo.FSID, podName)
}

func createMount(k8sClient k8s.K8SInterface, targetPath string, mountInfo pfs.MountInfo) error {
	// create pod
	newPod := GenPodForTest(targetPath, mountInfo)
	_, err := k8sClient.CreatePod(newPod)
	if err != nil {
		log.Errorf("createMount: Create pod for fsID %s err: %v", mountInfo.FSID, err)
		return err
	}
	// create entry in apiserver
	if err := addRefOfMount(targetPath, mountInfo); err != nil {
		log.Errorf("createMount: addRefOfMount fsID:%s, targetPath: %+v err: %v", mountInfo.FSID, targetPath, err)
		return err
	}
	return nil
}

func addRefOfMount(targetPath string, mountInfo pfs.MountInfo) error {
	//cacheStore := models.GetFSCacheStore()
	//cache := models.FSCache{
	//	FSID:       mountInfo.FSID,
	//	NodeName:   csiconfig.NodeName,
	//	MountPoint: targetPath,
	//	//CacheDir: ?
	//}
	//if err := cacheStore.AddFSCache(&cache); err != nil {
	//	log.Errorf("AddRefOfMount: cacheStore.AddFSCache %+v err: %v", cache, err)
	//	return err
	//}
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

func GenPodForTest(targetPath string, mountInfo pfs.MountInfo) *v1.Pod {
	pod := csiconfig.GeneratePodTemplate()
	pod.Name = GeneratePodNameByFsID(mountInfo.FSID)
	cmd := getcmd(mountInfo)
	pod.Spec.Containers[0].Command = []string{"sh", "-c", cmd}

	dir := corev1.HostPathDirectoryOrCreate
	volumes := []corev1.Volume{
		{
			Name: "pfs-cache-dir",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/data/luoyuedong/1.4/cache",
					Type: &dir,
				},
			},
		},
		{
			Name: "pfs-fuse",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/data/luoyuedong/1.4/pfs-fuse",
				},
			},
		},
	}
	mp := corev1.MountPropagationBidirectional
	volumeMounts := []corev1.VolumeMount{
		{
			Name:             "pfs-cache-dir",
			MountPath:        "/var/paddleflow-cache",
			MountPropagation: &mp,
		},
		{
			Name:      "pfs-fuse",
			MountPath: "/home/paddleflow/pfs-fuse",
		},
	}
	pod.Spec.Volumes = volumes
	pod.Spec.Containers[0].VolumeMounts = volumeMounts
	return pod
}

func getcmd(mountInfo pfs.MountInfo) string {
	mp := "/home/paddleflow/mnt/" + mountInfo.FSID
	mkdir := "mkdir -p " + mp + ";"
	pfsMountPath := "/home/paddleflow/pfs-fuse mount "
	mountPath := "--mount-point=" + mp + " "
	options := []string{
		"--server=paddleflow-server:8083",
		"--user-name=root",
		"--password=paddleflow",
		"--block-size=104576",
		"--data-mem-size=5000",
		"--fs-id=" + mountInfo.FSID,
	}
	e := ";sleep 10h"
	cmd := mkdir + pfsMountPath + mountPath + strings.Join(options, " ") + e
	return cmd
}
