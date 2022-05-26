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

package controller

import (
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/client/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/client/pfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/mount"
)

var mountPointController *MountPointController
var checkerSync sync.Once

// checkerStopChan informs stop commands
var checkerStopChan = make(chan bool)

// checkerUpdateChan informs it is time to update pods information from kubelet
var checkerUpdateChan = make(chan bool)

// MountPointController will check the status of the mount point and remount unconnected mount point
type MountPointController struct {
	nodeID           string
	masterNodesAware bool
	podMap           map[string]v1.Pod
	removePods       sync.Map
	rateLimiter      chan struct{}

	kubeClient kubernetes.Interface
	// pvInformer and pvLister
	pvInformer coreinformers.PersistentVolumeInformer
	pvLister   corelisters.PersistentVolumeLister
	pvSynced   cache.InformerSynced

	queue                workqueue.RateLimitingInterface
	fsMountParametersMap map[string]pfs.FSMountParameter
}

func GetMountPointController(nodeID string) *MountPointController {
	if mountPointController == nil {
		return Initialize(nodeID, false)
	}
	return mountPointController
}

func Initialize(nodeID string, masterNodesAware bool) *MountPointController {
	k8sClient, err := k8s.New(common.GetK8SConfigPathEnv(), common.GetK8STimeoutEnv())
	if err != nil {
		log.Errorf("init k8sClient failed: %v", err)
		return nil
	}

	sharedInformers := informers.NewSharedInformerFactory(k8sClient.Clientset, 0)
	pvInformer := sharedInformers.Core().V1().PersistentVolumes()

	mountPointController = &MountPointController{
		nodeID:           nodeID,
		masterNodesAware: masterNodesAware,
		podMap:           make(map[string]v1.Pod),
		removePods:       sync.Map{},
		rateLimiter:      make(chan struct{}, common.GetPodsHandleConcurrency()),

		pvInformer:           pvInformer,
		pvLister:             pvInformer.Lister(),
		pvSynced:             pvInformer.Informer().HasSynced,
		fsMountParametersMap: make(map[string]pfs.FSMountParameter),
	}
	pvInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: mountPointController.pvAddedUpdated,
		UpdateFunc: func(old, new interface{}) {
			mountPointController.pvAddedUpdated(new)
		},
	})

	if err := mountPointController.UpdatePodMap(); err != nil {
		log.Errorf("update podMap failed: %v", err.Error())
	}
	return mountPointController
}

func (m *MountPointController) Start(stopCh <-chan struct{}) {
	log.Info("MountPointController start")

	go m.pvInformer.Informer().Run(stopCh)

	cache.WaitForCacheSync(stopCh, m.pvSynced)

	go m.WaitToUpdatePodMap()

	for {
		var wg sync.WaitGroup

		updateMounts := true

		for k, pod := range m.podMap {
			log.Debugf("now begin to check pod UID[%s] with state[%s]", pod.UID, pod.Status.Phase)
			if _, ok := m.removePods.Load(k); ok {
				continue
			}
			m.rateLimiter <- struct{}{}

			wg.Add(1)
			go func(p v1.Pod) {
				defer func() {
					wg.Done()
					<-m.rateLimiter
				}()
				if isTerminating(p) {
					/**
					目前对Terminating判断条件还不够充分，没有处理restart的情况。先下掉处理Terminating逻辑。
					从造成Terminating原因考虑，减少Terminating出现：
					1）自动恢复逻辑添加running判断，减少Pending中或者Terminating中Pod的恢复
					2）unmount操作减少遗留文件
					if err := m.handleTerminatingPod(p); err != nil {
						log.Logger.Error("handle terminating pod[%v] failed: %v", p.UID, err)
					}
					**/
					m.RemovePod(string(p.UID))
				} else if isRunning(p) {
					m.handleRunningPod(p, updateMounts, m.fsMountParametersMap)
				}
			}(pod)
		}
		wg.Wait()

		select {
		case <-checkerUpdateChan:
			log.Info("begin to update podMap")
			if err := m.UpdatePodMap(); err != nil {
				log.Errorf("update podMap failed: %v", err)
			}
		case <-checkerStopChan:
			log.Info("MountPointController stopped")
			return
		case <-time.After(time.Duration(common.GetMountPointCheckIntervalTime()) * time.Second):
		}
	}
}

// RemovePod During the pod update interval, add the pod UID that has called NodeUnPublishVolume to the map `removePods`
func (m *MountPointController) RemovePod(podUID string) {
	m.removePods.Store(podUID, true)
}

// UpdatePodMap Synchronize all pod information of the node from kubelet
func (m *MountPointController) UpdatePodMap() error {
	log.Debug("begin to update pods map")
	client, err := k8s.GetK8sClient()
	if err != nil {
		log.Errorf("get k8s client failed: %v", err)
		return err
	}

	pods, err := client.ProxyGetPods(m.nodeID)
	if err != nil {
		log.Errorf("proxy get pods failed: %v", err)
		return err
	}

	podMap := make(map[string]v1.Pod)
	for _, pod := range pods.Items {
		if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending {
			continue
		}
		podMap[string(pod.UID)] = pod
	}
	m.podMap = podMap
	m.removePods = sync.Map{}

	pvs, err := client.ListPersistentVolume(metav1.ListOptions{})
	for _, pv := range pvs.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == "paddleflowstorage" {
			m.fsMountParametersMap[pv.Name] = getPFSParameters(pv.Spec.CSI.VolumeAttributes)
		}
	}
	return nil
}

// WaitToUpdatePodMap wait to update PodMap
func (m *MountPointController) WaitToUpdatePodMap() {
	log.Debug("Wait to update podMap")
	for {
		select {
		case <-checkerStopChan:
			log.Info("WaitToUpdate stopped")
			return
		case <-time.After(time.Duration(common.GetPodsUpdateIntervalTime()) * time.Second):
			checkerUpdateChan <- true
		}
	}
}

func (m *MountPointController) Stop() {
	log.Info("MountPointController stop")
	checkerStopChan <- true
}

func (m *MountPointController) handleRunningPod(pod v1.Pod, updateMounts bool, fsMountParametersMap map[string]pfs.FSMountParameter) {
	podVolumeMounts := k8s.GetVolumeMounts(&pod)
	for _, volumeMount := range podVolumeMounts {
		if err := m.CheckAndRemountVolumeMount(volumeMount, fsMountParametersMap); err != nil {
			log.Errorf("check and remount volume mount[%v] failed: %s", volumeMount, err)
		}

		if updateMounts {
			if err := m.UpdateMounts(volumeMount); err != nil {
				log.Errorf("update Mounts with volumeMount[%v] failed: %v",
					volumeMount, err)
			}
		}
	}
}

func (m *MountPointController) CheckAndRemountVolumeMount(volumeMount k8s.VolumeMount, fsMountParametersMap map[string]pfs.FSMountParameter) error {
	// TODO(dongzezhao) get mountParameters from VolumeMount
	fsMountParams, ok := m.fsMountParametersMap[volumeMount.VolumeName]
	if !ok {
		log.Error("get pfs parameters failed")
		return fmt.Errorf("get pfs parameters failed")
	}

	// pods need to restore source mount path mountpoints
	sourceMountPath := common.GetSourceMountPathByPod(volumeMount.PodUID, volumeMount.VolumeName)
	if m.CheckIfNeedRemount(sourceMountPath) {
		if err := m.Remount(fsMountParams, volumeMount); err != nil {
			log.Errorf("remount source[%s] failed: %v", sourceMountPath, err)
			return fmt.Errorf("remount source[%s] failed: %v\n", sourceMountPath, err)
		}
		bindMountPath := common.GetVolumeBindMountPathByPod(volumeMount.PodUID, volumeMount.VolumeName)
		if ok, err := mount.IsMountPoint(bindMountPath); ok && err != nil {
			if err := remountBind(sourceMountPath, bindMountPath, volumeMount); err != nil {
				log.Errorf("remount bind[%s] failed: %v", bindMountPath, err)
				return fmt.Errorf("remount bind[%s] failed: %v\n", bindMountPath, err)
			}
		}
	}
	return nil
}

// CheckIfNeedRemount The conditions for remount: the path is the mount point and the error message returned by the `mountpoint` command
// contains "Transport endpoint is not connected"
func (m *MountPointController) CheckIfNeedRemount(path string) bool {
	isMountPoint, err := mount.IsMountPoint(path)
	if err != nil && isMountPoint {
		return true
	}
	return false
}

func (m *MountPointController) Remount(params pfs.FSMountParameter, volumeMount k8s.VolumeMount) error {
	log.Infof("Remount: now begin to remount volume[%s], pod[%s]", "", volumeMount.PodUID)
	mountPath := common.GetSourceMountPathByPod(volumeMount.PodUID, volumeMount.VolumeName)

	// umount old mount point
	output, err := mount.ExecCmdWithTimeout(mount.UMountCmdName, []string{mountPath})
	if err != nil {
		log.Errorf("exec cmd[umount %s] failed: %v, output[%s]", mountPath, err, string(output))
		if !strings.Contains(string(output), mount.NotMounted) {
			if err := mount.ForceUnmount(mountPath); err != nil {
				return err
			}
		}
	}

	// remount source dir
	mountInfo := pfs.GetMountInfo(params.FSID, params.Server, false)
	mountInfo.LocalPath = mountPath
	command, args := mountInfo.GetMountCmd()
	log.Debugf("begin to exec cmd [%s %v]", command, args)
	output, err = mount.ExecCmdWithTimeout(command, args)
	if err != nil {
		log.Errorf("exec cmd[%s %v] failed: %v, output[%v]", command, args, err, string(output))
		return err
	}
	return nil
}

func remountBind(sourcePath, mountPath string, volumeMount k8s.VolumeMount) error {
	output, err := mount.ExecMountBind(sourcePath, mountPath, volumeMount.ReadOnly)
	if err != nil {
		log.Errorf("exec mount bind cmd failed: %v, output[%s]", err, string(output))
		return err
	}
	return nil
}

// UpdateMounts update mount
func (m *MountPointController) UpdateMounts(volumeMount k8s.VolumeMount) error {
	// TODO(dongzezhao): update mounts

	return nil
}

func isRunning(pod v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending {
		return false
	}

	// 考虑container重启的情况，对于Running状态的Pod，仅在Terminating状态不需要恢复
	if pod.DeletionTimestamp != nil {
		return false
	}
	return true
}

func isTerminating(pod v1.Pod) bool {
	if pod.Status.Phase != v1.PodPending && pod.Status.Phase != v1.PodRunning {
		return false
	}

	if pod.DeletionTimestamp != nil {
		return true
	}
	return false
}

// pvAddedUpdated reacts to pv added/updated events
func (c *MountPointController) pvAddedUpdated(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PV informer returned non-PV object: %#v", obj))
		return
	}

	// update pv
	if pv.Spec.StorageClassName == "paddleflowstorage" {
		c.fsMountParametersMap[pv.Name] = getPFSParameters(pv.Spec.CSI.VolumeAttributes)
	}
}

func getPFSParameters(params map[string]string) pfs.FSMountParameter {
	fsID := params["pfs.fs.id"]
	server := params["pfs.server"]
	userID := params["pfs.user.name"]
	return pfs.FSMountParameter{
		FSID:   fsID,
		Server: server,
		UserID: userID,
	}
}
