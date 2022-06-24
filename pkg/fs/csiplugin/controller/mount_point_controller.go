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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/mount"
)

var mountPointController *MountPointController
var checkerSync sync.Once

// checkerStopChan informs stop commands
var checkerStopChan = make(chan bool)

// checkerUpdateChan informs it is time to update pods information from kubelet
var checkerUpdateChan = make(chan bool)

type pvParams struct {
	fsID   string
	server string
}

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

	queue       workqueue.RateLimitingInterface
	pvParamsMap map[string]pvParams
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

	sharedInformers := informers.NewSharedInformerFactory(k8sClient, 0)
	pvInformer := sharedInformers.Core().V1().PersistentVolumes()

	mountPointController = &MountPointController{
		nodeID:           nodeID,
		masterNodesAware: masterNodesAware,
		podMap:           make(map[string]v1.Pod),
		removePods:       sync.Map{},
		rateLimiter:      make(chan struct{}, common.GetPodsHandleConcurrency()),

		pvInformer:  pvInformer,
		pvLister:    pvInformer.Lister(),
		pvSynced:    pvInformer.Informer().HasSynced,
		pvParamsMap: make(map[string]pvParams),
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
					m.handleRunningPod(p, updateMounts)
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
			m.pvParamsMap[pv.Name] = getPFSParameters(pv.Spec.CSI.VolumeAttributes)
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

func (m *MountPointController) handleRunningPod(pod v1.Pod, updateMounts bool) {
	podVolumeMounts := getPodVolumeMounts(&pod)
	for _, volumeMount := range podVolumeMounts {
		if err := m.CheckAndRemountVolumeMount(volumeMount); err != nil {
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

func (m *MountPointController) CheckAndRemountVolumeMount(volumeMount volumeMountInfo) error {
	// TODO(dongzezhao) get mountParameters from volumeMountInfo
	fsMountParams, ok := m.pvParamsMap[volumeMount.VolumeName]
	if !ok {
		log.Errorf("get pfs parameters [%s] not exist", volumeMount.VolumeName)
		return fmt.Errorf("get pfs parameters [%s] not exist", volumeMount.VolumeName)
	}

	// pods need to restore source mount path mountpoints
	mountPath := common.GetVolumeBindMountPathByPod(volumeMount.PodUID, volumeMount.VolumeName)
	i := 0
	for {
		isMount, err := mount.IsMountPoint(schema.GetBindSource(fsMountParams.fsID))
		if isMount && err == nil {
			break
		}
		i += 1
		time.Sleep(1 * time.Second)
		if i > 2 {
			return fmt.Errorf("path[%s] not mount, please check mount pod", schema.GetBindSource(fsMountParams.fsID))
		}
	}
	if m.CheckIfNeedRemount(mountPath) {
		if err := m.Remount(fsMountParams.fsID, mountPath, volumeMount.ReadOnly); err != nil {
			log.Errorf("remount fs[%s] to mountPath[%s] failed: %v", fsMountParams.fsID, mountPath, err)
			return fmt.Errorf("remount fs[%s] to mountPath[%s] failed: %v", fsMountParams.fsID, mountPath, err)
		}
	}
	return nil
}

// CheckIfNeedRemount The conditions for remount: the path is the mount point and the error message returned by the `mountpoint` command
// contains "Transport endpoint is not connected"
func (m *MountPointController) CheckIfNeedRemount(path string) bool {
	isMountPoint, err := mount.IsMountPoint(path)
	log.Tracef("mountpoint path[%s] : isMountPoint[%t], err:%v", path, isMountPoint, err)
	if err != nil && isMountPoint {
		return true
	}
	return false
}

func (m *MountPointController) Remount(fsID, mountPath string, readOnly bool) error {
	log.Tracef("Remount: fsID[%s], mountPath[%s]", fsID, mountPath)
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
	// bind source path to mount path
	log.Infof("Remount: bind source[%s] to target[%s], readOnly[%t]", schema.GetBindSource(fsID), mountPath, readOnly)
	output, err = mount.ExecMountBind(schema.GetBindSource(fsID), mountPath, readOnly)
	if err != nil {
		log.Errorf("exec mount bind cmd failed: %v, output[%s]", err, string(output))
		return err
	}
	return nil
}

// UpdateMounts update mount
func (m *MountPointController) UpdateMounts(volumeMount volumeMountInfo) error {
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
func (m *MountPointController) pvAddedUpdated(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("PV informer returned non-PV object: %#v", obj))
		return
	}

	// update pv
	if pv.Spec.StorageClassName == "paddleflowstorage" {
		m.pvParamsMap[pv.Name] = getPFSParameters(pv.Spec.CSI.VolumeAttributes)
	}
}

func getPFSParameters(params map[string]string) pvParams {
	fsID := params[schema.PfsFsID]
	server := params[schema.PfsServer]
	return pvParams{
		fsID:   fsID,
		server: server,
	}
}
