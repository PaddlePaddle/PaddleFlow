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
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/mount"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

var mountPointController *MountPointController

// checkerStopChan informs stop commands
var checkerStopChan = make(chan bool)

// checkerUpdateChan informs it is time to update pods information from kubelet
var checkerUpdateChan = make(chan bool)

type pvParams struct {
	fsID    string
	fsInfo  string
	fsCache string
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
	k8sClient, err := utils.New(utils.GetK8SConfigPathEnv(), utils.GetK8STimeoutEnv())
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
		rateLimiter:      make(chan struct{}, utils.GetPodsHandleConcurrency()),

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
		case <-time.After(time.Duration(utils.GetMountPointCheckIntervalTime()) * time.Second):
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
	client, err := utils.GetK8sClient()
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
			m.pvParamsMap[pv.Name] = buildPfsPvParams(pv.Spec.CSI.VolumeAttributes)
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
		case <-time.After(time.Duration(utils.GetPodsUpdateIntervalTime()) * time.Second):
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
	pvParams_, ok := m.pvParamsMap[volumeMount.VolumeName]
	if !ok {
		log.Errorf("get pfs parameters [%s] not exist", volumeMount.VolumeName)
		return fmt.Errorf("get pfs parameters [%s] not exist", volumeMount.VolumeName)
	}

	// pods need to restore source mount path mountpoints
	mountPath := utils.GetVolumeBindMountPathByPod(volumeMount.PodUID, volumeMount.VolumeName)
	mountInfo, err := mount.ConstructMountInfo(pvParams_.fsInfo, pvParams_.fsCache, mountPath, nil, volumeMount.ReadOnly)
	if err != nil {
		err := fmt.Errorf("ConstructMountInfo from pvParams: %+v failed: %v", pvParams_, err)
		log.Errorf(err.Error())
		return err
	}

	if checkIfNeedRemount(mountPath) {
		if err := remount(volumeMount, mountInfo); err != nil {
			err := fmt.Errorf("remount info: %+v failed: %v", mountInfo, err)
			log.Errorf(err.Error())
			return err
		}
	}
	return nil
}

func waitForBindSourceReady(bindSource string) bool {
	i := 0
	for {
		isMount, err := utils.IsMountPoint(bindSource)
		if isMount && err == nil {
			break
		}
		i += 1
		if i > 2 {
			log.Warnf("bind source[%s] not mounted, wait until next check or manually fix mount pod", bindSource)
			return false
		}
		time.Sleep(1 * time.Second)
	}
	return true
}

// CheckIfNeedRemount The conditions for remount: the path is the mount point and the error message returned by the `mountpoint` command
// contains "Transport endpoint is not connected"
func checkIfNeedRemount(path string) bool {
	isMountPoint, err := utils.IsMountPoint(path)
	log.Tracef("mountpoint path[%s] : isMountPoint[%t], err:%v", path, isMountPoint, err)
	if err != nil && isMountPoint {
		return true
	}
	return false
}

func remount(volumeMount volumeMountInfo, mountInfo mount.Info) error {
	log.Tracef("remount: mountInfo %+v", mountInfo)

	if !mountInfo.FS.IndependentMountProcess && mountInfo.FS.Type != common.GlusterFSType {
		// wait for source path ready
		if !waitForBindSourceReady(schema.GetBindSource(mountInfo.FS.ID)) {
			return nil
		}
	} else {
		// mount source path
		isMountPoint, err := utils.IsMountPoint(mountInfo.SourcePath)
		log.Tracef("mountpoint path[%s] : isMountPoint[%t], err:%v", mountInfo.SourcePath, isMountPoint, err)

		switch isMountPoint {
		case true:
			if err != nil {
				// unmount source path
				if err := utils.ManualUnmount(mountInfo.SourcePath); err != nil {
					err := fmt.Errorf("process remount[%s] failed when ManualUnmount source path %s. err: %v",
						mountInfo.FS.ID, mountInfo.SourcePath, err)
					return err
				}
				// mount source path
				output, err := utils.ExecCmdWithTimeout(mountInfo.Cmd, mountInfo.Args)
				if err != nil {
					log.Errorf("remount: process exec mount cmd failed: [%v], output[%v]", err, string(output))
					return err
				}
			}
		case false:
			if err != nil {
				err := fmt.Errorf("fs[%s] source path[%s] not mp and err: %v", mountInfo.FS.ID, mountInfo.SourcePath, err)
				return err
			} else {
				// mount source path
				output, err := utils.ExecCmdWithTimeout(mountInfo.Cmd, mountInfo.Args)
				if err != nil {
					log.Errorf("remount: process exec mount cmd failed: [%v], output[%v]", err, string(output))
					return err
				}
			}
		}
	}

	// bind source path to mount path
	output, err := utils.ExecMountBind(mountInfo.SourcePath, mountInfo.TargetPath, mountInfo.ReadOnly)
	if err != nil {
		log.Errorf("remount: pod exec mount bind cmd failed: %v, output[%s]", err, string(output))
		return err
	}

	// todo subpath need recovery
	log.Debugf("volumeMount info %+v", volumeMount)
	for _, subPath := range volumeMount.SubPaths {
		output, err := utils.ExecMountBind(subPath.SourcePath, subPath.TargetPath, subPath.ReadOnly)
		if err != nil {
			log.Errorf("exec mount cmd failed: %v, output[%s]", err, string(output))
			return err
		}
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
		m.pvParamsMap[pv.Name] = buildPfsPvParams(pv.Spec.CSI.VolumeAttributes)
	}
}

func buildPfsPvParams(params map[string]string) pvParams {
	fsID := params[schema.PFSID]
	fsInfo := params[schema.PFSInfo]
	fsCache := params[schema.PFSCache]
	return pvParams{
		fsID:    fsID,
		fsInfo:  fsInfo,
		fsCache: fsCache,
	}
}
