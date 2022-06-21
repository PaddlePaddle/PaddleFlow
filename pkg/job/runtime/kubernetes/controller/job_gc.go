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
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	commonschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	JobGCControllerName = "JobGarbageCollector"
)

type FinishedJobInfo struct {
	Namespace          string
	Name               string
	OwnerName          string
	OwnerReferences    []metav1.OwnerReference
	Duration           time.Duration
	LastTransitionTime metav1.Time
	GVK                schema.GroupVersionKind
}

func FindOwnerReferenceName(ownerReferences []metav1.OwnerReference) string {
	if len(ownerReferences) == 0 {
		return ""
	}
	return ownerReferences[0].Name
}

func NewJobGC() Controller {
	return &JobGarbageCollector{}
}

type JobGarbageCollector struct {
	sync.Mutex
	opt *k8s.DynamicClientOption

	WaitedCleanQueue workqueue.DelayingInterface

	// informerMap contains GroupVersionKind and informer for different kubernetes job
	informerMap map[schema.GroupVersionKind]cache.SharedIndexInformer
	// listerMap contains GroupVersionKind and lister for different kubernetes job
	listerMap map[schema.GroupVersionKind]cache.GenericLister
}

func (j *JobGarbageCollector) Name() string {
	return JobGCControllerName
}

func (j *JobGarbageCollector) Initialize(opt *k8s.DynamicClientOption) error {
	if opt == nil || opt.ClusterInfo == nil {
		return fmt.Errorf("init %s controller failed", j.Name())
	}
	log.Infof("Initialize %s controller for cluster [%s]!", j.Name(), opt.ClusterInfo.Name)
	j.opt = opt
	j.WaitedCleanQueue = workqueue.NewDelayingQueue()
	j.informerMap = make(map[schema.GroupVersionKind]cache.SharedIndexInformer)
	j.listerMap = make(map[schema.GroupVersionKind]cache.GenericLister)

	for gvk := range k8s.GVKJobStatusMap {
		gvrMap, err := j.opt.GetGVR(gvk)
		if err != nil {
			log.Warnf("cann't find GroupVersionKind [%s], err: %v", gvk, err)
		} else {
			j.informerMap[gvk] = j.GetDynamicInformer(gvrMap.Resource)
			j.listerMap[gvk] = j.GetDynamicLister(gvrMap.Resource)
			j.informerMap[gvk].AddEventHandler(cache.FilteringResourceEventHandler{
				FilterFunc: responsibleForJob,
				Handler: cache.ResourceEventHandlerFuncs{
					UpdateFunc: j.update,
				},
			})
		}
	}
	return nil
}

func (j *JobGarbageCollector) Run(stopCh <-chan struct{}) {
	if !config.GlobalServerConfig.Job.Reclaim.CleanJob {
		log.Infof("Skip %s controller!", j.Name())
		return
	}
	if len(j.informerMap) == 0 {
		log.Infof("Cluster hasn't any GroupVersionKind, skip %s controller!", j.Name())
		return
	}
	go j.opt.DynamicFactory.Start(stopCh)

	for _, informer := range j.informerMap {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			log.Errorf("timed out waiting for caches to %s", j.Name())
			return
		}
	}
	log.Infof("Start %s controller for cluster [%s] successfully!", j.Name(), j.opt.ClusterInfo.Name)
	// clean exist & completed job
	j.preCleanFinishedJob()
	// watch job event to handle new job_gc events
	go wait.Until(j.runWorker, 0, stopCh)
}

func (j *JobGarbageCollector) GetDynamicInformer(gvr schema.GroupVersionResource) cache.SharedIndexInformer {
	return j.opt.DynamicFactory.ForResource(gvr).Informer()
}

func (j *JobGarbageCollector) GetDynamicLister(gvr schema.GroupVersionResource) cache.GenericLister {
	return j.opt.DynamicFactory.ForResource(gvr).Lister()
}

func (j *JobGarbageCollector) runWorker() {
	for j.processWorkItem() {
	}
}

func (j *JobGarbageCollector) processWorkItem() bool {
	obj, shutdown := j.WaitedCleanQueue.Get()
	if shutdown {
		log.Infof("shutdown waited clean queue for %s controller.", j.Name())
		return false
	}
	defer j.WaitedCleanQueue.Done(obj)
	info, ok := obj.(*FinishedJobInfo)
	if !ok {
		log.Errorf("job[%v] is not a valid finish job request struct.", obj)
		return true
	}
	log.Infof("clean job info=%+v", info)

	gvrMap, err := j.opt.GetGVR(info.GVK)
	if err != nil {
		log.Errorf("find the GroupVersionResource of GroupVersionKind[%s] failed.", info.GVK)
		return true
	}

	err = j.opt.DynamicClient.Resource(gvrMap.Resource).Namespace(info.Namespace).Delete(context.TODO(),
		info.Name, metav1.DeleteOptions{})
	if err != nil {
		log.Errorf("clean [%s] job [%s/%s] failed, error：%v",
			info.GVK, info.Namespace, info.Name, err.Error())
		return true
	}
	log.Infof("auto clean [%s] job [%s/%s] succeed.",
		info.GVK, info.Namespace, info.Name)
	return true
}

// preCleanFinishedJob is applied to clean job when server start
func (j *JobGarbageCollector) preCleanFinishedJob() {
	labelStr := fmt.Sprintf("%s=%s", commonschema.JobOwnerLabel, commonschema.JobOwnerValue)
	labelSelector, err := labels.Parse(labelStr)
	if err != nil {
		log.Errorf("parse label selector[%s] failed, err: %v", labelStr, err)
		return
	}
	for _, list := range j.listerMap {
		jobs, err := list.List(labelSelector)
		if err != nil {
			log.Errorf("list VC job with dynamic client failed: [%+v].", err)
			continue
		}
		for _, job := range jobs {
			j.update(nil, job)
		}
	}
}
