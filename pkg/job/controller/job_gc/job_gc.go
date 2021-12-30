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

package job_gc

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	"paddleflow/pkg/job/controller/framework"
)

func init() {
	err := framework.RegisterController(&JobGarbageCollector{})
	if err != nil {
		log.Errorf("init JobSync failed. error:%s", err.Error())
	}
}

type JobGarbageCollector struct {
	sync.Mutex
	opt *framework.ControllerOption

	WaitedCleanQueue workqueue.DelayingInterface
	vcJobInformer    cache.SharedIndexInformer
	vcJobLister      cache.GenericLister

	sparkApplicationInformer cache.SharedIndexInformer
	sparkApplicationLister   cache.GenericLister
}

func (j *JobGarbageCollector) Name() string {
	return "JobGarbageCollector"
}

func (j *JobGarbageCollector) Initialize(opt *framework.ControllerOption) error {
	j.opt = opt
	j.WaitedCleanQueue = workqueue.NewDelayingQueue()

	sparkAppGVR, err := k8s.GetGVRByGVK(k8s.SparkAppGVK)
	if err != nil {
		log.Warnf("cann't find GroupVersionKind [%s]", k8s.SparkAppGVK)
	} else {
		j.sparkApplicationInformer = j.GetDynamicInformer(sparkAppGVR)
		j.sparkApplicationLister = j.GetDynamicLister(sparkAppGVR)
		j.sparkApplicationInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			UpdateFunc: j.updateSparkApp,
		})
	}
	vcJobGVR, err := k8s.GetGVRByGVK(k8s.VCJobGVK)
	if err != nil {
		log.Warnf("cann't find GroupVersionKind [%s]", k8s.VCJobGVK)
	} else {
		j.vcJobInformer = j.GetDynamicInformer(vcJobGVR)
		j.vcJobLister = j.GetDynamicLister(vcJobGVR)
		j.vcJobInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			UpdateFunc: j.updateVCJob,
		})
	}

	return nil
}

func (j *JobGarbageCollector) Run(stopCh <-chan struct{}) {
	if !config.GlobalServerConfig.Job.Reclaim.CleanJob {
		log.Infof("Skip %s controller!", j.Name())
		return
	}

	log.Infof("Start %s controller!", j.Name())
	go j.opt.DynamicFactory.Start(stopCh)

	if j.sparkApplicationInformer != nil {
		if !cache.WaitForCacheSync(stopCh, j.sparkApplicationInformer.HasSynced) {
			runtime.HandleError(fmt.Errorf("timed out waiting for caches to job_sync"))
			return
		}
	}
	if j.vcJobInformer != nil {
		if !cache.WaitForCacheSync(stopCh, j.vcJobInformer.HasSynced) {
			runtime.HandleError(fmt.Errorf("timed out waiting for caches to job_sync"))
			return
		}
	}
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
		return false
	}
	defer j.WaitedCleanQueue.Done(obj)
	info, ok := obj.(*framework.FinishedJobInfo)
	if !ok {
		log.Errorf("job[%s] is not a valid finish job request struct.", info.Name)
		return true
	}
	log.Debugf("clean job info=%+v", info)

	gvr, err := k8s.GetGVRByGVK(info.GVK)
	if err != nil {
		log.Errorf("find the GroupVersionResource of GroupVersionKind[%s] failed.", info.GVK)
		return true
	}

	err = j.opt.DynamicClient.Resource(gvr).Namespace(info.Namespace).Delete(context.TODO(),
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
	if j.vcJobLister != nil {
		j.preCleanVCJob()
	}
	if j.sparkApplicationLister != nil {
		j.preCleanSparkApp()
	}
}
