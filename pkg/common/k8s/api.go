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

package k8s

import (
	"sync"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	commomschema "paddleflow/pkg/common/schema"
)

var (
	PodGVK       = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	VCJobGVK     = schema.GroupVersionKind{Group: "batch.volcano.sh", Version: "v1alpha1", Kind: "Job"}
	VCQueueGVK   = schema.GroupVersionKind{Group: "scheduling.volcano.sh", Version: "v1beta1", Kind: "Queue"}
	EQuotaGVK    = schema.GroupVersionKind{Group: "scheduling.volcano.sh", Version: "v1beta1", Kind: "ElasticResourceQuota"}
	SparkAppGVK  = schema.GroupVersionKind{Group: "sparkoperator.k8s.io", Version: "v1beta2", Kind: "SparkApplication"}
	PaddleJobGVK = schema.GroupVersionKind{Group: "batch.paddlepaddle.org", Version: "v1", Kind: "PaddleJob"}

	// GVKToJobType maps GroupVersionKind to PaddleFlow JobType
	GVKToJobType = map[schema.GroupVersionKind]commomschema.JobType{
		VCJobGVK:     commomschema.TypeVcJob,
		SparkAppGVK:  commomschema.TypeSparkJob,
		PaddleJobGVK: commomschema.TypePaddleJob,
	}
	// GVKJobStatusMap contains GroupVersionKind and get status
	GVKJobStatusMap = map[schema.GroupVersionKind]GetStatusFunc{
		VCJobGVK:     VCJobStatus,
		SparkAppGVK:  SparkAppStatus,
		PaddleJobGVK: PaddleJobStatus,
	}
	// GVKToQuotaType GroupVersionKind lists for PaddleFlow QuotaType
	GVKToQuotaType = []schema.GroupVersionKind{
		VCQueueGVK,
		EQuotaGVK,
	}
)

type StatusInfo struct {
	OriginStatus string
	Status       commomschema.JobStatus
	Message      string
}

type StatusIsUpdatedFunc func(interface{}, interface{}) (bool, string)
type GetStatusFunc func(interface{}) (StatusInfo, error)

// DynamicClientOption for kubernetes dynamic client
type DynamicClientOption struct {
	DynamicClient   dynamic.Interface
	DynamicFactory  dynamicinformer.DynamicSharedInformerFactory
	DiscoveryClient discovery.DiscoveryInterface
	Config          *rest.Config
	// GVKToGVR contains GroupVersionKind map to GroupVersionResource
	GVKToGVR sync.Map
}

func CreateDynamicClientOpt(config *rest.Config) (*DynamicClientOption, error) {
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Errorf("init dynamic client failed. error:%s", err)
		return nil, err
	}
	factory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		log.Errorf("create discovery client failed: %v", err)
		return nil, err
	}
	return &DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  factory,
		DiscoveryClient: discoveryClient,
		Config:          config,
	}, nil
}

func (dc *DynamicClientOption) GetGVR(gvk schema.GroupVersionKind) (meta.RESTMapping, error) {
	gvr, ok := dc.GVKToGVR.Load(gvk.String())
	if ok {
		return gvr.(meta.RESTMapping), nil
	}
	return dc.findGVR(&gvk)
}

func (dc *DynamicClientOption) findGVR(gvk *schema.GroupVersionKind) (meta.RESTMapping, error) {
	// DiscoveryClient queries API server about the resources
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc.DiscoveryClient))
	// Find GVR
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		log.Errorf("find GVR with restMapping failed: %v", err)
		return meta.RESTMapping{}, err
	}
	// Store GVR
	log.Debugf("The GVR of GVK[%s] is [%s]", gvk.String(), mapping.Resource.String())
	dc.GVKToGVR.Store(gvk.String(), *mapping)
	return *mapping, nil
}
