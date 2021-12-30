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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"paddleflow/pkg/common/config"
)

var (
	PodGVK      = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	VCJobGVK    = schema.GroupVersionKind{Group: "batch.volcano.sh", Version: "v1alpha1", Kind: "Job"}
	SparkAppGVK = schema.GroupVersionKind{Group: "sparkoperator.k8s.io", Version: "v1beta2", Kind: "SparkApplication"}

	GVKToGVR sync.Map
)

func GetGVRByGVK(gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	gvr, ok := GVKToGVR.Load(gvk.String())
	if ok {
		return gvr.(schema.GroupVersionResource), nil
	}
	cfg := config.InitKubeConfig(config.GlobalServerConfig.KubeConfig)
	return findGVR(&gvk, cfg)
}

func findGVR(gvk *schema.GroupVersionKind, cfg *rest.Config) (schema.GroupVersionResource, error) {
	// DiscoveryClient queries API server about the resources
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		log.Errorf("create discovery client failed: %v", err)
		return schema.GroupVersionResource{}, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))
	// Find GVR
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		log.Errorf("find GVR with restMapping failed: %v", err)
		return schema.GroupVersionResource{}, err
	}
	// Store GVR
	log.Debugf("The GVR of GVK[%s] is [%s]", gvk.String(), mapping.Resource.String())
	GVKToGVR.Store(gvk.String(), mapping.Resource)
	return mapping.Resource, nil
}
