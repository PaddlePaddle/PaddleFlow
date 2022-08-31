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

package client

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
)

// KubeRuntimeClient for kubernetes client
type KubeRuntimeClient struct {
	DynamicClient   dynamic.Interface
	DynamicFactory  dynamicinformer.DynamicSharedInformerFactory
	DiscoveryClient discovery.DiscoveryInterface
	// TODO: adjust field Config
	Config      *rest.Config
	ClusterInfo *pfschema.Cluster
	// GVKToGVR contains GroupVersionKind map to GroupVersionResource
	GVKToGVR sync.Map

	// informerMap contains GroupVersionKind and informer for different kubernetes job
	informerMap map[schema.GroupVersionKind]cache.SharedIndexInformer
	// podInformer contains the informer of task
	podInformer cache.SharedIndexInformer
	podLister   cache.GenericLister
}

func CreateKubeRuntimeClient(config *rest.Config, cluster *pfschema.Cluster) (framework.RuntimeClientInterface, error) {
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
	if cluster == nil {
		log.Errorf("cluster info is nil")
		return nil, fmt.Errorf("cluster info is nil")
	}
	return &KubeRuntimeClient{
		DynamicClient:   dynamicClient,
		DynamicFactory:  factory,
		DiscoveryClient: discoveryClient,
		Config:          config,
		ClusterInfo:     cluster,
		informerMap:     make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
	}, nil
}

func (krc *KubeRuntimeClient) RegisterListeners(jobQueue, taskQueue workqueue.RateLimitingInterface) error {
	for gvk := range k8s.GVKJobStatusMap {
		gvrMap, err := krc.GetGVR(gvk)
		if err != nil {
			log.Warnf("cann't find GroupVersionKind %s, err: %v", gvk.String(), err)
		} else {
			jobBuilder, find := framework.GetJobBuilder(pfschema.KubernetesType, gvk.String())
			if !find {
				log.Warnf("cann't find GroupVersionKind %s, err: %v", gvk.String(), err)
				continue
			}

			krc.informerMap[gvk] = krc.DynamicFactory.ForResource(gvrMap.Resource).Informer()

			jobBuilder(krc).RegisterJobListener(context.TODO(), jobQueue, krc.informerMap[gvk])
		}
	}

	podGVRMap, err := krc.GetGVR(k8s.PodGVK)
	if err != nil {
		return err
	}
	krc.podInformer = krc.DynamicFactory.ForResource(podGVRMap.Resource).Informer()
	// Register task handle
	jobBuilder, find := framework.GetJobBuilder(pfschema.KubernetesType, k8s.PodGVK.String())
	if !find {
		log.Warnf("cann't find GroupVersionKind %s, err: %v", k8s.PodGVK.String(), err)
	}
	jobBuilder(krc).RegisterTaskListener(context.TODO(), taskQueue, krc.podInformer)

	krc.podLister = krc.DynamicFactory.ForResource(podGVRMap.Resource).Lister()

	return nil
}

func (krc *KubeRuntimeClient) StartLister(stopCh <-chan struct{}) {
	if len(krc.informerMap) == 0 {
		log.Infof("Cluster hasn't any GroupVersionKind, skip %s controller!", krc.Cluster())
		return
	}
	go krc.DynamicFactory.Start(stopCh)

	for _, informer := range krc.informerMap {
		if !cache.WaitForCacheSync(stopCh, informer.HasSynced) {
			log.Errorf("timed out waiting for caches to %s", krc.Cluster())
			return
		}
	}
	if !cache.WaitForCacheSync(stopCh, krc.podInformer.HasSynced) {
		log.Errorf("timed out waiting for pod caches to %s", krc.Cluster())
		return
	}
	return
}

func (krc *KubeRuntimeClient) Cluster() string {
	msg := ""
	if krc.ClusterInfo != nil {
		msg = fmt.Sprintf("name %s with cluster type %s", krc.ClusterInfo.Name, krc.ClusterInfo.Type)
	}
	return msg
}

func (krc *KubeRuntimeClient) ClusterID() string {
	clusterID := ""
	if krc.ClusterInfo != nil {
		clusterID = krc.ClusterInfo.ID
	}
	return clusterID
}

func (krc *KubeRuntimeClient) ListNodeQuota(ctx context.Context) (pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error) {
	// TODO: add ListNodeQuota logic
	return pfschema.QuotaSummary{}, []pfschema.NodeQuotaInfo{}, nil
}

func (krc *KubeRuntimeClient) GetGVR(gvk schema.GroupVersionKind) (meta.RESTMapping, error) {
	gvr, ok := krc.GVKToGVR.Load(gvk.String())
	if ok {
		return gvr.(meta.RESTMapping), nil
	}
	return krc.findGVR(&gvk)
}

func (krc *KubeRuntimeClient) findGVR(gvk *schema.GroupVersionKind) (meta.RESTMapping, error) {
	// DiscoveryClient queries API server about the resources
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(krc.DiscoveryClient))
	// Find GVR
	mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		log.Warningf("find GVR with restMapping failed: %v", err)
		return meta.RESTMapping{}, err
	}
	// Store GVR
	log.Debugf("The GVR of GVK[%s] is [%s]", gvk.String(), mapping.Resource.String())
	krc.GVKToGVR.Store(gvk.String(), *mapping)
	return *mapping, nil
}

func frameworkVersionToGVK(fv pfschema.FrameworkVersion) schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(fv.APIVersion, fv.Framework)
}

func (krc *KubeRuntimeClient) Get(namespace string, name string, fv pfschema.FrameworkVersion) (interface{}, error) {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to get kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if krc == nil {
		return nil, fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return nil, err
	}
	var obj *unstructured.Unstructured
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		obj, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Get(context.TODO(), name, v1.GetOptions{})
	} else {
		obj, err = krc.DynamicClient.Resource(gvrMap.Resource).Get(context.TODO(), name, v1.GetOptions{})
	}
	if err != nil {
		log.Errorf("get kubernetes %s resource[%s/%s] failed. error:[%s]", gvk.String(), namespace, name, err.Error())
	}
	return obj, err
}

func (krc *KubeRuntimeClient) Create(resource interface{}, fv pfschema.FrameworkVersion) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to create kuberentes resource[%s]", gvk.String())
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}

	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	obj.SetKind(gvk.Kind)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	// Create the object with dynamic client
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(obj.GetNamespace()).Create(context.TODO(), obj, v1.CreateOptions{})
	} else {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Create(context.TODO(), obj, v1.CreateOptions{})
	}
	if err != nil {
		log.Errorf("create kuberentes resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}

func (krc *KubeRuntimeClient) Delete(namespace string, name string, fv pfschema.FrameworkVersion) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to delete kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	propagationPolicy := v1.DeletePropagationBackground
	deleteOptions := v1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Delete(context.TODO(), name, deleteOptions)
	} else {
		err = krc.DynamicClient.Resource(gvrMap.Resource).Delete(context.TODO(), name, deleteOptions)
	}
	if err != nil {
		log.Errorf("delete kubernetes  resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}

func (krc *KubeRuntimeClient) Patch(namespace, name string, fv pfschema.FrameworkVersion, data []byte) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to patch kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	patchType := types.StrategicMergePatchType
	patchOptions := v1.PatchOptions{}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Patch(context.TODO(), name, patchType, data, patchOptions)
	} else {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Patch(context.TODO(), name, patchType, data, patchOptions)
	}
	if err != nil {
		log.Errorf("patch kubernetes resource: %s failed. error: %s", gvk.String(), err.Error())
	}
	return err
}

func (krc *KubeRuntimeClient) Update(resource interface{}, fv pfschema.FrameworkVersion) error {
	gvk := frameworkVersionToGVK(fv)
	log.Debugf("executor begin to update kubernetes resource[%s]", gvk.String())
	if krc == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := krc.GetGVR(gvk)
	if err != nil {
		return err
	}

	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		log.Errorf("convert to unstructured failed, err: %v", err)
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	obj.SetKind(gvk.Kind)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	// Create the object with dynamic client
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Namespace(obj.GetNamespace()).Update(context.TODO(), obj, v1.UpdateOptions{})
	} else {
		_, err = krc.DynamicClient.Resource(gvrMap.Resource).Update(context.TODO(), obj, v1.UpdateOptions{})
	}
	if err != nil {
		log.Errorf("update kuberentes resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}
