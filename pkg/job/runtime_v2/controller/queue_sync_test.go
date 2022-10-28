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
	"net/http/httptest"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queue/elasticqueue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queue/vcqueue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var (
	VCQueueGVR = schema.GroupVersionResource{Group: "scheduling.volcano.sh", Version: "v1beta1", Resource: "queues"}
	EQuotaGVR  = schema.GroupVersionResource{Group: "scheduling.volcano.sh", Version: "v1beta1", Resource: "elasticresourcequotas"}
)

func newFakeQueueSyncController() *QueueSync {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	ctrl := &QueueSync{}
	opt := &client.KubeRuntimeClient{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo: &pfschema.Cluster{
			Name: "default-cluster",
			ID:   "cluster-123",
			Type: "Kubernetes",
		},
		JobInformerMap:   make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
		QueueInformerMap: make(map[schema.GroupVersionKind]cache.SharedIndexInformer),
		Config:           &restclient.Config{Host: server.URL},
	}
	err := ctrl.Initialize(opt)
	if err != nil {
		log.Errorf("initialize controller failed: %v", err)
	}
	return ctrl
}

func TestQueueSync(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		gvr       schema.GroupVersionResource
		gvk       schema.GroupVersionKind
		fv        pfschema.FrameworkVersion
		oldQueue  interface{}
		newQueue  interface{}
	}{
		{
			name:      "volcano queue capability quota",
			queueName: "q1",
			gvr:       VCQueueGVR,
			gvk:       k8s.VCQueueGVK,
			fv:        vcqueue.KubeVCQueueQuotaType,
			oldQueue: &v1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "q1",
				},
				Spec: v1beta1.QueueSpec{
					Capability: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("10"),
						v1.ResourceMemory: resource.MustParse("20Gi"),
					},
				},
			},
			newQueue: &v1beta1.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "q1",
				},
				Spec: v1beta1.QueueSpec{
					Capability: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("10"),
						v1.ResourceMemory: resource.MustParse("30Gi"),
					},
				},
			},
		},
		{
			name:      "elastic resource quota",
			queueName: "elasticQuota1",
			gvr:       EQuotaGVR,
			gvk:       k8s.EQuotaGVK,
			fv:        elasticqueue.KubeElasticQueueQuotaType,
			oldQueue: &v1beta1.ElasticResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "elasticQuota1",
				},
				Spec: v1beta1.ElasticResourceQuotaSpec{
					Max: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("20"),
						v1.ResourceMemory: resource.MustParse("40Gi"),
					},
					Min: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("10"),
						v1.ResourceMemory: resource.MustParse("20Gi"),
					},
				},
			},
			newQueue: &v1beta1.ElasticResourceQuota{
				ObjectMeta: metav1.ObjectMeta{
					Name: "elasticQuota1",
				},
				Spec: v1beta1.ElasticResourceQuotaSpec{
					Max: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("20"),
						v1.ResourceMemory: resource.MustParse("45Gi"),
					},
					Min: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("10"),
						v1.ResourceMemory: resource.MustParse("25Gi"),
					},
				},
			},
		},
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Reclaim: config.ReclaimConfig{
				CleanJob: true,
			},
		},
	}

	driver.InitMockDB()
	c := newFakeQueueSyncController()
	stopCh := make(chan struct{})
	defer close(stopCh)
	c.Run(stopCh)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.NotEqual(t, nil, c)
			err := storage.Queue.CreateQueue(&model.Queue{
				Name: test.queueName,
			})
			assert.Equal(t, nil, err)

			oldObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.oldQueue)
			assert.Equal(t, nil, err)
			unOldObj := &unstructured.Unstructured{Object: oldObj}
			unOldObj.SetGroupVersionKind(test.gvk)

			err = c.runtimeClient.Create(unOldObj, test.fv)
			assert.Equal(t, nil, err)

			newObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(test.newQueue)
			assert.Equal(t, nil, err)
			unNewObj := &unstructured.Unstructured{Object: newObj}
			unNewObj.SetGroupVersionKind(test.gvk)

			err = c.runtimeClient.Update(unNewObj, test.fv)
			assert.Equal(t, nil, err)
			err = c.runtimeClient.Delete(unNewObj.GetNamespace(), unNewObj.GetName(), test.fv)
			assert.Equal(t, nil, err)
		})
	}
	time.Sleep(2 * time.Second)
}
