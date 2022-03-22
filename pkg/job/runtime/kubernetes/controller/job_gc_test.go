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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	restclient "k8s.io/client-go/rest"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/k8s"
	commonschema "paddleflow/pkg/common/schema"
)

var (
	VCJobGVR   = schema.GroupVersionResource{Group: "batch.volcano.sh", Version: "v1alpha1", Resource: "jobs"}
	VCQueueGVR = schema.GroupVersionResource{Group: "scheduling.volcano.sh", Version: "v1beta1", Resource: "queues"}
	EQuotaGVR  = schema.GroupVersionResource{Group: "scheduling.volcano.sh", Version: "v1beta1", Resource: "elasticresourcequotas"}
	PodGVR     = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
)

func NewUnstructured(gvk schema.GroupVersionKind, namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
				"labels": map[string]interface{}{
					commonschema.JobOwnerLabel:       commonschema.JobOwnerValue,
					commonschema.VolcanoJobNameLabel: name,
					commonschema.JobIDLabel:          name,
				},
			},
			"status": make(map[string]interface{}),
		},
	}
}

var DiscoveryHandlerFunc = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	var obj interface{}
	switch req.URL.Path {
	case "/apis/batch.volcano.sh/v1alpha1":
		obj = &metav1.APIResourceList{
			GroupVersion: "batch.volcano.sh/v1alpha1",
			APIResources: []metav1.APIResource{
				{Name: "jobs", Namespaced: true, Kind: "Job"},
			},
		}
	case "/apis/scheduling.volcano.sh/v1beta1":
		obj = &metav1.APIResourceList{
			GroupVersion: "scheduling.volcano.sh/v1beta1",
			APIResources: []metav1.APIResource{
				{Name: "queues", Namespaced: false, Kind: "Queue"},
				{Name: "elasticresourcequotas", Namespaced: false, Kind: "ElasticResourceQuota"},
			},
		}
	case "/apis/sparkoperator.k8s.io/v1beta2":
		obj = &metav1.APIResourceList{
			GroupVersion: "sparkoperator.k8s.io/v1beta2",
			APIResources: []metav1.APIResource{
				{Name: "sparkapplications", Namespaced: true, Kind: "SparkApplication"},
			},
		}
	case "/apis/batch.paddlepaddle.org/v1":
		obj = &metav1.APIResourceList{
			GroupVersion: "batch.paddlepaddle.org/v1",
			APIResources: []metav1.APIResource{
				{Name: "paddlejobs", Namespaced: true, Kind: "PaddleJob"},
			},
		}
	case "/api/v1":
		obj = &metav1.APIResourceList{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod"},
				{Name: "namespaces", Namespaced: false, Kind: "Namespace"},
			},
		}
	case "/api":
		obj = &metav1.APIVersions{
			Versions: []string{
				"v1",
			},
		}
	case "/apis":
		obj = &metav1.APIGroupList{
			Groups: []metav1.APIGroup{
				{
					Name: "batch.volcano.sh",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "batch.volcano.sh/v1alpha1", Version: "v1alpha1"},
					},
				},
				{
					Name: "scheduling.volcano.sh",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "scheduling.volcano.sh/v1beta1", Version: "v1beta1"},
					},
				},
				{
					Name: "sparkoperator.k8s.io",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "sparkoperator.k8s.io/v1beta2", Version: "v1beta2"},
					},
				},
				{
					Name: "batch.paddlepaddle.org",
					Versions: []metav1.GroupVersionForDiscovery{
						{GroupVersion: "batch.paddlepaddle.org/v1", Version: "v1"},
					},
				},
			},
		}
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	output, err := json.Marshal(obj)
	if err != nil {
		fmt.Printf("unexpected encoding error: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(output)
})

func newFakeGCController() *JobGarbageCollector {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)

	var server = httptest.NewServer(DiscoveryHandlerFunc)
	defer server.Close()
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	ctrl := &JobGarbageCollector{}
	opt := &k8s.DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
	}
	ctrl.Initialize(opt)
	return ctrl
}

func TestJobGC(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		skipClean bool
		oldObj    *unstructured.Unstructured
		newObj    *unstructured.Unstructured
		oldStatus *batchv1alpha1.JobStatus
		newStatus *batchv1alpha1.JobStatus
	}{
		{
			name:      "vcjob status from pending turn to running",
			namespace: "default",
			skipClean: true,
			oldObj:    NewUnstructured(k8s.VCJobGVK, "default", "vcjob1"),
			oldStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Pending,
				},
				Pending: 1,
				Running: 1,
			},
			newObj: NewUnstructured(k8s.VCJobGVK, "default", "vcjob1"),
			newStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Running,
				},
				Pending: 1,
				Running: 1,
			},
		},
		{
			name:      "vcjob status from running turn to failed",
			namespace: "default",
			skipClean: false,
			oldObj:    NewUnstructured(k8s.VCJobGVK, "default", "vcjob1"),
			oldStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Running,
				},
				Pending: 1,
				Running: 1,
			},
			newObj: NewUnstructured(k8s.VCJobGVK, "default", "vcjob1"),
			newStatus: &batchv1alpha1.JobStatus{
				State: batchv1alpha1.JobState{
					Phase: batchv1alpha1.Failed,
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config.GlobalServerConfig.Job.Reclaim.SkipCleanFailedJob = test.skipClean
			c := newFakeGCController()
			var err error
			test.oldObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.oldStatus)
			assert.Equal(t, nil, err)

			test.newObj.Object["status"], err = runtime.DefaultUnstructuredConverter.ToUnstructured(test.newStatus)
			assert.Equal(t, nil, err)

			_, err = c.opt.DynamicClient.Resource(VCJobGVR).Namespace(test.namespace).Create(context.TODO(), test.oldObj, metav1.CreateOptions{})
			assert.Equal(t, nil, err)
			c.update(test.oldObj, test.newObj)
			stopCh := make(chan struct{})
			defer close(stopCh)
			c.Run(stopCh)
			time.Sleep(2 * time.Second)
		})
	}

}
