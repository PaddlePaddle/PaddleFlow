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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	commonschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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

func newFakeGCController() *JobGarbageCollector {
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	fakeDiscovery := discovery.NewDiscoveryClientForConfigOrDie(&restclient.Config{Host: server.URL})

	ctrl := &JobGarbageCollector{}
	opt := &k8s.DynamicClientOption{
		DynamicClient:   dynamicClient,
		DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
		DiscoveryClient: fakeDiscovery,
		ClusterInfo:     &commonschema.Cluster{Name: "test-cluster"},
	}
	err := ctrl.Initialize(opt)
	if err != nil {
		panic("init fake gc controller failed")
	}
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
