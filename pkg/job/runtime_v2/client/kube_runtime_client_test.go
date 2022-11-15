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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"net/http/httptest"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/util/kubeutil"
)

func TestExecutor(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	runtimeClient := NewFakeKubeRuntimeClient(server)

	// create namespaced kubernetes resource
	gvk := k8s.VCJobGVK
	name := "vcjob"
	namespace := "default"

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"status": make(map[string]interface{}),
		}}
	patchJSON, err := json.Marshal(struct {
		metav1.ObjectMeta
	}{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"label1": "value1",
			},
			Annotations: map[string]string{
				"anno1": "value1",
			},
		},
	})
	t.Logf("patch resource %s", string(patchJSON))
	assert.Equal(t, nil, err)
	frameworkVersion := pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	// create kubernetes resource with dynamic client
	err = runtimeClient.Create(obj, frameworkVersion)
	assert.Equal(t, nil, err)
	// patch kubernetes resource with dynamic client
	err = runtimeClient.Patch(namespace, name, frameworkVersion, patchJSON)
	assert.Equal(t, nil, err)
	// get kubernetes resource with dynamic client
	_, err = runtimeClient.Get(namespace, name, frameworkVersion)
	assert.Equal(t, nil, err)
	t.Logf("get patched resource %v", obj)
	// delete kubernetes resource with dynamic client
	err = runtimeClient.Delete(namespace, name, frameworkVersion)
	assert.Equal(t, nil, err)
	// kubernetes resource is not found
	err = runtimeClient.Delete(namespace, name, frameworkVersion)
	assert.NotEqual(t, nil, err)

	// create non namespaced kubernetes resource
	gvk = k8s.VCQueueGVK
	obj = &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gvk.GroupVersion().String(),
			"kind":       gvk.Kind,
			"metadata": map[string]interface{}{
				"name": name,
			},
			"status": make(map[string]interface{}),
		},
	}
	// create kubernetes resource with dynamic client
	frameworkVersion = pfschema.NewFrameworkVersion(gvk.Kind, gvk.GroupVersion().String())
	err = runtimeClient.Create(obj, frameworkVersion)
	assert.Equal(t, nil, err)
	// get kubernetes resource with dynamic client
	_, err = runtimeClient.Get(namespace, name, frameworkVersion)
	assert.Equal(t, nil, err)
	// delete kubernetes resource with dynamic client
	err = runtimeClient.Delete(namespace, name, frameworkVersion)
	assert.Equal(t, nil, err)
}

func TestNodeTaskListener(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	runtimeClient := NewFakeKubeRuntimeClient(server)
	// init 2w pods
	var err error
	var mockPodName = "test-pod-name"
	var mockPodNamespace = "default"
	var mockDeletionGracePeriod int64 = 30
	var podCount = 1000
	var namespaceList = []string{"default", "test1", "test2", "test3", "test4"}
	var nodeNameList = []string{"instance-0", "instance-1", "instance-2", "instance-3"}
	var reqList = []corev1.ResourceList{
		kubeutil.BuildResourceList("1", "2Gi"),
		kubeutil.BuildResourceList("2", "4Gi"),
		kubeutil.BuildResourceList("1", "5Gi"),
		kubeutil.BuildResourceList("1", "8Gi"),
		kubeutil.BuildResourceList("0", "0Gi"),
	}
	err = kubeutil.CreatePods(runtimeClient.Client, podCount, namespaceList, nodeNameList, kubeutil.PhaseList, reqList)
	assert.Equal(t, nil, err)

	process := func(q workqueue.RateLimitingInterface) bool {
		obj, shutdown := q.Get()
		if shutdown {
			t.Logf("fail to pop node task sync item from queue")
			return false
		}
		taskSync := obj.(*api.NodeTaskSyncInfo)
		defer q.Done(taskSync)
		q.Forget(taskSync)
		return true
	}

	taskQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	// register pod listener
	err = runtimeClient.RegisterListener(pfschema.ListenerTypeNodeTask, taskQueue)
	assert.Equal(t, nil, err)
	// start pod listener
	stopCh := make(chan struct{})
	err = runtimeClient.StartListener(pfschema.ListenerTypeNodeTask, stopCh)
	assert.Equal(t, nil, err)

	go wait.Until(func() {
		for process(taskQueue) {
		}
	}, 0, stopCh)

	fakePod := kubeutil.BuildFakePod(mockPodName, mockPodNamespace, "", corev1.PodPending, reqList[0])
	_, err = runtimeClient.Client.CoreV1().Pods(mockPodNamespace).Create(context.TODO(), fakePod, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// update pod Running
	fakePod.Spec.NodeName = "instance-01"
	fakePod.Status.Phase = corev1.PodRunning
	fakePod, err = runtimeClient.Client.CoreV1().Pods(mockPodNamespace).Update(context.TODO(), fakePod, metav1.UpdateOptions{})
	assert.Equal(t, nil, err)
	// update pod DeletionGracePeriodSeconds
	fakePod.ObjectMeta.DeletionGracePeriodSeconds = &mockDeletionGracePeriod
	fakePod, err = runtimeClient.Client.CoreV1().Pods(mockPodNamespace).Update(context.TODO(), fakePod, metav1.UpdateOptions{})
	assert.Equal(t, nil, err)
	// update pod
	fakePod.Status.Phase = corev1.PodSucceeded
	_, err = runtimeClient.Client.CoreV1().Pods(mockPodNamespace).Update(context.TODO(), fakePod, metav1.UpdateOptions{})
	assert.Equal(t, nil, err)
	// delete node task
	err = runtimeClient.Client.CoreV1().Pods(mockPodNamespace).Delete(context.TODO(), mockPodName, metav1.DeleteOptions{})
	assert.Equal(t, nil, err)

	for taskQueue.Len() != 0 {
		time.Sleep(100 * time.Millisecond)
	}
	close(stopCh)
}

func TestNodeListener(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	var reqList = []corev1.ResourceList{
		kubeutil.BuildResourceList("56", "256Gi"),
		kubeutil.BuildResourceList("48", "128Gi"),
		kubeutil.BuildResourceList("64", "512Gi"),
		kubeutil.BuildResourceList("96", "768Gi"),
	}
	var labelList = []map[string]string{
		{
			pfschema.PFNodeLabels: "cpu-1",
		},
		{
			"xxx/resource-pool": "cpu-2",
		},
		{
			"xxx/resource-pool": "xpu-3",
		},
	}
	process := func(q workqueue.RateLimitingInterface) bool {
		obj, shutdown := q.Get()
		if shutdown {
			t.Logf("fail to pop node sync item from queue")
			return false
		}
		nodeSync := obj.(*api.NodeSyncInfo)
		defer q.Done(nodeSync)

		t.Logf("try to handle node sync: %v", nodeSync)
		q.Forget(nodeSync)
		return true
	}

	runtimeClient := NewFakeKubeRuntimeClient(server)
	// init 2k nodes
	var nodeCount = 2000
	err := kubeutil.CreateNodes(runtimeClient.Client, nodeCount, reqList, kubeutil.NodeCondList, labelList)
	assert.Equal(t, nil, err)

	nodeQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	// register node listener
	err = runtimeClient.RegisterListener(pfschema.ListenerTypeNode, nodeQueue)
	assert.Equal(t, nil, err)
	// start node listener
	stopCh := make(chan struct{})
	err = runtimeClient.StartListener(pfschema.ListenerTypeNode, stopCh)
	assert.Equal(t, nil, err)

	go wait.Until(func() {
		for process(nodeQueue) {
		}
	}, 0, stopCh)

	for nodeQueue.Len() != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	close(stopCh)
}
