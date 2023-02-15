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
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic/dynamicinformer"
	fakedynamicclient "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	fakedclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/util/kubeutil"
)

func mockK3SRuntimeClient(initCluster bool) *K3SRuntimeClient {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	scheme := runtime.NewScheme()
	dynamicClient := fakedynamicclient.NewSimpleDynamicClient(scheme)
	kubeClient := fakedclient.NewSimpleClientset()

	if initCluster {
		return &K3SRuntimeClient{
			Client:          kubeClient,
			InformerFactory: informers.NewSharedInformerFactory(kubeClient, 0),
			DynamicClient:   dynamicClient,
			DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
			ClusterInfo: &schema.Cluster{
				Name: "default-cluster",
				ID:   uuid.GenerateID("cluster"),
				Type: schema.K3SType,
			},
			Config:         &rest.Config{Host: server.URL},
			JobInformerMap: make(map[k8sschema.GroupVersionResource]cache.SharedIndexInformer),
		}
	} else {
		return &K3SRuntimeClient{
			Client:          kubeClient,
			InformerFactory: informers.NewSharedInformerFactory(kubeClient, 0),
			DynamicClient:   dynamicClient,
			DynamicFactory:  dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0),
			Config:          &rest.Config{Host: server.URL},
			JobInformerMap:  make(map[k8sschema.GroupVersionResource]cache.SharedIndexInformer),
		}
	}
}

func createMockNode(client kubernetes.Interface) error {
	nodeName := "mockNodeName"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				"cpu":    resource.MustParse("22"),
				"memory": resource.MustParse("22Gi"),
			},
			Allocatable: corev1.ResourceList{
				"cpu":    resource.MustParse("20"),
				"memory": resource.MustParse("20Gi"),
			},
		},
	}
	// create node
	_, err := client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	return err
}

func TestK3SRuntimeClient_Cluster(t *testing.T) {
	runtimeClient := mockK3SRuntimeClient(true)
	assert.NotNil(t, runtimeClient.Cluster())
	assert.Equal(t, runtimeClient.ClusterName(), "default-cluster")
	assert.NotNil(t, runtimeClient.ClusterID())
	runtimeClient = mockK3SRuntimeClient(false)
	assert.Contains(t, runtimeClient.ClusterID(), "defaultK3SID")
	assert.Contains(t, runtimeClient.Cluster(), "SingleNode")
	assert.Contains(t, runtimeClient.ClusterName(), "K3S Get Node Err")
	// mock node
	err := createMockNode(runtimeClient.Client)
	assert.Nil(t, err)
	assert.Contains(t, runtimeClient.ClusterName(), "mockNodeName")
}

func TestK3SRuntimeClient_ListNodeQuota(t *testing.T) {
	// todo://add list node quota
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	runtimeClient := NewFakeK3SRuntimeClient(server)
	_, ret2, err := runtimeClient.ListNodeQuota(context.TODO())
	assert.Nil(t, ret2)
	assert.Nil(t, err)
}

func TestK3SExecutor(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	runtimeClient := NewFakeK3SRuntimeClient(server)

	// create namespaced kubernetes resource
	gvk := k8s.PodGVK
	name := "pod"
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
	// update
	err = runtimeClient.Update(obj, frameworkVersion)
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
}

func TestK3SNodeTaskListener(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	runtimeClient := NewFakeK3SRuntimeClient(server)
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

func TestK3SNodeListener(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	os.Setenv(pfschema.EnvPFResourceFilter, "test_,fuse")

	var reqList = []corev1.ResourceList{
		kubeutil.BuildResourceList("56", "256Gi"),
		kubeutil.BuildResourceList("48", "128Gi"),
		kubeutil.BuildResourceList("64", "512Gi"),
		kubeutil.BuildResourceList("96", "768Gi"),
		{
			corev1.ResourceCPU:    resource.MustParse("24"),
			corev1.ResourceMemory: resource.MustParse("256Gi"),
			corev1.ResourcePods:   resource.MustParse("110"),
			"nvidia.com/gpu":      resource.MustParse("8"),
		},
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

	runtimeClient := NewFakeK3SRuntimeClient(server)

	type args struct {
		name              string
		listenerType      string
		startListenerErr  error
		cachetListenerErr error
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "success",
			args: args{
				listenerType: pfschema.ListenerTypeNode,
			},
			wantErr: nil,
		},
		{
			name: "startListenerErr",
			args: args{
				listenerType:     pfschema.ListenerTypeNode,
				startListenerErr: fmt.Errorf("timed out waiting for"),
			},
			wantErr: nil,
		},
		{
			name: "ListenerTypeQueue",
			args: args{
				listenerType:     pfschema.ListenerTypeQueue,
				startListenerErr: fmt.Errorf("is not supported"),
			},
			wantErr: nil,
		},
		{
			name: "timeout",
			args: args{
				listenerType:      pfschema.ListenerTypeJob,
				startListenerErr:  nil,
				cachetListenerErr: fmt.Errorf("timed out waiting for"),
			},
			wantErr: nil,
		},
	}

	// init 2k nodes
	var nodeCount = 2000
	err := kubeutil.CreateNodes(runtimeClient.Client, nodeCount, reqList, kubeutil.NodeCondList, labelList)
	assert.Equal(t, nil, err)

	nodeQueue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	// register node listener
	err = runtimeClient.RegisterListener(pfschema.ListenerTypeNode, nodeQueue)
	assert.Equal(t, nil, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.startListenerErr != nil {
				//WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool
				var p1 = gomonkey.ApplyMethodFunc(reflect.TypeOf(runtimeClient.InformerFactory), "WaitForCacheSync",
					func(b <-chan struct{}) map[reflect.Type]bool {
						m := make(map[reflect.Type]bool)
						m[reflect.TypeOf(pfschema.ListenerTypeNode)] = false
						m[reflect.TypeOf(pfschema.ListenerTypeNodeTask)] = false
						return m
					})
				defer p1.Reset()
				// start node listener
				stopCh := make(chan struct{})
				//defer close(stopCh)
				err = runtimeClient.StartListener(tt.args.listenerType, stopCh)
				if err != nil {
					assert.Error(t, tt.args.startListenerErr)
					assert.Contains(t, err.Error(), tt.args.startListenerErr.Error())
				}

				go wait.Until(func() {
					for process(nodeQueue) {
					}
				}, 0, stopCh)

				for nodeQueue.Len() != 0 {
					time.Sleep(10 * time.Millisecond)
				}
				close(stopCh)
			} else if tt.args.cachetListenerErr != nil {
				var p1 = gomonkey.ApplyFunc(cache.WaitForCacheSync, func(_ <-chan struct{}, _ ...cache.InformerSynced) bool {
					return false
				})
				defer p1.Reset()
				// start node listener
				stopCh := make(chan struct{})

				gvr := k8s.GetJobGVR(pfschema.Framework(schema.FrameworkStandalone))
				runtimeClient.podInformer = runtimeClient.DynamicFactory.ForResource(gvr).Informer()
				err = runtimeClient.StartListener(tt.args.listenerType, stopCh)
				if err != nil {
					assert.Error(t, tt.args.cachetListenerErr)
					assert.Contains(t, err.Error(), tt.args.cachetListenerErr.Error())
				}

				go wait.Until(func() {
					for process(nodeQueue) {
					}
				}, 0, stopCh)

				for nodeQueue.Len() != 0 {
					time.Sleep(10 * time.Millisecond)
				}
				close(stopCh)
			} else {
				// start node listener
				stopCh := make(chan struct{})
				err = runtimeClient.StartListener(tt.args.listenerType, stopCh)
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
		})
	}

}

func TestK3SGetJobTypeFramework(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	runtimeClient := NewFakeK3SRuntimeClient(server)

	type args struct {
		fv              pfschema.FrameworkVersion
		expectJobType   pfschema.JobType
		expectFramework pfschema.Framework
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "success",
			args: args{
				fv:              schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String()),
				expectJobType:   schema.TypeSingle,
				expectFramework: schema.FrameworkStandalone,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobType, framework := runtimeClient.GetJobTypeFramework(tt.args.fv)
			assert.Equal(t, tt.args.expectJobType, jobType)
			assert.Equal(t, tt.args.expectFramework, framework)
		})
	}
}

func TestK3SJobFrameworkVersion(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	runtimeClient := NewFakeK3SRuntimeClient(server)

	type args struct {
		fv        pfschema.FrameworkVersion
		jobType   pfschema.JobType
		framework pfschema.Framework
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "success",
			args: args{
				fv:        schema.NewFrameworkVersion(k8s.PodGVK.Kind, k8s.PodGVK.GroupVersion().String()),
				jobType:   schema.TypeSingle,
				framework: schema.FrameworkStandalone,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fv := runtimeClient.JobFrameworkVersion(tt.args.jobType, tt.args.framework)
			assert.Equal(t, tt.args.fv, fv)
		})
	}
}

func TestK3SGetTaskLogV2(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	runtimeClient := NewFakeK3SRuntimeClient(server)
	createK3SMockLog(t, runtimeClient)
	type args struct {
		namespace     string
		name          string
		logpage       utils.LogPage
		RuntimeClient *K3SRuntimeClient
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errMsg  string
	}{
		{
			name: "nil name",
			args: args{
				name:          "",
				namespace:     "default",
				logpage:       utils.LogPage{},
				RuntimeClient: runtimeClient,
			},
			wantErr: false,
			errMsg:  "",
		},
		{
			name: "nil namespace",
			args: args{
				name:          "abc",
				namespace:     "",
				logpage:       utils.LogPage{},
				RuntimeClient: runtimeClient,
			},
			wantErr: false,
			errMsg:  "",
		},
		{
			name: "test nil runtimeClient",
			args: args{
				name:          "test",
				namespace:     "default",
				logpage:       utils.LogPage{},
				RuntimeClient: &K3SRuntimeClient{},
			},
			wantErr: false,
			errMsg:  "",
		},
		{
			name: "test runtimeClient",
			args: args{
				name:          mockPodName,
				namespace:     mockNS,
				logpage:       utils.LogPage{},
				RuntimeClient: runtimeClient,
			},
			wantErr: false,
			errMsg:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)

			res, err := runtimeClient.GetTaskLogV2(tt.args.namespace, tt.args.name, tt.args.logpage)
			t.Logf("case[%s] get k8s logs, response=%+v", tt.name, res)
			if tt.wantErr {
				assert.Error(t, err)
				if err != nil {
					t.Logf("wantError: %s", err.Error())
				}
			} else {
				assert.NoError(t, err)
				t.Logf("name=%s, res=%#v", tt.name, res)
			}
		})
	}
}

func createK3SEvents(t *testing.T, krc *K3SRuntimeClient, objectName, namespace string) {
	for i := 0; i < 10; i++ {
		mockEventName := uuid.GenerateIDWithLength("randomName", 5)
		event := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      mockEventName,
				Namespace: namespace,
			},
			InvolvedObject: corev1.ObjectReference{
				Name:      objectName,
				Namespace: namespace,
			},
			Reason:         "start",
			Message:        "end msg",
			FirstTimestamp: metav1.NewTime(time.Now()),
			LastTimestamp:  metav1.NewTime(time.Now()),
		}
		_, err := krc.Client.CoreV1().Events(namespace).Create(context.TODO(), event, metav1.CreateOptions{})
		assert.Equal(t, nil, err)
	}
}

func createK3SMockLog(t *testing.T, krc *K3SRuntimeClient) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				"cpu":    resource.MustParse("22"),
				"memory": resource.MustParse("22Gi"),
			},
			Allocatable: corev1.ResourceList{
				"cpu":    resource.MustParse("20"),
				"memory": resource.MustParse("20Gi"),
			},
		},
	}
	podSpec := corev1.PodSpec{
		NodeName: nodeName,
		Containers: []corev1.Container{
			{
				Name: "c1",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2Gi"),
					},
				},
				Image:   "busybox",
				Command: []string{"/bin/sh", "-c", "for i in {1..10};do;echo 'helloworld\n';done;"},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mockPodName,
			Namespace: mockNS,
			Labels: map[string]string{
				"app": mockPodName,
			},
		},
		Spec: podSpec,
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	deploy := &appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mockDeployName,
			Namespace: mockNS,
		},
		Spec: appv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:      mockPodName,
					Namespace: mockNS,
					Labels: map[string]string{
						"app": mockPodName,
					},
				},
				Spec: podSpec,
			},
		},
	}
	// create node
	_, err := krc.Client.CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create pod
	_, err = krc.Client.CoreV1().Pods(mockNS).Create(context.TODO(), pod, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create deployment
	_, err = krc.Client.AppsV1().Deployments(mockNS).Create(context.TODO(), deploy, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create random events
	createK3SEvents(t, krc, mockPodName, mockNS)
	createK3SEvents(t, krc, mockDeployName, mockNS)
}
