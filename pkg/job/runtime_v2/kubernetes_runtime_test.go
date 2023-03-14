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

package runtime_v2

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job"
	_ "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

const (
	testJobID   = "test_pf_id"
	jobManifest = `apiVersion: v1
kind: Pod
metadata:
  creationTimestamp: null
  labels:
    volcano.sh/queue-name: default
  name: job-normal-00000001
  namespace: default
spec:
  containers:
    - image: nginx
      name: job-normal-00000001
  priorityClassName: normal
  terminationGracePeriodSeconds: 30
  schedulerName: volcano
status: {}
`
	mockNS         = "default"
	mockPodName    = "fakePod"
	mockDeployName = "fakeDeployName"
	nodeName       = "node1"
)

// init trace logger
func initTestTraceLogger() error {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	conf := trace_logger.TraceLoggerConfig{
		Dir:             tmpDir,
		FilePrefix:      "trace_logger",
		Level:           "debug",
		MaxKeepDays:     2,
		MaxFileNum:      10,
		MaxFileSizeInMB: 1,
		IsCompress:      false,
		Timeout:         "2s",
		MaxCacheSize:    2,
	}

	return trace_logger.InitTraceLoggerManager(conf)
}

func TestKubeRuntimeJob(t *testing.T) {
	if err := initTestTraceLogger(); !assert.Equal(t, nil, err) {
		return
	}
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	pfJob := &api.PFJob{
		ID:                testJobID,
		Namespace:         "default",
		JobType:           schema.TypeSingle,
		ExtensionTemplate: []byte(jobManifest),
		Conf: schema.Conf{
			Env: map[string]string{
				schema.EnvJobQueueName: "default",
				schema.EnvJobFlavour:   "flavour1",
			},
			Flavour: schema.Flavour{
				ResourceInfo: schema.ResourceInfo{
					CPU: "1",
					Mem: "1Gi",
				},
			},
		},
		Tasks: []schema.Member{
			{
				Replicas: 1,
				Conf: schema.Conf{
					Name:    "normal",
					Command: "sleep 200",
					Image:   "busybox:v1",
					Flavour: schema.Flavour{Name: "mockFlavourName", ResourceInfo: schema.ResourceInfo{CPU: "2", Mem: "2"}},
				},
			},
		},
	}
	driver.InitMockDB()
	config.GlobalServerConfig = &config.ServerConfig{}
	err := storage.Job.CreateJob(&model.Job{
		ID: testJobID,
		Config: &schema.Conf{
			Env: map[string]string{
				schema.EnvJobNamespace: "default",
			},
		},
	})
	assert.NoError(t, err)
	stopCh := make(chan struct{})
	defer close(stopCh)
	fwVersion := client.KubeFrameworkVersion(k8s.PodGVK)
	// create kubernetes job
	err = kubeRuntime.Job(fwVersion).Submit(context.TODO(), pfJob)
	assert.NoError(t, err)
	// stop kubernetes job
	err = kubeRuntime.Job(fwVersion).Stop(context.TODO(), pfJob)
	assert.NoError(t, err)
	t.SkipNow()
}

func TestKubeRuntimePVAndPVC(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}
	driver.InitMockDB()

	config.DefaultPV = &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"pfs.fs.id":  "$(pfs.fs.id)",
						"pfs.server": "$(pfs.server)",
					},
					VolumeHandle: "pfs-$(pfs.fs.id)-$(namespace)-pv",
				},
			},
		},
	}
	config.DefaultPVC = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pfs-$(pfs.fs.id)-pvc",
			Namespace:  "$(namespace)",
			Finalizers: []string{"meow"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
	}
	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			Host: "paddleflow-server",
			Port: 8999,
		},
	}

	namespace := "default"
	fsID := "fs-test"
	fs := model.FileSystem{
		Model: model.Model{
			ID: fsID,
		},
		Type:    "s3",
		SubPath: "elsie",
	}
	err := storage.Filesystem.CreatFileSystem(&fs)
	assert.Nil(t, err)
	fsCache := model.FSCacheConfig{
		FsID:       fsID,
		CacheDir:   "/data/paddleflow-fs/mnt",
		MetaDriver: "nutsdb",
	}
	err = storage.Filesystem.CreateFSCacheConfig(&fsCache)
	assert.Nil(t, err)

	pvc := fmt.Sprintf("pfs-%s-pvc", fsID)
	// create pv
	pv, err := kubeRuntime.CreatePV(namespace, fsID)
	assert.Equal(t, nil, err)
	// create pvc
	err = kubeRuntime.CreatePVC(namespace, fsID, pv)
	assert.Equal(t, nil, err)
	// patch pvc
	err = kubeRuntime.PatchPVCFinalizerNull(namespace, pvc)
	assert.Equal(t, nil, err)
	pvcNew, err := kubeRuntime.GetPersistentVolumeClaims(namespace, pvc, metav1.GetOptions{})
	assert.Equal(t, nil, err)
	assert.Equal(t, pvc, pvcNew.Name)
	assert.Equal(t, 0, len(pvcNew.Finalizers))
	// delete pvc
	err = kubeRuntime.DeletePersistentVolumeClaim(namespace, pvc, metav1.DeleteOptions{})
	assert.Equal(t, nil, err)
	// delete pv
	err = kubeRuntime.DeletePersistentVolume(pv, metav1.DeleteOptions{})
	assert.Equal(t, nil, err)
}

func TestKubeRuntimeObjectOperation(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	gvk := k8sschema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}

	namespace := "default"
	name := "cm1"
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string]string{
			"test.conf": "a:b1\nkey2:value2",
		},
	}

	cmObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cm)
	assert.Equal(t, nil, err)
	err = kubeRuntime.CreateObject(&unstructured.Unstructured{Object: cmObj})
	assert.Equal(t, nil, err)

	// update ConfigMap
	cm.Data["test2.conf"] = "a2:b2\nkey2:value2"
	cmObj, err = runtime.DefaultUnstructuredConverter.ToUnstructured(cm)
	assert.Equal(t, nil, err)
	err = kubeRuntime.UpdateObject(&unstructured.Unstructured{Object: cmObj})
	assert.Equal(t, nil, err)

	// get ConfigMap
	obj, err := kubeRuntime.GetObject(namespace, name, gvk)
	assert.Equal(t, nil, err)
	t.Logf("get object: %v", obj)
}

func TestKubeRuntime_CreateNamespace(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	type args struct {
		namespace string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{"test-namespace", args{"test"}, nil},
		{"repeat-namespace", args{"test"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, err := kubeRuntime.CreateNamespace(tt.args.namespace, metav1.CreateOptions{})
			if err != nil {
				assert.NotNil(t, tt.wantErr)
				assert.Contains(t, err.Error(), tt.wantErr.Error())
			} else {
				assert.Nil(t, tt.wantErr)
				t.Logf("case[%s] create ns resp=%#v", tt.name, ns)
			}
		})
	}
}

func TestKubeRuntimeVCQueue(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	maxResources, err := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "20",
		resources.ResMemory: "20Gi",
	})
	assert.Equal(t, nil, err)
	q := api.NewQueueInfo(model.Queue{
		Model: model.Model{
			ID: "test_queue_id",
		},
		Name:         "test_queue_name",
		Namespace:    "default",
		QuotaType:    schema.TypeVolcanoCapabilityQuota,
		MaxResources: maxResources,
	})
	// create vc queue
	err = kubeRuntime.CreateQueue(q)
	assert.Equal(t, nil, err)
	// update vc queue
	err = kubeRuntime.UpdateQueue(q)
	assert.Equal(t, nil, err)
	// delete vc queue
	err = kubeRuntime.DeleteQueue(q)
	assert.Equal(t, nil, err)
}

func TestKubeRuntimeElasticQuota(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	maxResources, err := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "20",
		resources.ResMemory: "20Gi",
	})
	assert.Equal(t, nil, err)
	minResources, err1 := resources.NewResourceFromMap(map[string]string{
		resources.ResCPU:    "10",
		resources.ResMemory: "10Gi",
	})
	assert.Equal(t, nil, err1)

	q := api.NewQueueInfo(model.Queue{
		Model: model.Model{
			ID: "test_queue_id",
		},
		Name:         "test_queue_name",
		Namespace:    "default",
		QuotaType:    schema.TypeElasticQuota,
		MaxResources: maxResources,
		MinResources: minResources,
	})
	// create elastic quota
	err = kubeRuntime.CreateQueue(q)
	assert.Equal(t, nil, err)
	// update elastic quota
	err = kubeRuntime.UpdateQueue(q)
	assert.Equal(t, nil, err)
	// delete elastic quota
	err = kubeRuntime.DeleteQueue(q)
	assert.Equal(t, nil, err)
}

func TestKubeRuntimeNodeResource(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{},
	}
	namespace := "default"
	nodeName := "node1"
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
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
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
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	// create node
	_, err := kubeRuntime.clientset().CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create pod
	_, err = kubeRuntime.clientset().CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// list node quota
	quotaSummary, nodeQuotaInfos, err := kubeRuntime.ListNodeQuota()
	assert.Equal(t, nil, err)
	t.Logf("quota summary: %v", quotaSummary)
	t.Logf("node  quota info: %v", nodeQuotaInfos)
}

func TestKubeRuntime_SyncController(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}
	ch := make(chan struct{})
	kubeRuntime.SyncController(ch)
}

func TestKubeRuntime_GetMixedLog(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}
	createMockLog(t, kubeRuntime)

	type args struct {
		req              schema.MixedLogRequest
		mockListFailed   bool
		mockGetPodFailed bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errMsg  string
	}{
		{
			name: "empty request",
			args: args{

				req: schema.MixedLogRequest{
					Name: "",
				},
			},
			wantErr: true,
		},
		{
			name: "empty request, only name",
			args: args{
				req: schema.MixedLogRequest{
					Name: "abc",
				},
			},
			wantErr: true,
		},
		{
			name: string(schema.TypePodJob),
			args: args{
				req: schema.MixedLogRequest{
					Name:           "",
					Namespace:      "default",
					Framework:      "",
					LineLimit:      "default",
					SizeLimit:      10000,
					IsReadFromTail: true,
					ResourceType:   string(schema.TypePodJob),
				},
			},
			wantErr: true,
		},
		{
			name: string(schema.TypePodJob),
			args: args{
				req: schema.MixedLogRequest{
					Name:           mockPodName,
					Namespace:      "default",
					Framework:      "",
					LineLimit:      "default",
					SizeLimit:      10000,
					IsReadFromTail: true,
					ResourceType:   string(schema.TypePodJob),
				},
			},
			wantErr: true,
		},
		{
			name: "get pod failed",
			args: args{
				req: schema.MixedLogRequest{
					Name:           mockPodName,
					Namespace:      "default",
					Framework:      "",
					LineLimit:      "default",
					SizeLimit:      10000,
					IsReadFromTail: true,
					ResourceType:   string(schema.TypePodJob),
				},
				mockGetPodFailed: true,
			},
			wantErr: true,
		},
		{
			name: "name or ns is nil",
			args: args{
				req: schema.MixedLogRequest{
					Name:           "",
					Namespace:      "default",
					Framework:      "",
					LineLimit:      "default",
					SizeLimit:      10000,
					IsReadFromTail: true,
					ResourceType:   string(schema.TypeDeployment),
				},
			},
			wantErr: true,
		},
		{
			name: "parse linelimit default faild",
			args: args{
				req: schema.MixedLogRequest{
					Name:           mockDeployName,
					Namespace:      mockNS,
					Framework:      "",
					LineLimit:      "default",
					SizeLimit:      10000,
					IsReadFromTail: true,
					ResourceType:   string(schema.TypeDeployment),
				},
			},
			wantErr: true,
		},
		{
			name: "deploy",
			args: args{
				req: schema.MixedLogRequest{
					Name:           mockDeployName,
					Namespace:      mockNS,
					Framework:      "",
					LineLimit:      "5",
					SizeLimit:      10000,
					IsReadFromTail: true,
					ResourceType:   string(schema.TypeDeployment),
				},
			},
			wantErr: false,
		},
		{
			name: "deploy",
			args: args{
				req: schema.MixedLogRequest{
					Name:           mockDeployName,
					Namespace:      mockNS,
					Framework:      "",
					LineLimit:      "5",
					SizeLimit:      10000,
					IsReadFromTail: true,
					ResourceType:   string(schema.TypeDeployment),
				},
				mockListFailed: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)
			if tt.args.mockListFailed || tt.args.mockGetPodFailed {
				patch := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().Pods("")), "Get", func(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error) {
					return nil, fmt.Errorf("err")
				})
				defer patch.Reset()
				patch2 := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().Pods("")), "List", func(ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
					return nil, fmt.Errorf("err")
				})
				defer patch2.Reset()

				res, err := kubeRuntime.getLog(tt.args.req)
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
				t.SkipNow()
			} else {
				res, err := kubeRuntime.getLog(tt.args.req)
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
			}

		})
	}

}

func createMockLog(t *testing.T, kr *KubeRuntime) {
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
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mockPodName,
			Namespace: mockNS,
			Labels: map[string]string{
				"app": mockDeployName,
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
	_, err := kr.clientset().CoreV1().Nodes().Create(context.TODO(), node, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create pod
	for i := 0; i < 10; i++ {
		t.Logf("create pod %s", pod.Name)
		_, err = kr.clientset().CoreV1().Pods(mockNS).Create(context.TODO(), pod, metav1.CreateOptions{})
		assert.NoError(t, err)
		pod.Name = uuid.GenerateIDWithLength("pod", 5)
	}

	// create deployment
	_, err = kr.clientset().AppsV1().Deployments(mockNS).Create(context.TODO(), deploy, metav1.CreateOptions{})
	assert.Equal(t, nil, err)
	// create random events
	createEvents(t, kr, mockPodName, mockNS)
	createEvents(t, kr, mockDeployName, mockNS)
}

func createEvents(t *testing.T, kr *KubeRuntime, objectName, namespace string) {
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
		_, err := kr.clientset().CoreV1().Events(namespace).Create(context.TODO(), event, metav1.CreateOptions{})
		assert.Equal(t, nil, err)
	}
}

func TestKubeRuntime_GetEvents(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeKubeRuntimeClient(server)

	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}
	createMockLog(t, kubeRuntime)

	type args struct {
		name         string
		namespace    string
		kr           *KubeRuntime
		getPodFailed bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		errMsg  string
	}{
		{
			name:    "empty request",
			args:    args{},
			wantErr: false,
		},
		{
			name: "empty request, only name",
			args: args{
				name:      mockPodName,
				namespace: "",
				kr:        kubeRuntime,
			},
			wantErr: false,
		},
		{
			name: "empty request, only namespace",
			args: args{
				name:      "",
				namespace: mockNS,
				kr:        kubeRuntime,
			},
			wantErr: false,
		},
		{
			name: "success1",
			args: args{
				name:      mockPodName,
				namespace: mockNS,
				kr:        kubeRuntime,
			},
			wantErr: false,
		},
		{
			name: "nil kubeRuntime",
			args: args{
				name:         mockPodName,
				namespace:    mockNS,
				kr:           kubeRuntime,
				getPodFailed: true,
			},
			wantErr: true,
		},
		{
			name: "nil kubeRuntime for deploy",
			args: args{
				name:         mockDeployName,
				namespace:    mockNS,
				kr:           kubeRuntime,
				getPodFailed: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("name=%s args=[%#v], wantError=%v", tt.name, tt.args, tt.wantErr)

			if tt.args.getPodFailed {
				patch := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().Events("")), "List", func(ctx context.Context, opts metav1.ListOptions) (*corev1.EventList, error) {
					return nil, fmt.Errorf("err")
				})
				defer patch.Reset()
				_, err := kubeRuntime.GetEvents(tt.args.namespace, tt.args.name)
				assert.Error(t, err)
				if err != nil {
					t.Logf("wantError: %s", err.Error())
				}
				t.SkipNow()
			}
			res, err := kubeRuntime.GetEvents(tt.args.namespace, tt.args.name)
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

func TestKubeRuntime_CreatePVC(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	type fields struct {
		cluster    schema.Cluster
		kubeClient framework.RuntimeClientInterface
	}
	type args struct {
		namespace string
		fsId      string
		pv        string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "copy err",
			fields: fields{
				cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
				kubeClient: kubeClient,
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
		{
			name: "get pvc error",
			fields: fields{
				cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
				kubeClient: kubeClient,
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
		{
			name: "create pvc error",
			fields: fields{
				cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
				kubeClient: kubeClient,
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}
	config.DefaultPV = &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"pfs.fs.id":  "$(pfs.fs.id)",
						"pfs.server": "$(pfs.server)",
					},
					VolumeHandle: "pfs-$(pfs.fs.id)-$(namespace)-pv",
				},
			},
		},
	}
	config.DefaultPVC = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pfs-$(pfs.fs.id)-pvc",
			Namespace:  "$(namespace)",
			Finalizers: []string{"meow"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
	}
	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			Host: "paddleflow-server",
			Port: 8999,
		},
	}

	// patch copier.Copy function
	patchCopy := gomonkey.ApplyFunc(copier.Copy, func(dst, src interface{}) error {
		return fmt.Errorf("copy err")
	})

	// patch kr.getPersistentVolumeClaim function
	patchGet := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().PersistentVolumeClaims("")), "Get", func(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.PersistentVolumeClaim, error) {
		return nil, fmt.Errorf("get err")
	})

	// patch kr.createPersistentVolumeClaim function
	patchCreate := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().PersistentVolumeClaims("")), "Create", func(ctx context.Context, persistentVolumeClaim *corev1.PersistentVolumeClaim, opts metav1.CreateOptions) (*corev1.PersistentVolumeClaim, error) {
		return nil, fmt.Errorf("create err")
	})
	defer patchCreate.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kr := &KubeRuntime{
				cluster:    tt.fields.cluster,
				kubeClient: tt.fields.kubeClient,
			}
			tt.wantErr(t, kr.CreatePVC(tt.args.namespace, tt.args.fsId, tt.args.pv), fmt.Sprintf("CreatePVC(%v, %v, %v)", tt.args.namespace, tt.args.fsId, tt.args.pv))
		})
		if tt.name == "copy err" {
			patchCopy.Reset()
		}
		if tt.name == "get pvc error" {
			patchGet.Reset()
			// patch kr.getPersistentVolumeClaim function
			patchGet = gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().PersistentVolumeClaims("")), "Get", func(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.PersistentVolumeClaim, error) {
				return nil, nil
			})
			defer patchGet.Reset()
		}
	}
}

func TestKubeRuntime_CreatePV(t *testing.T) {
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()

	kubeClient := client.NewFakeKubeRuntimeClient(server)
	kubeRuntime := &KubeRuntime{
		cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
		kubeClient: kubeClient,
	}

	type fields struct {
		cluster    schema.Cluster
		kubeClient framework.RuntimeClientInterface
	}
	type args struct {
		namespace string
		fsID      string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "copy err",
			fields: fields{
				cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
				kubeClient: kubeClient,
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
		{
			name: "get pv error",
			fields: fields{
				cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
				kubeClient: kubeClient,
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
		{
			name: "create pv error",
			fields: fields{
				cluster:    schema.Cluster{Name: "test-cluster", Type: "Kubernetes"},
				kubeClient: kubeClient,
			},
			args: args{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return true
			},
		},
	}

	config.DefaultPV = &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"pfs.fs.id":  "$(pfs.fs.id)",
						"pfs.server": "$(pfs.server)",
					},
					VolumeHandle: "pfs-$(pfs.fs.id)-$(namespace)-pv",
				},
			},
		},
	}
	config.DefaultPVC = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "pfs-$(pfs.fs.id)-pvc",
			Namespace:  "$(namespace)",
			Finalizers: []string{"meow"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pfs-$(pfs.fs.id)-$(namespace)-pv",
		},
	}
	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			Host: "paddleflow-server",
			Port: 8999,
		},
	}

	// patch copier.Copy function
	patchCopy := gomonkey.ApplyFunc(copier.Copy, func(dst, src interface{}) error {
		return fmt.Errorf("copy err")
	})

	// patch kr.getPersistentVolumeClaim function
	patchGet := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().PersistentVolumes()), "Get", func(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.PersistentVolume, error) {
		return nil, fmt.Errorf("get err")
	})

	// patch kr.createPersistentVolumeClaim function
	patchCreate := gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().PersistentVolumes()), "Create", func(ctx context.Context, persistentVolume *corev1.PersistentVolume, opts metav1.CreateOptions) (*corev1.PersistentVolume, error) {
		return nil, fmt.Errorf("create err")
	})
	defer patchCreate.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kr := &KubeRuntime{
				cluster:    tt.fields.cluster,
				kubeClient: tt.fields.kubeClient,
			}
			got, err := kr.CreatePV(tt.args.namespace, tt.args.fsID)
			if !tt.wantErr(t, err, fmt.Sprintf("CreatePV(%v, %v)", tt.args.namespace, tt.args.fsID)) {
				return
			}
			assert.Equalf(t, tt.want, got, "CreatePV(%v, %v)", tt.args.namespace, tt.args.fsID)
		})
		if tt.name == "copy err" {
			patchCopy.Reset()
		}
		if tt.name == "get pvc error" {
			patchGet.Reset()
			// patch kr.getPersistentVolumeClaim function
			patchGet = gomonkey.ApplyMethodFunc(reflect.TypeOf(kubeRuntime.clientset().CoreV1().PersistentVolumes()), "Get", func(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.PersistentVolume, error) {
				return nil, nil
			})
			defer patchGet.Reset()
		}
	}
}
